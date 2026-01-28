package com.raftdb.replication;

import com.raftdb.core.NodeId;
import com.raftdb.core.RaftState;
import com.raftdb.log.LogEntry;
import com.raftdb.log.LogManager;
import com.raftdb.rpc.RpcTransport;
import com.raftdb.rpc.proto.AppendEntriesRequest;
import com.raftdb.rpc.proto.AppendEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Manages log replication from leader to followers.
 *
 * Leader maintains for each follower:
 * - nextIndex: next log entry to send (initialized to leader's last log index + 1)
 * - matchIndex: highest log entry known to be replicated (initialized to 0)
 */
public class ReplicationManager {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    private static final long REPLICATION_INTERVAL_MS = 50;

    private final NodeId selfId;
    private final RaftState state;
    private final LogManager logManager;
    private final RpcTransport transport;
    private final List<NodeId> peers;

    // Per-follower replication state
    private final Map<NodeId, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<NodeId, Long> matchIndex = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> replicationTask;

    private Consumer<Long> onHigherTermDiscovered;
    private Runnable onCommitAdvanced;

    public ReplicationManager(
            NodeId selfId,
            RaftState state,
            LogManager logManager,
            RpcTransport transport,
            List<NodeId> peers) {
        this.selfId = selfId;
        this.state = state;
        this.logManager = logManager;
        this.transport = transport;
        this.peers = peers;
        this.scheduler = Executors.newScheduledThreadPool(peers.size() + 1, r -> {
            Thread t = new Thread(r, "replication-" + selfId);
            t.setDaemon(true);
            return t;
        });
    }

    public void setOnHigherTermDiscovered(Consumer<Long> callback) {
        this.onHigherTermDiscovered = callback;
    }

    public void setOnCommitAdvanced(Runnable callback) {
        this.onCommitAdvanced = callback;
    }

    /**
     * Start replication. Called when becoming leader.
     * Initialize nextIndex and matchIndex for all followers.
     */
    public void start() {
        MDC.put("nodeId", selfId.toString());
        logger.info("ReplicationManager started");

        // Initialize replication state
        long lastIndex = logManager.getLastIndex();
        for (NodeId peer : peers) {
            nextIndex.put(peer, lastIndex + 1);
            matchIndex.put(peer, 0L);
        }

        // Immediately send heartbeats with empty entries
        replicateToAll();

        // Schedule periodic replication
        replicationTask = scheduler.scheduleAtFixedRate(
                this::replicateToAll,
                REPLICATION_INTERVAL_MS,
                REPLICATION_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stop replication. Called when no longer leader.
     */
    public void stop() {
        if (replicationTask != null) {
            replicationTask.cancel(false);
            replicationTask = null;
        }
        nextIndex.clear();
        matchIndex.clear();
        logger.info("ReplicationManager stopped");
    }

    /**
     * Shutdown completely.
     */
    public void shutdown() {
        stop();
        scheduler.shutdown();
    }

    /**
     * Trigger immediate replication (e.g., after new log entry).
     */
    public void triggerReplication() {
        scheduler.execute(this::replicateToAll);
    }

    private void replicateToAll() {
        MDC.put("nodeId", selfId.toString());

        if (!state.isLeader()) {
            return;
        }

        // Single node cluster: no peers to replicate to, but still need to commit
        if (peers.isEmpty()) {
            tryAdvanceCommitIndex();
            return;
        }

        for (NodeId peer : peers) {
            replicateTo(peer);
        }
    }

    private void replicateTo(NodeId peer) {
        long peerNextIndex = nextIndex.getOrDefault(peer, 1L);
        long prevLogIndex = peerNextIndex - 1;
        long prevLogTerm = logManager.getTerm(prevLogIndex);

        // Get entries to send
        List<LogEntry> entries = logManager.getEntriesFrom(peerNextIndex);

        AppendEntriesRequest.Builder requestBuilder = AppendEntriesRequest.newBuilder()
                .setTerm(state.getCurrentTerm())
                .setLeaderId(selfId.toString())
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .setLeaderCommit(state.getCommitIndex());

        // Add entries
        for (LogEntry entry : entries) {
            requestBuilder.addEntries(entry.toProto());
        }

        AppendEntriesRequest request = requestBuilder.build();

        if (!entries.isEmpty()) {
            logger.debug("Replicating {} entries to {} (nextIndex={})",
                    entries.size(), peer, peerNextIndex);
        }

        transport.sendAppendEntries(peer, request)
                .thenAccept(response -> handleResponse(peer, request, response))
                .exceptionally(e -> {
                    logger.trace("Replication to {} failed: {}", peer, e.getMessage());
                    return null;
                });
    }

    private void handleResponse(NodeId peer, AppendEntriesRequest request, AppendEntriesResponse response) {
        MDC.put("nodeId", selfId.toString());

        // Check for higher term
        if (response.getTerm() > state.getCurrentTerm()) {
            logger.info("Discovered higher term {} from {}", response.getTerm(), peer);
            if (onHigherTermDiscovered != null) {
                onHigherTermDiscovered.accept(response.getTerm());
            }
            return;
        }

        if (!state.isLeader()) {
            return;
        }

        if (response.getSuccess()) {
            // Update nextIndex and matchIndex
            long newMatchIndex = request.getPrevLogIndex() + request.getEntriesCount();
            if (newMatchIndex > matchIndex.getOrDefault(peer, 0L)) {
                matchIndex.put(peer, newMatchIndex);
                nextIndex.put(peer, newMatchIndex + 1);

                if (request.getEntriesCount() > 0) {
                    logger.debug("Replication to {} succeeded, matchIndex={}", peer, newMatchIndex);
                }

                // Try to advance commit index
                tryAdvanceCommitIndex();
            }
        } else {
            // Replication failed, decrement nextIndex and retry
            // Use conflict info if available for faster rollback
            long newNextIndex;
            if (response.getConflictIndex() > 0) {
                newNextIndex = response.getConflictIndex();
            } else {
                newNextIndex = Math.max(1, nextIndex.getOrDefault(peer, 1L) - 1);
            }
            nextIndex.put(peer, newNextIndex);
            logger.debug("Replication to {} failed, nextIndex decremented to {}", peer, newNextIndex);
        }
    }

    /**
     * Try to advance commit index based on matchIndex values.
     * Commit index can advance to N if:
     * - N > commitIndex
     * - A majority of matchIndex[i] >= N
     * - log[N].term == currentTerm
     */
    private void tryAdvanceCommitIndex() {
        long currentCommit = state.getCommitIndex();
        long lastIndex = logManager.getLastIndex();
        long currentTerm = state.getCurrentTerm();

        // Try each index from lastIndex down to commitIndex+1
        for (long n = lastIndex; n > currentCommit; n--) {
            // Only commit entries from current term
            if (logManager.getTerm(n) != currentTerm) {
                continue;
            }

            // Count replicas (including self)
            int replicaCount = 1;  // Self
            for (NodeId peer : peers) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    replicaCount++;
                }
            }

            // Check majority
            int majority = (peers.size() + 1) / 2 + 1;
            if (replicaCount >= majority) {
                logger.info("Advancing commit index from {} to {}", currentCommit, n);
                state.setCommitIndex(n);

                if (onCommitAdvanced != null) {
                    onCommitAdvanced.run();
                }
                break;
            }
        }
    }

    /**
     * Get match index for a peer (for testing/debugging).
     */
    public long getMatchIndex(NodeId peer) {
        return matchIndex.getOrDefault(peer, 0L);
    }

    /**
     * Get next index for a peer (for testing/debugging).
     */
    public long getNextIndex(NodeId peer) {
        return nextIndex.getOrDefault(peer, 0L);
    }
}
