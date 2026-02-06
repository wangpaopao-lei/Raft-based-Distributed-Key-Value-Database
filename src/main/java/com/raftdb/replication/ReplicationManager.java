package com.raftdb.replication;

import com.raftdb.core.NodeId;
import com.raftdb.core.RaftState;
import com.raftdb.log.LogEntry;
import com.raftdb.log.LogManager;
import com.raftdb.rpc.RpcTransport;
import com.raftdb.rpc.proto.AppendEntriesRequest;
import com.raftdb.rpc.proto.AppendEntriesResponse;
import com.raftdb.rpc.proto.InstallSnapshotRequest;
import com.raftdb.rpc.proto.InstallSnapshotResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Manages log replication from the leader to all followers.
 *
 * <p>This class implements the log replication mechanism described in the Raft paper.
 * It is only active when this node is the leader. The manager handles:
 * <ul>
 *   <li>Sending AppendEntries RPCs to replicate log entries</li>
 *   <li>Sending heartbeats to maintain leadership</li>
 *   <li>Tracking replication progress for each follower</li>
 *   <li>Advancing the commit index when entries are replicated to a majority</li>
 *   <li>Sending snapshots to followers that are too far behind</li>
 * </ul>
 *
 * <h2>Per-Follower State</h2>
 * <p>The leader maintains two indices for each follower:
 * <ul>
 *   <li><b>nextIndex</b> - Index of the next log entry to send to that follower
 *       (initialized to leader's last log index + 1)</li>
 *   <li><b>matchIndex</b> - Index of the highest log entry known to be replicated
 *       on that follower (initialized to 0)</li>
 * </ul>
 *
 * <h2>Replication Flow</h2>
 * <ol>
 *   <li>Leader sends AppendEntries with entries starting at nextIndex</li>
 *   <li>If successful, update nextIndex and matchIndex</li>
 *   <li>If unsuccessful due to log inconsistency, decrement nextIndex and retry</li>
 *   <li>If follower is too far behind (nextIndex &le; snapshotIndex), send snapshot</li>
 *   <li>When matchIndex advances on a majority, advance commitIndex</li>
 * </ol>
 *
 * <h2>Heartbeat</h2>
 * <p>Empty AppendEntries RPCs are sent periodically (every 50ms) to:
 * <ul>
 *   <li>Prevent followers from starting elections</li>
 *   <li>Propagate commit index updates</li>
 * </ul>
 *
 * @author raft-kv
 * @see LogManager
 * @see com.raftdb.core.RaftNode
 */
public class ReplicationManager {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    /** Interval between replication/heartbeat rounds in milliseconds. */
    private static final long REPLICATION_INTERVAL_MS = 50;

    private final NodeId selfId;
    private final RaftState state;
    private final LogManager logManager;
    private final RpcTransport transport;
    private final List<NodeId> peers;

    /** Next log index to send to each follower. */
    private final Map<NodeId, Long> nextIndex = new ConcurrentHashMap<>();

    /** Highest log index known to be replicated on each follower. */
    private final Map<NodeId, Long> matchIndex = new ConcurrentHashMap<>();

    /** Peers currently receiving a snapshot (to avoid concurrent snapshot transfers). */
    private final Set<NodeId> snapshotInProgress = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> replicationTask;

    private Consumer<Long> onHigherTermDiscovered;
    private Runnable onCommitAdvanced;

    /** Provider for snapshot data when a follower needs to be brought up to date. */
    private Function<NodeId, InstallSnapshotRequest> snapshotProvider;

    /**
     * Constructs a new ReplicationManager.
     *
     * @param selfId the ID of this node
     * @param state the Raft state object
     * @param logManager the log manager
     * @param transport the RPC transport for sending requests
     * @param peers the list of peer node IDs
     */
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

    /**
     * Sets the callback to invoke when a higher term is discovered from a follower.
     *
     * @param callback the callback receiving the higher term value
     */
    public void setOnHigherTermDiscovered(Consumer<Long> callback) {
        this.onHigherTermDiscovered = callback;
    }

    /**
     * Sets the callback to invoke when the commit index advances.
     *
     * @param callback the callback to invoke
     */
    public void setOnCommitAdvanced(Runnable callback) {
        this.onCommitAdvanced = callback;
    }

    /**
     * Sets the provider for creating InstallSnapshot requests.
     *
     * @param provider function that returns a snapshot request for a given peer,
     *                 or null if no snapshot is available
     */
    public void setSnapshotProvider(Function<NodeId, InstallSnapshotRequest> provider) {
        this.snapshotProvider = provider;
    }

    /**
     * Starts replication. Called when becoming leader.
     * Initializes nextIndex and matchIndex for all followers.
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
        long snapshotIndex = logManager.getSnapshotIndex();

        // If peer needs entries that have been compacted, send snapshot instead
        if (peerNextIndex <= snapshotIndex && snapshotProvider != null) {
            sendSnapshotTo(peer);
            return;
        }

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

    /**
     * Send a snapshot to a follower that is too far behind.
     */
    private void sendSnapshotTo(NodeId peer) {
        // Avoid sending multiple snapshots to the same peer concurrently
        if (!snapshotInProgress.add(peer)) {
            return;
        }

        InstallSnapshotRequest request = snapshotProvider.apply(peer);
        if (request == null) {
            snapshotInProgress.remove(peer);
            return;
        }

        logger.info("Sending snapshot to {} (lastIndex={}, size={})",
                peer, request.getLastIncludedIndex(), request.getData().size());

        transport.sendInstallSnapshot(peer, request)
                .thenAccept(response -> handleSnapshotResponse(peer, request, response))
                .exceptionally(e -> {
                    logger.warn("Snapshot to {} failed: {}", peer, e.getMessage());
                    snapshotInProgress.remove(peer);
                    return null;
                });
    }

    /**
     * Handle InstallSnapshot response.
     */
    private void handleSnapshotResponse(NodeId peer, InstallSnapshotRequest request, InstallSnapshotResponse response) {
        MDC.put("nodeId", selfId.toString());
        snapshotInProgress.remove(peer);

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

        // Snapshot accepted, update nextIndex and matchIndex
        long snapshotLastIndex = request.getLastIncludedIndex();
        nextIndex.put(peer, snapshotLastIndex + 1);
        matchIndex.put(peer, snapshotLastIndex);

        logger.info("Snapshot to {} complete, nextIndex={}", peer, snapshotLastIndex + 1);

        // Try to advance commit index
        tryAdvanceCommitIndex();
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
     * Confirms that this node is still the leader by sending heartbeats to all peers
     * and waiting for responses from a majority.
     *
     * <p>This is used for linearizable reads to ensure the leader hasn't been
     * superseded by another leader. The method sends empty AppendEntries (heartbeats)
     * to all peers and waits for acknowledgments from a majority.
     *
     * @param timeoutMs maximum time to wait for confirmation in milliseconds
     * @return a CompletableFuture that completes with true if leadership is confirmed,
     *         false if not enough peers responded or a higher term was discovered
     */
    public CompletableFuture<Boolean> confirmLeadership(long timeoutMs) {
        if (!state.isLeader()) {
            return CompletableFuture.completedFuture(false);
        }

        // Single node cluster: always confirmed
        if (peers.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }

        CompletableFuture<Boolean> result = new CompletableFuture<>();

        // We need majority including self: (N+1)/2 + 1 total, so (N+1)/2 from peers
        int needed = (peers.size() + 1) / 2;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicBoolean completed = new AtomicBoolean(false);

        long term = state.getCurrentTerm();

        // Send heartbeat to all peers
        for (NodeId peer : peers) {
            long prevLogIndex = nextIndex.getOrDefault(peer, 1L) - 1;
            long prevLogTerm = logManager.getTerm(prevLogIndex);

            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                    .setTerm(term)
                    .setLeaderId(selfId.toString())
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .setLeaderCommit(state.getCommitIndex())
                    .build();

            transport.sendAppendEntries(peer, request)
                    .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                    .whenComplete((response, error) -> {
                        if (completed.get()) {
                            return;
                        }

                        if (error != null) {
                            int failures = failureCount.incrementAndGet();
                            // Too many failures, cannot reach majority
                            if (failures > peers.size() - needed) {
                                if (completed.compareAndSet(false, true)) {
                                    result.complete(false);
                                }
                            }
                            return;
                        }

                        // Check for higher term
                        if (response.getTerm() > term) {
                            if (onHigherTermDiscovered != null) {
                                onHigherTermDiscovered.accept(response.getTerm());
                            }
                            if (completed.compareAndSet(false, true)) {
                                result.complete(false);
                            }
                            return;
                        }

                        // Count success (even if log check failed, the peer acknowledged our term)
                        int successes = successCount.incrementAndGet();
                        if (successes >= needed) {
                            if (completed.compareAndSet(false, true)) {
                                logger.debug("Leadership confirmed with {}/{} peer responses", successes, peers.size());
                                result.complete(true);
                            }
                        }
                    });
        }

        // Timeout handler
        scheduler.schedule(() -> {
            if (completed.compareAndSet(false, true)) {
                logger.debug("Leadership confirmation timed out");
                result.complete(false);
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);

        return result;
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
