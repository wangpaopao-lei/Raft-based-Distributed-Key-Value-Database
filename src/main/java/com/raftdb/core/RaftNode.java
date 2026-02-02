package com.raftdb.core;

import com.raftdb.election.ElectionManager;
import com.raftdb.log.LogEntry;
import com.raftdb.log.LogManager;
import com.raftdb.persistence.PersistenceManager;
import com.raftdb.replication.ReplicationManager;
import com.raftdb.rpc.RpcHandler;
import com.raftdb.rpc.RpcTransport;
import com.raftdb.rpc.proto.AppendEntriesRequest;
import com.raftdb.rpc.proto.AppendEntriesResponse;
import com.raftdb.rpc.proto.InstallSnapshotRequest;
import com.raftdb.rpc.proto.InstallSnapshotResponse;
import com.raftdb.rpc.proto.VoteRequest;
import com.raftdb.rpc.proto.VoteResponse;
import com.raftdb.snapshot.SnapshotManager;
import com.raftdb.statemachine.Command;
import com.raftdb.statemachine.SkipListKVStore;
import com.raftdb.statemachine.StateMachine;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Main Raft node implementation.
 *
 * Assembles all components and coordinates their interactions.
 */
public class RaftNode implements RpcHandler {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    // Snapshot threshold: take snapshot when log exceeds this many entries
    private static final int SNAPSHOT_THRESHOLD = 1000;

    private final NodeId id;
    private final RaftState state;
    private final LogManager logManager;
    private final ElectionManager electionManager;
    private final ReplicationManager replicationManager;
    private final StateMachine stateMachine;
    private final RpcTransport transport;
    private final List<NodeId> peers;

    // Optional persistence
    private final PersistenceManager persistence;

    // Optional snapshot manager
    private SnapshotManager snapshotManager;

    // Pending client requests waiting for commit
    private final Map<Long, CompletableFuture<byte[]>> pendingRequests = new ConcurrentHashMap<>();

    // Lock for log append operations
    private final ReentrantLock appendLock = new ReentrantLock();

    // Lock for snapshot operations
    private final ReentrantLock snapshotLock = new ReentrantLock();

    // Executor for applying committed entries
    private final ExecutorService applyExecutor;

    // Data directory for persistence
    private final Path dataDir;

    /**
     * Create a RaftNode without persistence (for testing).
     */
    public RaftNode(NodeId id, List<NodeId> peers, RpcTransport transport) {
        this(id, peers, transport, null);
    }

    /**
     * Create a RaftNode with persistence.
     *
     * @param dataDir directory for storing persistent state (null for in-memory only)
     */
    public RaftNode(NodeId id, List<NodeId> peers, RpcTransport transport, Path dataDir) {
        this.id = id;
        this.peers = peers;
        this.transport = transport;
        this.dataDir = dataDir;
        this.state = new RaftState();
        this.logManager = new LogManager();
        this.stateMachine = new SkipListKVStore();

        // Setup persistence if dataDir provided
        if (dataDir != null) {
            this.persistence = new PersistenceManager(dataDir);
        } else {
            this.persistence = null;
        }

        this.electionManager = new ElectionManager(
                id,
                state,
                transport,
                peers,
                logManager::getLastIndex,
                logManager::getLastTerm
        );

        this.replicationManager = new ReplicationManager(
                id,
                state,
                logManager,
                transport,
                peers
        );

        // Setup snapshot callback for replication manager
        this.replicationManager.setSnapshotProvider(this::getSnapshotForReplication);

        this.applyExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "apply-" + id);
            t.setDaemon(true);
            return t;
        });

        // Wire up callbacks
        electionManager.setOnBecomeLeader(this::onBecomeLeader);
        electionManager.setOnBecomeFollower(this::onBecomeFollower);
        replicationManager.setOnHigherTermDiscovered(this::onHigherTermDiscovered);
        replicationManager.setOnCommitAdvanced(this::applyCommittedEntries);
    }

    /**
     * Start the Raft node (full startup including transport).
     * Use this when transport is managed by RaftNode.
     */
    public void start() {
        MDC.put("nodeId", id.toString());
        logger.info("Starting RaftNode {}", id);

        // Start transport
        transport.start(this);

        // Initialize persistence and election
        initialize();
    }

    /**
     * Initialize the Raft node (persistence recovery, election timer).
     * Use this when transport is started separately (e.g., by RaftServer with KVService).
     */
    public void initialize() {
        MDC.put("nodeId", id.toString());
        logger.info("Initializing RaftNode {}", id);

        // Initialize persistence and recover state
        if (persistence != null) {
            try {
                persistence.init();

                // Initialize snapshot manager
                snapshotManager = new SnapshotManager(dataDir);

                // Recover term and votedFor
                state.restore(persistence.getCurrentTerm(), persistence.getVotedFor());

                // Setup persist callback
                state.setPersistCallback((term, votedFor) -> {
                    try {
                        persistence.saveMeta(term, votedFor);
                    } catch (IOException e) {
                        logger.error("Failed to persist meta", e);
                        throw new RuntimeException(e);
                    }
                });

                // Recover from snapshot first
                if (snapshotManager.hasSnapshot()) {
                    byte[] snapshotData = snapshotManager.loadSnapshot();
                    long snapshotIndex = snapshotManager.getLastIncludedIndex();
                    long snapshotTerm = snapshotManager.getLastIncludedTerm();

                    stateMachine.restoreSnapshot(snapshotData);
                    logManager.setSnapshot(snapshotIndex, snapshotTerm);
                    state.setCommitIndex(snapshotIndex);
                    state.setLastApplied(snapshotIndex);

                    logger.info("Restored from snapshot: index={}, term={}", snapshotIndex, snapshotTerm);
                }

                // Recover log
                logManager.setPersistence(persistence);

                // Replay log to recover state machine (entries after snapshot)
                replayLog();

                logger.info("Recovered state: term={}, votedFor={}, lastLogIndex={}",
                        state.getCurrentTerm(), state.getVotedFor(), logManager.getLastIndex());

            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize persistence", e);
            }
        }

        // Start election timer
        electionManager.start();

        logger.info("RaftNode {} initialized as FOLLOWER", id);
    }

    /**
     * Replay committed log entries to recover state machine.
     * Starts from snapshot index + 1 (entries after snapshot).
     */
    private void replayLog() {
        long snapshotIndex = logManager.getSnapshotIndex();
        long lastIndex = logManager.getLastIndex();

        if (lastIndex <= snapshotIndex) {
            return;
        }

        long startIndex = snapshotIndex + 1;
        logger.info("Replaying log entries {} to {} to recover state machine", startIndex, lastIndex);

        for (long i = startIndex; i <= lastIndex; i++) {
            LogEntry entry = logManager.getEntry(i);
            if (entry != null && entry.command().length > 0) {
                Command command = Command.fromBytes(entry.command());
                stateMachine.apply(command);
            }
        }

        // After recovery, all persisted entries are considered committed
        // (they were committed before the crash)
        state.setCommitIndex(lastIndex);
        state.setLastApplied(lastIndex);

        logger.info("State machine recovered, commitIndex={}", lastIndex);
    }

    /**
     * Stop the Raft node.
     */
    public void stop() {
        MDC.put("nodeId", id.toString());
        logger.info("Stopping RaftNode {}", id);

        replicationManager.shutdown();
        electionManager.stop();
        transport.shutdown();
        applyExecutor.shutdown();

        // Close persistence
        if (persistence != null) {
            persistence.close();
        }

        // Close snapshot manager
        if (snapshotManager != null) {
            snapshotManager.close();
        }

        // Fail all pending requests
        for (CompletableFuture<byte[]> future : pendingRequests.values()) {
            future.completeExceptionally(new RuntimeException("Node stopped"));
        }
        pendingRequests.clear();

        logger.info("RaftNode {} stopped", id);
    }

    // ==================== Client Request Handling ====================

    /**
     * Submit a command to the Raft cluster.
     * Only the leader can accept commands.
     *
     * @param command the command to execute
     * @return future that completes when command is committed and applied
     */
    public CompletableFuture<byte[]> submitCommand(Command command) {
        MDC.put("nodeId", id.toString());

        if (!state.isLeader()) {
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            future.completeExceptionally(new NotLeaderException(state.getLeaderId()));
            return future;
        }

        appendLock.lock();
        try {
            // Append to local log (will be persisted)
            long index = logManager.append(state.getCurrentTerm(), command.toBytes());
            logger.debug("Appended command at index {}: {}", index, command);

            // Create future for this request
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            pendingRequests.put(index, future);

            // Trigger immediate replication
            replicationManager.triggerReplication();

            // Set timeout
            future.orTimeout(5, TimeUnit.SECONDS)
                    .exceptionally(e -> {
                        pendingRequests.remove(index);
                        return null;
                    });

            return future;
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Read a value from the state machine.
     * For linearizable reads, this should go through Raft.
     * This is a simple stale read for now.
     */
    public byte[] read(byte[] key) {
        return stateMachine.get(key);
    }

    // ==================== Callbacks ====================

    private void onBecomeLeader() {
        MDC.put("nodeId", id.toString());
        logger.info("Became LEADER for term {}", state.getCurrentTerm());
        electionManager.cancelElectionTimer();
        replicationManager.start();
    }

    private void onBecomeFollower() {
        MDC.put("nodeId", id.toString());
        logger.debug("Became FOLLOWER");
        replicationManager.stop();
        electionManager.resetElectionTimer();

        // Fail pending requests - we're no longer leader
        for (CompletableFuture<byte[]> future : pendingRequests.values()) {
            future.completeExceptionally(new NotLeaderException(state.getLeaderId()));
        }
        pendingRequests.clear();
    }

    private void onHigherTermDiscovered(long newTerm) {
        MDC.put("nodeId", id.toString());
        state.updateTerm(newTerm);
        onBecomeFollower();
    }

    /**
     * Apply committed entries to state machine.
     */
    private void applyCommittedEntries() {
        applyExecutor.execute(() -> {
            MDC.put("nodeId", id.toString());

            long commitIndex = state.getCommitIndex();
            long lastApplied = state.getLastApplied();

            while (lastApplied < commitIndex) {
                lastApplied++;
                LogEntry entry = logManager.getEntry(lastApplied);

                if (entry != null && entry.command().length > 0) {
                    Command command = Command.fromBytes(entry.command());
                    logger.debug("Applying entry {}: {}", lastApplied, command);

                    byte[] result = stateMachine.apply(command);

                    // Complete pending request if we're the leader
                    CompletableFuture<byte[]> future = pendingRequests.remove(lastApplied);
                    if (future != null) {
                        future.complete(result);
                    }
                }

                state.setLastApplied(lastApplied);
            }

            // Check if we should take a snapshot
            maybeSnapshot();
        });
    }

    // ==================== Snapshot Operations ====================

    /**
     * Check if we should take a snapshot based on log size.
     */
    private void maybeSnapshot() {
        if (snapshotManager == null) {
            return;
        }

        int logSize = logManager.size();
        if (logSize >= SNAPSHOT_THRESHOLD) {
            try {
                takeSnapshot();
            } catch (IOException e) {
                logger.error("Failed to take snapshot", e);
            }
        }
    }

    /**
     * Take a snapshot at the current commit index.
     */
    public void takeSnapshot() throws IOException {
        snapshotLock.lock();
        try {
            if (snapshotManager == null) {
                throw new IllegalStateException("Snapshot not enabled (no data directory)");
            }

            long snapshotIndex = state.getLastApplied();
            if (snapshotIndex <= logManager.getSnapshotIndex()) {
                logger.debug("No new entries to snapshot");
                return;
            }

            long snapshotTerm = logManager.getTerm(snapshotIndex);
            if (snapshotTerm == 0) {
                // Entry might be compacted, use snapshot term
                snapshotTerm = logManager.getSnapshotTerm();
            }

            logger.info("Taking snapshot at index {}, term {}", snapshotIndex, snapshotTerm);

            // Serialize state machine
            byte[] snapshotData = stateMachine.takeSnapshot();

            // Save snapshot
            snapshotManager.saveSnapshot(snapshotData, snapshotIndex, snapshotTerm);

            // Compact log
            logManager.compactTo(snapshotIndex, snapshotTerm);

            logger.info("Snapshot complete: {} bytes, {} log entries remaining",
                    snapshotData.length, logManager.size());

        } finally {
            snapshotLock.unlock();
        }
    }

    /**
     * Manually trigger a snapshot (for testing or admin).
     */
    public void triggerSnapshot() throws IOException {
        takeSnapshot();
    }

    /**
     * Get snapshot data for replication to a follower.
     * Called by ReplicationManager when follower is too far behind.
     */
    private InstallSnapshotRequest getSnapshotForReplication(NodeId target) {
        if (snapshotManager == null || !snapshotManager.hasSnapshot()) {
            return null;
        }

        try {
            byte[] data = snapshotManager.loadSnapshot();
            return InstallSnapshotRequest.newBuilder()
                    .setTerm(state.getCurrentTerm())
                    .setLeaderId(id.toString())
                    .setLastIncludedIndex(snapshotManager.getLastIncludedIndex())
                    .setLastIncludedTerm(snapshotManager.getLastIncludedTerm())
                    .setOffset(0)
                    .setData(ByteString.copyFrom(data))
                    .setDone(true)  // For simplicity, send entire snapshot at once
                    .build();
        } catch (IOException e) {
            logger.error("Failed to load snapshot for replication", e);
            return null;
        }
    }

    /**
     * Install a snapshot received from the leader.
     */
    private void installSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) throws IOException {
        snapshotLock.lock();
        try {
            logger.info("Installing snapshot: lastIndex={}, lastTerm={}, size={}",
                    lastIncludedIndex, lastIncludedTerm, data.length);

            // Save snapshot to disk
            if (snapshotManager != null) {
                snapshotManager.saveSnapshot(data, lastIncludedIndex, lastIncludedTerm);
            }

            // Restore state machine from snapshot
            stateMachine.restoreSnapshot(data);

            // Reset log
            logManager.resetToSnapshot(lastIncludedIndex, lastIncludedTerm);

            // Update state
            if (lastIncludedIndex > state.getCommitIndex()) {
                state.setCommitIndex(lastIncludedIndex);
            }
            if (lastIncludedIndex > state.getLastApplied()) {
                state.setLastApplied(lastIncludedIndex);
            }

            logger.info("Snapshot installed successfully");

        } finally {
            snapshotLock.unlock();
        }
    }

    // ==================== RpcHandler Implementation ====================

    @Override
    public VoteResponse handleVoteRequest(VoteRequest request) {
        MDC.put("nodeId", id.toString());
        return electionManager.handleVoteRequest(request);
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        MDC.put("nodeId", id.toString());

        long currentTerm = state.getCurrentTerm();
        long requestTerm = request.getTerm();

        // If request term > current term, update term (will be persisted)
        if (requestTerm > currentTerm) {
            state.updateTerm(requestTerm);
            currentTerm = requestTerm;
            onBecomeFollower();
        }

        // Reject if request term < current term
        if (requestTerm < currentTerm) {
            return AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .build();
        }

        // Valid message from leader
        state.setLeaderId(NodeId.of(request.getLeaderId()));
        electionManager.resetElectionTimer();

        // Check log consistency
        long prevLogIndex = request.getPrevLogIndex();
        long prevLogTerm = request.getPrevLogTerm();

        if (prevLogIndex > 0) {
            long myTermAtPrev = logManager.getTerm(prevLogIndex);

            // We don't have this entry
            if (prevLogIndex > logManager.getLastIndex()) {
                logger.debug("Missing entry at index {}", prevLogIndex);
                return AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .setConflictIndex(logManager.getLastIndex() + 1)
                        .build();
            }

            // Term mismatch
            if (myTermAtPrev != prevLogTerm) {
                logger.debug("Term mismatch at index {}: {} != {}", prevLogIndex, myTermAtPrev, prevLogTerm);
                // Find first index of conflicting term
                long conflictTerm = myTermAtPrev;
                long conflictIndex = prevLogIndex;
                while (conflictIndex > 1 && logManager.getTerm(conflictIndex - 1) == conflictTerm) {
                    conflictIndex--;
                }
                return AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .setConflictTerm(conflictTerm)
                        .setConflictIndex(conflictIndex)
                        .build();
            }
        }

        // Append entries (will be persisted)
        if (request.getEntriesCount() > 0) {
            List<LogEntry> entries = request.getEntriesList().stream()
                    .map(LogEntry::fromProto)
                    .toList();

            boolean success = logManager.appendEntries(prevLogIndex, prevLogTerm, entries);
            if (!success) {
                return AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .build();
            }

            logger.debug("Appended {} entries, last index now {}", entries.size(), logManager.getLastIndex());
        }

        // Update commit index
        if (request.getLeaderCommit() > state.getCommitIndex()) {
            long newCommit = Math.min(request.getLeaderCommit(), logManager.getLastIndex());
            state.setCommitIndex(newCommit);
            logger.debug("Updated commit index to {}", newCommit);

            // Apply committed entries
            applyCommittedEntries();
        }

        return AppendEntriesResponse.newBuilder()
                .setTerm(currentTerm)
                .setSuccess(true)
                .setMatchIndex(logManager.getLastIndex())
                .build();
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        MDC.put("nodeId", id.toString());

        long currentTerm = state.getCurrentTerm();
        long requestTerm = request.getTerm();

        // If request term > current term, update term
        if (requestTerm > currentTerm) {
            state.updateTerm(requestTerm);
            currentTerm = requestTerm;
            onBecomeFollower();
        }

        // Reject if request term < current term
        if (requestTerm < currentTerm) {
            return InstallSnapshotResponse.newBuilder()
                    .setTerm(currentTerm)
                    .build();
        }

        // Valid message from leader
        state.setLeaderId(NodeId.of(request.getLeaderId()));
        electionManager.resetElectionTimer();

        // For simplicity, we assume the entire snapshot is sent in one request (done=true)
        if (request.getDone()) {
            try {
                installSnapshot(
                        request.getLastIncludedIndex(),
                        request.getLastIncludedTerm(),
                        request.getData().toByteArray()
                );
            } catch (IOException e) {
                logger.error("Failed to install snapshot", e);
            }
        }

        return InstallSnapshotResponse.newBuilder()
                .setTerm(currentTerm)
                .build();
    }

    // ==================== Getters ====================

    public NodeId getId() {
        return id;
    }

    public RaftState getState() {
        return state;
    }

    public LogManager getLogManager() {
        return logManager;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public boolean isLeader() {
        return state.isLeader();
    }

    public NodeId getLeaderId() {
        return state.getLeaderId();
    }

    public long getCurrentTerm() {
        return state.getCurrentTerm();
    }

    public long getCommitIndex() {
        return state.getCommitIndex();
    }

    public long getLastApplied() {
        return state.getLastApplied();
    }

    // ==================== Exception ====================

    public static class NotLeaderException extends RuntimeException {
        private final NodeId leaderId;

        public NotLeaderException(NodeId leaderId) {
            super("Not leader. Leader is: " + leaderId);
            this.leaderId = leaderId;
        }

        public NodeId getLeaderId() {
            return leaderId;
        }
    }
}
