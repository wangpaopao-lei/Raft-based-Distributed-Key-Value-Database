package com.raftdb.election;

import com.raftdb.core.NodeId;
import com.raftdb.core.RaftState;
import com.raftdb.core.Role;
import com.raftdb.rpc.RpcTransport;
import com.raftdb.rpc.proto.VoteRequest;
import com.raftdb.rpc.proto.VoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Manages leader election process.
 *
 * Responsibilities:
 * - Election timeout detection
 * - Starting elections as candidate
 * - Vote counting
 * - Role transitions
 */
public class ElectionManager {

    private static final Logger logger = LoggerFactory.getLogger(ElectionManager.class);

    // Election timeout range (ms)
    private static final int ELECTION_TIMEOUT_MIN = 150;
    private static final int ELECTION_TIMEOUT_MAX = 300;

    private final NodeId selfId;
    private final RaftState state;
    private final RpcTransport transport;
    private final List<NodeId> peers;
    private final Supplier<Long> lastLogIndexSupplier;
    private final Supplier<Long> lastLogTermSupplier;

    private final ScheduledExecutorService scheduler;
    private final Random random = new Random();

    private ScheduledFuture<?> electionTimer;

    // Callbacks
    private Runnable onBecomeLeader;
    private Runnable onBecomeFollower;

    public ElectionManager(
            NodeId selfId,
            RaftState state,
            RpcTransport transport,
            List<NodeId> peers,
            Supplier<Long> lastLogIndexSupplier,
            Supplier<Long> lastLogTermSupplier) {
        this.selfId = selfId;
        this.state = state;
        this.transport = transport;
        this.peers = peers;
        this.lastLogIndexSupplier = lastLogIndexSupplier;
        this.lastLogTermSupplier = lastLogTermSupplier;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "election-" + selfId);
            t.setDaemon(true);
            return t;
        });
    }

    public void setOnBecomeLeader(Runnable callback) {
        this.onBecomeLeader = callback;
    }

    public void setOnBecomeFollower(Runnable callback) {
        this.onBecomeFollower = callback;
    }

    /**
     * Start the election manager.
     */
    public void start() {
        MDC.put("nodeId", selfId.toString());
        logger.info("ElectionManager started");
        resetElectionTimer();
    }

    /**
     * Stop the election manager.
     */
    public void stop() {
        cancelElectionTimer();
        scheduler.shutdown();
        logger.info("ElectionManager stopped");
    }

    /**
     * Reset election timer. Called when:
     * - Receiving valid heartbeat from leader
     * - Granting vote to a candidate
     * - Starting a new election
     */
    public void resetElectionTimer() {
        cancelElectionTimer();

        int timeout = ELECTION_TIMEOUT_MIN + random.nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);

        electionTimer = scheduler.schedule(
                this::onElectionTimeout,
                timeout,
                TimeUnit.MILLISECONDS
        );

        logger.trace("Election timer reset to {}ms", timeout);
    }

    /**
     * Cancel the election timer. Called when becoming leader.
     */
    public void cancelElectionTimer() {
        if (electionTimer != null && !electionTimer.isDone()) {
            electionTimer.cancel(false);
        }
    }

    /**
     * Handle election timeout - start an election.
     */
    private void onElectionTimeout() {
        MDC.put("nodeId", selfId.toString());

        if (state.isLeader()) {
            // Leaders don't need election timeout
            return;
        }

        logger.info("Election timeout, starting election");
        startElection();
    }

    /**
     * Start an election as candidate.
     */
    private void startElection() {
        // Increment term and vote for self
        long newTerm = state.incrementTerm();
        state.becomeCandidate();
        state.tryVoteFor(selfId);

        logger.info("Starting election for term {}", newTerm);

        // Reset timer for this election
        resetElectionTimer();

        // Build vote request
        VoteRequest request = VoteRequest.newBuilder()
                .setTerm(newTerm)
                .setCandidateId(selfId.toString())
                .setLastLogIndex(lastLogIndexSupplier.get())
                .setLastLogTerm(lastLogTermSupplier.get())
                .build();

        // Count votes (start with 1 for self)
        int majority = (peers.size() + 1) / 2 + 1;
        AtomicInteger votes = new AtomicInteger(1);

        logger.debug("Need {} votes to win (cluster size: {})", majority, peers.size() + 1);

        // Check if already won (single node cluster)
        if (votes.get() >= majority) {
            winElection();
            return;
        }

        // Request votes from all peers in parallel
        List<CompletableFuture<Void>> futures = peers.stream()
                .map(peer -> requestVote(peer, request, newTerm, votes, majority))
                .toList();

        // Wait for all requests to complete (with timeout)
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(ELECTION_TIMEOUT_MIN, TimeUnit.MILLISECONDS)
                .exceptionally(e -> null);  // Ignore timeout
    }

    private CompletableFuture<Void> requestVote(
            NodeId peer,
            VoteRequest request,
            long electionTerm,
            AtomicInteger votes,
            int majority) {

        return transport.sendVoteRequest(peer, request)
                .thenAccept(response -> {
                    MDC.put("nodeId", selfId.toString());
                    handleVoteResponse(response, electionTerm, votes, majority);
                })
                .exceptionally(e -> {
                    logger.warn("Failed to get vote from {}: {}", peer, e.getMessage());
                    return null;
                });
    }

    private synchronized void handleVoteResponse(
            VoteResponse response,
            long electionTerm,
            AtomicInteger votes,
            int majority) {

        // Check if we've moved to a new term
        if (response.getTerm() > state.getCurrentTerm()) {
            logger.info("Discovered higher term {}, stepping down", response.getTerm());
            state.updateTerm(response.getTerm());
            if (onBecomeFollower != null) {
                onBecomeFollower.run();
            }
            return;
        }

        // Ignore if election is stale or we're no longer candidate
        if (electionTerm != state.getCurrentTerm() || !state.isCandidate()) {
            return;
        }

        if (response.getVoteGranted()) {
            int currentVotes = votes.incrementAndGet();
            logger.debug("Received vote, total: {}/{}", currentVotes, majority);

            if (currentVotes >= majority && state.isCandidate()) {
                winElection();
            }
        }
    }

    private void winElection() {
        logger.info("Won election for term {}", state.getCurrentTerm());
        state.becomeLeader();
        state.setLeaderId(selfId);
        cancelElectionTimer();

        if (onBecomeLeader != null) {
            onBecomeLeader.run();
        }
    }

    /**
     * Handle incoming vote request.
     *
     * Grant vote if:
     * 1. Candidate's term >= currentTerm
     * 2. Haven't voted in this term, or already voted for this candidate
     * 3. Candidate's log is at least as up-to-date as ours
     */
    public VoteResponse handleVoteRequest(VoteRequest request) {
        MDC.put("nodeId", selfId.toString());

        long currentTerm = state.getCurrentTerm();
        long requestTerm = request.getTerm();

        // If request term > current term, update term
        if (requestTerm > currentTerm) {
            state.updateTerm(requestTerm);
            currentTerm = requestTerm;
            if (onBecomeFollower != null) {
                onBecomeFollower.run();
            }
        }

        // Reject if request term < current term
        if (requestTerm < currentTerm) {
            logger.debug("Rejecting vote for {} - stale term {} < {}",
                    request.getCandidateId(), requestTerm, currentTerm);
            return VoteResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }

        // Check if we can vote for this candidate
        NodeId candidateId = NodeId.of(request.getCandidateId());
        boolean canVote = state.tryVoteFor(candidateId);

        // Check log is up-to-date
        boolean logUpToDate = isLogUpToDate(request.getLastLogTerm(), request.getLastLogIndex());

        boolean voteGranted = canVote && logUpToDate;

        if (voteGranted) {
            logger.info("Granting vote to {} for term {}", candidateId, requestTerm);
            resetElectionTimer();
        } else {
            logger.debug("Rejecting vote for {} - canVote={}, logUpToDate={}",
                    candidateId, canVote, logUpToDate);
        }

        return VoteResponse.newBuilder()
                .setTerm(currentTerm)
                .setVoteGranted(voteGranted)
                .build();
    }

    /**
     * Check if candidate's log is at least as up-to-date as ours.
     * Raft determines which log is more up-to-date by comparing:
     * 1. Last entry's term (higher is more up-to-date)
     * 2. If terms are equal, longer log is more up-to-date
     */
    private boolean isLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = lastLogTermSupplier.get();
        long myLastIndex = lastLogIndexSupplier.get();

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }
}
