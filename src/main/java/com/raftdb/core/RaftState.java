package com.raftdb.core;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Raft node state, including persistent state and volatile state.
 *
 * Persistent state (must survive restart):
 * - currentTerm: latest term server has seen
 * - votedFor: candidateId that received vote in current term
 *
 * Volatile state:
 * - role: current role (FOLLOWER, CANDIDATE, LEADER)
 * - leaderId: current known leader
 * - commitIndex: highest log entry known to be committed
 * - lastApplied: highest log entry applied to state machine
 */
public class RaftState {

    // Persistent state
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicReference<NodeId> votedFor = new AtomicReference<>(null);

    // Volatile state
    private volatile Role role = Role.FOLLOWER;
    private volatile NodeId leaderId = null;
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);

    // Persistence callback: called when term or votedFor changes
    private BiConsumer<Long, NodeId> persistCallback;

    /**
     * Set callback for persisting state changes.
     */
    public void setPersistCallback(BiConsumer<Long, NodeId> callback) {
        this.persistCallback = callback;
    }

    /**
     * Restore state from persisted values (called during recovery).
     */
    public void restore(long term, NodeId votedFor) {
        this.currentTerm.set(term);
        this.votedFor.set(votedFor);
    }

    private void persist() {
        if (persistCallback != null) {
            persistCallback.accept(currentTerm.get(), votedFor.get());
        }
    }

    // ==================== Term Operations ====================

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * Update term if the given term is greater than current term.
     * If updated, reset votedFor and convert to follower.
     *
     * @return true if term was updated
     */
    public boolean updateTerm(long newTerm) {
        long current = currentTerm.get();
        if (newTerm > current) {
            if (currentTerm.compareAndSet(current, newTerm)) {
                votedFor.set(null);
                role = Role.FOLLOWER;
                persist();
                return true;
            }
        }
        return false;
    }

    /**
     * Increment term for starting an election.
     */
    public long incrementTerm() {
        long newTerm = currentTerm.incrementAndGet();
        persist();
        return newTerm;
    }

    // ==================== Vote Operations ====================

    public NodeId getVotedFor() {
        return votedFor.get();
    }

    /**
     * Try to vote for a candidate.
     *
     * @return true if vote was granted
     */
    public boolean tryVoteFor(NodeId candidateId) {
        boolean granted = votedFor.compareAndSet(null, candidateId)
                || candidateId.equals(votedFor.get());
        if (granted && votedFor.get() != null) {
            persist();
        }
        return granted;
    }

    public void resetVotedFor() {
        votedFor.set(null);
    }

    // ==================== Role Operations ====================

    public Role getRole() {
        return role;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public boolean isFollower() {
        return role == Role.FOLLOWER;
    }

    public boolean isCandidate() {
        return role == Role.CANDIDATE;
    }

    public void becomeFollower() {
        this.role = Role.FOLLOWER;
    }

    public void becomeCandidate() {
        this.role = Role.CANDIDATE;
    }

    public void becomeLeader() {
        this.role = Role.LEADER;
    }

    // ==================== Leader Tracking ====================

    public NodeId getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(NodeId leaderId) {
        this.leaderId = leaderId;
    }

    // ==================== Commit Index ====================

    public long getCommitIndex() {
        return commitIndex.get();
    }

    public void setCommitIndex(long index) {
        commitIndex.set(index);
    }

    public boolean tryAdvanceCommitIndex(long newIndex) {
        long current = commitIndex.get();
        if (newIndex > current) {
            return commitIndex.compareAndSet(current, newIndex);
        }
        return false;
    }

    // ==================== Last Applied ====================

    public long getLastApplied() {
        return lastApplied.get();
    }

    public void setLastApplied(long index) {
        lastApplied.set(index);
    }

    public long incrementLastApplied() {
        return lastApplied.incrementAndGet();
    }

    @Override
    public String toString() {
        return String.format("RaftState{term=%d, role=%s, votedFor=%s, leader=%s, commit=%d}",
                currentTerm.get(), role, votedFor.get(), leaderId, commitIndex.get());
    }
}
