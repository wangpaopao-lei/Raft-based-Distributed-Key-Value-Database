package com.raftdb.core;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Encapsulates all state maintained by a Raft node.
 *
 * <p>This class manages both persistent state (which must survive restarts) and
 * volatile state (which can be reconstructed after a crash). Thread-safe operations
 * are provided for all state modifications.
 *
 * <h2>Persistent State (on stable storage)</h2>
 * <ul>
 *   <li>{@code currentTerm} - The latest term the server has seen (initialized to 0,
 *       increases monotonically)</li>
 *   <li>{@code votedFor} - The candidate ID that received this node's vote in the
 *       current term (or null if none)</li>
 * </ul>
 *
 * <h2>Volatile State (on all servers)</h2>
 * <ul>
 *   <li>{@code role} - Current role: FOLLOWER, CANDIDATE, or LEADER</li>
 *   <li>{@code leaderId} - The ID of the current known leader (for redirecting clients)</li>
 *   <li>{@code commitIndex} - Index of the highest log entry known to be committed</li>
 *   <li>{@code lastApplied} - Index of the highest log entry applied to state machine</li>
 * </ul>
 *
 * <p>The class supports a persistence callback mechanism that is invoked whenever
 * {@code currentTerm} or {@code votedFor} changes, allowing the caller to persist
 * these values to stable storage.
 *
 * @author raft-kv
 * @see Role
 * @see RaftNode
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
     * Sets the callback to be invoked when persistent state changes.
     *
     * <p>The callback receives the current term and voted-for node ID whenever
     * either value is modified. This allows the caller to persist these values
     * to stable storage.
     *
     * @param callback the persistence callback, receiving (term, votedFor)
     */
    public void setPersistCallback(BiConsumer<Long, NodeId> callback) {
        this.persistCallback = callback;
    }

    /**
     * Restores state from persisted values during crash recovery.
     *
     * <p>This method should be called during node startup to restore the
     * persistent state that was saved before a crash or shutdown.
     *
     * @param term the persisted current term
     * @param votedFor the persisted voted-for node ID (may be null)
     */
    public void restore(long term, NodeId votedFor) {
        this.currentTerm.set(term);
        this.votedFor.set(votedFor);
    }

    /**
     * Invokes the persistence callback if one is registered.
     */
    private void persist() {
        if (persistCallback != null) {
            persistCallback.accept(currentTerm.get(), votedFor.get());
        }
    }

    // ==================== Term Operations ====================

    /**
     * Returns the current term.
     *
     * @return the current term number
     */
    public long getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * Updates the current term if the given term is greater.
     *
     * <p>If the term is updated, this method also:
     * <ul>
     *   <li>Resets {@code votedFor} to null</li>
     *   <li>Converts the node to follower role</li>
     *   <li>Triggers persistence of the new state</li>
     * </ul>
     *
     * @param newTerm the new term to potentially adopt
     * @return {@code true} if the term was updated, {@code false} otherwise
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
     * Increments the current term by one.
     *
     * <p>This is typically called when starting a new election. The new term
     * is persisted before being returned.
     *
     * @return the new term after incrementing
     */
    public long incrementTerm() {
        long newTerm = currentTerm.incrementAndGet();
        persist();
        return newTerm;
    }

    // ==================== Vote Operations ====================

    /**
     * Returns the node ID that this node voted for in the current term.
     *
     * @return the voted-for node ID, or {@code null} if no vote was cast
     */
    public NodeId getVotedFor() {
        return votedFor.get();
    }

    /**
     * Attempts to cast a vote for the specified candidate.
     *
     * <p>A vote is granted if either:
     * <ul>
     *   <li>This node has not yet voted in the current term (votedFor is null)</li>
     *   <li>This node has already voted for the same candidate</li>
     * </ul>
     *
     * <p>If the vote is granted and changes the voted-for value, the new state
     * is persisted.
     *
     * @param candidateId the ID of the candidate requesting the vote
     * @return {@code true} if the vote was granted, {@code false} otherwise
     */
    public boolean tryVoteFor(NodeId candidateId) {
        boolean granted = votedFor.compareAndSet(null, candidateId)
                || candidateId.equals(votedFor.get());
        if (granted && votedFor.get() != null) {
            persist();
        }
        return granted;
    }

    /**
     * Resets the voted-for value to null.
     *
     * <p>Note: This method does not trigger persistence. Term updates that
     * reset the vote will handle persistence.
     */
    public void resetVotedFor() {
        votedFor.set(null);
    }

    // ==================== Role Operations ====================

    /**
     * Returns the current role of this node.
     *
     * @return the current role
     */
    public Role getRole() {
        return role;
    }

    /**
     * Checks if this node is currently the leader.
     *
     * @return {@code true} if this node is the leader, {@code false} otherwise
     */
    public boolean isLeader() {
        return role == Role.LEADER;
    }

    /**
     * Checks if this node is currently a follower.
     *
     * @return {@code true} if this node is a follower, {@code false} otherwise
     */
    public boolean isFollower() {
        return role == Role.FOLLOWER;
    }

    /**
     * Checks if this node is currently a candidate.
     *
     * @return {@code true} if this node is a candidate, {@code false} otherwise
     */
    public boolean isCandidate() {
        return role == Role.CANDIDATE;
    }

    /**
     * Transitions this node to the follower role.
     */
    public void becomeFollower() {
        this.role = Role.FOLLOWER;
    }

    /**
     * Transitions this node to the candidate role.
     */
    public void becomeCandidate() {
        this.role = Role.CANDIDATE;
    }

    /**
     * Transitions this node to the leader role.
     */
    public void becomeLeader() {
        this.role = Role.LEADER;
    }

    // ==================== Leader Tracking ====================

    /**
     * Returns the ID of the current known leader.
     *
     * @return the leader's node ID, or {@code null} if unknown
     */
    public NodeId getLeaderId() {
        return leaderId;
    }

    /**
     * Sets the ID of the current known leader.
     *
     * @param leaderId the leader's node ID
     */
    public void setLeaderId(NodeId leaderId) {
        this.leaderId = leaderId;
    }

    // ==================== Commit Index ====================

    /**
     * Returns the index of the highest log entry known to be committed.
     *
     * @return the commit index
     */
    public long getCommitIndex() {
        return commitIndex.get();
    }

    /**
     * Sets the commit index.
     *
     * @param index the new commit index
     */
    public void setCommitIndex(long index) {
        commitIndex.set(index);
    }

    /**
     * Attempts to advance the commit index to a new value.
     *
     * <p>The commit index is only updated if the new index is greater than
     * the current value.
     *
     * @param newIndex the new commit index to potentially adopt
     * @return {@code true} if the commit index was advanced, {@code false} otherwise
     */
    public boolean tryAdvanceCommitIndex(long newIndex) {
        long current = commitIndex.get();
        if (newIndex > current) {
            return commitIndex.compareAndSet(current, newIndex);
        }
        return false;
    }

    // ==================== Last Applied ====================

    /**
     * Returns the index of the highest log entry applied to the state machine.
     *
     * @return the last applied index
     */
    public long getLastApplied() {
        return lastApplied.get();
    }

    /**
     * Sets the last applied index.
     *
     * @param index the new last applied index
     */
    public void setLastApplied(long index) {
        lastApplied.set(index);
    }

    /**
     * Increments the last applied index by one.
     *
     * @return the new last applied index after incrementing
     */
    public long incrementLastApplied() {
        return lastApplied.incrementAndGet();
    }

    /**
     * Returns a string representation of this state for debugging purposes.
     *
     * @return a string containing the current term, role, voted-for, leader, and commit index
     */
    @Override
    public String toString() {
        return String.format("RaftState{term=%d, role=%s, votedFor=%s, leader=%s, commit=%d}",
                currentTerm.get(), role, votedFor.get(), leaderId, commitIndex.get());
    }
}
