package com.raftdb.core;

/**
 * Exception thrown when an operation requires leadership but the node is not the leader.
 *
 * <p>This exception is typically thrown when:
 * <ul>
 *   <li>A client submits a write command to a follower</li>
 *   <li>A linearizable read is requested from a non-leader node</li>
 *   <li>The node loses leadership during an operation</li>
 * </ul>
 *
 * <p>The exception includes the known leader ID (if available) to help clients
 * redirect their requests to the correct node.
 *
 * @author raft-kv
 */
public class NotLeaderException extends RuntimeException {

    private final NodeId leaderId;

    /**
     * Creates a NotLeaderException with an unknown leader.
     */
    public NotLeaderException() {
        this(null);
    }

    /**
     * Creates a NotLeaderException with the known leader ID.
     *
     * @param leaderId the current leader's ID, or null if unknown
     */
    public NotLeaderException(NodeId leaderId) {
        super(leaderId != null
                ? "Not leader. Current leader: " + leaderId
                : "Not leader. Leader unknown.");
        this.leaderId = leaderId;
    }

    /**
     * Returns the known leader ID.
     *
     * @return the leader ID, or null if unknown
     */
    public NodeId getLeaderId() {
        return leaderId;
    }
}
