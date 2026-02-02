package com.raftdb.core;

import java.util.Objects;

/**
 * Immutable unique identifier for a Raft node in the cluster.
 *
 * <p>Each node in a Raft cluster must have a unique identifier that is used for:
 * <ul>
 *   <li>Identifying the source and target of RPC messages</li>
 *   <li>Tracking vote grants during leader election</li>
 *   <li>Recording the voted-for candidate in persistent state</li>
 *   <li>Identifying the current leader to redirect client requests</li>
 * </ul>
 *
 * <p>This class is implemented as a Java record, providing immutability and
 * automatic implementations of {@code equals()}, {@code hashCode()}, and basic accessors.
 *
 * <p>Example usage:
 * <pre>{@code
 * NodeId nodeId = NodeId.of("node-1");
 * System.out.println(nodeId.id()); // "node-1"
 * }</pre>
 *
 * @author raft-kv
 * @see RaftNode
 */
public record NodeId(String id) {

    /**
     * Constructs a new {@code NodeId} with validation.
     *
     * @param id the unique identifier string for this node
     * @throws NullPointerException if {@code id} is null
     * @throws IllegalArgumentException if {@code id} is blank
     */
    public NodeId {
        Objects.requireNonNull(id, "Node ID cannot be null");
        if (id.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be blank");
        }
    }

    /**
     * Factory method to create a new {@code NodeId}.
     *
     * @param id the unique identifier string for this node
     * @return a new {@code NodeId} instance
     * @throws NullPointerException if {@code id} is null
     * @throws IllegalArgumentException if {@code id} is blank
     */
    public static NodeId of(String id) {
        return new NodeId(id);
    }

    /**
     * Returns the string representation of this node identifier.
     *
     * @return the identifier string
     */
    @Override
    public String toString() {
        return id;
    }
}
