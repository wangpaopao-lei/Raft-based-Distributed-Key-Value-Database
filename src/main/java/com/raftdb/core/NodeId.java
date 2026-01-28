package com.raftdb.core;

import java.util.Objects;

/**
 * Unique identifier for a Raft node.
 */
public record NodeId(String id) {

    public NodeId {
        Objects.requireNonNull(id, "Node ID cannot be null");
        if (id.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be blank");
        }
    }

    public static NodeId of(String id) {
        return new NodeId(id);
    }

    @Override
    public String toString() {
        return id;
    }
}
