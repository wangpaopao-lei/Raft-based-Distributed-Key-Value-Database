package com.raftdb.core;

/**
 * Raft node roles.
 */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
