package com.raftdb.core;

/**
 * Enumeration of the possible roles a Raft node can assume.
 *
 * <p>In the Raft consensus algorithm, each node operates in one of three roles:
 * <ul>
 *   <li>{@link #FOLLOWER} - The default passive role that responds to requests from leaders and candidates</li>
 *   <li>{@link #CANDIDATE} - A transitional role when a node is attempting to become leader</li>
 *   <li>{@link #LEADER} - The active role responsible for log replication and client interactions</li>
 * </ul>
 *
 * <p>Role transitions follow the Raft state machine:
 * <pre>
 *     FOLLOWER ──(election timeout)──► CANDIDATE
 *         ▲                                │
 *         │                    (wins election)
 *         │                                ▼
 *         └───(discovers higher term)─── LEADER
 * </pre>
 *
 * @author raft-kv
 * @see RaftState
 * @see RaftNode
 */
public enum Role {

    /**
     * Follower role - the default passive state.
     *
     * <p>A follower:
     * <ul>
     *   <li>Responds to RPCs from leaders and candidates</li>
     *   <li>Redirects client requests to the leader</li>
     *   <li>Converts to candidate if election timeout elapses without receiving heartbeat</li>
     * </ul>
     */
    FOLLOWER,

    /**
     * Candidate role - actively seeking votes to become leader.
     *
     * <p>A candidate:
     * <ul>
     *   <li>Increments its term and votes for itself</li>
     *   <li>Sends RequestVote RPCs to all other nodes</li>
     *   <li>Becomes leader if it receives votes from a majority</li>
     *   <li>Reverts to follower if it discovers a higher term or a valid leader</li>
     * </ul>
     */
    CANDIDATE,

    /**
     * Leader role - responsible for managing the replicated log.
     *
     * <p>A leader:
     * <ul>
     *   <li>Handles all client requests</li>
     *   <li>Replicates log entries to followers via AppendEntries RPCs</li>
     *   <li>Sends periodic heartbeats to maintain authority</li>
     *   <li>Steps down to follower if it discovers a higher term</li>
     * </ul>
     */
    LEADER
}
