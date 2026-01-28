package com.raftdb.rpc;

import com.raftdb.rpc.proto.AppendEntriesRequest;
import com.raftdb.rpc.proto.AppendEntriesResponse;
import com.raftdb.rpc.proto.VoteRequest;
import com.raftdb.rpc.proto.VoteResponse;

/**
 * Handler for incoming Raft RPC requests.
 * Implemented by RaftNode.
 */
public interface RpcHandler {

    /**
     * Handle an incoming vote request from a candidate.
     */
    VoteResponse handleVoteRequest(VoteRequest request);

    /**
     * Handle an incoming append entries request from the leader.
     * Also used for heartbeats (when entries is empty).
     */
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);
}
