package com.raftdb.rpc;

import com.raftdb.core.NodeId;
import com.raftdb.rpc.proto.AppendEntriesRequest;
import com.raftdb.rpc.proto.AppendEntriesResponse;
import com.raftdb.rpc.proto.VoteRequest;
import com.raftdb.rpc.proto.VoteResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Transport layer abstraction for Raft RPC communication.
 *
 * Implementations:
 * - LocalTransport: In-memory for testing/development
 * - GrpcTransport: Real network communication (Phase 5)
 */
public interface RpcTransport {

    /**
     * Send a vote request to a target node.
     */
    CompletableFuture<VoteResponse> sendVoteRequest(NodeId target, VoteRequest request);

    /**
     * Send an append entries request (log replication or heartbeat) to a target node.
     */
    CompletableFuture<AppendEntriesResponse> sendAppendEntries(NodeId target, AppendEntriesRequest request);

    /**
     * Start the transport, begin listening for incoming requests.
     */
    void start(RpcHandler handler);

    /**
     * Shutdown the transport.
     */
    void shutdown();
}
