package com.raftdb.rpc;

import com.raftdb.core.NodeId;
import com.raftdb.rpc.proto.AppendEntriesRequest;
import com.raftdb.rpc.proto.AppendEntriesResponse;
import com.raftdb.rpc.proto.InstallSnapshotRequest;
import com.raftdb.rpc.proto.InstallSnapshotResponse;
import com.raftdb.rpc.proto.VoteRequest;
import com.raftdb.rpc.proto.VoteResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction layer for Raft RPC communication between nodes.
 *
 * <p>This interface defines the transport mechanism used by Raft nodes to
 * communicate with each other. It supports all three Raft RPC types:
 * <ul>
 *   <li><b>RequestVote</b> - Used during leader election</li>
 *   <li><b>AppendEntries</b> - Used for log replication and heartbeats</li>
 *   <li><b>InstallSnapshot</b> - Used to transfer snapshots to lagging followers</li>
 * </ul>
 *
 * <h2>Implementations</h2>
 * <ul>
 *   <li>{@link LocalTransport} - In-memory transport for testing (single process)</li>
 *   <li>{@link GrpcTransport} - gRPC-based transport for production (real network)</li>
 * </ul>
 *
 * <h2>Threading Model</h2>
 * <p>All send methods return {@link CompletableFuture} to support asynchronous
 * communication. Implementations should be thread-safe as multiple threads may
 * send requests concurrently.
 *
 * @author raft-kv
 * @see RpcHandler
 * @see LocalTransport
 * @see GrpcTransport
 */
public interface RpcTransport {

    /**
     * Sends a vote request to a target node during leader election.
     *
     * <p>This RPC is invoked by candidates to gather votes from other nodes.
     * The response indicates whether the vote was granted.
     *
     * @param target the ID of the target node
     * @param request the vote request containing term and log information
     * @return a future that completes with the vote response
     */
    CompletableFuture<VoteResponse> sendVoteRequest(NodeId target, VoteRequest request);

    /**
     * Sends an append entries request to a target node.
     *
     * <p>This RPC serves dual purposes:
     * <ul>
     *   <li><b>Heartbeat</b> - When entries list is empty, maintains leader authority</li>
     *   <li><b>Log Replication</b> - When entries list is non-empty, replicates log entries</li>
     * </ul>
     *
     * @param target the ID of the target node
     * @param request the append entries request containing log entries and commit info
     * @return a future that completes with the append entries response
     */
    CompletableFuture<AppendEntriesResponse> sendAppendEntries(NodeId target, AppendEntriesRequest request);

    /**
     * Sends an install snapshot request to a target node.
     *
     * <p>This RPC is used when a follower is so far behind that the leader
     * no longer has the log entries needed to bring it up to date. Instead,
     * the leader sends its snapshot.
     *
     * @param target the ID of the target node
     * @param request the snapshot request containing the snapshot data
     * @return a future that completes with the install snapshot response
     */
    CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(NodeId target, InstallSnapshotRequest request);

    /**
     * Starts the transport and begins listening for incoming requests.
     *
     * <p>After this method is called, the transport should be ready to:
     * <ul>
     *   <li>Receive incoming RPCs and delegate them to the handler</li>
     *   <li>Send outgoing RPCs to other nodes</li>
     * </ul>
     *
     * @param handler the handler that will process incoming RPC requests
     */
    void start(RpcHandler handler);

    /**
     * Shuts down the transport and releases all resources.
     *
     * <p>After this method is called, the transport should:
     * <ul>
     *   <li>Stop accepting new incoming connections</li>
     *   <li>Close all existing connections</li>
     *   <li>Release network resources (ports, threads, etc.)</li>
     * </ul>
     */
    void shutdown();
}
