package com.raftdb.server;

import com.raftdb.core.NodeId;
import com.raftdb.core.NotLeaderException;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.proto.*;
import com.raftdb.statemachine.Command;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * gRPC service implementation for client key-value operations.
 *
 * <p>This service handles client requests for GET, PUT, and DELETE operations
 * by delegating to the underlying {@link RaftNode}. It implements the
 * {@code KVService} defined in the protobuf schema.
 *
 * <h2>Request Handling</h2>
 * <ul>
 *   <li><b>GET</b> - Reads directly from the state machine (may be stale)</li>
 *   <li><b>PUT/DELETE</b> - Submitted through Raft consensus for replication</li>
 * </ul>
 *
 * <h2>Leader Redirection</h2>
 * <p>Write operations (PUT, DELETE) can only be processed by the leader.
 * If this node is not the leader, the response includes a {@code leader_hint}
 * containing the current leader's address, allowing clients to redirect.
 *
 * <h2>Timeout</h2>
 * <p>Operations have a default timeout of 5 seconds. If the operation doesn't
 * complete within this time (e.g., due to leader election), an error is returned.
 *
 * @author raft-kv
 * @see RaftNode
 * @see com.raftdb.client.RaftKVClient
 */
public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(KVServiceImpl.class);

    /** Default timeout for operations in seconds. */
    private static final long DEFAULT_TIMEOUT_SECONDS = 5;

    private final RaftNode raftNode;

    /**
     * Creates a new KVServiceImpl backed by the given Raft node.
     *
     * @param raftNode the Raft node to delegate operations to
     */
    public KVServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    /**
     * Handles a client request for a key-value operation.
     *
     * @param request the client request containing the operation and key/value
     * @param responseObserver the observer to send the response to
     */
    @Override
    public void execute(ClientRequest request, StreamObserver<ClientResponse> responseObserver) {
        try {
            ClientResponse response = handleRequest(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error handling client request", e);
            responseObserver.onNext(ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Internal error: " + e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    private ClientResponse handleRequest(ClientRequest request) {
        logger.debug("Received request: op={}, key={}",
                request.getOperation(),
                request.getKey().toStringUtf8());

        // For GET operations, we can read directly (stale read)
        // TODO: Phase 7 will add linearizable reads
        if (request.getOperation() == OperationType.GET) {
            return handleGet(request);
        }

        // For write operations, must be leader
        if (!raftNode.isLeader()) {
            return notLeaderResponse();
        }

        return switch (request.getOperation()) {
            case PUT -> handlePut(request);
            case DELETE -> handleDelete(request);
            default -> ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Unknown operation: " + request.getOperation())
                    .build();
        };
    }

    private ClientResponse handleGet(ClientRequest request) {
        byte[] key = request.getKey().toByteArray();

        // Check if linearizable read is requested
        if (request.getLinearizable()) {
            return handleLinearizableGet(request);
        }

        // Default: stale read (fast but may return old data)
        byte[] value = raftNode.read(key);

        if (value != null) {
            return ClientResponse.newBuilder()
                    .setSuccess(true)
                    .setValue(com.google.protobuf.ByteString.copyFrom(value))
                    .build();
        } else {
            return ClientResponse.newBuilder()
                    .setSuccess(true)  // Success but no value (key not found)
                    .build();
        }
    }

    /**
     * Handle linearizable GET request.
     * This ensures the read reflects all committed writes.
     */
    private ClientResponse handleLinearizableGet(ClientRequest request) {
        byte[] key = request.getKey().toByteArray();

        try {
            byte[] value = raftNode.linearizableRead(key)
                    .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (value != null) {
                return ClientResponse.newBuilder()
                        .setSuccess(true)
                        .setValue(com.google.protobuf.ByteString.copyFrom(value))
                        .build();
            } else {
                return ClientResponse.newBuilder()
                        .setSuccess(true)  // Success but no value (key not found)
                        .build();
            }
        } catch (InterruptedException | ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof NotLeaderException nle) {
                return notLeaderResponse(nle.getLeaderId());
            }
            logger.error("Linearizable read failed", e);
            return ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Read failed: " + e.getMessage())
                    .build();
        } catch (TimeoutException e) {
            return ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Read timed out")
                    .build();
        }
    }

    private ClientResponse handlePut(ClientRequest request) {
        byte[] key = request.getKey().toByteArray();
        byte[] value = request.getValue().toByteArray();

        try {
            Command cmd = Command.put(key, value);
            raftNode.submitCommand(cmd).get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            return ClientResponse.newBuilder()
                    .setSuccess(true)
                    .build();

        } catch (TimeoutException e) {
            return ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Request timed out")
                    .build();
        } catch (Exception e) {
            // Check if we lost leadership
            if (!raftNode.isLeader()) {
                return notLeaderResponse();
            }
            return ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Failed to execute: " + e.getMessage())
                    .build();
        }
    }

    private ClientResponse handleDelete(ClientRequest request) {
        byte[] key = request.getKey().toByteArray();

        try {
            Command cmd = Command.delete(key);
            raftNode.submitCommand(cmd).get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            return ClientResponse.newBuilder()
                    .setSuccess(true)
                    .build();

        } catch (TimeoutException e) {
            return ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Request timed out")
                    .build();
        } catch (Exception e) {
            // Check if we lost leadership
            if (!raftNode.isLeader()) {
                return notLeaderResponse();
            }
            return ClientResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Failed to execute: " + e.getMessage())
                    .build();
        }
    }

    private ClientResponse notLeaderResponse() {
        return notLeaderResponse(raftNode.getLeaderId());
    }

    private ClientResponse notLeaderResponse(NodeId leaderId) {
        ClientResponse.Builder builder = ClientResponse.newBuilder()
                .setSuccess(false)
                .setError("Not leader");

        // Add leader hint if known
        if (leaderId != null) {
            builder.setLeaderHint(leaderId.id());
        }

        return builder.build();
    }
}
