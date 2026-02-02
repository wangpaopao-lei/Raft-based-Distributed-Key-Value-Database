package com.raftdb.server;

import com.raftdb.core.NodeId;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.proto.*;
import com.raftdb.statemachine.Command;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * gRPC service implementation for KV operations.
 *
 * Handles client requests by delegating to the Raft node.
 * If not leader, returns a redirect hint.
 */
public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(KVServiceImpl.class);

    private static final long DEFAULT_TIMEOUT_SECONDS = 5;

    private final RaftNode raftNode;

    public KVServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

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
        ClientResponse.Builder builder = ClientResponse.newBuilder()
                .setSuccess(false)
                .setError("Not leader");

        // Add leader hint if known
        NodeId leaderId = raftNode.getLeaderId();
        if (leaderId != null) {
            builder.setLeaderHint(leaderId.id());
        }

        return builder.build();
    }
}
