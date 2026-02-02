package com.raftdb.rpc;

import com.raftdb.core.NodeId;
import com.raftdb.rpc.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

/**
 * gRPC-based transport for real network communication.
 *
 * Each node runs a gRPC server and maintains client connections to peers.
 */
public class GrpcTransport implements RpcTransport {

    private static final Logger logger = LoggerFactory.getLogger(GrpcTransport.class);

    private static final int DEFAULT_TIMEOUT_MS = 500;

    private final NodeId selfId;
    private final String host;
    private final int port;
    private final Map<NodeId, String> peerAddresses;  // nodeId -> "host:port"

    private Server server;
    private RpcHandler handler;

    // Client stubs for each peer (lazily created and cached)
    private final Map<NodeId, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<NodeId, RaftServiceGrpc.RaftServiceFutureStub> stubs = new ConcurrentHashMap<>();

    // Executor for async operations
    private final ExecutorService executor;

    /**
     * Create a GrpcTransport.
     *
     * @param selfId this node's ID
     * @param host host to bind the server to
     * @param port port to bind the server to
     * @param peerAddresses map of peer nodeId to "host:port"
     */
    public GrpcTransport(NodeId selfId, String host, int port, Map<NodeId, String> peerAddresses) {
        this.selfId = selfId;
        this.host = host;
        this.port = port;
        this.peerAddresses = new ConcurrentHashMap<>(peerAddresses);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Create a GrpcTransport binding to all interfaces.
     */
    public GrpcTransport(NodeId selfId, int port, Map<NodeId, String> peerAddresses) {
        this(selfId, "0.0.0.0", port, peerAddresses);
    }

    @Override
    public void start(RpcHandler handler) {
        this.handler = handler;

        try {
            server = ServerBuilder.forPort(port)
                    .addService(new RaftServiceImpl())
                    .build()
                    .start();

            logger.info("gRPC server started on {}:{}", host, port);

        } catch (IOException e) {
            throw new RuntimeException("Failed to start gRPC server on port " + port, e);
        }
    }

    @Override
    public void shutdown() {
        // Shutdown all client channels
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
            }
        }
        channels.clear();
        stubs.clear();

        // Shutdown server
        if (server != null) {
            server.shutdown();
            try {
                server.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                server.shutdownNow();
            }
        }

        executor.shutdown();
        logger.info("gRPC transport shutdown for {}", selfId);
    }

    @Override
    public CompletableFuture<VoteResponse> sendVoteRequest(NodeId target, VoteRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                RaftServiceGrpc.RaftServiceFutureStub stub = getStub(target);
                VoteResponse response = stub.requestVote(request)
                        .get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                logger.debug("{} -> {} VoteRequest(term={}) -> granted={}",
                        selfId, target, request.getTerm(), response.getVoteGranted());

                return response;

            } catch (TimeoutException e) {
                throw new CompletionException("Vote request to " + target + " timed out", e);
            } catch (Exception e) {
                throw new CompletionException("Vote request to " + target + " failed", e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(NodeId target, AppendEntriesRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                RaftServiceGrpc.RaftServiceFutureStub stub = getStub(target);
                AppendEntriesResponse response = stub.appendEntries(request)
                        .get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                if (request.getEntriesCount() > 0) {
                    logger.debug("{} -> {} AppendEntries(entries={}) -> success={}",
                            selfId, target, request.getEntriesCount(), response.getSuccess());
                }

                return response;

            } catch (TimeoutException e) {
                throw new CompletionException("AppendEntries to " + target + " timed out", e);
            } catch (Exception e) {
                throw new CompletionException("AppendEntries to " + target + " failed", e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(NodeId target, InstallSnapshotRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Use longer timeout for snapshot transfer
                int timeoutMs = Math.max(DEFAULT_TIMEOUT_MS, request.getData().size() / 1000 + 5000);

                RaftServiceGrpc.RaftServiceFutureStub stub = getStub(target);
                InstallSnapshotResponse response = stub.installSnapshot(request)
                        .get(timeoutMs, TimeUnit.MILLISECONDS);

                logger.debug("{} -> {} InstallSnapshot(size={}) -> term={}",
                        selfId, target, request.getData().size(), response.getTerm());

                return response;

            } catch (TimeoutException e) {
                throw new CompletionException("InstallSnapshot to " + target + " timed out", e);
            } catch (Exception e) {
                throw new CompletionException("InstallSnapshot to " + target + " failed", e);
            }
        }, executor);
    }

    /**
     * Get or create a stub for the target node.
     */
    private RaftServiceGrpc.RaftServiceFutureStub getStub(NodeId target) {
        return stubs.computeIfAbsent(target, id -> {
            ManagedChannel channel = getChannel(id);
            return RaftServiceGrpc.newFutureStub(channel);
        });
    }

    /**
     * Get or create a channel to the target node.
     */
    private ManagedChannel getChannel(NodeId target) {
        return channels.computeIfAbsent(target, id -> {
            String address = peerAddresses.get(id);
            if (address == null) {
                throw new IllegalArgumentException("Unknown peer: " + id);
            }

            String[] parts = address.split(":");
            String peerHost = parts[0];
            int peerPort = Integer.parseInt(parts[1]);

            logger.debug("Creating channel to {} at {}:{}", id, peerHost, peerPort);

            return ManagedChannelBuilder.forAddress(peerHost, peerPort)
                    .usePlaintext()  // No TLS for now
                    .build();
        });
    }

    /**
     * Add or update a peer address.
     */
    public void updatePeerAddress(NodeId peerId, String address) {
        String oldAddress = peerAddresses.put(peerId, address);

        // If address changed, close old channel
        if (oldAddress != null && !oldAddress.equals(address)) {
            ManagedChannel oldChannel = channels.remove(peerId);
            stubs.remove(peerId);
            if (oldChannel != null) {
                oldChannel.shutdown();
            }
        }
    }

    /**
     * Get the port this transport is listening on.
     */
    public int getPort() {
        return port;
    }

    /**
     * gRPC service implementation that delegates to RpcHandler.
     */
    private class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {

        @Override
        public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
            MDC.put("nodeId", selfId.toString());
            try {
                VoteResponse response = handler.handleVoteRequest(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error handling VoteRequest", e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
            MDC.put("nodeId", selfId.toString());
            try {
                AppendEntriesResponse response = handler.handleAppendEntries(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error handling AppendEntries", e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage())
                        .asRuntimeException());
            }
        }

        @Override
        public void installSnapshot(InstallSnapshotRequest request, StreamObserver<InstallSnapshotResponse> responseObserver) {
            MDC.put("nodeId", selfId.toString());
            try {
                InstallSnapshotResponse response = handler.handleInstallSnapshot(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Error handling InstallSnapshot", e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage())
                        .asRuntimeException());
            }
        }
    }
}
