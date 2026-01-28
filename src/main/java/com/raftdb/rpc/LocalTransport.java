package com.raftdb.rpc;

import com.raftdb.core.NodeId;
import com.raftdb.rpc.proto.AppendEntriesRequest;
import com.raftdb.rpc.proto.AppendEntriesResponse;
import com.raftdb.rpc.proto.VoteRequest;
import com.raftdb.rpc.proto.VoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * In-memory transport for local testing.
 * All nodes share a static registry to find each other.
 */
public class LocalTransport implements RpcTransport {

    private static final Logger logger = LoggerFactory.getLogger(LocalTransport.class);

    // Shared registry of all nodes in the local cluster
    private static final Map<NodeId, RpcHandler> REGISTRY = new ConcurrentHashMap<>();

    private final NodeId selfId;
    private final ExecutorService executor;
    private final long networkDelayMs;  // Simulated network delay

    public LocalTransport(NodeId selfId) {
        this(selfId, 0);
    }

    public LocalTransport(NodeId selfId, long networkDelayMs) {
        this.selfId = selfId;
        this.networkDelayMs = networkDelayMs;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void start(RpcHandler handler) {
        REGISTRY.put(selfId, handler);
        logger.info("LocalTransport started for node {}", selfId);
    }

    @Override
    public void shutdown() {
        REGISTRY.remove(selfId);
        executor.shutdown();
        logger.info("LocalTransport shutdown for node {}", selfId);
    }

    @Override
    public CompletableFuture<VoteResponse> sendVoteRequest(NodeId target, VoteRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            simulateNetworkDelay();

            RpcHandler handler = REGISTRY.get(target);
            if (handler == null) {
                throw new RuntimeException("Node not found: " + target);
            }

            logger.debug("{} -> {} VoteRequest(term={})", selfId, target, request.getTerm());
            VoteResponse response = handler.handleVoteRequest(request);
            logger.debug("{} <- {} VoteResponse(granted={})", selfId, target, response.getVoteGranted());

            return response;
        }, executor);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(NodeId target, AppendEntriesRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            simulateNetworkDelay();

            RpcHandler handler = REGISTRY.get(target);
            if (handler == null) {
                throw new RuntimeException("Node not found: " + target);
            }

            boolean isHeartbeat = request.getEntriesCount() == 0;
            if (isHeartbeat) {
                logger.trace("{} -> {} Heartbeat(term={})", selfId, target, request.getTerm());
            } else {
                logger.debug("{} -> {} AppendEntries(term={}, entries={})",
                        selfId, target, request.getTerm(), request.getEntriesCount());
            }

            AppendEntriesResponse response = handler.handleAppendEntries(request);

            return response;
        }, executor);
    }

    private void simulateNetworkDelay() {
        if (networkDelayMs > 0) {
            try {
                Thread.sleep(networkDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Clear the registry. Useful for test cleanup.
     */
    public static void clearRegistry() {
        REGISTRY.clear();
    }

    /**
     * Check if a node is registered.
     */
    public static boolean isRegistered(NodeId nodeId) {
        return REGISTRY.containsKey(nodeId);
    }
}
