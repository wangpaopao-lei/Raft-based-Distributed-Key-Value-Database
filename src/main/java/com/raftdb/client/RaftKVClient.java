package com.raftdb.client;

import com.raftdb.rpc.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Client SDK for interacting with a Raft KV cluster.
 *
 * Features:
 * - Automatic leader discovery and redirect handling
 * - Retry on failures
 * - Connection pooling
 *
 * Usage:
 * <pre>
 *   try (RaftKVClient client = RaftKVClient.connect("localhost:9001,localhost:9002,localhost:9003")) {
 *       client.put("key", "value");
 *       String value = client.get("key");
 *       client.delete("key");
 *   }
 * </pre>
 */
public class RaftKVClient implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RaftKVClient.class);

    private static final int DEFAULT_TIMEOUT_SECONDS = 10;
    private static final int MAX_RETRIES = 5;

    private final List<String> serverAddresses;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, KVServiceGrpc.KVServiceBlockingStub> stubs = new ConcurrentHashMap<>();

    private volatile String leaderAddress;
    private final java.util.concurrent.atomic.AtomicInteger currentServerIndex = new java.util.concurrent.atomic.AtomicInteger(0);

    private RaftKVClient(List<String> serverAddresses) {
        this.serverAddresses = new ArrayList<>(serverAddresses);
        if (serverAddresses.isEmpty()) {
            throw new IllegalArgumentException("At least one server address is required");
        }
        this.leaderAddress = serverAddresses.get(0);
    }

    /**
     * Connect to a Raft cluster.
     *
     * @param addresses comma-separated list of server addresses (host:port)
     */
    public static RaftKVClient connect(String addresses) {
        String[] parts = addresses.split(",");
        return new RaftKVClient(Arrays.asList(parts));
    }

    /**
     * Connect to a Raft cluster.
     *
     * @param addresses list of server addresses (host:port)
     */
    public static RaftKVClient connect(List<String> addresses) {
        return new RaftKVClient(addresses);
    }

    /**
     * Put a key-value pair.
     *
     * @param key the key
     * @param value the value
     * @throws RaftClientException if the operation fails after retries
     */
    public void put(String key, String value) {
        put(key.getBytes(), value.getBytes());
    }

    /**
     * Put a key-value pair.
     *
     * @param key the key
     * @param value the value
     * @throws RaftClientException if the operation fails after retries
     */
    public void put(byte[] key, byte[] value) {
        ClientRequest request = ClientRequest.newBuilder()
                .setOperation(OperationType.PUT)
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();

        executeWithRetry(request);
    }

    /**
     * Get a value by key.
     *
     * @param key the key
     * @return the value, or null if not found
     * @throws RaftClientException if the operation fails after retries
     */
    public String get(String key) {
        byte[] value = getBytes(key.getBytes());
        return value != null ? new String(value) : null;
    }

    /**
     * Get a value by key.
     *
     * @param key the key
     * @return the value, or null if not found
     * @throws RaftClientException if the operation fails after retries
     */
    public byte[] getBytes(byte[] key) {
        ClientRequest request = ClientRequest.newBuilder()
                .setOperation(OperationType.GET)
                .setKey(ByteString.copyFrom(key))
                .build();

        ClientResponse response = executeWithRetry(request);

        if (response.getValue().isEmpty()) {
            return null;
        }
        return response.getValue().toByteArray();
    }

    /**
     * Delete a key.
     *
     * @param key the key to delete
     * @throws RaftClientException if the operation fails after retries
     */
    public void delete(String key) {
        delete(key.getBytes());
    }

    /**
     * Delete a key.
     *
     * @param key the key to delete
     * @throws RaftClientException if the operation fails after retries
     */
    public void delete(byte[] key) {
        ClientRequest request = ClientRequest.newBuilder()
                .setOperation(OperationType.DELETE)
                .setKey(ByteString.copyFrom(key))
                .build();

        executeWithRetry(request);
    }

    /**
     * Execute a request with automatic retry and leader redirect.
     */
    private ClientResponse executeWithRetry(ClientRequest request) {
        Exception lastException = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            String address = leaderAddress;

            try {
                KVServiceGrpc.KVServiceBlockingStub stub = getStub(address);
                ClientResponse response = stub
                        .withDeadlineAfter(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .execute(request);

                if (response.getSuccess()) {
                    return response;
                }

                // Handle "not leader" response
                if (response.getError().contains("Not leader")) {
                    String hint = response.getLeaderHint();
                    if (!hint.isEmpty()) {
                        // Leader hint is node ID, need to find address
                        // For now, just try next server
                        logger.debug("Not leader, got hint: {}", hint);
                    }
                    leaderAddress = getNextServer();
                    logger.debug("Switched to server: {}", leaderAddress);
                    continue;
                }

                // Other error
                throw new RaftClientException(response.getError());

            } catch (StatusRuntimeException e) {
                lastException = e;
                logger.debug("Request to {} failed: {}", address, e.getMessage());

                // Try next server
                leaderAddress = getNextServer();

            } catch (RaftClientException e) {
                throw e;
            } catch (Exception e) {
                lastException = e;
                logger.debug("Request to {} failed: {}", address, e.getMessage());
                leaderAddress = getNextServer();
            }
        }

        throw new RaftClientException("Failed after " + MAX_RETRIES + " retries", lastException);
    }

    /**
     * Get the next server address (round-robin, thread-safe).
     */
    private String getNextServer() {
        int index = currentServerIndex.updateAndGet(i -> (i + 1) % serverAddresses.size());
        return serverAddresses.get(index);
    }

    /**
     * Get or create a stub for the given address.
     */
    private KVServiceGrpc.KVServiceBlockingStub getStub(String address) {
        return stubs.computeIfAbsent(address, addr -> {
            ManagedChannel channel = getChannel(addr);
            return KVServiceGrpc.newBlockingStub(channel);
        });
    }

    /**
     * Get or create a channel for the given address.
     */
    private ManagedChannel getChannel(String address) {
        return channels.computeIfAbsent(address, addr -> {
            String[] parts = addr.trim().split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            return ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
        });
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
            }
        }
        channels.clear();
        stubs.clear();
    }

    /**
     * Exception thrown by the Raft client.
     */
    public static class RaftClientException extends RuntimeException {
        public RaftClientException(String message) {
            super(message);
        }

        public RaftClientException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
