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
 * Client SDK for interacting with a Raft-KV distributed key-value cluster.
 *
 * <p>This client provides a simple interface for performing key-value operations
 * against a Raft cluster. It handles the complexities of distributed systems
 * including leader discovery, automatic retries, and failover.
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li><b>Automatic Leader Discovery</b> - Automatically finds and connects to the leader</li>
 *   <li><b>Transparent Redirects</b> - Handles "not leader" responses by switching servers</li>
 *   <li><b>Retry on Failure</b> - Retries operations on transient failures</li>
 *   <li><b>Connection Pooling</b> - Reuses gRPC connections for efficiency</li>
 *   <li><b>Thread Safety</b> - Safe for concurrent use from multiple threads</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * try (RaftKVClient client = RaftKVClient.connect("localhost:9001,localhost:9002,localhost:9003")) {
 *     client.put("user:123", "Alice");
 *     String name = client.get("user:123");  // "Alice"
 *     client.delete("user:123");
 * }
 * }</pre>
 *
 * <h2>Error Handling</h2>
 * <p>Operations throw {@link RaftClientException} if they fail after all retries.
 *
 * @author raft-kv
 * @see RaftCli
 */
public class RaftKVClient implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RaftKVClient.class);

    private static final int DEFAULT_TIMEOUT_SECONDS = 10;
    private static final int MAX_RETRIES = 8;
    private static final long INITIAL_RETRY_DELAY_MS = 50;
    private static final long MAX_RETRY_DELAY_MS = 2000;

    private final List<String> serverAddresses;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, KVServiceGrpc.KVServiceBlockingStub> stubs = new ConcurrentHashMap<>();

    private volatile String leaderAddress;
    private final java.util.concurrent.atomic.AtomicInteger currentServerIndex = new java.util.concurrent.atomic.AtomicInteger(0);

    /**
     * Constructs a client with the given server addresses.
     *
     * @param serverAddresses list of server addresses in host:port format
     * @throws IllegalArgumentException if the address list is empty
     */
    private RaftKVClient(List<String> serverAddresses) {
        this.serverAddresses = new ArrayList<>(serverAddresses);
        if (serverAddresses.isEmpty()) {
            throw new IllegalArgumentException("At least one server address is required");
        }
        this.leaderAddress = serverAddresses.get(0);
    }

    /**
     * Creates a client connected to a Raft cluster.
     *
     * @param addresses comma-separated list of server addresses (e.g., "host1:9001,host2:9002")
     * @return a new client instance
     */
    public static RaftKVClient connect(String addresses) {
        String[] parts = addresses.split(",");
        return new RaftKVClient(Arrays.asList(parts));
    }

    /**
     * Creates a client connected to a Raft cluster.
     *
     * @param addresses list of server addresses in host:port format
     * @return a new client instance
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
     * Get a value by key with linearizable consistency.
     * This ensures the read reflects all committed writes.
     *
     * @param key the key
     * @return the value, or null if not found
     * @throws RaftClientException if the operation fails after retries
     */
    public String getLinearizable(String key) {
        byte[] value = getBytesLinearizable(key.getBytes());
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
     * Get a value by key with linearizable consistency.
     * This ensures the read reflects all committed writes.
     *
     * @param key the key
     * @return the value, or null if not found
     * @throws RaftClientException if the operation fails after retries
     */
    public byte[] getBytesLinearizable(byte[] key) {
        ClientRequest request = ClientRequest.newBuilder()
                .setOperation(OperationType.GET)
                .setKey(ByteString.copyFrom(key))
                .setLinearizable(true)
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
                    sleepWithBackoff(attempt);
                    continue;
                }

                // Other error
                throw new RaftClientException(response.getError());

            } catch (StatusRuntimeException e) {
                lastException = e;
                logger.debug("Request to {} failed: {}", address, e.getMessage());

                // Try next server
                leaderAddress = getNextServer();

                // Exponential backoff
                sleepWithBackoff(attempt);

            } catch (RaftClientException e) {
                throw e;
            } catch (Exception e) {
                lastException = e;
                logger.debug("Request to {} failed: {}", address, e.getMessage());

                // Exponential backoff
                sleepWithBackoff(attempt);
                leaderAddress = getNextServer();
            }
        }

        throw new RaftClientException("Failed after " + MAX_RETRIES + " retries", lastException);
    }

    /**
     * Sleep with exponential backoff.
     */
    private void sleepWithBackoff(int attempt) {
        long delay = Math.min(INITIAL_RETRY_DELAY_MS * (1L << attempt), MAX_RETRY_DELAY_MS);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
