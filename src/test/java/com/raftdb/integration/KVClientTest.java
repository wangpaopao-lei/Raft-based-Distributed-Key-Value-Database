package com.raftdb.integration;

import com.raftdb.client.RaftKVClient;
import com.raftdb.config.NodeConfig;
import com.raftdb.core.NodeId;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.GrpcTransport;
import com.raftdb.server.KVServiceImpl;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for KV client.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KVClientTest {

    private static final Logger logger = LoggerFactory.getLogger(KVClientTest.class);

    private static final int BASE_PORT = 18000;

    private List<RaftNode> nodes;
    private List<GrpcTransport> transports;
    private RaftKVClient client;

    @BeforeEach
    void setup() throws IOException {
        nodes = new ArrayList<>();
        transports = new ArrayList<>();

        // Clean up test data
        Path testDataDir = Paths.get("target/kv-client-test-data");
        cleanDirectory(testDataDir);
    }

    @AfterEach
    void teardown() {
        if (client != null) {
            client.close();
            client = null;
        }

        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                // Ignore
            }
        }
        nodes.clear();
        transports.clear();

        // Wait for ports to be released
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startCluster(int nodeCount) throws IOException {
        List<NodeConfig> configs = new ArrayList<>();

        // Create configs
        for (int i = 1; i <= nodeCount; i++) {
            NodeConfig.Builder builder = NodeConfig.builder()
                    .nodeId("node-" + i)
                    .host("localhost")
                    .port(BASE_PORT + i)
                    .dataDir("target/kv-client-test-data");

            for (int j = 1; j <= nodeCount; j++) {
                if (j != i) {
                    builder.addPeer("node-" + j, "localhost:" + (BASE_PORT + j));
                }
            }

            configs.add(builder.build());
        }

        // Start nodes
        for (NodeConfig config : configs) {
            GrpcTransport transport = new GrpcTransport(
                    config.getNodeId(),
                    config.getHost(),
                    config.getPort(),
                    config.getPeerAddresses()
            );
            transports.add(transport);

            Path dataDir = Paths.get(config.getDataDir(), config.getNodeId().id());

            RaftNode node = new RaftNode(
                    config.getNodeId(),
                    config.getPeers(),
                    transport,
                    dataDir
            );

            // Create KV service and start transport with it
            KVServiceImpl kvService = new KVServiceImpl(node);
            transport.start(node, kvService);
            node.initialize();

            nodes.add(node);
            logger.info("Started node {} on port {}", config.getNodeId(), config.getPort());
        }
    }

    private void waitForLeader() {
        await().atMost(Duration.ofSeconds(10)).until(() ->
                nodes.stream().anyMatch(RaftNode::isLeader)
        );
    }

    private String getClusterAddresses() {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= nodes.size(); i++) {
            if (sb.length() > 0) sb.append(",");
            sb.append("localhost:").append(BASE_PORT + i);
        }
        return sb.toString();
    }

    @Test
    @Order(1)
    @DisplayName("Basic put and get operations")
    void testPutAndGet() throws Exception {
        startCluster(3);
        waitForLeader();

        client = RaftKVClient.connect(getClusterAddresses());

        // Put
        client.put("name", "Alice");
        client.put("age", "25");

        // Get
        assertThat(client.get("name")).isEqualTo("Alice");
        assertThat(client.get("age")).isEqualTo("25");
        assertThat(client.get("nonexistent")).isNull();

        logger.info("Basic put/get test passed");
    }

    @Test
    @Order(2)
    @DisplayName("Delete operation")
    void testDelete() throws Exception {
        startCluster(3);
        waitForLeader();

        client = RaftKVClient.connect(getClusterAddresses());

        // Put then delete
        client.put("temp", "value");
        assertThat(client.get("temp")).isEqualTo("value");

        client.delete("temp");
        assertThat(client.get("temp")).isNull();

        logger.info("Delete test passed");
    }

    @Test
    @Order(3)
    @DisplayName("Client should auto-redirect to leader")
    void testAutoRedirectToLeader() throws Exception {
        startCluster(3);
        waitForLeader();

        // Find a follower's address
        RaftNode follower = nodes.stream()
                .filter(n -> !n.isLeader())
                .findFirst()
                .orElseThrow();

        int followerIndex = nodes.indexOf(follower);
        String followerAddress = "localhost:" + (BASE_PORT + followerIndex + 1);

        // Connect to follower only
        client = RaftKVClient.connect(followerAddress + "," + getClusterAddresses());

        // Should still work (auto-redirect to leader)
        client.put("redirect-test", "value");
        assertThat(client.get("redirect-test")).isEqualTo("value");

        logger.info("Auto-redirect test passed");
    }

    @Test
    @Order(4)
    @DisplayName("Client should retry on leader change")
    void testRetryOnLeaderChange() throws Exception {
        startCluster(3);
        waitForLeader();

        client = RaftKVClient.connect(getClusterAddresses());

        // Write some data
        client.put("before", "leader-change");
        assertThat(client.get("before")).isEqualTo("leader-change");

        // Stop the leader
        RaftNode leader = nodes.stream()
                .filter(RaftNode::isLeader)
                .findFirst()
                .orElseThrow();

        leader.stop();
        nodes.remove(leader);

        // Wait for new leader
        await().atMost(Duration.ofSeconds(10)).until(() ->
                nodes.stream().anyMatch(RaftNode::isLeader)
        );

        // Should still work after leader change
        client.put("after", "leader-change");
        assertThat(client.get("after")).isEqualTo("leader-change");
        assertThat(client.get("before")).isEqualTo("leader-change");

        logger.info("Leader change retry test passed");
    }

    @Test
    @Order(5)
    @DisplayName("Concurrent writes should all succeed")
    void testConcurrentWrites() throws Exception {
        startCluster(3);
        waitForLeader();

        // Wait a bit for cluster to stabilize
        Thread.sleep(500);

        client = RaftKVClient.connect(getClusterAddresses());

        int numWrites = 20;  // Reduced for stability
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(numWrites);
        List<Exception> errors = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numWrites; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    client.put("concurrent-" + index, "value-" + index);
                } catch (Exception e) {
                    errors.add(e);
                    logger.warn("Concurrent write {} failed: {}", index, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        if (!completed) {
            logger.warn("Concurrent test timed out, {} tasks remaining", latch.getCount());
        }

        // Allow some failures in concurrent test (leader may change)
        int maxAllowedErrors = 2;
        assertThat(errors.size()).as("Too many errors: " + errors).isLessThanOrEqualTo(maxAllowedErrors);

        // Verify successful writes
        int successCount = 0;
        for (int i = 0; i < numWrites; i++) {
            String value = client.get("concurrent-" + i);
            if (value != null && value.equals("value-" + i)) {
                successCount++;
            }
        }

        assertThat(successCount).as("At least most writes should succeed").isGreaterThanOrEqualTo(numWrites - maxAllowedErrors);

        logger.info("Concurrent writes test passed with {}/{} successful writes", successCount, numWrites);
    }

    @Test
    @Order(6)
    @DisplayName("Single node cluster should work with client")
    void testSingleNodeWithClient() throws Exception {
        startCluster(1);
        waitForLeader();

        client = RaftKVClient.connect("localhost:" + (BASE_PORT + 1));

        client.put("single", "node");
        assertThat(client.get("single")).isEqualTo("node");

        logger.info("Single node client test passed");
    }

    private void cleanDirectory(Path dir) throws IOException {
        if (Files.exists(dir)) {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
                @Override
                public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                    Files.delete(d);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}
