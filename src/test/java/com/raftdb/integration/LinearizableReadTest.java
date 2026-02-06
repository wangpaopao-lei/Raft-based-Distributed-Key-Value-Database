package com.raftdb.integration;

import com.raftdb.core.NodeId;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.LocalTransport;
import com.raftdb.statemachine.Command;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Phase 7: Linearizable Reads (ReadIndex Protocol).
 *
 * <p>These tests verify that linearizable reads return data that
 * reflects all committed writes before the read began.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LinearizableReadTest {

    private static final Logger logger = LoggerFactory.getLogger(LinearizableReadTest.class);

    private List<RaftNode> nodes;
    private List<Path> dataDirs;

    @BeforeEach
    void setUp() throws Exception {
        LocalTransport.clearRegistry();
        nodes = new ArrayList<>();
        dataDirs = new ArrayList<>();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                // Ignore
            }
        }
        LocalTransport.clearRegistry();

        // Clean up data directories
        for (Path dir : dataDirs) {
            deleteDirectory(dir);
        }
    }

    private void deleteDirectory(Path dir) throws Exception {
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (Exception e) {
                            // Ignore
                        }
                    });
        }
    }

    private List<RaftNode> createCluster(int size) throws Exception {
        List<NodeId> allNodes = new ArrayList<>();
        for (int i = 1; i <= size; i++) {
            allNodes.add(NodeId.of("node-" + i));
        }

        List<RaftNode> cluster = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            NodeId nodeId = allNodes.get(i);
            List<NodeId> peers = new ArrayList<>(allNodes);
            peers.remove(nodeId);

            // Create data directory
            Path dataDir = Files.createTempDirectory("raft-test-" + nodeId.id());
            dataDirs.add(dataDir);

            LocalTransport transport = new LocalTransport(nodeId);
            RaftNode node = new RaftNode(nodeId, peers, transport, dataDir);
            cluster.add(node);
            nodes.add(node);
        }

        // Initialize and start all nodes
        for (RaftNode node : cluster) {
            node.initialize();
            node.start();
        }

        return cluster;
    }

    private RaftNode waitForLeader(List<RaftNode> cluster, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            for (RaftNode node : cluster) {
                if (node.isLeader()) {
                    return node;
                }
            }
            Thread.sleep(50);
        }

        throw new TimeoutException("No leader elected within " + timeoutMs + "ms");
    }

    @Test
    @Order(1)
    @DisplayName("Linearizable read returns committed value")
    void testLinearizableReadReturnsCommittedValue() throws Exception {
        List<RaftNode> cluster = createCluster(3);
        RaftNode leader = waitForLeader(cluster, 5000);

        // Write a value
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();

        Command putCmd = Command.put(key, value);
        leader.submitCommand(putCmd).get(5, TimeUnit.SECONDS);

        // Wait for replication
        Thread.sleep(200);

        // Linearizable read should return the value
        byte[] result = leader.linearizableRead(key).get(5, TimeUnit.SECONDS);

        assertNotNull(result, "Linearizable read should return committed value");
        assertArrayEquals(value, result, "Value should match what was written");

        logger.info("✓ Linearizable read returned committed value");
    }

    @Test
    @Order(2)
    @DisplayName("Linearizable read on non-leader fails")
    void testLinearizableReadOnNonLeaderFails() throws Exception {
        List<RaftNode> cluster = createCluster(3);
        RaftNode leader = waitForLeader(cluster, 5000);

        // Find a follower
        RaftNode follower = cluster.stream()
                .filter(n -> !n.isLeader())
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No follower found"));

        // Linearizable read on follower should fail
        byte[] key = "test-key".getBytes();

        ExecutionException ex = assertThrows(ExecutionException.class, () -> {
            follower.linearizableRead(key).get(5, TimeUnit.SECONDS);
        });

        assertTrue(ex.getCause().getMessage().contains("Not leader") ||
                        ex.getCause().getClass().getSimpleName().contains("NotLeader"),
                "Should fail with NotLeaderException");

        logger.info("✓ Linearizable read on follower correctly rejected");
    }

    @Test
    @Order(3)
    @DisplayName("Linearizable read sees latest write")
    void testLinearizableReadSeesLatestWrite() throws Exception {
        List<RaftNode> cluster = createCluster(3);
        RaftNode leader = waitForLeader(cluster, 5000);

        byte[] key = "counter".getBytes();

        // Write multiple values
        for (int i = 1; i <= 5; i++) {
            byte[] value = ("value-" + i).getBytes();
            Command putCmd = Command.put(key, value);
            leader.submitCommand(putCmd).get(5, TimeUnit.SECONDS);
        }

        // Linearizable read should return the latest value
        byte[] result = leader.linearizableRead(key).get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertEquals("value-5", new String(result), "Should see the latest written value");

        logger.info("✓ Linearizable read sees latest write");
    }

    @Test
    @Order(4)
    @DisplayName("Linearizable read for non-existent key returns null")
    void testLinearizableReadNonExistentKey() throws Exception {
        List<RaftNode> cluster = createCluster(3);
        RaftNode leader = waitForLeader(cluster, 5000);

        // Wait for cluster to stabilize
        Thread.sleep(500);

        byte[] key = "non-existent-key".getBytes();
        byte[] result = leader.linearizableRead(key).get(5, TimeUnit.SECONDS);

        assertNull(result, "Should return null for non-existent key");

        logger.info("✓ Linearizable read for non-existent key returns null");
    }

    @Test
    @Order(5)
    @DisplayName("Linearizable read confirms leadership")
    void testLinearizableReadConfirmsLeadership() throws Exception {
        List<RaftNode> cluster = createCluster(3);
        RaftNode leader = waitForLeader(cluster, 5000);

        // Write a value first
        byte[] key = "leadership-test".getBytes();
        byte[] value = "confirmed".getBytes();
        Command putCmd = Command.put(key, value);
        leader.submitCommand(putCmd).get(5, TimeUnit.SECONDS);

        // Allow replication
        Thread.sleep(200);

        // Multiple linearizable reads should all succeed
        for (int i = 0; i < 3; i++) {
            byte[] result = leader.linearizableRead(key).get(5, TimeUnit.SECONDS);
            assertNotNull(result);
            assertArrayEquals(value, result);
        }

        logger.info("✓ Multiple linearizable reads succeeded, leadership confirmed");
    }

    @Test
    @Order(6)
    @DisplayName("Single node cluster linearizable read")
    void testSingleNodeLinearizableRead() throws Exception {
        // Single node cluster
        List<NodeId> allNodes = List.of(NodeId.of("node-1"));
        NodeId nodeId = allNodes.get(0);

        Path dataDir = Files.createTempDirectory("raft-test-" + nodeId.id());
        dataDirs.add(dataDir);

        LocalTransport transport = new LocalTransport(nodeId);
        RaftNode node = new RaftNode(nodeId, List.of(), transport, dataDir);
        nodes.add(node);

        node.initialize();
        node.start();

        // Wait to become leader
        Thread.sleep(1000);
        assertTrue(node.isLeader(), "Single node should become leader");

        // Write and read
        byte[] key = "single-node-key".getBytes();
        byte[] value = "single-node-value".getBytes();

        Command putCmd = Command.put(key, value);
        node.submitCommand(putCmd).get(5, TimeUnit.SECONDS);

        // Linearizable read
        byte[] result = node.linearizableRead(key).get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertArrayEquals(value, result);

        logger.info("✓ Single node linearizable read succeeded");
    }

    @Test
    @Order(7)
    @DisplayName("Concurrent writes and linearizable reads")
    void testConcurrentWritesAndReads() throws Exception {
        List<RaftNode> cluster = createCluster(3);
        RaftNode leader = waitForLeader(cluster, 5000);

        byte[] key = "concurrent-key".getBytes();
        int numOperations = 10;

        // Initial write
        Command initial = Command.put(key, "0".getBytes());
        leader.submitCommand(initial).get(5, TimeUnit.SECONDS);
        Thread.sleep(200);

        // Run concurrent writes and reads
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();

        // Writers
        for (int i = 1; i <= numOperations; i++) {
            int val = i;
            futures.add(executor.submit(() -> {
                try {
                    Command put = Command.put(key, String.valueOf(val).getBytes());
                    leader.submitCommand(put).get(5, TimeUnit.SECONDS);
                    Thread.sleep(50);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Readers - linearizable reads should always see a consistent state
        for (int i = 0; i < numOperations; i++) {
            futures.add(executor.submit(() -> {
                try {
                    Thread.sleep(20);
                    byte[] result = leader.linearizableRead(key).get(5, TimeUnit.SECONDS);
                    assertNotNull(result, "Linearizable read should return a value");
                    int val = Integer.parseInt(new String(result));
                    assertTrue(val >= 0 && val <= numOperations,
                            "Value should be within valid range");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Wait for all operations
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }

        executor.shutdown();

        // Final read should see the last value
        byte[] finalResult = leader.linearizableRead(key).get(5, TimeUnit.SECONDS);
        assertNotNull(finalResult);

        logger.info("✓ Concurrent writes and linearizable reads completed successfully");
    }

    @Test
    @Order(8)
    @DisplayName("Stale read vs linearizable read comparison")
    void testStaleVsLinearizableRead() throws Exception {
        List<RaftNode> cluster = createCluster(3);
        RaftNode leader = waitForLeader(cluster, 5000);

        byte[] key = "compare-key".getBytes();
        byte[] value = "latest-value".getBytes();

        // Write a value
        Command putCmd = Command.put(key, value);
        leader.submitCommand(putCmd).get(5, TimeUnit.SECONDS);

        // Wait for replication
        Thread.sleep(200);

        // Both reads should return the same value in stable conditions
        byte[] staleResult = leader.read(key);  // Stale read (fast)
        byte[] linearResult = leader.linearizableRead(key).get(5, TimeUnit.SECONDS);  // Linearizable (confirmed)

        assertArrayEquals(value, staleResult, "Stale read should return value");
        assertArrayEquals(value, linearResult, "Linearizable read should return value");

        logger.info("✓ Both stale and linearizable reads returned correct value");
    }
}
