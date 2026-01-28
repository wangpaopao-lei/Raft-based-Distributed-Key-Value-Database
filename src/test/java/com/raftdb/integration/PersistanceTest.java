package com.raftdb.integration;

import com.raftdb.core.NodeId;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.LocalTransport;
import com.raftdb.statemachine.Command;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for persistence and recovery.
 */
class PersistenceTest {

    private static final Logger logger = LoggerFactory.getLogger(PersistenceTest.class);

    private static final Path TEST_DATA_DIR = Paths.get("target/test-data");

    private List<RaftNode> nodes;
    private List<Path> dataDirs;

    @BeforeEach
    void setUp() throws IOException {
        LocalTransport.clearRegistry();
        nodes = new ArrayList<>();
        dataDirs = new ArrayList<>();

        // Clean up test data directory
        if (Files.exists(TEST_DATA_DIR)) {
            deleteDirectory(TEST_DATA_DIR);
        }
        Files.createDirectories(TEST_DATA_DIR);
    }

    @AfterEach
    void tearDown() throws IOException {
        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                // Ignore
            }
        }
        LocalTransport.clearRegistry();

        // Clean up
        if (Files.exists(TEST_DATA_DIR)) {
            deleteDirectory(TEST_DATA_DIR);
        }
    }

    @Test
    @DisplayName("Node should recover state after restart")
    void testRecoveryAfterRestart() throws Exception {
        // Create cluster with persistence
        List<NodeId> nodeIds = List.of(
                NodeId.of("node-1"),
                NodeId.of("node-2"),
                NodeId.of("node-3")
        );

        createClusterWithPersistence(nodeIds);

        // Wait for leader
        RaftNode leader = waitForLeader();
        logger.info("Leader: {}", leader.getId());

        // Write some data
        leader.submitCommand(Command.put("key1", "value1")).get(2, TimeUnit.SECONDS);
        leader.submitCommand(Command.put("key2", "value2")).get(2, TimeUnit.SECONDS);
        leader.submitCommand(Command.put("key3", "value3")).get(2, TimeUnit.SECONDS);

        // Wait for replication
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                assertThat(node.getLogManager().getLastIndex()).isEqualTo(3);
            }
        });

        // Record state before restart
        long termBefore = leader.getCurrentTerm();

        // Stop all nodes
        logger.info("Stopping all nodes...");
        for (RaftNode node : nodes) {
            node.stop();
        }
        nodes.clear();
        LocalTransport.clearRegistry();

        // Restart all nodes
        logger.info("Restarting all nodes...");
        createClusterWithPersistence(nodeIds);

        // Wait for leader election
        RaftNode newLeader = waitForLeader();
        logger.info("New leader after restart: {}", newLeader.getId());

        // Verify term is at least as high as before
        assertThat(newLeader.getCurrentTerm()).isGreaterThanOrEqualTo(termBefore);

        // Verify data recovered
        for (RaftNode node : nodes) {
            assertThat(node.getLogManager().getLastIndex()).isEqualTo(3);
            assertThat(new String(node.read("key1".getBytes()))).isEqualTo("value1");
            assertThat(new String(node.read("key2".getBytes()))).isEqualTo("value2");
            assertThat(new String(node.read("key3".getBytes()))).isEqualTo("value3");
        }

        logger.info("All nodes recovered successfully");
    }

    @Test
    @DisplayName("Single node restart should rejoin cluster")
    void testSingleNodeRestart() throws Exception {
        // Create cluster with persistence
        List<NodeId> nodeIds = List.of(
                NodeId.of("node-1"),
                NodeId.of("node-2"),
                NodeId.of("node-3")
        );

        createClusterWithPersistence(nodeIds);

        // Wait for leader
        RaftNode leader = waitForLeader();

        // Write some data
        leader.submitCommand(Command.put("before", "restart")).get(2, TimeUnit.SECONDS);

        // Wait for replication
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                assertThat(node.read("before".getBytes())).isNotNull();
            }
        });

        // Pick a follower to restart
        RaftNode follower = nodes.stream()
                .filter(n -> !n.isLeader())
                .findFirst()
                .orElseThrow();
        NodeId followerId = follower.getId();
        Path followerDataDir = dataDirs.get(nodes.indexOf(follower));

        logger.info("Restarting follower: {}", followerId);

        // Stop the follower (this automatically unregisters from LocalTransport)
        follower.stop();
        nodes.remove(follower);

        // Write more data while follower is down
        leader = waitForLeader();  // Leader might have changed
        leader.submitCommand(Command.put("during", "downtime")).get(2, TimeUnit.SECONDS);

        // Restart the follower
        List<NodeId> peers = nodeIds.stream()
                .filter(n -> !n.equals(followerId))
                .toList();
        RaftNode restartedFollower = new RaftNode(
                followerId,
                peers,
                new LocalTransport(followerId),
                followerDataDir
        );
        restartedFollower.start();
        nodes.add(restartedFollower);

        // Wait for the follower to catch up
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(new String(restartedFollower.read("before".getBytes()))).isEqualTo("restart");
            assertThat(new String(restartedFollower.read("during".getBytes()))).isEqualTo("downtime");
        });

        logger.info("Follower {} caught up after restart", followerId);
    }

    @Test
    @DisplayName("Data files should be created in data directory")
    void testDataFilesCreated() throws Exception {
        // Create single node with persistence
        NodeId nodeId = NodeId.of("node-1");
        Path dataDir = TEST_DATA_DIR.resolve("node-1");

        RaftNode node = new RaftNode(
                nodeId,
                List.of(),  // No peers
                new LocalTransport(nodeId),
                dataDir
        );
        nodes.add(node);
        dataDirs.add(dataDir);

        node.start();

        // Should have created meta file after startup
        await().atMost(1, TimeUnit.SECONDS).until(() ->
                Files.exists(dataDir.resolve("meta.dat"))
        );

        // Wait to become leader (single node cluster)
        await().atMost(2, TimeUnit.SECONDS).until(node::isLeader);

        // Write data to create log file
        node.submitCommand(Command.put("test", "data")).get(2, TimeUnit.SECONDS);

        // Verify files exist
        assertThat(Files.exists(dataDir.resolve("meta.dat"))).isTrue();
        assertThat(Files.exists(dataDir.resolve("log.dat"))).isTrue();

        logger.info("Data files created at {}", dataDir);
    }

    @Test
    @DisplayName("Term and votedFor should persist across restarts")
    void testMetaPersistence() throws Exception {
        NodeId nodeId = NodeId.of("node-1");
        Path dataDir = TEST_DATA_DIR.resolve("node-1");

        // Start node
        RaftNode node = new RaftNode(
                nodeId,
                List.of(),
                new LocalTransport(nodeId),
                dataDir
        );
        node.start();
        nodes.add(node);
        dataDirs.add(dataDir);

        // Wait for election (will increment term)
        await().atMost(2, TimeUnit.SECONDS).until(node::isLeader);
        long termAfterElection = node.getCurrentTerm();
        assertThat(termAfterElection).isGreaterThan(0);

        // Stop node
        node.stop();
        nodes.clear();
        LocalTransport.clearRegistry();

        // Restart node
        RaftNode restartedNode = new RaftNode(
                nodeId,
                List.of(),
                new LocalTransport(nodeId),
                dataDir
        );
        restartedNode.start();
        nodes.add(restartedNode);

        // Term should be at least as high as before
        assertThat(restartedNode.getCurrentTerm()).isGreaterThanOrEqualTo(termAfterElection);

        logger.info("Term persisted: {} -> {}", termAfterElection, restartedNode.getCurrentTerm());
    }

    // ==================== Helper Methods ====================

    private void createClusterWithPersistence(List<NodeId> nodeIds) {
        for (NodeId id : nodeIds) {
            List<NodeId> peers = nodeIds.stream()
                    .filter(n -> !n.equals(id))
                    .toList();

            Path dataDir = TEST_DATA_DIR.resolve(id.id());
            dataDirs.add(dataDir);

            RaftNode node = new RaftNode(id, peers, new LocalTransport(id), dataDir);
            nodes.add(node);
        }

        for (RaftNode node : nodes) {
            node.start();
        }
    }

    private RaftNode waitForLeader() {
        await().atMost(3, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> countLeaders() == 1);

        return nodes.stream()
                .filter(RaftNode::isLeader)
                .findFirst()
                .orElseThrow();
    }

    private int countLeaders() {
        return (int) nodes.stream()
                .filter(RaftNode::isLeader)
                .count();
    }

    private void deleteDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
