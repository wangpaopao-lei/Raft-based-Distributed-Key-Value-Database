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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for snapshot functionality.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SnapshotTest {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotTest.class);

    private List<RaftNode> nodes;
    private List<Path> dataDirs;

    @BeforeEach
    void setup() throws IOException {
        LocalTransport.clearRegistry();
        nodes = new ArrayList<>();
        dataDirs = new ArrayList<>();

        // Clean up test data directory
        Path testDataDir = Paths.get("target/test-data");
        if (Files.exists(testDataDir)) {
            Files.walkFileTree(testDataDir, new SimpleFileVisitor<>() {
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

    @AfterEach
    void teardown() {
        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                // Ignore
            }
        }
        LocalTransport.clearRegistry();
    }

    private void createClusterWithPersistence(List<NodeId> nodeIds) throws IOException {
        for (NodeId nodeId : nodeIds) {
            List<NodeId> peers = nodeIds.stream()
                    .filter(id -> !id.equals(nodeId))
                    .toList();

            Path dataDir = Paths.get("target/test-data", nodeId.id());
            dataDirs.add(dataDir);

            RaftNode node = new RaftNode(
                    nodeId,
                    peers,
                    new LocalTransport(nodeId),
                    dataDir
            );
            node.start();
            nodes.add(node);
        }
    }

    private RaftNode waitForLeader() {
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                nodes.stream().anyMatch(RaftNode::isLeader)
        );
        return nodes.stream().filter(RaftNode::isLeader).findFirst().orElseThrow();
    }

    @Test
    @Order(1)
    @DisplayName("Manual snapshot should compact log")
    void testManualSnapshot() throws Exception {
        // Create single node cluster
        List<NodeId> nodeIds = List.of(NodeId.of("node-1"));
        createClusterWithPersistence(nodeIds);

        RaftNode leader = waitForLeader();

        // Write some data
        for (int i = 0; i < 10; i++) {
            leader.submitCommand(Command.put("key" + i, "value" + i)).get(2, TimeUnit.SECONDS);
        }

        // Verify log has entries
        assertThat(leader.getLogManager().getLastIndex()).isGreaterThanOrEqualTo(10);

        // Take snapshot
        leader.triggerSnapshot();

        // Verify snapshot files exist
        Path snapshotFile = dataDirs.get(0).resolve("snapshot.dat");
        Path metaFile = dataDirs.get(0).resolve("snapshot.meta");

        assertThat(Files.exists(snapshotFile)).isTrue();
        assertThat(Files.exists(metaFile)).isTrue();

        // Verify log was compacted (should be empty or minimal)
        assertThat(leader.getLogManager().size()).isEqualTo(0);

        // Verify data is still readable
        for (int i = 0; i < 10; i++) {
            assertThat(new String(leader.read(("key" + i).getBytes()))).isEqualTo("value" + i);
        }

        logger.info("Snapshot test passed");
    }

    @Test
    @Order(2)
    @DisplayName("Node should recover from snapshot after restart")
    void testRecoveryFromSnapshot() throws Exception {
        // Create single node cluster
        NodeId nodeId = NodeId.of("node-1");
        List<NodeId> nodeIds = List.of(nodeId);
        createClusterWithPersistence(nodeIds);

        RaftNode leader = waitForLeader();

        // Write data
        for (int i = 0; i < 10; i++) {
            leader.submitCommand(Command.put("key" + i, "value" + i)).get(2, TimeUnit.SECONDS);
        }

        // Take snapshot
        leader.triggerSnapshot();

        // Write more data after snapshot
        for (int i = 10; i < 15; i++) {
            leader.submitCommand(Command.put("key" + i, "value" + i)).get(2, TimeUnit.SECONDS);
        }

        // Stop and restart
        leader.stop();
        nodes.clear();

        RaftNode newNode = new RaftNode(
                nodeId,
                List.of(),
                new LocalTransport(nodeId),
                dataDirs.get(0)
        );
        newNode.start();
        nodes.add(newNode);

        // Wait for leader
        await().atMost(3, TimeUnit.SECONDS).until(newNode::isLeader);

        // Verify all data is present (from snapshot + log)
        for (int i = 0; i < 15; i++) {
            byte[] value = newNode.read(("key" + i).getBytes());
            assertThat(value).isNotNull();
            assertThat(new String(value)).isEqualTo("value" + i);
        }

        logger.info("Recovery from snapshot test passed");
    }

    @Test
    @Order(3)
    @DisplayName("Follower should catch up via snapshot when too far behind")
    void testSnapshotReplication() throws Exception {
        // Create 3-node cluster
        List<NodeId> nodeIds = List.of(
                NodeId.of("node-1"),
                NodeId.of("node-2"),
                NodeId.of("node-3")
        );
        createClusterWithPersistence(nodeIds);

        RaftNode leader = waitForLeader();

        // Write initial data
        for (int i = 0; i < 10; i++) {
            leader.submitCommand(Command.put("key" + i, "value" + i)).get(2, TimeUnit.SECONDS);
        }

        // Wait for replication
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                assertThat(node.read("key9".getBytes())).isNotNull();
            }
        });

        // Stop a follower
        RaftNode follower = nodes.stream()
                .filter(n -> !n.isLeader())
                .findFirst()
                .orElseThrow();
        NodeId followerId = follower.getId();
        Path followerDataDir = dataDirs.get(nodes.indexOf(follower));

        follower.stop();
        nodes.remove(follower);

        // Delete follower's data to simulate a new/far-behind node
        if (Files.exists(followerDataDir)) {
            Files.walkFileTree(followerDataDir, new SimpleFileVisitor<>() {
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

        // Write more data and take snapshot on leader
        leader = waitForLeader();
        for (int i = 10; i < 20; i++) {
            leader.submitCommand(Command.put("key" + i, "value" + i)).get(2, TimeUnit.SECONDS);
        }
        leader.triggerSnapshot();

        // Restart the follower (now empty, needs snapshot)
        List<NodeId> peers = nodeIds.stream()
                .filter(id -> !id.equals(followerId))
                .toList();

        RaftNode newFollower = new RaftNode(
                followerId,
                peers,
                new LocalTransport(followerId),
                followerDataDir
        );
        newFollower.start();
        nodes.add(newFollower);

        // Wait for follower to catch up via snapshot
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            // Verify all data is present on the restarted follower
            for (int i = 0; i < 20; i++) {
                byte[] value = newFollower.read(("key" + i).getBytes());
                assertThat(value).withFailMessage("key%d should exist", i).isNotNull();
                assertThat(new String(value)).isEqualTo("value" + i);
            }
        });

        logger.info("Snapshot replication test passed");
    }

    @Test
    @Order(4)
    @DisplayName("State machine snapshot should be serializable and deserializable")
    void testStateMachineSnapshot() throws Exception {
        // Create single node
        List<NodeId> nodeIds = List.of(NodeId.of("node-1"));
        createClusterWithPersistence(nodeIds);

        RaftNode leader = waitForLeader();

        // Write various data
        leader.submitCommand(Command.put("string", "hello")).get(2, TimeUnit.SECONDS);
        leader.submitCommand(Command.put("number", "12345")).get(2, TimeUnit.SECONDS);
        leader.submitCommand(Command.put("special", "特殊字符")).get(2, TimeUnit.SECONDS);
        leader.submitCommand(Command.put("empty", "")).get(2, TimeUnit.SECONDS);

        // Delete one key
        leader.submitCommand(Command.delete("empty")).get(2, TimeUnit.SECONDS);

        // Take snapshot
        leader.triggerSnapshot();

        // Stop and restart
        leader.stop();
        nodes.clear();

        RaftNode newNode = new RaftNode(
                NodeId.of("node-1"),
                List.of(),
                new LocalTransport(NodeId.of("node-1")),
                dataDirs.get(0)
        );
        newNode.start();
        nodes.add(newNode);

        await().atMost(3, TimeUnit.SECONDS).until(newNode::isLeader);

        // Verify data
        assertThat(new String(newNode.read("string".getBytes()))).isEqualTo("hello");
        assertThat(new String(newNode.read("number".getBytes()))).isEqualTo("12345");
        assertThat(new String(newNode.read("special".getBytes()))).isEqualTo("特殊字符");
        assertThat(newNode.read("empty".getBytes())).isNull(); // Was deleted

        logger.info("State machine snapshot test passed");
    }
}
