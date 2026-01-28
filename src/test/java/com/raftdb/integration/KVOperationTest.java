package com.raftdb.integration;

import com.raftdb.core.NodeId;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.LocalTransport;
import com.raftdb.statemachine.Command;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for KV operations.
 */
class KVOperationTest {

    private static final Logger logger = LoggerFactory.getLogger(KVOperationTest.class);

    private List<RaftNode> nodes;

    @BeforeEach
    void setUp() {
        LocalTransport.clearRegistry();
        nodes = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (RaftNode node : nodes) {
            node.stop();
        }
        LocalTransport.clearRegistry();
    }

    @Test
    @DisplayName("Leader should accept PUT and replicate to followers")
    void testPutReplication() throws Exception {
        // Create 3-node cluster
        createCluster(3);

        // Wait for leader election
        RaftNode leader = waitForLeader();
        logger.info("Leader: {}", leader.getId());

        // Submit PUT command
        CompletableFuture<byte[]> future = leader.submitCommand(Command.put("key1", "value1"));

        // Wait for commit
        byte[] result = future.get(2, TimeUnit.SECONDS);
        assertThat(result).isNull(); // PUT returns null

        logger.info("PUT committed");

        // Verify data is replicated to all nodes
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                byte[] value = node.read("key1".getBytes());
                assertThat(value).isNotNull();
                assertThat(new String(value)).isEqualTo("value1");
            }
        });

        logger.info("Data replicated to all nodes");
    }

    @Test
    @DisplayName("Should support GET operation")
    void testGetOperation() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader();

        // PUT then GET
        leader.submitCommand(Command.put("key2", "value2")).get(2, TimeUnit.SECONDS);

        // Read from leader
        byte[] value = leader.read("key2".getBytes());
        assertThat(new String(value)).isEqualTo("value2");

        // Read from followers (eventually consistent)
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                byte[] v = node.read("key2".getBytes());
                assertThat(v).isNotNull();
                assertThat(new String(v)).isEqualTo("value2");
            }
        });
    }

    @Test
    @DisplayName("Should support DELETE operation")
    void testDeleteOperation() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader();

        // PUT
        leader.submitCommand(Command.put("key3", "value3")).get(2, TimeUnit.SECONDS);
        assertThat(new String(leader.read("key3".getBytes()))).isEqualTo("value3");

        // DELETE
        leader.submitCommand(Command.delete("key3")).get(2, TimeUnit.SECONDS);

        // Verify deleted on all nodes
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                assertThat(node.read("key3".getBytes())).isNull();
            }
        });
    }

    @Test
    @DisplayName("Should handle multiple sequential writes")
    void testSequentialWrites() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader();

        // Write multiple keys
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            leader.submitCommand(Command.put(key, value)).get(2, TimeUnit.SECONDS);
        }

        // Verify all data replicated
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                for (int i = 0; i < 10; i++) {
                    byte[] value = node.read(("key-" + i).getBytes());
                    assertThat(value).isNotNull();
                    assertThat(new String(value)).isEqualTo("value-" + i);
                }
            }
        });

        // Verify log length
        for (RaftNode node : nodes) {
            assertThat(node.getLogManager().getLastIndex()).isEqualTo(10);
        }
    }

    @Test
    @DisplayName("Should handle concurrent writes")
    void testConcurrentWrites() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader();

        // Submit multiple commands concurrently
        List<CompletableFuture<byte[]>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            String key = "concurrent-" + i;
            String value = "value-" + i;
            futures.add(leader.submitCommand(Command.put(key, value)));
        }

        // Wait for all to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(5, TimeUnit.SECONDS);

        // Verify all data
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                for (int i = 0; i < 20; i++) {
                    byte[] value = node.read(("concurrent-" + i).getBytes());
                    assertThat(value).isNotNull();
                    assertThat(new String(value)).isEqualTo("value-" + i);
                }
            }
        });
    }

    @Test
    @DisplayName("Non-leader should reject writes")
    void testNonLeaderRejectsWrites() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader();

        // Find a follower
        RaftNode follower = nodes.stream()
                .filter(n -> !n.isLeader())
                .findFirst()
                .orElseThrow();

        // Submit to follower should fail
        CompletableFuture<byte[]> future = follower.submitCommand(Command.put("key", "value"));

        // Should complete exceptionally with NotLeaderException
        assertThat(future).isCompletedExceptionally();
    }

    @Test
    @DisplayName("New leader should continue replication after leader failure")
    void testLeaderFailover() throws Exception {
        createCluster(3);
        RaftNode leader = waitForLeader();

        // Write some data
        leader.submitCommand(Command.put("before-failover", "value1")).get(2, TimeUnit.SECONDS);

        // Wait for replication
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                assertThat(node.read("before-failover".getBytes())).isNotNull();
            }
        });

        // Kill leader
        logger.info("Killing leader: {}", leader.getId());
        leader.stop();
        nodes.remove(leader);

        // Wait for new leader
        RaftNode newLeader = waitForLeader();
        logger.info("New leader: {}", newLeader.getId());
        assertThat(newLeader.getId()).isNotEqualTo(leader.getId());

        // Write to new leader
        newLeader.submitCommand(Command.put("after-failover", "value2")).get(2, TimeUnit.SECONDS);

        // Verify both old and new data exist
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                assertThat(new String(node.read("before-failover".getBytes()))).isEqualTo("value1");
                assertThat(new String(node.read("after-failover".getBytes()))).isEqualTo("value2");
            }
        });
    }

    @Test
    @DisplayName("Should maintain consistency during network partition healing")
    void testPartitionHealing() throws Exception {
        createCluster(5);
        RaftNode leader = waitForLeader();

        // Write initial data
        leader.submitCommand(Command.put("stable-key", "initial")).get(2, TimeUnit.SECONDS);

        // Wait for full replication
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                assertThat(node.read("stable-key".getBytes())).isNotNull();
            }
        });

        // Write more data
        for (int i = 0; i < 5; i++) {
            leader.submitCommand(Command.put("key-" + i, "value-" + i)).get(2, TimeUnit.SECONDS);
        }

        // All nodes should have consistent data
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            long expectedCommit = leader.getCommitIndex();
            for (RaftNode node : nodes) {
                assertThat(node.getCommitIndex()).isEqualTo(expectedCommit);
            }
        });
    }

    // ==================== Helper Methods ====================

    private void createCluster(int size) {
        List<NodeId> nodeIds = new ArrayList<>();
        for (int i = 1; i <= size; i++) {
            nodeIds.add(NodeId.of("node-" + i));
        }

        for (NodeId id : nodeIds) {
            List<NodeId> peers = nodeIds.stream()
                    .filter(n -> !n.equals(id))
                    .toList();

            RaftNode node = new RaftNode(id, peers, new LocalTransport(id));
            nodes.add(node);
        }

        for (RaftNode node : nodes) {
            node.start();
        }
    }

    private RaftNode waitForLeader() {
        await().atMost(2, TimeUnit.SECONDS)
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
}
