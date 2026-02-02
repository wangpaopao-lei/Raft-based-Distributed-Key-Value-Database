package com.raftdb.integration;

import com.raftdb.core.RaftNode;
import com.raftdb.statemachine.Command;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for gRPC-based Raft cluster.
 *
 * These tests verify that the Raft implementation works correctly
 * with real network communication via gRPC.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GrpcClusterTest {

    private static final Logger logger = LoggerFactory.getLogger(GrpcClusterTest.class);

    // Use different port ranges for different tests to avoid conflicts
    private static final int BASE_PORT = 19000;

    private GrpcTestCluster cluster;

    @AfterEach
    void teardown() {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }

        // Wait a bit for ports to be released
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Three node cluster should elect a leader")
    void testThreeNodeClusterElection() throws Exception {
        cluster = new GrpcTestCluster(BASE_PORT);
        cluster.start(3);

        // Wait for leader election
        RaftNode leader = cluster.waitForLeader(Duration.ofSeconds(10));

        assertThat(leader).isNotNull();
        assertThat(leader.isLeader()).isTrue();

        // Verify only one leader
        long leaderCount = cluster.getNodes().stream()
                .filter(RaftNode::isLeader)
                .count();
        assertThat(leaderCount).isEqualTo(1);

        logger.info("Leader elected: {}", leader.getId());
    }

    @Test
    @Order(2)
    @DisplayName("Data should be replicated across nodes via gRPC")
    void testDataReplication() throws Exception {
        cluster = new GrpcTestCluster(BASE_PORT + 10);
        cluster.start(3);

        RaftNode leader = cluster.waitForLeader();

        // Write data through leader
        leader.submitCommand(Command.put("grpc-key", "grpc-value"))
                .get(5, TimeUnit.SECONDS);

        // Wait for replication
        cluster.waitForReplication("grpc-key", Duration.ofSeconds(5));

        // Verify all nodes have the data
        for (RaftNode node : cluster.getNodes()) {
            byte[] value = node.read("grpc-key".getBytes());
            assertThat(value).isNotNull();
            assertThat(new String(value)).isEqualTo("grpc-value");
        }

        logger.info("Data replicated to all nodes");
    }

    @Test
    @Order(3)
    @DisplayName("Cluster should elect new leader after leader failure")
    void testLeaderFailover() throws Exception {
        cluster = new GrpcTestCluster(BASE_PORT + 20);
        cluster.start(3);

        RaftNode oldLeader = cluster.waitForLeader();
        String oldLeaderId = oldLeader.getId().id();
        long oldTerm = oldLeader.getCurrentTerm();

        logger.info("Initial leader: {} at term {}", oldLeaderId, oldTerm);

        // Write some data first
        oldLeader.submitCommand(Command.put("before-failover", "value1"))
                .get(5, TimeUnit.SECONDS);
        cluster.waitForReplication("before-failover", Duration.ofSeconds(3));

        // Stop the leader
        cluster.stopNode(oldLeaderId);
        logger.info("Stopped leader: {}", oldLeaderId);

        // Wait for new leader election
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            RaftNode newLeader = cluster.getLeader();
            return newLeader != null && !newLeader.getId().id().equals(oldLeaderId);
        });

        RaftNode newLeader = cluster.getLeader();
        assertThat(newLeader).isNotNull();
        assertThat(newLeader.getId().id()).isNotEqualTo(oldLeaderId);
        assertThat(newLeader.getCurrentTerm()).isGreaterThan(oldTerm);

        logger.info("New leader: {} at term {}", newLeader.getId(), newLeader.getCurrentTerm());

        // Verify old data is still accessible
        assertThat(new String(newLeader.read("before-failover".getBytes()))).isEqualTo("value1");

        // Write new data with new leader
        newLeader.submitCommand(Command.put("after-failover", "value2"))
                .get(5, TimeUnit.SECONDS);

        // Verify new data
        cluster.waitForReplication("after-failover", Duration.ofSeconds(3));
    }

    @Test
    @Order(4)
    @DisplayName("Multiple writes should be replicated correctly")
    void testMultipleWrites() throws Exception {
        cluster = new GrpcTestCluster(BASE_PORT + 30);
        cluster.start(3);

        RaftNode leader = cluster.waitForLeader();

        // Write multiple key-value pairs
        int writeCount = 20;
        for (int i = 0; i < writeCount; i++) {
            leader.submitCommand(Command.put("key-" + i, "value-" + i))
                    .get(5, TimeUnit.SECONDS);
        }

        // Wait for all to be replicated
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            for (RaftNode node : cluster.getNodes()) {
                for (int i = 0; i < writeCount; i++) {
                    byte[] value = node.read(("key-" + i).getBytes());
                    assertThat(value)
                            .withFailMessage("key-%d not found on %s", i, node.getId())
                            .isNotNull();
                    assertThat(new String(value)).isEqualTo("value-" + i);
                }
            }
        });

        logger.info("All {} writes replicated", writeCount);
    }

    @Test
    @Order(5)
    @DisplayName("Node should catch up after restart")
    void testNodeRestart() throws Exception {
        cluster = new GrpcTestCluster(BASE_PORT + 40);
        cluster.start(3, true);  // With persistence

        RaftNode leader = cluster.waitForLeader();

        // Write initial data
        leader.submitCommand(Command.put("restart-test", "initial"))
                .get(5, TimeUnit.SECONDS);
        cluster.waitForReplication("restart-test", Duration.ofSeconds(3));

        // Pick a follower to restart
        List<RaftNode> followers = cluster.getFollowers();
        assertThat(followers).isNotEmpty();

        RaftNode follower = followers.get(0);
        String followerId = follower.getId().id();
        int followerPort = BASE_PORT + 40 + Integer.parseInt(followerId.split("-")[1]);

        logger.info("Restarting follower: {} on port {}", followerId, followerPort);

        // Stop the follower
        cluster.stopNode(followerId);

        // Write more data while follower is down
        leader = cluster.waitForLeader();  // Leader might have changed
        leader.submitCommand(Command.put("during-downtime", "value"))
                .get(5, TimeUnit.SECONDS);

        // Restart the follower
        List<String> peerIds = cluster.getNodes().stream()
                .map(n -> n.getId().id())
                .filter(id -> !id.equals(followerId))
                .toList();

        RaftNode restartedNode = cluster.restartNode(
                followerId,
                followerPort,
                peerIds
        );

        // Wait for the node to catch up
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            byte[] value1 = restartedNode.read("restart-test".getBytes());
            byte[] value2 = restartedNode.read("during-downtime".getBytes());

            assertThat(value1).isNotNull();
            assertThat(new String(value1)).isEqualTo("initial");
            assertThat(value2).isNotNull();
            assertThat(new String(value2)).isEqualTo("value");
        });

        logger.info("Restarted node caught up successfully");
    }

    @Test
    @Order(6)
    @DisplayName("Five node cluster should tolerate two failures")
    void testFiveNodeClusterFaultTolerance() throws Exception {
        cluster = new GrpcTestCluster(BASE_PORT + 50);
        cluster.start(5);

        RaftNode leader = cluster.waitForLeader();

        // Write initial data
        leader.submitCommand(Command.put("five-node-test", "value"))
                .get(5, TimeUnit.SECONDS);
        cluster.waitForReplication("five-node-test", Duration.ofSeconds(5));

        // Stop two followers (cluster should still work with 3/5 nodes)
        List<RaftNode> followers = cluster.getFollowers();
        assertThat(followers.size()).isGreaterThanOrEqualTo(2);

        String follower1 = followers.get(0).getId().id();
        String follower2 = followers.get(1).getId().id();

        cluster.stopNode(follower1);
        cluster.stopNode(follower2);

        logger.info("Stopped two followers: {} and {}", follower1, follower2);

        // Cluster should still be functional
        leader = cluster.waitForLeader();
        assertThat(leader).isNotNull();

        // Write more data
        leader.submitCommand(Command.put("after-failures", "still-working"))
                .get(5, TimeUnit.SECONDS);

        // Verify on remaining nodes
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            for (RaftNode node : cluster.getNodes()) {
                byte[] value = node.read("after-failures".getBytes());
                assertThat(value).isNotNull();
                assertThat(new String(value)).isEqualTo("still-working");
            }
        });

        logger.info("Five-node cluster survived two failures");
    }

    @Test
    @Order(7)
    @DisplayName("Single node cluster should work")
    void testSingleNodeCluster() throws Exception {
        cluster = new GrpcTestCluster(BASE_PORT + 60);
        cluster.start(1);

        RaftNode leader = cluster.waitForLeader();
        assertThat(leader).isNotNull();
        assertThat(leader.isLeader()).isTrue();

        // Write and read
        leader.submitCommand(Command.put("single", "node"))
                .get(5, TimeUnit.SECONDS);

        assertThat(new String(leader.read("single".getBytes()))).isEqualTo("node");

        logger.info("Single node cluster works");
    }
}
