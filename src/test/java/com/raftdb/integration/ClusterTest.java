package com.raftdb.integration;

import com.raftdb.core.NodeId;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.LocalTransport;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Raft cluster.
 */
class ClusterTest {

    private static final Logger logger = LoggerFactory.getLogger(ClusterTest.class);

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
    @DisplayName("3-node cluster should elect a leader")
    void testLeaderElection() {
        // Create 3-node cluster
        List<NodeId> nodeIds = List.of(
                NodeId.of("node-1"),
                NodeId.of("node-2"),
                NodeId.of("node-3")
        );

        for (NodeId id : nodeIds) {
            List<NodeId> peers = nodeIds.stream()
                    .filter(n -> !n.equals(id))
                    .toList();

            RaftNode node = new RaftNode(id, peers, new LocalTransport(id));
            nodes.add(node);
        }

        // Start all nodes
        for (RaftNode node : nodes) {
            node.start();
        }

        // Wait for leader election
        await().atMost(2, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> countLeaders() == 1);

        // Verify exactly one leader
        RaftNode leader = findLeader();
        assertThat(leader).isNotNull();
        logger.info("Leader elected: {} (term {})", leader.getId(), leader.getCurrentTerm());

        // Verify all nodes agree on the leader
        for (RaftNode node : nodes) {
            if (!node.isLeader()) {
                await().atMost(1, TimeUnit.SECONDS)
                        .until(() -> leader.getId().equals(node.getLeaderId()));
            }
        }

        logger.info("All nodes agree on leader: {}", leader.getId());
    }

    @Test
    @DisplayName("5-node cluster should elect a leader")
    void testLeaderElectionFiveNodes() {
        // Create 5-node cluster
        List<NodeId> nodeIds = List.of(
                NodeId.of("node-1"),
                NodeId.of("node-2"),
                NodeId.of("node-3"),
                NodeId.of("node-4"),
                NodeId.of("node-5")
        );

        for (NodeId id : nodeIds) {
            List<NodeId> peers = nodeIds.stream()
                    .filter(n -> !n.equals(id))
                    .toList();

            RaftNode node = new RaftNode(id, peers, new LocalTransport(id));
            nodes.add(node);
        }

        // Start all nodes
        for (RaftNode node : nodes) {
            node.start();
        }

        // Wait for leader election
        await().atMost(2, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> countLeaders() == 1);

        RaftNode leader = findLeader();
        assertThat(leader).isNotNull();
        logger.info("Leader elected: {} (term {})", leader.getId(), leader.getCurrentTerm());
    }

    @Test
    @DisplayName("Leader should maintain leadership with heartbeats")
    void testLeaderMaintainsLeadership() throws InterruptedException {
        // Create 3-node cluster
        List<NodeId> nodeIds = List.of(
                NodeId.of("node-1"),
                NodeId.of("node-2"),
                NodeId.of("node-3")
        );

        for (NodeId id : nodeIds) {
            List<NodeId> peers = nodeIds.stream()
                    .filter(n -> !n.equals(id))
                    .toList();

            RaftNode node = new RaftNode(id, peers, new LocalTransport(id));
            nodes.add(node);
        }

        // Start all nodes
        for (RaftNode node : nodes) {
            node.start();
        }

        // Wait for leader election
        await().atMost(2, TimeUnit.SECONDS)
                .until(() -> countLeaders() == 1);

        RaftNode leader = findLeader();
        long initialTerm = leader.getCurrentTerm();

        // Wait for a while - leader should maintain leadership
        Thread.sleep(500);

        // Should still have exactly one leader
        assertThat(countLeaders()).isEqualTo(1);

        // Leader should be the same
        assertThat(findLeader().getId()).isEqualTo(leader.getId());

        // Term should not have changed (no re-elections)
        assertThat(leader.getCurrentTerm()).isEqualTo(initialTerm);

        logger.info("Leader {} maintained leadership for 500ms", leader.getId());
    }

    @Test
    @DisplayName("Cluster should elect new leader if current leader stops")
    void testReElectionOnLeaderFailure() throws InterruptedException {
        // Create 3-node cluster
        List<NodeId> nodeIds = List.of(
                NodeId.of("node-1"),
                NodeId.of("node-2"),
                NodeId.of("node-3")
        );

        for (NodeId id : nodeIds) {
            List<NodeId> peers = nodeIds.stream()
                    .filter(n -> !n.equals(id))
                    .toList();

            RaftNode node = new RaftNode(id, peers, new LocalTransport(id));
            nodes.add(node);
        }

        // Start all nodes
        for (RaftNode node : nodes) {
            node.start();
        }

        // Wait for initial leader election
        await().atMost(2, TimeUnit.SECONDS)
                .until(() -> countLeaders() == 1);

        RaftNode oldLeader = findLeader();
        NodeId oldLeaderId = oldLeader.getId();
        long oldTerm = oldLeader.getCurrentTerm();
        logger.info("Initial leader: {} (term {})", oldLeaderId, oldTerm);

        // Stop the leader
        oldLeader.stop();
        nodes.remove(oldLeader);

        // Wait for new leader election
        await().atMost(2, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> countLeaders() == 1);

        RaftNode newLeader = findLeader();
        logger.info("New leader: {} (term {})", newLeader.getId(), newLeader.getCurrentTerm());

        // New leader should be different
        assertThat(newLeader.getId()).isNotEqualTo(oldLeaderId);

        // New term should be higher
        assertThat(newLeader.getCurrentTerm()).isGreaterThan(oldTerm);
    }

    // ==================== Helper Methods ====================

    private int countLeaders() {
        return (int) nodes.stream()
                .filter(RaftNode::isLeader)
                .count();
    }

    private RaftNode findLeader() {
        return nodes.stream()
                .filter(RaftNode::isLeader)
                .findFirst()
                .orElse(null);
    }
}
