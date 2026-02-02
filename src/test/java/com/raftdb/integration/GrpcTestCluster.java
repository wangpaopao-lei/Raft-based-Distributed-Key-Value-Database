package com.raftdb.integration;

import com.raftdb.config.NodeConfig;
import com.raftdb.core.NodeId;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.GrpcTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Helper class for managing a test cluster with gRPC transport.
 */
public class GrpcTestCluster implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GrpcTestCluster.class);

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<GrpcTransport> transports = new ArrayList<>();
    private final List<Path> dataDirs = new ArrayList<>();
    private final int basePort;

    public GrpcTestCluster(int basePort) {
        this.basePort = basePort;
    }

    /**
     * Start a cluster with the specified number of nodes.
     */
    public void start(int nodeCount) throws IOException {
        start(nodeCount, true);
    }

    /**
     * Start a cluster with the specified number of nodes.
     *
     * @param nodeCount number of nodes
     * @param withPersistence whether to enable persistence
     */
    public void start(int nodeCount, boolean withPersistence) throws IOException {
        // Clean up test data
        Path testDataDir = Paths.get("target/grpc-test-data");
        cleanDirectory(testDataDir);

        // Generate node configs
        List<NodeConfig> configs = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++) {
            String nodeId = "node-" + i;
            int port = basePort + i;

            NodeConfig.Builder builder = NodeConfig.builder()
                    .nodeId(nodeId)
                    .host("localhost")
                    .port(port);

            // Add all other nodes as peers
            for (int j = 1; j <= nodeCount; j++) {
                if (j != i) {
                    builder.addPeer("node-" + j, "localhost:" + (basePort + j));
                }
            }

            if (withPersistence) {
                builder.dataDir(testDataDir.toString());
            }

            configs.add(builder.build());
        }

        // Start each node
        for (NodeConfig config : configs) {
            GrpcTransport transport = new GrpcTransport(
                    config.getNodeId(),
                    config.getHost(),
                    config.getPort(),
                    config.getPeerAddresses()
            );
            transports.add(transport);

            Path dataDir = withPersistence
                    ? Paths.get(config.getDataDir(), config.getNodeId().id())
                    : null;
            if (dataDir != null) {
                dataDirs.add(dataDir);
            }

            RaftNode node = new RaftNode(
                    config.getNodeId(),
                    config.getPeers(),
                    transport,
                    dataDir
            );
            node.start();
            nodes.add(node);

            logger.info("Started node {} on port {}", config.getNodeId(), config.getPort());
        }
    }

    /**
     * Wait for a leader to be elected.
     */
    public RaftNode waitForLeader() {
        return waitForLeader(Duration.ofSeconds(10));
    }

    /**
     * Wait for a leader to be elected with custom timeout.
     */
    public RaftNode waitForLeader(Duration timeout) {
        await().atMost(timeout).until(() ->
                nodes.stream().anyMatch(RaftNode::isLeader)
        );
        return getLeader();
    }

    /**
     * Get the current leader (or null if none).
     */
    public RaftNode getLeader() {
        return nodes.stream()
                .filter(RaftNode::isLeader)
                .findFirst()
                .orElse(null);
    }

    /**
     * Get all nodes.
     */
    public List<RaftNode> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    /**
     * Get a node by ID.
     */
    public RaftNode getNode(String nodeId) {
        return nodes.stream()
                .filter(n -> n.getId().id().equals(nodeId))
                .findFirst()
                .orElse(null);
    }

    /**
     * Get a node by ID.
     */
    public RaftNode getNode(NodeId nodeId) {
        return getNode(nodeId.id());
    }

    /**
     * Stop a specific node.
     */
    public void stopNode(String nodeId) {
        RaftNode node = getNode(nodeId);
        if (node != null) {
            node.stop();
            nodes.remove(node);
            logger.info("Stopped node {}", nodeId);
        }
    }

    /**
     * Stop a specific node.
     */
    public void stopNode(NodeId nodeId) {
        stopNode(nodeId.id());
    }

    /**
     * Restart a previously stopped node.
     */
    public RaftNode restartNode(String nodeId, int port, List<String> peerIds) throws IOException {
        Map<NodeId, String> peerAddresses = new HashMap<>();
        for (String peerId : peerIds) {
            // Find peer's port
            int peerIndex = Integer.parseInt(peerId.split("-")[1]);
            peerAddresses.put(NodeId.of(peerId), "localhost:" + (basePort + peerIndex));
        }

        GrpcTransport transport = new GrpcTransport(
                NodeId.of(nodeId),
                "localhost",
                port,
                peerAddresses
        );
        transports.add(transport);

        Path dataDir = Paths.get("target/grpc-test-data", nodeId);

        RaftNode node = new RaftNode(
                NodeId.of(nodeId),
                peerIds.stream().map(NodeId::of).toList(),
                transport,
                dataDir
        );
        node.start();
        nodes.add(node);

        logger.info("Restarted node {} on port {}", nodeId, port);
        return node;
    }

    /**
     * Get follower nodes.
     */
    public List<RaftNode> getFollowers() {
        return nodes.stream()
                .filter(n -> !n.isLeader())
                .toList();
    }

    /**
     * Wait for data to be replicated to all nodes.
     */
    public void waitForReplication(String key, Duration timeout) {
        byte[] keyBytes = key.getBytes();
        await().atMost(timeout).untilAsserted(() -> {
            for (RaftNode node : nodes) {
                byte[] value = node.read(keyBytes);
                if (value == null) {
                    throw new AssertionError("Key " + key + " not found on " + node.getId());
                }
            }
        });
    }

    @Override
    public void close() {
        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                logger.warn("Error stopping node {}: {}", node.getId(), e.getMessage());
            }
        }
        nodes.clear();
        transports.clear();
        dataDirs.clear();
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
