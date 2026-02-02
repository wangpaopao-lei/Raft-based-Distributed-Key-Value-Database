package com.raftdb.server;

import com.raftdb.config.NodeConfig;
import com.raftdb.core.RaftNode;
import com.raftdb.rpc.GrpcTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Raft server main class.
 *
 * Starts a single Raft node with gRPC transport.
 *
 * Usage:
 *   java -jar raft-kv.jar --id=node-1 --port=9001 \
 *       --peers=node-2:localhost:9002,node-3:localhost:9003 \
 *       --data-dir=data/node-1
 */
public class RaftServer {

    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);

    private final NodeConfig config;
    private RaftNode raftNode;
    private GrpcTransport transport;

    public RaftServer(NodeConfig config) {
        this.config = config;
    }

    /**
     * Start the Raft server.
     */
    public void start() {
        logger.info("Starting Raft server with config: {}", config);

        // Create gRPC transport
        transport = new GrpcTransport(
                config.getNodeId(),
                config.getHost(),
                config.getPort(),
                config.getPeerAddresses()
        );

        // Create data directory path
        Path dataDir = Paths.get(config.getDataDir(), config.getNodeId().id());

        // Create and start Raft node
        raftNode = new RaftNode(
                config.getNodeId(),
                config.getPeers(),
                transport,
                dataDir
        );

        raftNode.start();

        logger.info("Raft server started: {} listening on port {}",
                config.getNodeId(), config.getPort());
    }

    /**
     * Stop the Raft server.
     */
    public void stop() {
        logger.info("Stopping Raft server: {}", config.getNodeId());

        if (raftNode != null) {
            raftNode.stop();
        }

        logger.info("Raft server stopped: {}", config.getNodeId());
    }

    /**
     * Get the underlying Raft node.
     */
    public RaftNode getRaftNode() {
        return raftNode;
    }

    /**
     * Get the configuration.
     */
    public NodeConfig getConfig() {
        return config;
    }

    /**
     * Check if this node is the leader.
     */
    public boolean isLeader() {
        return raftNode != null && raftNode.isLeader();
    }

    /**
     * Block until shutdown signal.
     */
    public void awaitTermination() throws InterruptedException {
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            stop();
        }));

        // Block forever (until shutdown hook is triggered)
        synchronized (this) {
            while (raftNode != null) {
                wait(1000);
            }
        }
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        try {
            NodeConfig config = NodeConfig.fromArgs(args);
            RaftServer server = new RaftServer(config);

            server.start();
            server.awaitTermination();

        } catch (Exception e) {
            logger.error("Failed to start Raft server", e);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar raft-kv.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --id=<node-id>       Node identifier (required)");
        System.out.println("  --host=<host>        Host to bind to (default: 0.0.0.0)");
        System.out.println("  --port=<port>        Port to listen on (default: 9000)");
        System.out.println("  --peers=<peers>      Comma-separated list of peers");
        System.out.println("                       Format: node-id:host:port,node-id:host:port");
        System.out.println("  --data-dir=<dir>     Data directory (default: data)");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java -jar raft-kv.jar --id=node-1 --port=9001 \\");
        System.out.println("      --peers=node-2:localhost:9002,node-3:localhost:9003 \\");
        System.out.println("      --data-dir=data");
    }
}
