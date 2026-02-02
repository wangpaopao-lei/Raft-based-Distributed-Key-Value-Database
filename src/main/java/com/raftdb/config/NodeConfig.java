package com.raftdb.config;

import com.raftdb.core.NodeId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a Raft node.
 */
public class NodeConfig {

    private final NodeId nodeId;
    private final String host;
    private final int port;
    private final Map<NodeId, String> peerAddresses;
    private final String dataDir;

    private NodeConfig(Builder builder) {
        this.nodeId = builder.nodeId;
        this.host = builder.host;
        this.port = builder.port;
        this.peerAddresses = new HashMap<>(builder.peerAddresses);
        this.dataDir = builder.dataDir;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Map<NodeId, String> getPeerAddresses() {
        return peerAddresses;
    }

    public List<NodeId> getPeers() {
        return List.copyOf(peerAddresses.keySet());
    }

    public String getDataDir() {
        return dataDir;
    }

    public String getAddress() {
        return host + ":" + port;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Parse command line arguments into NodeConfig.
     *
     * Expected format:
     *   --id=node-1 --port=9001 --peers=node-2:localhost:9002,node-3:localhost:9003 --data-dir=data/node-1
     */
    public static NodeConfig fromArgs(String[] args) {
        Builder builder = builder();

        for (String arg : args) {
            if (arg.startsWith("--id=")) {
                builder.nodeId(NodeId.of(arg.substring(5)));
            } else if (arg.startsWith("--host=")) {
                builder.host(arg.substring(7));
            } else if (arg.startsWith("--port=")) {
                builder.port(Integer.parseInt(arg.substring(7)));
            } else if (arg.startsWith("--peers=")) {
                // Format: node-2:localhost:9002,node-3:localhost:9003
                String peersStr = arg.substring(8);
                if (!peersStr.isEmpty()) {
                    for (String peerSpec : peersStr.split(",")) {
                        String[] parts = peerSpec.split(":", 2);
                        if (parts.length == 2) {
                            builder.addPeer(NodeId.of(parts[0]), parts[1]);
                        }
                    }
                }
            } else if (arg.startsWith("--data-dir=")) {
                builder.dataDir(arg.substring(11));
            }
        }

        return builder.build();
    }

    @Override
    public String toString() {
        return "NodeConfig{" +
                "nodeId=" + nodeId +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", peers=" + peerAddresses.keySet() +
                ", dataDir='" + dataDir + '\'' +
                '}';
    }

    public static class Builder {
        private NodeId nodeId;
        private String host = "0.0.0.0";
        private int port = 9000;
        private final Map<NodeId, String> peerAddresses = new HashMap<>();
        private String dataDir = "data";

        public Builder nodeId(NodeId nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = NodeId.of(nodeId);
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder addPeer(NodeId peerId, String address) {
            this.peerAddresses.put(peerId, address);
            return this;
        }

        public Builder addPeer(String peerId, String address) {
            this.peerAddresses.put(NodeId.of(peerId), address);
            return this;
        }

        public Builder addPeer(String peerId, String host, int port) {
            this.peerAddresses.put(NodeId.of(peerId), host + ":" + port);
            return this;
        }

        public Builder dataDir(String dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public NodeConfig build() {
            if (nodeId == null) {
                throw new IllegalStateException("nodeId is required");
            }
            return new NodeConfig(this);
        }
    }
}
