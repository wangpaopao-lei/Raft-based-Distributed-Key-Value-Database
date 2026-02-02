# Raft-KV

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)]()
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A distributed key-value database built from scratch, implementing the [Raft consensus algorithm](https://raft.github.io/) in Java.

## ✨ Features

- **Leader Election** - Randomized election timeout, split vote handling
- **Log Replication** - Consistency check, conflict resolution, fast rollback optimization
- **Persistence** - Durable storage for term, votedFor, and log entries with crash recovery
- **Snapshot** - Log compaction with state machine snapshots and snapshot-based recovery
- **Strong Consistency** - Linearizable writes via Raft consensus

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           RaftNode                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   State     │  │    Log      │  │      StateMachine       │  │
│  │  (term,     │  │  (entries,  │  │  ┌─────────────────┐    │  │
│  │   role,     │  │   index)    │  │  │ SkipListKVStore │    │  │
│  │   votedFor) │  │             │  │  └─────────────────┘    │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│         │                │                      │               │
│  ┌──────┴────────────────┴──────────────────────┴────────────┐  │
│  │                   PersistenceManager                      │  │
│  │              (meta.dat, log.dat, snapshot.*)              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                  │
│  ┌───────────────────────────┴───────────────────────────────┐  │
│  │  ElectionManager  │  ReplicationManager  │ SnapshotManager│  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                  │
│  ┌───────────────────────────┴───────────────────────────────┐  │
│  │                      RpcTransport                         │  │
│  │              (LocalTransport / GrpcTransport)             │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
src/main/java/com/raftdb/
├── core/                 # Core components
│   ├── RaftNode.java     # Main coordinator, assembles all components
│   ├── RaftState.java    # Node state (term, role, votedFor, commitIndex)
│   ├── NodeId.java       # Node identifier
│   └── Role.java         # FOLLOWER, CANDIDATE, LEADER
│
├── election/             # Leader election
│   └── ElectionManager.java  # Election timeout, vote request/response
│
├── log/                  # Log management
│   ├── LogManager.java   # Log storage, append, truncate, compaction
│   └── LogEntry.java     # Single log entry (index, term, command)
│
├── replication/          # Log replication (Leader only)
│   └── ReplicationManager.java  # Heartbeat, AppendEntries, commit advancement
│
├── persistence/          # Durable storage
│   └── PersistenceManager.java  # Save/load term, votedFor, log entries
│
├── snapshot/             # Log compaction
│   └── SnapshotManager.java  # Snapshot creation, storage, recovery
│
├── statemachine/         # Application state
│   ├── StateMachine.java     # Interface for state machine
│   ├── SkipListKVStore.java  # KV store using ConcurrentSkipListMap
│   └── Command.java          # PUT, GET, DELETE operations
│
└── rpc/                  # Network layer
    ├── RpcTransport.java     # Transport interface
    ├── RpcHandler.java       # Request handler interface
    └── LocalTransport.java   # In-memory transport for testing
```

## 🚀 Quick Start

### Prerequisites

- Java 21+
- Maven 3.8+

### Build

```bash
git clone https://github.com/wangpaopao-lei/raft-kv.git
cd raft-kv
mvn clean compile
```

### Run Tests

```bash
mvn test
```

### Example Usage

```java
// Create a 3-node cluster
List<NodeId> nodeIds = List.of(
    NodeId.of("node-1"),
    NodeId.of("node-2"),
    NodeId.of("node-3")
);

// Create nodes with persistence
List<RaftNode> nodes = new ArrayList<>();
for (NodeId id : nodeIds) {
    List<NodeId> peers = nodeIds.stream()
        .filter(n -> !n.equals(id))
        .toList();
    
    RaftNode node = new RaftNode(
        id, 
        peers, 
        new LocalTransport(id),
        Paths.get("data", id.id())  // persistence directory
    );
    node.start();
    nodes.add(node);
}

// Wait for leader election
RaftNode leader = nodes.stream()
    .filter(RaftNode::isLeader)
    .findFirst()
    .orElseThrow();

// Write data (goes through Raft consensus)
leader.submitCommand(Command.put("name", "raft-kv"))
    .get(5, TimeUnit.SECONDS);

// Read data
byte[] value = leader.read("name".getBytes());
System.out.println(new String(value));  // "raft-kv"
```

## 🔬 Raft Implementation Details

### Leader Election

- **Election Timeout**: Randomized between 150-300ms to avoid split votes
- **Vote Request**: Candidate requests votes with its log info (lastLogIndex, lastLogTerm)
- **Vote Decision**: Grant vote only if candidate's log is at least as up-to-date
- **Single Node Optimization**: Immediately become leader if no peers

### Log Replication

- **AppendEntries RPC**: Leader sends log entries with consistency check info
- **Consistency Check**: Follower verifies prevLogIndex and prevLogTerm match
- **Conflict Resolution**: On mismatch, follower reports conflict info for fast rollback
- **Commit Advancement**: Leader commits when majority has replicated

### Persistence

Files stored in data directory:
```
data/node-1/
├── meta.dat       # currentTerm (8 bytes) + votedFor (string)
├── log.dat        # Append-only log entries
├── snapshot.dat   # State machine snapshot
└── snapshot.meta  # lastIncludedIndex + lastIncludedTerm
```

- **Atomic Writes**: Write to temp file + rename for crash safety
- **Fsync**: Ensure durability before responding to RPCs

### Snapshot

- **Trigger**: When log exceeds threshold (default 1000 entries)
- **Process**: Serialize StateMachine → Save to disk → Compact log
- **InstallSnapshot RPC**: Leader sends snapshot to far-behind followers

## 📊 Test Coverage

| Component | Tests |
|-----------|-------|
| Leader Election | ✅ Single leader, term increment, re-election |
| Log Replication | ✅ Basic replication, follower catch-up, conflict resolution |
| Persistence | ✅ Crash recovery, data integrity |
| Snapshot | ✅ Compaction, recovery from snapshot, snapshot replication |

Run all tests:
```bash
mvn test
```

## 🗺️ Roadmap

- [x] **Phase 1**: Leader Election
- [x] **Phase 2**: Log Replication  
- [x] **Phase 3**: Persistence & Recovery
- [x] **Phase 4**: Snapshot & Log Compaction
- [ ] **Phase 5**: gRPC Network Transport
- [ ] **Phase 6**: Client API & CLI
- [ ] **Phase 7**: Linearizable Reads

## 📚 References

- [Raft Paper](https://raft.github.io/raft.pdf) - In Search of an Understandable Consensus Algorithm
- [Raft Visualization](https://raft.github.io/) - Interactive Raft visualization
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/) - Common implementation pitfalls

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Diego Ongaro and John Ousterhout for the Raft algorithm
- The etcd and TiKV projects for implementation inspiration
