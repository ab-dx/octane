# Octane

A Raft-based distributed key-value store implemented in C++ using gRPC for inter-node communication.

## Components

### KVStore (`src/core/store.cpp`, `include/kvstore/store.hpp`)

In-memory key-value store with thread-safe operations using a shared mutex pattern.

- `get(key)` - Read with shared lock (concurrent readers allowed)
- `set(key, value)` - Write with exclusive lock
- `remove(key)` - Delete with exclusive lock

Each mutation is persisted to WAL before updating in-memory state.

### WriteAheadLog (`src/core/wal.cpp`, `include/kvstore/wal.hpp`)

Disk-based write-ahead log for crash recovery. Each operation is appended to a
 WAL file, allowing reconstruction of in-memory state after crash.

- `append_set(key, value)` - Log SET operations
- `append_remove(key)` - Log REMOVE operations
- `recover()` - Rebuild state from WAL on startup

### RaftNode (`src/core/raft_node.cpp`, `include/kvstore/raft_node.hpp`)

Raft consensus implementation with three roles:

- **Follower** - Responds to leader heartbeats and vote requests
- **Candidate** - Initiates election, requests votes from peers
- **Leader** - Accepts client commands, replicates log entries

**Key Raft Features**:
- Leader election with randomized election timeouts
- Log replication via AppendEntries RPC
- Term-based leader validity
- Majority quorum for commit confirmation

### Protocol (`proto/raft.proto`)

gRPC service definition for Raft inter-node communication:

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `RequestVote` | Candidate → Followers | Leader election |
| `AppendEntries` | Leader → Followers | Log replication + heartbeats |
| `SubmitCommand` | Client → Any Node | Submit KV operations |

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                         Client                                 │
└─────────────────────────────┬──────────────────────────────────┘
                              │ gRPC
                              ▼
┌───────────────────────────────────────────────────────────┐
│                     RaftNode (Leader)                     │
│  ┌───────────┐  ┌───────────┐  ┌──────────────────────┐   │
│  │  KVStore  │  │WriteAhead │  │  Raft Consensus Layer│   │
│  │ (In-Mem)  │  │   Log     │  │ - Leader Election    │   │
│  │           │  │           │  │ - Log Replication    │   │
│  └───────────┘  └───────────┘  │ - Term Management    │   │
│                                └──────────────────────┘   │
└─────────────────────────────┬─────────────────────────────┘
                              │ AppendEntries / RequestVote
                              ▼ (gRPC)
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ RaftNode     │    │ RaftNode     │    │ RaftNode     │
│ (Follower)   │    │ (Follower)   │    │ (Candidate)  │
└──────────────┘    └──────────────┘    └──────────────┘
```


## Prerequisites

- C++17 compiler
- CMake 3.14+
- gRPC 1.40+ (with C++ plugin)
- Protobuf 3.9+

## Build

```bash
mkdir -p build && cd build
cmake ..
make
```

This produces:
- `kv_server` - Raft node server
- `kv_client` - Client CLI tool

## Running a 3-Node Cluster

**Terminal 1** - Node 0 (port 50051)
```bash
./kv_server 0 50051 50052 50053 50054
```

**Terminal 2** - Node 1 (port 50052)
```bash
./kv_server 1 50052 50051 50053 50054
```

**Terminal 3** - Node 2 (port 50053)
```bash
./kv_server 2 50053 50051 50052 50054
```

**Usage** via client:
```bash
./kv_client localhost:50051 set foo bar
./kv_client localhost:50051 get foo
./kv_client localhost:50051 remove foo
```

If the client redirects to a follower, use the leader hint address:
```bash
./kv_client <leader_hint_address> set key value
```

## Design Choices

1. **gRPC for transport** - Bidirectional streaming support, strong typing via Protobuf
2. **In-memory store** - Low-latency reads, WAL for persistence
3. **Shared mutex** - Allows concurrent reads, blocks on writes
4. **Randomized election timeouts** - Prevents election livelock
5. **Log-structured recovery** - Simplifies WAL implementation, supports replay
