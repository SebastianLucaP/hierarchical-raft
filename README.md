# Edge-to-Cloud Hierarchical Raft Consensus

This project implements a 3-tier distributed consensus architecture based on the Raft protocol, designed specifically for Edge-to-Cloud environments. It demonstrates how to achieve fault tolerance, data consistency, and automatic recovery across multiple distinct clusters without a single global coordinator.

## Architecture

The system is separated into three independent tiers:
1. **Leaf Clusters (Tier 1):** Two independent edge clusters (`leaf-a` and `leaf-b`, 3 nodes each) that ingest external sensor data via HTTP POST, achieve local consensus, and forward it upstream.
2. **Regional Cluster (Tier 2):** An intermediate aggregation layer (3 nodes) that receives data from both leaf clusters, achieves regional consensus, and forwards it to the cloud.
3. **Cloud Cluster (Tier 3):** The final global state store (3 nodes) that acts as the terminal destination.

## Key Features
- **Smart Gateway Routing:** Cross-cluster communication uses a "leader hint" redirection mechanism to immediately find the upstream leader and minimize network overhead.
- **Watermark Replication:** Follower nodes are kept in sync with the leader's upstream sync progress to prevent duplicate data transmission after a cluster failover or reboot.
- **Atomic State Persistence:** Node state (term, log, votes) survives container restarts by using safe file swaps.
- **Zero External Dependencies:** Built entirely using Python's standard library (`xmlrpc`, `http.server`, `threading`).

## Prerequisites
- Docker and Docker Compose

## Getting Started

1. **Start the clusters:**
   ```bash
   docker compose up --build -d
   ```

2. **Test data ingestion:**
   You can send sensor data to any Leaf node's HTTP server (port 8081-8083 for Leaf A, 8091-8093 for Leaf B).
   If you send data to a follower, it will return a 307 Redirect with a hint to the leader.
   
   A helper script is provided to send test data:
   ```bash
   ./test_cluster.sh
   ```
   
   Or you can do it manually (using `-L` to automatically follow redirects):
   ```bash
   curl -X POST -L http://localhost:8081/data -H "Content-Type: application/json" -d '{"payload": "temperature=22.5"}'
   ```

3. **Monitor the logs to see data propagate to the Cloud:**
   ```bash
   docker compose logs -f
   ```

4. **Clean up and reset:**
   ```bash
   docker compose down
   rm data/state_*.json  # Clear persistent state for a fresh start
   ```
