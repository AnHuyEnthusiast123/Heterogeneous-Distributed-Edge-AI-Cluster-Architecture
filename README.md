# Heterogeneous-Distributed-Edge-AI-Cluster-Architecture

## Abstract

This research presents the design, implementation, and evaluation of a heterogeneous distributed Edge-AI cluster architecture aimed at optimizing the performance of AI inference workloads at the edge. 

The system is built upon a Master–Worker model, where a central Edge Server acts as the orchestrator, coordinating multiple heterogeneous worker nodes based on Milk-V Duo and Luckfox Pico platforms. A hybrid **PSO–ACO–GA** scheduler is proposed to address the multi-objective task scheduling problem on heterogeneous hardware. The scheduler integrates Model Affinity, batching mechanisms, and real-time state sharing through the Gossip protocol.

The system was evaluated on both a simulated environment (Docker Compose) and a real heterogeneous hardware cluster. Experimental results demonstrate that the proposed PSO–ACO–GA scheduler achieves superior performance in terms of makespan, throughput, and fault tolerance compared to baseline algorithms, while maintaining stable operation even on small-scale clusters.

---

## System Overview

The Edge-AI Cluster is designed to process continuous AI inference tasks on heterogeneous and resource-constrained edge devices. The system was successfully deployed on real hardware as shown below:

**Figure 1: Real hardware setup of the Edge-AI Cluster**
[Real Hardware Setup]<img width="4624" height="2604" alt="Real_cluster" src="https://github.com/user-attachments/assets/71f3014e-4c8d-46f9-af65-eb3c5ca7ec90" />

*Physical deployment consisting of one Edge Server (laptop) connected to three heterogeneous worker nodes (2× Milk-V Duo and 1× Luckfox Pico) via USB hubs and Ethernet.*

---

## Hardware Architecture

The system follows a two-tier hardware architecture:

**Figure 2: Hardware Architecture**
[Hardware Architecture]<img width="1536" height="1024" alt="Hardware_Architecture" src="https://github.com/user-attachments/assets/4cef73c9-64dc-4f33-940c-a5a4c1b7568d" />

*The architecture consists of a central Orchestrator (Tier 1) and multiple heterogeneous Edge Inference nodes (Tier 2) connected through a local network. Worker nodes vary in architecture (x86, ARM, RISC-V) and AI accelerators (CPU, NPU, TPU).*

This design allows the system to exploit different hardware capabilities while maintaining a unified scheduling interface.

---

## Software Architecture

The software is designed with clear separation between the Edge Server and Worker nodes:

**Figure 3: Detailed Software Architecture**
![Software Architecture]<img width="2120" height="1480" alt="Software_Architecture" src="https://github.com/user-attachments/assets/f0370558-5b8c-4e1a-ad8f-dd63f4652237" />

*The Edge Server (Go) acts as the central coordinator, integrating Serf for discovery, MQTT for job dispatch, and the PSO–ACO–GA Scheduler. Each Worker runs a lightweight Go agent that handles task execution, model inference via NPU, and health monitoring.*

Key components include:
- **Serf + mDNS**: Cluster membership and decentralized state sharing
- **PSO–ACO–GA Scheduler**: Intelligent task assignment
- **Worker Agent**: Task execution and automatic recovery

---

## Scheduling Algorithm Design

The core of the system is the hybrid **PSO–ACO–GA** scheduler:

**Figure 4: PSO–ACO–GA Scheduling Workflow**
![Scheduling Flowchart]<img width="960" height="420" alt="graphviz" src="https://github.com/user-attachments/assets/811c44ca-45cc-4a23-ae89-81d8473d8bd0" />


*The scheduling process consists of three coordinated stages:*
- *PSO: Rapid exploration and warm-start pheromone initialization*
- *ACO: Main scheduling engine using pheromone and multi-criteria heuristic (Model Affinity, Batching, Resource, Energy, Load Balance)*
- *GA: Asynchronous parameter tuning to adapt to workload changes*

---

## Real-time Monitoring Dashboard

A web-based dashboard was developed for real-time monitoring and task management:

**Figure 5: Cluster Dashboard**
![Cluster Dashboard]<img width="974" height="965" alt="Dashboard" src="https://github.com/user-attachments/assets/8a07971e-a723-49df-9398-3e21f2c67b48" />

*The dashboard displays cluster status (Total Nodes, Queued Jobs, Memory), allows task creation (with Input Mode and Task Type selection), and shows detailed status of each Worker Node including memory usage, current task, and control actions (View Stream / Stop).*

---

## Conceptual Architecture

At a higher level, the system can be viewed as follows:

**Figure 6: High-level System Architecture**
![Conceptual Architecture]<img width="1536" height="1024" alt="Overview_Architecture" src="https://github.com/user-attachments/assets/edeee38f-0dd7-43f3-92da-49c125edd8f4" />

*The Edge Server contains two main modules: Scheduler (decides the best worker) and Orchestrator (manages task lifecycle). Tasks are assigned to heterogeneous workers, which return results and status updates.*

---

## Conclusion

This project successfully demonstrates a complete Edge-AI cluster solution that combines:
- Heterogeneous hardware deployment (real Milk-V Duo + Luckfox Pico)
- A hybrid metaheuristic scheduler (PSO–ACO–GA)
- Decentralized state management via Gossip protocol
- Practical fault tolerance and real-time monitoring

The system has been validated both in simulation and on real hardware, achieving good performance in makespan, throughput, and fault recovery even with limited resources.

---

**Technologies Used:**
- Languages: Go (Scheduler), Python (Inference Service)
- Platforms: Docker, ONNX Runtime, Serf Gossip
- Hardware: Milk-V Duo, Luckfox Pico, Intel x86

**Authors:**
- Trần An Huy (22520574)
- Trần Quang Huy (22520578)
- ThS. Trương Văn Cương  
University of Information Technology – Vietnam National University Ho Chi Minh City

## Prerequisites

- **Hardware** (Optional for Simulation):
  - Edge devices (e.g., Milk-V boards for real deployment).
  - Host machine for simulation and server.

- **Software**:
  - Docker and Docker Compose (for simulation).
  - Go 1.21+ (for building edge-server and milkv-node).
  - MQTT broker (e.g., Mosquitto, default port 1883).
  - Optional: go2rtc for RTSP playback, ffmpeg for video conversion.

- **Environment**:
  - Embedded Linux on worker nodes (for real deployment, supporting riscv64 cross-compilation).
  - Network connectivity for MQTT and HTTP (e.g., edge IP for Serf join).

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-repo/edge-cluster.git
   cd edge-cluster
   ```

2. Build the Go binaries:
   - For edge server:
     ```
     cd edge-server
     go build -o edge-server main.go
     ```
   - For worker node (cross-compile for riscv64 if needed):
     ```
     cd milkv-node
     GOARCH=riscv64 go build -o milkv-node main.go
     ```

3. Transfer binaries to worker nodes (for real deployment):
   ```
   scp milkv-node root@worker-ip:/root/
   ```

4. Place resources (binaries/models) in `RESOURCE_DIR` on the host (default `/home/qht/cluster-docker/resource`).

5. For simulation, navigate to Docker directory:
   ```
   cd cluster-docker
   ```

## Usage

### Simulation Mode (Local Testing without Hardware)

- Start the cluster:
  ```
  cd cluster-docker
  docker compose up -d
  ```
  Wait 10–20 seconds, verify with:
  ```
  curl -s http://localhost:8081/api/jobs/available
  ```

- Configure scheduler (edit docker-compose.yml or override):
  ```
  SCHEDULER_MODE=Greedy docker compose up -d
  ```
  Modes: PSO_ACO_GA, ACO_GA, Greedy, Round_Robin (default).

- Web UI: Open `http://localhost:8080`.

- Run simulation jobs:
  ```
  JOBS=1000 API_BASE=http://localhost:8081 ./tools/eval/run_1000.sh
  ```

- Auto job generator:
  ```
  docker compose --profile loadgen up -d
  ```

- Stop:
  ```
  docker compose down
  ```

### Real Deployment

- On Edge Server (Host):
  ```
  MQTT_BROKER=tcp://localhost:1883 ./edge-server
  ```
  (Configure env vars: FILE_STORAGE_DIR, RESOURCE_DIR).

- On Worker Node (Edge Device):
  ```
  EDGE_SERVER=edge-ip:8000 EDGE_API_BASE=http://edge-ip:8081 ./milkv-node
  ```
  Workers discover edge via mDNS or env, join Serf, and subscribe to MQTT for jobs.
   - Web UI (8080) with WS for cluster snapshots.
   - REST API (8081) for jobs/files/resources.
