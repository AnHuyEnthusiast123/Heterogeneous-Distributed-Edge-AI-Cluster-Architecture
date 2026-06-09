# Heterogeneous Distributed Edge-AI Cluster Architecture

**Design, Implementation and Evaluation of a Heterogeneous Distributed Edge-AI Cluster for Performance-Optimized AI Workloads**

## Overview

Modern Edge-AI applications such as intelligent surveillance, people analytics, drowsiness monitoring, and smart city systems increasingly require AI inference to be executed directly on edge devices rather than in centralized cloud infrastructures.

While edge computing reduces latency and bandwidth consumption, it introduces a new challenge:

> **How can multiple heterogeneous edge devices collaboratively process AI workloads efficiently?**

In practice, edge devices often differ significantly in computational capability, memory capacity, AI accelerators, and energy efficiency. Some devices may contain NPUs, while others rely only on CPUs. As workload volume increases, naive task assignment strategies frequently lead to resource imbalance, underutilization, and performance degradation.

To address this problem, this project proposes a complete **Heterogeneous Distributed Edge-AI Cluster Architecture** that combines:

- **Distributed Edge Computing**
- **Heterogeneous AI Hardware**
- **Hybrid Metaheuristic Scheduling (PSO–ACO–GA)**
- **Real-Time Resource Monitoring**
- **Fault-Tolerant Cluster Management**

The proposed system was implemented and validated on both simulated environments (Docker Compose) and real hardware clusters using Milk-V Duo and Luckfox Pico devices.

## System Objectives

The project aims to:

- Build a distributed Edge-AI cluster architecture following the Master–Worker model.
- Support heterogeneous edge hardware with different ISAs and AI accelerators.
- Dynamically allocate AI tasks to the most suitable worker using intelligent scheduling.
- Improve cluster throughput and overall resource utilization.
- Reduce workload completion time (**Makespan**).
- Maintain stable operation under node failures (fault tolerance).
- Provide real-time monitoring, visualization, and management through a web dashboard.

## Real Hardware Deployment

### Figure 1 — Physical Edge-AI Cluster Deployment

![Real Hardware Setup](https://github.com/user-attachments/assets/71f3014e-4c8d-46f9-af65-eb3c5ca7ec90)

**Description**

This figure shows the actual hardware deployment used during final experimentation and evaluation.

The cluster consists of:

| Component     | Quantity | Role                          |
|---------------|----------|-------------------------------|
| Edge Server   | 1        | Central orchestrator and scheduler |
| Milk-V Duo    | 2        | Edge AI inference workers     |
| Luckfox Pico  | 1        | Edge AI inference worker      |
| USB Hub       | Multiple | Power delivery and connectivity |
| Ethernet      | Shared   | Low-latency cluster communication |

**Responsibilities**

**The Edge Server** is responsible for:
- Worker discovery and cluster membership management
- Real-time cluster monitoring and state aggregation
- Intelligent task scheduling using the hybrid PSO–ACO–GA algorithm
- Resource tracking and load balancing
- Hosting the web-based monitoring dashboard
- MQTT communication and overall job lifecycle management

**Worker nodes** execute AI inference tasks (Object Detection, Drowsiness Detection, Heatmap Generation, People Counting, Face Capture) and periodically report their status, resource usage, and health back to the Edge Server.

This deployment demonstrates that the proposed architecture operates effectively on low-cost, resource-constrained embedded hardware rather than relying solely on simulation environments.

## Hardware Architecture

### Figure 2 — Hardware Architecture

![Hardware Architecture](https://github.com/user-attachments/assets/4cef73c9-64dc-4f33-940c-a5a4c1b7568d)

**Architectural Design**

The hardware architecture follows a clean **two-tier model** designed to embrace device heterogeneity.

**Tier 1 — Edge Server (Orchestrator)**

The Edge Server acts as the central coordinator of the cluster. It runs on a more powerful machine (typically an x86 laptop or small server) and handles all control-plane operations:
- Task reception and lifecycle management
- Execution of the hybrid PSO–ACO–GA scheduler
- Cluster state management and global resource view
- MQTT broker integration for job dispatch
- Web dashboard and REST API services
- Failure detection and recovery coordination

**Tier 2 — Heterogeneous Edge Workers**

Worker nodes are responsible for performing actual AI inference. The architecture explicitly supports a wide range of hardware platforms:
- x86 CPUs
- ARM processors
- RISC-V processors (Milk-V Duo SG200x)
- AI accelerators: NPU, TPU, GPU

Because each device exhibits different processing characteristics, memory capacity, power profiles, and model affinity, the scheduler must intelligently determine which worker is best suited for each incoming AI task.

**Benefits**
- High scalability — easily add more heterogeneous workers
- Hardware flexibility — no requirement for identical devices
- Better resource utilization across diverse hardware capabilities
- Cost-effective deployment using affordable embedded boards

## Software Architecture

### Figure 3 — Detailed Software Architecture

![Software Architecture](https://github.com/user-attachments/assets/f0370558-5b8c-4e1a-ad8f-dd63f4652237)

**Overview**

The software architecture cleanly separates responsibilities into two major layers: the **Edge Server Layer** and the **Worker Layer**.

**Edge Server Layer** (Implemented in Go)

- **Orchestrator**  
  Responsible for job lifecycle management, task dispatching via MQTT, failure recovery, and global resource tracking.

- **Scheduler (PSO–ACO–GA)**  
  The intelligent core responsible for worker selection, load balancing, and multi-objective performance optimization (makespan, energy, fairness).

- **MQTT Broker Integration**  
  Used for reliable task delivery, event notifications, and result transmission between the server and workers.

- **Serf + Gossip Protocol**  
  Provides decentralized node discovery, membership management, and distributed state sharing across the cluster without a single point of failure.

**Worker Layer**

Each worker runs a lightweight Go agent with the following responsibilities:
- Receiving jobs via MQTT subscription
- Executing AI models using ONNX Runtime or vendor NPU SDKs (INT8 quantized models)
- Monitoring local resources (CPU, memory, energy estimation)
- Reporting health status and current workload back to the Edge Server
- Automatic recovery and task re-execution on failure

**High-Level Workflow**

### Figure 4 — Conceptual System Architecture

![Conceptual Architecture](https://github.com/user-attachments/assets/edeee38f-0dd7-43f3-92da-49c125edd8f4)

**End-to-End Execution Flow**

1. Client submits an AI task (via dashboard or REST API).
2. Edge Server receives the request and registers it in the job queue.
3. The hybrid PSO–ACO–GA Scheduler evaluates all available workers using pheromone information and multi-criteria heuristics.
4. The best-suited worker is selected based on model affinity, current load, energy cost, and batching opportunities.
5. The task is dispatched to the selected worker through MQTT.
6. The Worker executes the AI inference (object detection, drowsiness detection, etc.).
7. The result (including bounding boxes, confidence scores, or generated heatmap) is returned to the Edge Server.
8. The Dashboard and monitoring system are updated in real time via WebSocket.

This workflow enables efficient utilization of heterogeneous resources while maintaining centralized yet fault-tolerant management.

## Hybrid PSO–ACO–GA Scheduler

### Figure 5 — Scheduling Workflow

![Scheduling Flowchart](https://github.com/user-attachments/assets/811c44ca-45cc-4a23-ae89-81d8473d8bd0)

**Motivation**

Task scheduling in heterogeneous edge environments is a classic **NP-hard** multi-objective optimization problem. Traditional algorithms such as Round Robin or simple Greedy scheduling often fail to adapt to dynamic workloads and hardware diversity, leading to poor makespan, energy waste, and load imbalance.

To address this challenge, a **hybrid metaheuristic scheduler** combining Particle Swarm Optimization (PSO), Ant Colony Optimization (ACO), and Genetic Algorithm (GA) was developed.

**Stage 1 — PSO (Exploration & Warm-start)**

Particle Swarm Optimization performs rapid exploration of the solution space.
- Generates high-quality candidate solutions quickly
- Identifies promising worker–task mappings
- Initializes the pheromone matrix for ACO (warm-start)

**Advantages**: Fast convergence and efficient global exploration.

**Stage 2 — ACO (Main Scheduling Engine)**

Ant Colony Optimization serves as the primary constructive scheduling engine.
The heuristic function evaluates workers using five key criteria:
- Resource Availability (CPU, memory)
- Current Worker Load
- **Model Affinity** (how well a specific AI model runs on a worker’s NPU)
- Estimated Energy Consumption
- Batching Opportunities (grouping compatible tasks)

ACO continuously refines decisions using accumulated pheromone trails from previous high-quality schedules.

**Stage 3 — GA (Parameter Adaptation)**

Genetic Algorithm operates asynchronously in the background.
- Performs hyperparameter optimization of heuristic weights
- Adapts scheduler behavior to changing workload patterns
- Continuously improves long-term scheduling quality based on observed makespan, energy, and fairness metrics

**Scheduling Objectives**

The scheduler simultaneously optimizes:
- **Makespan** (total workload completion time)
- **Throughput**
- **Average Latency**
- **Total Energy Consumption**
- **Load Balance** (Gini Index fairness)

## Supported AI Workloads

The cluster supports multiple practical AI inference services on edge devices:

**Object Detection**
- YOLO-based detection on COCO classes
- Real-time bounding box inference
- Suitable for surveillance and analytics

**Drowsiness Detection**
- Driver monitoring applications
- Eye closure and yawning detection
- 9-class behavior classification

**Face Capture**
- RetinaFace-based face detection and extraction
- Event-triggered image storage

**People Counting**
- Crowd analytics and density estimation
- Entry/exit line counting

**Heatmap Generation**
- Population density visualization
- Activity hotspot detection using Gaussian distribution on detections

## Monitoring Dashboard

### Figure 6 — Real-Time Cluster Dashboard

![Cluster Dashboard](https://github.com/user-attachments/assets/8a07971e-a723-49df-9398-3e21f2c67b48)

**Dashboard Features**

The web-based dashboard provides real-time visibility and control over cluster operations.

**Cluster Metrics**
- Total Nodes and Active Workers
- Queued Jobs and pending tasks
- Aggregate Memory Usage and Resource Consumption
- Live updates via WebSocket

**Task Management**
- Create new jobs directly from the UI
- Select task type (Object Detection, Drowsiness, Heatmap, etc.)
- Choose input source (video file, RTSP stream, or image)
- Monitor execution progress in real time

**Worker Monitoring**
Each worker card displays:
- Online/Offline status
- Current memory utilization
- Active workload and queue length
- Task execution history

**Remote Controls**
Available actions per worker:
- View live video stream
- Stop current task
- Restart worker agent

## Experimental Validation

The proposed architecture was rigorously evaluated in two environments:

**Simulation Environment**
- Docker Compose with multiple virtual workers
- Controlled and repeatable workload generation (up to 2000 tasks)
- Easy ablation studies and parameter tuning

**Real Hardware Cluster**
- 2× Milk-V Duo (SG200x with NPU)
- 1× Luckfox Pico
- 1× Edge Server (x86)

**Evaluation Metrics**
- Makespan
- Throughput
- Average Latency
- Fault Tolerance (worker failure and recovery scenarios)
- Resource Utilization and Energy awareness
- Load Balance (Gini Index)

Experimental results demonstrate that the **PSO–ACO–GA scheduler consistently outperforms baseline approaches** (Greedy, standalone PSO, ACO, GA) in makespan and throughput while maintaining stable operation under varying workload conditions and node failures.

## Technologies

**Languages**
- Go (primary language for Edge Server, Scheduler, and Worker agents)
- Python (supporting inference scripts and evaluation tools)

**AI Frameworks & Models**
- ONNX Runtime
- YOLO (quantized INT8)
- RetinaFace
- Custom drowsiness and heatmap models

**Communication & Coordination**
- MQTT (task dispatch and telemetry)
- HTTP REST API
- WebSocket (real-time dashboard updates)
- mDNS + Serf Gossip Protocol (decentralized discovery and state sharing)

**Infrastructure**
- Docker & Docker Compose (simulation and packaging)
- Embedded Linux (worker deployment)

**Hardware Platforms**
- Milk-V Duo (RISC-V + NPU)
- Luckfox Pico
- Intel x86 Edge Server (development & orchestration)

## Installation

### Clone Repository

```bash
git clone https://github.com/AnHuyEnthusiast123/edge-ai-cluster.git
cd edge-ai-cluster
```

### Build Edge Server

```bash
cd edge-server
go build -o edge-server main.go
```

### Build Worker Node (for RISC-V devices)

```bash
cd milkv-node
GOARCH=riscv64 go build -o milkv-node main.go
```

### Deploy to Worker

```bash
scp milkv-node root@worker-ip:/root/
```

## Running the System

### Simulation Mode (Recommended for initial testing)

```bash
cd cluster-docker
docker compose build
docker compose up -d
```

Run evaluation script:

```bash
cd tools/eval
RUNS=1 ./run_1000.sh
```

Access the dashboard at:

```
http://localhost:8080
```

### Real Hardware Deployment

**Start Edge Server:**

```bash
MQTT_BROKER=tcp://localhost:1883 \
FILE_STORAGE_DIR=/path/to/storage \
RESOURCE_DIR=/path/to/models \
./edge-server
```

**Start Worker Node:**

```bash
EDGE_SERVER=<edge-server-ip>:8000 \
EDGE_API_BASE=http://<edge-server-ip>:8081 \
./milkv-node
```

Workers will automatically:
- Discover cluster services via mDNS
- Join the Serf gossip network
- Subscribe to MQTT topics for job assignments
- Report health and resource metrics periodically

## Authors

**Trần An Huy** – 22520574  
**Trần Quang Huy** – 22520578  

**Supervisor**  
ThS. Trương Văn Cương  

Faculty of Computer Engineering  
University of Information Technology (UIT)  
Vietnam National University Ho Chi Minh City (VNU-HCM)  
2026

## Citation

If you use this project for academic or research purposes, please cite the corresponding graduation thesis:

> **Design, Implementation and Evaluation of a Heterogeneous Distributed Edge-AI Cluster Architecture for Performance-Optimized AI Workloads**  
> Trần An Huy, Trần Quang Huy  
> University of Information Technology – Vietnam National University Ho Chi Minh City, 2026

---

*This README provides a complete, structured, and technically accurate overview of the thesis project following a clean and professional format.*
