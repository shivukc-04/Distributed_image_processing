# Distributed Image Processing Pipeline (Apache Kafka + Redis + OpenCV)

A high-performance, event-driven distributed system designed to process high-resolution images in parallel. The pipeline splits large images into a grid of spatial tiles, queues tasks asynchronously via **Apache Kafka**, tracks real-time progress and worker heartbeats in **Redis**, and dynamically executes parallel **OpenCV** transformations across 1 to 4 local worker processes.

---

## 🏗️ Architecture Overview

The system follows a decoupled **Master-Worker Pattern**:

```
                       ┌───────────────────────────────┐
                       │     Flask Master Application  │
                       │  (Image Splitting & Web UI)   │
                       └──────────────┬────────────────┘
                                      │
              ┌───────────────────────┴───────────────────────┐
              ▼                                               ▼
   ┌────────────────────┐                          ┌────────────────────┐
   │    Apache Kafka    │                          │    Redis Store     │
   │  (Task & Result    │                          │ (State Tracking &  │
   │    Queues)         │                          │ Worker Heartbeats) │
   └──────────┬─────────┘                          └──────────┬─────────┘
              │                                               │
              ├───────────────────────┼───────────────────────┤
              ▼                       ▼                       ▼
    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
    │     Worker 1     │    │     Worker 2     │    │   Worker N...    │
    │  (OpenCV Engine) │    │  (OpenCV Engine) │    │   (Up to 4)      │
    └──────────────────┘    └──────────────────┘    └──────────────────┘
              ▲                                               ▲
              └───────────────────────┴───────────────────────┘
                                      │
                       ┌──────────────┴────────────────┐
                       │   Local Auto-Scaler Daemon    │
                       │   (Monitors Lag & Spawns)     │
                       └───────────────────────────────┘
```

### Core Components

1. **Flask Master Server (`pipeline/app.py`)**:
   - **Tile Partitioning**: Slices uploaded high-resolution images (e.g., $3840 \times 2160$) into spatial tiles ($512 \times 512$ or dynamically calculated dimensions).
   - **Task Dispatch**: Publishes individual tile tasks to Kafka topics with metadata (image ID, tile coordinates, selected filter).
   - **Progress & Reconstruction**: Polls task completion status from Redis, tracks real-time tile reconstruction, and stitches final images upon completion.

2. **Message Broker (Apache Kafka)**:
   - Manages asynchronous task distribution (`tile_tasks` topic) and result notifications (`tile_results` topic).
   - Ensures uncommitted message offsets are retained if a worker disconnects mid-task.

3. **In-Memory State Store (Redis)**:
   - Stores tile completion matrices, spatial grid mapping, and system execution state.
   - Tracks active worker health using 10-second Time-To-Live (TTL) keys (`heartbeat:<worker_id>`).

4. **Worker Processes (`pipeline/worker.py`)**:
   - Consumes tile tasks from Kafka and executes OpenCV algorithms:
     - **Canny Edge Detection**
     - **Gamma Correction**
     - **Gaussian Denoising**
   - Runs a lightweight background daemon thread that refreshes `heartbeat:<worker_id>` in Redis every 4 seconds.

5. **Local Auto-Scaler (`pipeline/autoscaler.py`)**:
   - Periodically monitors Redis task queue depth and active worker heartbeats.
   - Dynamically spawns background worker subprocesses (`worker_auto_1` through `worker_auto_4`) via `subprocess.Popen` when queue lag builds up, and terminates idle workers when the queue empties.

---

## ⚡ Key Features Achieved

- **Parallel Processing**: Slices high-resolution visuals into parallel grid tiles to accelerate compute-heavy image filters across multiple CPU workers.
- **Dynamic Tile Sizing**: Automatically snaps tile dimensions based on total image area to maintain an optimal 12–16 tile processing grid.
- **Fault-Tolerant Heartbeats**: Workers issue 4-second TTL heartbeats to Redis. If a worker process crashes, its key automatically expires in 10 seconds and its uncommitted task is reassigned.
- **Demand-Based Local Auto-Scaling**: Scales worker processes from 1 up to 4 instances locally based on consumer queue lag without requiring manual container orchestrators.
- **Real-Time Monitoring UI**: Flask dashboard displays active worker count, queue depth, live percentage progress, and spatial tile reconstruction.

---

## 📂 Project Directory Structure

```
Scholarship_management/
├── pipeline/
│   ├── app.py           # Flask Master server & Web UI routes
│   ├── worker.py        # Worker consumer engine & OpenCV processing
│   ├── autoscaler.py    # Local queue monitor & subprocess scaler
│   ├── templates/       # HTML Dashboard interface
│   └── static/          # CSS styles & processed image output
├── requirements.txt     # Python dependencies
└── README.md            # System documentation
```

---

## 🚀 Setup & Execution Guide

### Prerequisites
- **Python 3.10+**
- **Redis Server** (running locally on port `6379`)
- **Apache Kafka & Zookeeper / KRaft** (running locally on port `9092`)

---

### Step 1: Start Redis
Start the Redis server in your terminal:
```bash
redis-server
```

---

### Step 2: Start Apache Kafka
Ensure Zookeeper/KRaft and the Kafka broker are active:
```bash
# Example for standard Kafka distribution
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
```

---

### Step 3: Launch the Master Application
Start the Flask Master server:
```bash
python pipeline/app.py
```
The dashboard will be accessible at `http://localhost:5000`.

---

### Step 4: Run Workers (Choose Option A or Option B)

#### **Option A: Manual Multi-Worker Mode**
Open separate terminal windows to launch individual workers with explicit IDs:

* **Terminal 1**:
  ```bash
  python pipeline/worker.py --id worker_1
  ```
* **Terminal 2**:
  ```bash
  python pipeline/worker.py --id worker_2
  ```

#### **Option B: Automatic Scaling Mode**
Run the auto-scaler script to let the system launch and terminate up to 4 worker processes automatically based on queue depth:
```bash
python pipeline/autoscaler.py
```

---

## 📊 Performance & Limits (Local Single-Node)

| Parameter | Specification / Value |
| :--- | :--- |
| **Max Local Workers** | 4 Subprocesses |
| **Default Tile Size** | $512 \times 512$ px (or dynamic $256\text{px}$–$1024\text{px}$) |
| **Heartbeat Interval** | 4 Seconds |
| **Heartbeat Expiration (TTL)** | 10 Seconds |
| **Supported Filters** | Canny Edge, Gamma Correction, Gaussian Denoising |

*Note: On a single host machine, workers share the physical CPU cores and RAM bandwidth. For true horizontal hardware scaling across distinct physical servers, the worker nodes can be containerized with Docker and deployed to a cloud cluster (e.g., AWS ECS / Kubernetes).*
