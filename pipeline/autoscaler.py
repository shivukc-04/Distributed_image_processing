import subprocess
import sys
import time
import redis

# Initialize Redis connection
r = redis.Redis(host="localhost", port=6379, db=0)

spawned_workers = {}  # Tracks spawned processes: { worker_id: process_obj }
MAX_WORKERS = 4
MIN_WORKERS = 1
TASKS_PER_WORKER_TARGET = 3  # Triggers dynamic scaling per 3 pending tasks


def run_autoscaler():
    print("🤖 Auto-Scaler Daemon Running...")
    while True:
        try:
            # Check pending tasks from Redis tracking queue and active heartbeats
            pending_tasks = r.llen("tile_task_queue")
            active_workers = len(r.keys("heartbeat:*"))
            print(
                f"[Autoscaler Monitor] Queue Depth: {pending_tasks} | Active Workers: {active_workers}"
            )

            # 1. SCALE UP: Pending tasks exist
            if pending_tasks > 0:
                # Calculate target capacity needed for the workload
                needed_workers = (
                    pending_tasks + TASKS_PER_WORKER_TARGET - 1
                ) // TASKS_PER_WORKER_TARGET
                desired_workers = min(
                    MAX_WORKERS, max(MIN_WORKERS, needed_workers)
                )

                # Spawn additional workers dynamically using sys.executable
                while len(spawned_workers) + 1 < desired_workers:
                    new_id = f"worker_auto_{len(spawned_workers) + 2}"
                    print(
                        f"⚡ High Load ({pending_tasks} tasks pending)! Scaling UP -> Launching {new_id}"
                    )

                    proc = subprocess.Popen(
                        [
                            sys.executable,
                            "worker.py",
                            "--id",
                            new_id,
                        ]
                    )
                    spawned_workers[new_id] = proc

            # 2. SCALE DOWN: Queue is completely processed
            elif pending_tasks <= 0 and len(spawned_workers) > 0:
                worker_id, proc = spawned_workers.popitem()
                print(
                    f"💤 Queue Empty. Scaling DOWN -> Terminating {worker_id}"
                )
                proc.terminate()

        except Exception as e:
            print(f"Error in Autoscaler: {e}")

        time.sleep(1)  # Checked every 1 second for fast response times


if __name__ == "__main__":
    run_autoscaler()
