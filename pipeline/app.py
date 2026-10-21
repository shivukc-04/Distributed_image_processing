import json
import math
import os
import shutil
import threading
import time
import uuid
import cv2
from confluent_kafka import Consumer, Producer
from flask import Flask, jsonify, render_template, request, send_from_directory
import numpy as np
import redis

# --- Configuration ---
UPLOAD_FOLDER = "uploads"
PROCESSED_FOLDER = "processed"
TILE_CACHE_FOLDER = "processed_tiles_cache"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)
os.makedirs(TILE_CACHE_FOLDER, exist_ok=True)

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TASK_TOPIC = "tasks"
RESULT_TOPIC = "results"

# --- Flask App Initialization ---
app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["PROCESSED_FOLDER"] = PROCESSED_FOLDER
app.config["TILE_CACHE_FOLDER"] = TILE_CACHE_FOLDER

# --- Redis Connection ---
try:
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    r.ping()
    print("Connected to Redis successfully!")
except Exception as e:
    print(f"Error connecting to Redis: {e}")
    exit(1)

# --- Kafka Producer with Payload Compression ---
kafka_producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "compression.type": "gzip",
    "message.max.bytes": 10485760,
}
producer = Producer(kafka_producer_config)


@app.route("/api/workers", methods=["GET"])
def get_active_workers():
    """Returns list of active workers currently sending heartbeats."""
    keys = r.keys("heartbeat:*")
    active_worker_ids = [k.replace("heartbeat:", "") for k in keys]
    return jsonify(
        {
            "active_worker_count": len(active_worker_ids),
            "workers": active_worker_ids,
        }
    )


@app.route("/dashboard")
def get_dashboard_data():
    """Provides monitoring data for the dashboard JS UI."""
    keys = r.keys("heartbeat:*")
    current_time = time.time()
    # Maps worker IDs directly to timestamp values expected by the UI template
    workers_status = {k.replace("heartbeat:", ""): current_time for k in keys}
    return jsonify(workers_status)


def calculate_dynamic_tile_size(img_width, img_height, target_tiles=16):
    """Calculates tile size dynamically so large images yield enough tasks to trigger autoscaling."""
    total_area = img_width * img_height
    target_area_per_tile = total_area / target_tiles
    raw_size = int(math.sqrt(target_area_per_tile))
    snapped_size = round(raw_size / 64) * 64
    return max(256, min(1024, snapped_size))


def split_and_publish_tasks(image_path, job_id, processing_type):
    try:
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError("Could not read image")

        img_height, img_width, _ = img.shape
        tile_size = calculate_dynamic_tile_size(
            img_width, img_height, target_tiles=16
        )

        tiles_x = int(np.ceil(img_width / tile_size))
        tiles_y = int(np.ceil(img_height / tile_size))
        total_tiles = tiles_x * tiles_y

        job_key = f"job:status:{job_id}"
        job_data = {
            "status": "processing",
            "total_tiles": total_tiles,
            "tiles_processed_count": 0,
            "image_shape": json.dumps(img.shape),
            "processing_type": processing_type,
            "original_filename": os.path.basename(image_path),
        }
        r.hset(job_key, mapping=job_data)

        job_tile_cache_path = os.path.join(
            app.config["TILE_CACHE_FOLDER"], job_id
        )
        os.makedirs(job_tile_cache_path, exist_ok=True)

        tile_count = 0
        for y in range(tiles_y):
            for x in range(tiles_x):
                x_start, y_start = x * tile_size, y * tile_size
                tile = img[
                    y_start : y_start + tile_size,
                    x_start : x_start + tile_size,
                ]

                # JPEG compression quality set to 85 to ensure small payload sizes
                _, buffer = cv2.imencode(
                    ".jpg", tile, [int(cv2.IMWRITE_JPEG_QUALITY), 85]
                )
                tile_data = buffer.tobytes()

                task_message = {
                    "job_id": job_id,
                    "tile_id": tile_count,
                    "tile_data": tile_data.decode("latin-1"),
                    "position": (x_start, y_start),
                    "total_tiles": total_tiles,
                    "processing_type": processing_type,
                }

                r.hset(
                    f"job:tiles:{job_id}",
                    tile_count,
                    json.dumps((x_start, y_start)),
                )

                # Track queue depth in Redis for autoscaler daemon
                r.rpush("tile_task_queue", f"{job_id}:{tile_count}")

                producer.produce(
                    TASK_TOPIC,
                    value=json.dumps(task_message),
                    key=None,
                )
                tile_count += 1

        producer.flush()
        print(
            f"Job {job_id}: Published {total_tiles} tasks (Tile size: {tile_size}x{tile_size})."
        )

    except Exception as e:
        r.hset(
            f"job:status:{job_id}",
            mapping={"status": "failed", "error": str(e)},
        )
        print(f"Error processing job {job_id}: {e}")


def reconstruct_image(job_id):
    print(f"Job {job_id}: Starting reconstruction...")
    job_key = f"job:status:{job_id}"
    job_tile_key = f"job:tiles:{job_id}"
    job_tile_cache_path = os.path.join(
        app.config["TILE_CACHE_FOLDER"], job_id
    )

    try:
        job_data = r.hgetall(job_key)
        original_shape = json.loads(job_data["image_shape"])
        tile_positions = r.hgetall(job_tile_key)

        final_image = np.zeros(original_shape, dtype=np.uint8)

        for tile_id, pos_json in tile_positions.items():
            position = json.loads(pos_json)
            tile_file_path = os.path.join(
                job_tile_cache_path, f"tile_{tile_id}.jpg"
            )

            tile_img = cv2.imread(tile_file_path)
            if tile_img is None:
                raise ValueError(f"Missing tile file: {tile_file_path}")

            h, w, _ = tile_img.shape
            final_image[
                position[1] : position[1] + h, position[0] : position[0] + w
            ] = tile_img

        result_filename = f"{job_id}_processed.jpg"
        result_path = os.path.join(
            app.config["PROCESSED_FOLDER"], result_filename
        )
        cv2.imwrite(result_path, final_image)

        r.hset(
            job_key,
            mapping={
                "status": "completed",
                "result_url": f"/processed/{result_filename}",
            },
        )

        r.delete(job_tile_key)
        shutil.rmtree(job_tile_cache_path)
        print(f"Job {job_id}: Reconstruction complete.")

    except Exception as e:
        r.hset(
            job_key,
            mapping={
                "status": "failed",
                "error": f"Reconstruction failed: {e}",
            },
        )
        print(f"Job {job_id}: Reconstruction failed: {e}")


def result_consumer_thread():
    consumer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "master-result-group",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([RESULT_TOPIC])
    print("Result consumer thread started...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Result Consumer error: {msg.error()}")
            continue

        try:
            result = json.loads(msg.value().decode("utf-8"))
            job_id = result["job_id"]
            job_key = f"job:status:{job_id}"

            job_status = r.hget(job_key, "status")
            if job_status != "processing":
                continue

            tile_data = np.frombuffer(
                bytes(result["processed_tile_data"], "latin-1"), dtype=np.uint8
            )
            processed_tile = cv2.imdecode(tile_data, cv2.IMREAD_COLOR)

            job_tile_cache_path = os.path.join(
                app.config["TILE_CACHE_FOLDER"], job_id
            )
            tile_save_path = os.path.join(
                job_tile_cache_path, f"tile_{result['tile_id']}.jpg"
            )

            cv2.imwrite(tile_save_path, processed_tile)

            # Pop completed tile item from task queue
            r.lpop("tile_task_queue")

            processed_count = r.hincrby(job_key, "tiles_processed_count", 1)
            total_tiles = int(r.hget(job_key, "total_tiles"))

            if processed_count == total_tiles:
                print(
                    f"Job {job_id}: All tiles received. Triggering reconstruction."
                )
                r.hset(job_key, "status", "reconstructing")
                threading.Thread(
                    target=reconstruct_image, args=(job_id,)
                ).start()

        except Exception as e:
            print(f"Error processing result: {e}")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_image():
    if "image" not in request.files:
        return jsonify({"error": "No image file provided"}), 400

    file = request.files["image"]
    processing_type = request.form.get("processing_type", "grayscale")

    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    if file:
        job_id = str(uuid.uuid4())
        filename = f"{job_id}_{file.filename}"
        image_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(image_path)

        threading.Thread(
            target=split_and_publish_tasks,
            args=(image_path, job_id, processing_type),
        ).start()

        return jsonify({"job_id": job_id, "status": "submitted"})


@app.route("/status/<job_id>")
def get_job_status(job_id):
    job_key = f"job:status:{job_id}"
    job_data = r.hgetall(job_key)

    if not job_data:
        return jsonify({"error": "Job not found"}), 404

    status = job_data.get("status")

    if status in ["processing", "reconstructing"]:
        return jsonify(
            {
                "status": status,
                "total_tiles": int(job_data.get("total_tiles", 0)),
                "tiles_processed": int(
                    job_data.get("tiles_processed_count", 0)
                ),
            }
        )
    elif status == "completed":
        return jsonify(
            {"status": "completed", "result_url": job_data.get("result_url")}
        )
    elif status == "failed":
        return jsonify(
            {
                "status": "failed",
                "error": job_data.get("error", "Unknown error"),
            }
        )
    else:
        return jsonify({"status": "unknown", "error": "Unknown job status"})


@app.route("/processed/<filename>")
def get_processed_image(filename):
    return send_from_directory(app.config["PROCESSED_FOLDER"], filename)


if __name__ == "__main__":
    threading.Thread(target=result_consumer_thread, daemon=True).start()
    app.run(debug=True, port=5000, use_reloader=False)
