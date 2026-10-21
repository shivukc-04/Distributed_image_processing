import argparse
import json
import os
import sys
import threading
import time
import cv2
from confluent_kafka import Consumer, Producer
import numpy as np
import redis

# --- Configuration ---
KAFKA_BROKER = "localhost:9092"
TASK_TOPIC = "tasks"
RESULT_TOPIC = "results"
WORKER_GROUP_ID = "image-processor-group"

# Initialize Redis Connection
r = redis.Redis(host="localhost", port=6379, db=0)


def start_heartbeat_daemon(worker_id):
    """Sends a heartbeat ping to Redis every 4 seconds with a 10s expiration (TTL)."""

    def heartbeat_loop():
        while True:
            try:
                r.set(f"heartbeat:{worker_id}", "ALIVE", ex=10)
            except Exception as e:
                print(
                    f"Worker {worker_id}: Redis Heartbeat failed: {e}"
                )
            time.sleep(4)

    thread = threading.Thread(target=heartbeat_loop, daemon=True)
    thread.start()


def process_image(image_tile, processing_type):
    """Applies the requested image transformation."""
    try:
        if processing_type == "grayscale":
            processed_tile = cv2.cvtColor(
                image_tile, cv2.COLOR_BGR2GRAY
            )
            processed_tile = cv2.cvtColor(
                processed_tile, cv2.COLOR_GRAY2BGR
            )

        elif processing_type == "blur":
            processed_tile = cv2.GaussianBlur(
                image_tile, (15, 15), 0
            )

        elif processing_type == "sharpen":
            kernel = np.array(
                [[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]]
            )
            processed_tile = cv2.filter2D(image_tile, -1, kernel)

        elif processing_type == "canny":
            processed_tile = cv2.Canny(image_tile, 100, 200)
            processed_tile = cv2.cvtColor(
                processed_tile, cv2.COLOR_GRAY2BGR
            )

        elif processing_type == "invert":
            processed_tile = cv2.bitwise_not(image_tile)

        elif processing_type == "sepia":
            kernel = np.array(
                [
                    [0.272, 0.534, 0.131],
                    [0.349, 0.686, 0.168],
                    [0.393, 0.769, 0.189],
                ]
            )
            processed_tile = cv2.transform(image_tile, kernel)
            processed_tile = np.clip(
                processed_tile, 0, 255
            ).astype(np.uint8)

        elif processing_type == "threshold":
            gray_tile = cv2.cvtColor(image_tile, cv2.COLOR_BGR2GRAY)
            _, processed_tile = cv2.threshold(
                gray_tile, 127, 255, cv2.THRESH_BINARY
            )
            processed_tile = cv2.cvtColor(
                processed_tile, cv2.COLOR_GRAY2BGR
            )

        elif processing_type == "emboss":
            kernel = np.array(
                [[-2, -1, 0], [-1, 1, 1], [0, 1, 2]]
            )
            processed_tile = cv2.filter2D(image_tile, -1, kernel)

        elif processing_type == "brightness_increase":
            processed_tile = cv2.convertScaleAbs(
                image_tile, alpha=1.0, beta=50
            )

        elif processing_type == "contrast_increase":
            processed_tile = cv2.convertScaleAbs(
                image_tile, alpha=1.5, beta=0
            )

        elif processing_type == "gamma_correct":
            gamma = 0.75
            invGamma = 1.0 / gamma
            table = np.array(
                [
                    ((i / 255.0) ** invGamma) * 255
                    for i in np.arange(0, 256)
                ]
            ).astype("uint8")
            processed_tile = cv2.LUT(image_tile, table)

        elif processing_type == "denoise":
            processed_tile = cv2.fastNlMeansDenoisingColored(
                image_tile, None, 10, 10, 7, 21
            )

        elif processing_type == "saturation_increase":
            hsv_tile = cv2.cvtColor(image_tile, cv2.COLOR_BGR2HSV)
            h, s, v = cv2.split(hsv_tile)
            s = cv2.multiply(s, 1.5)
            s = np.clip(s, 0, 255)
            final_hsv = cv2.merge([h, s, v])
            processed_tile = cv2.cvtColor(
                final_hsv, cv2.COLOR_HSV2BGR
            )

        else:
            print(
                f"Unknown processing type '{processing_type}'. Returning original tile."
            )
            processed_tile = image_tile

    except Exception as e:
        print(f"Error during processing '{processing_type}': {e}")
        processed_tile = image_tile

    return processed_tile


def start_worker(worker_id):
    """Main worker function: consumes tasks from Kafka, processes them, and produces results."""
    print(f"Starting worker {worker_id}...")

    # Kafka Producer setup
    producer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "message.max.bytes": 5242880,
    }
    producer = Producer(producer_config)

    # Kafka Consumer setup
    consumer_config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": WORKER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([TASK_TOPIC])

    print(
        f"Worker {worker_id}: Subscribed to '{TASK_TOPIC}'. Waiting for tasks..."
    )

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Worker {worker_id}: Consumer error: {msg.error()}")
            continue

        try:
            # 1. Deserialize task payload
            task = json.loads(msg.value().decode("utf-8"))

            # 2. Decode image tile
            tile_data = np.frombuffer(
                bytes(task["tile_data"], "latin-1"), dtype=np.uint8
            )
            image_tile = cv2.imdecode(tile_data, cv2.IMREAD_COLOR)

            if image_tile is None:
                raise ValueError("Could not decode image tile")

            print(
                f"Worker {worker_id}: Received task {task['tile_id']} for job {task['job_id']} (Type: {task['processing_type']})"
            )

            # 3. Process tile
            processed_tile = process_image(
                image_tile, task["processing_type"]
            )

            # 4. Encode result back to buffer
            _, buffer = cv2.imencode(".jpg", processed_tile)
            processed_data = buffer.tobytes()

            result_message = {
                "job_id": task["job_id"],
                "tile_id": task["tile_id"],
                "position": task["position"],
                "processed_tile_data": processed_data.decode(
                    "latin-1"
                ),
            }

            # 5. Emit results to Kafka
            producer.produce(
                RESULT_TOPIC,
                value=json.dumps(result_message),
                key=task["job_id"],
            )
            producer.flush()

            print(
                f"Worker {worker_id}: Completed task {task['tile_id']} for job {task['job_id']}"
            )

        except Exception as e:
            print(f"Worker {worker_id}: Failed to process task: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--id",
        type=str,
        default="worker_1",
        help="Unique Worker Identifier",
    )
    args = parser.parse_args()

    # 1. Start background Heartbeat pinging to Redis
    start_heartbeat_daemon(args.id)
    print(f"🚀 Worker '{args.id}' initialized with active Heartbeat!")

    # 2. Start Kafka consumer loop
    start_worker(args.id)
