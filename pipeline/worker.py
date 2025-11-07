# worker.py
import sys
import os
import json
import cv2
import numpy as np
import time
import threading
from confluent_kafka import Consumer, Producer

# --- Configuration ---
KAFKA_BROKER = '172.22.213.64:9092'
TASK_TOPIC = 'tasks'
RESULT_TOPIC = 'results'
HEARTBEAT_TOPIC = 'heartbeats'
WORKER_GROUP_ID = 'image-processor-group' 

# --- Worker Functions ---

def process_image(image_tile, processing_type):
    """
    Applies the requested image transformation.
    """
    try:
        # --- PREVIOUS TRANSFORMATIONS ---
        if processing_type == 'grayscale':
            processed_tile = cv2.cvtColor(image_tile, cv2.COLOR_BGR2GRAY)
            processed_tile = cv2.cvtColor(processed_tile, cv2.COLOR_GRAY2BGR)
        
        elif processing_type == 'blur':
            processed_tile = cv2.GaussianBlur(image_tile, (15, 15), 0)
        
        elif processing_type == 'sharpen':
            kernel = np.array([[-1, -1, -1],
                               [-1,  9, -1],
                               [-1, -1, -1]])
            processed_tile = cv2.filter2D(image_tile, -1, kernel)
            
        elif processing_type == 'canny':
            processed_tile = cv2.Canny(image_tile, 100, 200)
            processed_tile = cv2.cvtColor(processed_tile, cv2.COLOR_GRAY2BGR)
            
        elif processing_type == 'invert':
            processed_tile = cv2.bitwise_not(image_tile)
            
        elif processing_type == 'sepia':
            kernel = np.array([[0.272, 0.534, 0.131],
                               [0.349, 0.686, 0.168],
                               [0.393, 0.769, 0.189]])
            processed_tile = cv2.transform(image_tile, kernel)
            processed_tile = np.clip(processed_tile, 0, 255).astype(np.uint8)

        elif processing_type == 'threshold':
            gray_tile = cv2.cvtColor(image_tile, cv2.COLOR_BGR2GRAY)
            _, processed_tile = cv2.threshold(gray_tile, 127, 255, cv2.THRESH_BINARY)
            processed_tile = cv2.cvtColor(processed_tile, cv2.COLOR_GRAY2BGR)

        elif processing_type == 'emboss':
            kernel = np.array([[-2, -1, 0],
                               [-1,  1, 1],
                               [ 0,  1, 2]])
            processed_tile = cv2.filter2D(image_tile, -1, kernel)
            
        # --- 5 NEW REAL-WORLD TRANSFORMATIONS ---
        
        elif processing_type == 'brightness_increase':
            # Increases brightness by a fixed value (50)
            # cv2.convertScaleAbs handles clipping (keeps values in 0-255 range)
            processed_tile = cv2.convertScaleAbs(image_tile, alpha=1.0, beta=50)

        elif processing_type == 'contrast_increase':
            # Increases contrast by a factor of 1.5
            # alpha is the contrast multiplier, beta is added brightness
            processed_tile = cv2.convertScaleAbs(image_tile, alpha=1.5, beta=0)

        elif processing_type == 'gamma_correct':
            # Gamma correction (gamma < 1 brightens, > 1 darkens)
            # This is a non-linear brightness adjustment, good for shadows
            gamma = 0.75 # Make image brighter
            # Build a lookup table: val -> val^(1/gamma)
            invGamma = 1.0 / gamma
            table = np.array([((i / 255.0) ** invGamma) * 255
                for i in np.arange(0, 256)]).astype("uint8")
            # Apply the lookup table to every pixel
            processed_tile = cv2.LUT(image_tile, table)

        elif processing_type == 'denoise':
            # Applies a powerful denoising algorithm for color images
            # h=10: filter strength. Higher = more noise removed (and more blur)
            processed_tile = cv2.fastNlMeansDenoisingColored(image_tile, None, 10, 10, 7, 21)

        elif processing_type == 'saturation_increase':
            # Convert to HSV (Hue, Saturation, Value) color space
            hsv_tile = cv2.cvtColor(image_tile, cv2.COLOR_BGR2HSV)
            h, s, v = cv2.split(hsv_tile)
            
            # Increase the saturation (s) channel by 50%
            # We clip to 255 to avoid overflowing
            s = cv2.multiply(s, 1.5)
            s = np.clip(s, 0, 255)
            
            # Merge the channels back and convert back to BGR
            final_hsv = cv2.merge([h, s, v])
            processed_tile = cv2.cvtColor(final_hsv, cv2.COLOR_HSV2BGR)
            
        else:
            # If unknown type, just return the original
            print(f"Unknown processing type '{processing_type}'. Returning original tile.")
            processed_tile = image_tile
            
    except Exception as e:
        print(f"Error during processing '{processing_type}': {e}")
        # Return original tile on failure
        processed_tile = image_tile
        
    return processed_tile

def send_heartbeat(producer, worker_id):
    """
    Sends a heartbeat message to the 'heartbeats' topic
    every 5 seconds.
    """
    while True:
        try:
            heartbeat_message = {
                'worker_id': worker_id,
                'timestamp': time.time()
            }
            
            producer.produce(
                HEARTBEAT_TOPIC,
                value=json.dumps(heartbeat_message),
                key=worker_id
            )
            producer.flush()
        except Exception as e:
            print(f"Worker {worker_id}: Failed to send heartbeat: {e}")
        
        time.sleep(5) # Send heartbeat every 5 seconds


def start_worker(worker_id):
    """
    Main worker function: consumes tasks, processes them, 
    and produces results.
    """
    print(f"Starting worker {worker_id}...")

    # --- Kafka Producer (for results & heartbeats) ---
    producer_config = {'bootstrap.servers': KAFKA_BROKER, 'message.max.bytes': 5242880}
    producer = Producer(producer_config)
    
    # --- Kafka Consumer (for tasks) ---
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': WORKER_GROUP_ID, # Key for load balancing
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([TASK_TOPIC])

    # --- Start Heartbeat Thread ---
    heartbeat_thread = threading.Thread(
        target=send_heartbeat, 
        args=(producer, worker_id), 
        daemon=True
    )
    heartbeat_thread.start()

    print(f"Worker {worker_id}: Subscribed to '{TASK_TOPIC}'. Waiting for tasks...")

    # --- Main Task Processing Loop ---
    while True:
        msg = consumer.poll(1.0) 
        
        if msg is None:
            continue
        if msg.error():
            print(f"Worker {worker_id}: Consumer error: {msg.error()}")
            continue

        try:
            # 1. Receive and deserialize task
            task = json.loads(msg.value().decode('utf-8'))
            
            # Decode image data
            tile_data = np.frombuffer(bytes(task['tile_data'], 'latin-1'), dtype=np.uint8)
            image_tile = cv2.imdecode(tile_data, cv2.IMREAD_COLOR)
            
            if image_tile is None:
                raise ValueError("Could not decode image tile")

            print(f"Worker {worker_id}: Received task {task['tile_id']} for job {task['job_id']} (Type: {task['processing_type']})")
            
            # 2. Process the image tile
            processed_tile = process_image(image_tile, task['processing_type'])
            
            # 3. Encode the processed tile
            _, buffer = cv2.imencode('.jpg', processed_tile)
            processed_data = buffer.tobytes()
            
            # 4. Prepare result message
            result_message = {
                'job_id': task['job_id'],
                'tile_id': task['tile_id'],
                'position': task['position'],
                'processed_tile_data': processed_data.decode('latin-1') 
            }
            
            # 5. Publish result to 'results' topic
            producer.produce(
                RESULT_TOPIC,
                value=json.dumps(result_message),
                key=task['job_id']
            )
            producer.flush()
            
            print(f"Worker {worker_id}: Completed task {task['tile_id']} for job {task['job_id']}")

        except Exception as e:
            print(f"Worker {worker_id}: Failed to process task: {e}")
            
# --- Main Entry Point ---
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)
        
    worker_id = sys.argv[1]
    start_worker(worker_id)