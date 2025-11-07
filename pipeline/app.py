# app.py
import os
import uuid
import json
import cv2
import numpy as np
import threading
import time
import redis # <-- NEW: Import redis
from flask import Flask, request, render_template, jsonify, send_from_directory
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import shutil # <-- NEW: For cleaning up tile folders

# --- Configuration ---
UPLOAD_FOLDER = 'uploads'
PROCESSED_FOLDER = 'processed'
# NEW: A folder to store individual tiles temporarily
TILE_CACHE_FOLDER = 'processed_tiles_cache' 
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)
os.makedirs(TILE_CACHE_FOLDER, exist_ok=True)

TILE_SIZE_X = 512
TILE_SIZE_Y = 512

# Kafka Configuration
KAFKA_BROKER = '172.22.213.64:9092'
TASK_TOPIC = 'tasks'
RESULT_TOPIC = 'results'
HEARTBEAT_TOPIC = 'heartbeats'

# --- Flask App Initialization ---
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER
app.config['TILE_CACHE_FOLDER'] = TILE_CACHE_FOLDER

# --- NEW: Redis Connection ---
# decode_responses=True automatically converts Redis bytes to Python strings
try:
    r = redis.Redis(decode_responses=True)
    r.ping()
    print("Connected to Redis successfully!")
except Exception as e:
    print(f"Error connecting to Redis: {e}")
    print("Please ensure redis-server is running.")
    exit(1)

# --- Kafka Producer ---
kafka_producer_config = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(kafka_producer_config)

# --- Master Node: Image Splitting & Task Publishing ---
def split_and_publish_tasks(image_path, job_id, processing_type):
    try:
        img = cv2.imread(image_path)
        if img is None: raise ValueError("Could not read image")
        
        img_height, img_width, _ = img.shape
        if img_height < 1024 or img_width < 1024:
            r.hset(f"job:status:{job_id}", mapping={'status': 'failed', 'error': 'Image must be at least 1024x1024'})
            return

        tiles_x = int(np.ceil(img_width / TILE_SIZE_X))
        tiles_y = int(np.ceil(img_height / TILE_SIZE_Y))
        total_tiles = tiles_x * tiles_y
        
        # --- NEW: Use Redis Hash for job status ---
        job_key = f"job:status:{job_id}"
        job_data = {
            'status': 'processing',
            'total_tiles': total_tiles,
            'tiles_processed_count': 0, # We will increment this
            'image_shape': json.dumps(img.shape), # Store shape as JSON
            'processing_type': processing_type,
            'original_filename': os.path.basename(image_path)
        }
        # r.hset() sets multiple fields in a hash
        r.hset(job_key, mapping=job_data)
        
        # --- NEW: Create a cache folder for this job's tiles ---
        job_tile_cache_path = os.path.join(app.config['TILE_CACHE_FOLDER'], job_id)
        os.makedirs(job_tile_cache_path, exist_ok=True)

        tile_count = 0
        for y in range(tiles_y):
            for x in range(tiles_x):
                x_start, y_start = x * TILE_SIZE_X, y * TILE_SIZE_Y
                tile = img[y_start:y_start + TILE_SIZE_Y, x_start:x_start + TILE_SIZE_X]
                
                _, buffer = cv2.imencode('.jpg', tile)
                tile_data = buffer.tobytes()
                
                task_message = {
                    'job_id': job_id,
                    'tile_id': tile_count,
                    'tile_data': tile_data,
                    'position': (x_start, y_start),
                    'total_tiles': total_tiles,
                    'processing_type': processing_type
                }
                
                # --- NEW: Store tile position in Redis ---
                # We need this for reconstruction later
                r.hset(f"job:tiles:{job_id}", tile_count, json.dumps((x_start, y_start)))
                
                producer.produce(
                    TASK_TOPIC,
                    value=json.dumps(task_message, default=lambda x: x.decode('latin-1') if isinstance(x, bytes) else x),
                    key=None # Distribute tasks evenly
                )
                tile_count += 1
        
        producer.flush()
        print(f"Job {job_id}: Published {total_tiles} tasks.")

    except Exception as e:
        r.hset(f"job:status:{job_id}", mapping={'status': 'failed', 'error': str(e)})
        print(f"Error processing job {job_id}: {e}")

# --- NEW: Image Reconstruction (Moved to its own function) ---
def reconstruct_image(job_id):
    """
    Reconstructs an image from tiles stored on disk.
    This is called when all tiles are processed.
    """
    print(f"Job {job_id}: Starting reconstruction...")
    job_key = f"job:status:{job_id}"
    job_tile_key = f"job:tiles:{job_id}"
    job_tile_cache_path = os.path.join(app.config['TILE_CACHE_FOLDER'], job_id)
    
    try:
        # 1. Get job metadata from Redis
        job_data = r.hgetall(job_key)
        original_shape = json.loads(job_data['image_shape'])
        
        # 2. Get all tile positions from Redis
        tile_positions = r.hgetall(job_tile_key)
        
        # 3. Create a blank canvas
        final_image = np.zeros(original_shape, dtype=np.uint8)
        
        # 4. Loop through tiles, read from disk, and place on canvas
        for tile_id, pos_json in tile_positions.items():
            position = json.loads(pos_json)
            tile_file_path = os.path.join(job_tile_cache_path, f"tile_{tile_id}.jpg")
            
            tile_img = cv2.imread(tile_file_path)
            if tile_img is None:
                raise ValueError(f"Missing tile file: {tile_file_path}")
            
            h, w, _ = tile_img.shape
            final_image[position[1]:position[1] + h, position[0]:position[0] + w] = tile_img
        
        # 5. Save the final image
        result_filename = f"{job_id}_processed.jpg"
        result_path = os.path.join(app.config['PROCESSED_FOLDER'], result_filename)
        cv2.imwrite(result_path, final_image)
        
        # 6. Update job status in Redis
        r.hset(job_key, mapping={
            'status': 'completed',
            'result_url': f"/processed/{result_filename}"
        })
        
        # 7. Clean up
        r.delete(job_tile_key) # Delete tile position hash
        shutil.rmtree(job_tile_cache_path) # Delete temporary tile folder
        
        print(f"Job {job_id}: Reconstruction complete.")
        
    except Exception as e:
        r.hset(job_key, mapping={'status': 'failed', 'error': f"Reconstruction failed: {e}"})
        print(f"Job {job_id}: Reconstruction failed: {e}")

# --- Master Node: Result Consumption (Modified for Redis) ---
def result_consumer_thread():
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'master-result-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([RESULT_TOPIC])
    print("Result consumer thread started, connected to Redis...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Result Consumer error: {msg.error()}")
            continue

        try:
            result = json.loads(msg.value().decode('utf-8'))
            job_id = result['job_id']
            job_key = f"job:status:{job_id}"

            # Check if we are still processing this job
            job_status = r.hget(job_key, "status")
            if job_status != 'processing':
                continue # Job is already completed, failed, or unknown

            # --- NEW: Store tile on disk, not in memory ---
            tile_data = np.frombuffer(bytes(result['processed_tile_data'], 'latin-1'), dtype=np.uint8)
            processed_tile = cv2.imdecode(tile_data, cv2.IMREAD_COLOR)
            
            job_tile_cache_path = os.path.join(app.config['TILE_CACHE_FOLDER'], job_id)
            tile_save_path = os.path.join(job_tile_cache_path, f"tile_{result['tile_id']}.jpg")
            
            cv2.imwrite(tile_save_path, processed_tile)
            
            # --- NEW: Use atomic increment for counter ---
            # r.hincrby() safely increments a field in a hash
            processed_count = r.hincrby(job_key, "tiles_processed_count", 1)
            
            total_tiles = int(r.hget(job_key, "total_tiles"))
            
            # 3. Check if job is complete
            if processed_count == total_tiles:
                print(f"Job {job_id}: All tiles received. Triggering reconstruction.")
                r.hset(job_key, "status", "reconstructing")
                # Run reconstruction in a new thread to not block the consumer
                threading.Thread(target=reconstruct_image, args=(job_id,)).start()
        
        except Exception as e:
            print(f"Error processing result: {e}")

# --- Master Node: Heartbeat Monitoring (Modified for Redis) ---
def heartbeat_consumer_thread():
    """
    Runs in a separate thread, consuming from the 'heartbeats' topic
    to monitor worker health.
    """
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'master-heartbeat-group',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([HEARTBEAT_TOPIC])
    print("Heartbeat consumer thread started, connected to Redis...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Heartbeat Consumer error: {msg.error()}")
            continue
        
        try:
            heartbeat = json.loads(msg.value().decode('utf-8'))
            
            # --- UPDATED: Use Redis HASH ---
            # This creates a hash called 'workers:status'
            # and sets a field (the worker_id) to the current timestamp.
            # This entry will NOT expire, so we always have the last-seen time.
            r.hset("workers:status", heartbeat['worker_id'], heartbeat['timestamp'])
        
        except Exception as e:
            print(f"Error processing heartbeat: {e}")


# --- Client UI: Flask Endpoints (Modified for Redis) ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_image():
    if 'image' not in request.files:
        return jsonify({'error': 'No image file provided'}), 400
    
    file = request.files['image']
    processing_type = request.form.get('processing_type', 'grayscale')
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file:
        job_id = str(uuid.uuid4())
        filename = f"{job_id}_{file.filename}"
        image_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(image_path)
        
        threading.Thread(
            target=split_and_publish_tasks, 
            args=(image_path, job_id, processing_type)
        ).start()
        
        return jsonify({'job_id': job_id, 'status': 'submitted'})

@app.route('/status/<job_id>')
def get_job_status(job_id):
    """
    Allows the UI to poll for job status from Redis.
    """
    # --- NEW: Get job data from Redis Hash ---
    job_key = f"job:status:{job_id}"
    job_data = r.hgetall(job_key)
    
    if not job_data:
        return jsonify({'error': 'Job not found'}), 404
    
    status = job_data.get('status')
    
    if status == 'processing' or status == 'reconstructing':
        return jsonify({
            'status': status,
            'total_tiles': int(job_data.get('total_tiles', 0)),
            'tiles_processed': int(job_data.get('tiles_processed_count', 0))
        })
    elif status == 'completed':
        return jsonify({
            'status': 'completed',
            'result_url': job_data.get('result_url')
        })
    elif status == 'failed':
        return jsonify({
            'status': 'failed',
            'error': job_data.get('error', 'Unknown error')
        })
    else:
        return jsonify({'status': 'unknown', 'error': 'Unknown job status'})

@app.route('/processed/<filename>')
def get_processed_image(filename):
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename)

@app.route('/dashboard')
def get_dashboard_data():
    """
    Provides data for the monitoring dashboard from Redis.
    """
    # --- UPDATED: Get ALL workers from the HASH ---
    # This fetches the entire 'workers:status' hash
    all_workers_status = r.hgetall("workers:status")
    
    # Convert timestamps from strings (Redis stores strings) to floats
    # This is what the JavaScript expects
    workers_with_timestamps = {
        worker_id: float(timestamp) 
        for worker_id, timestamp in all_workers_status.items()
    }
        
    return jsonify(workers_with_timestamps)

# --- Main Application Runner ---
if __name__ == '__main__':
    threading.Thread(target=result_consumer_thread, daemon=True).start()
    threading.Thread(target=heartbeat_consumer_thread, daemon=True).start()
    app.run(debug=True, port=5000, use_reloader=False)