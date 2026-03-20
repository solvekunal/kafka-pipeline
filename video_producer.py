# import cv2
# import json
# import time
# from kafka import KafkaProducer
# import base64
# import tempfile
# import os

# # Kafka Configuration
# KAFKA_BROKER = 'localhost:29092'
# TOPIC_NAME = 'video-segments'

# # Video Configuration
# SEGMENT_DURATION = 60  # 1 minutes in seconds
# FPS = 20  # Frames per second
# FRAME_WIDTH = 640
# FRAME_HEIGHT = 480

# # Initialize Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers=[KAFKA_BROKER],
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     max_request_size=52428800  # 50MB for video chunks
# ) 

# # Initialize Camera
# camera = cv2.VideoCapture(0)
# camera.set(cv2.CAP_PROP_FRAME_WIDTH, FRAME_WIDTH)
# camera.set(cv2.CAP_PROP_FRAME_HEIGHT, FRAME_HEIGHT)
# camera.set(cv2.CAP_PROP_FPS, FPS)

# if not camera.isOpened():
#     print("Error: Could not open camera")
#     exit()

# print(f"Starting video capture - {SEGMENT_DURATION}s segments at {FPS} FPS...")
# segment_count = 0

# try:
#     while True:
#         segment_count += 1
#         start_time = time.time()
        
#         # Create temporary video file
#         temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
#         temp_path = temp_file.name
#         temp_file.close()
        
#         # Initialize video writer
#         fourcc = cv2.VideoWriter_fourcc(*'mp4v')
#         writer = cv2.VideoWriter(temp_path, fourcc, FPS, (FRAME_WIDTH, FRAME_HEIGHT))
        
#         print(f"\n=== Recording Segment {segment_count} ===")
#         frame_count = 0
        
#         # Record for SEGMENT_DURATION seconds
#         while (time.time() - start_time) < SEGMENT_DURATION:
#             ret, frame = camera.read()
            
#             if not ret:
#                 print("Failed to capture frame")
#                 break
            
#             writer.write(frame)
#             frame_count += 1
            
#             # Progress indicator every second
#             if frame_count % FPS == 0:
#                 elapsed = int(time.time() - start_time)
#                 remaining = SEGMENT_DURATION - elapsed
#                 print(f"Recording... {elapsed}s / {SEGMENT_DURATION}s (remaining: {remaining}s)", end='\r')
            
#             time.sleep(1 / FPS)
        
#         writer.release()
#         print(f"\nSegment {segment_count} recorded: {frame_count} frames")
        
#         # Read video file and encode to base64
#         with open(temp_path, 'rb') as f:
#             video_data = f.read()
        
#         video_base64 = base64.b64encode(video_data).decode('utf-8')
#         file_size_mb = len(video_data) / (1024 * 1024)
        
#         print(f"Encoded video size: {file_size_mb:.2f} MB")
        
#         # Create message payload
#         message = {
#             'segment_id': segment_count,
#             'timestamp': start_time,
#             'duration': SEGMENT_DURATION,
#             'fps': FPS,
#             'frame_count': frame_count,
#             'video_data': video_base64
#         }
        
#         # Send to Kafka
#         print("Sending to Kafka...")
#         producer.send(TOPIC_NAME, value=message)
#         producer.flush()
#         print(f"✓ Segment {segment_count} sent to Kafka")
        
#         # Clean up temp file
#         os.unlink(temp_path)
        
# except KeyboardInterrupt:
#     print("\n\nStopping producer...")
# finally:
#     camera.release()
#     producer.close()
#     print("Producer closed")









import cv2
import json
import time
from confluent_kafka import Producer, KafkaException
import base64
import tempfile
import os

# Kafka Configuration
KAFKA_BROKER = 'localhost:29092'
TOPIC_NAME = 'video-segments'

# Video Configuration
SEGMENT_DURATION = 60  # 1 minute in seconds
FPS = 20  # Frames per second
FRAME_WIDTH = 640
FRAME_HEIGHT = 480
MAX_MESSAGE_BYTES = 50 * 1024 * 1024  # 50MB

# Initialize Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'queue.buffering.max.messages': 10000,
    'message.max.bytes': MAX_MESSAGE_BYTES
}
producer = Producer(producer_conf)

# Delivery callback for Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for message: {err}")
    else:
        print(f"✓ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Initialize Camera
camera = cv2.VideoCapture(0)
camera.set(cv2.CAP_PROP_FRAME_WIDTH, FRAME_WIDTH)
camera.set(cv2.CAP_PROP_FRAME_HEIGHT, FRAME_HEIGHT)
camera.set(cv2.CAP_PROP_FPS, FPS)

if not camera.isOpened():
    print("Error: Could not open camera")
    exit()

print(f"Starting video capture - {SEGMENT_DURATION}s segments at {FPS} FPS...")
segment_count = 0

try:
    while True:
        segment_count += 1
        start_time = time.time()
        
        # Create temporary video file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
        temp_path = temp_file.name
        temp_file.close()
        
        # Initialize video writer
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        writer = cv2.VideoWriter(temp_path, fourcc, FPS, (FRAME_WIDTH, FRAME_HEIGHT))
        
        print(f"\n=== Recording Segment {segment_count} ===")
        frame_count = 0
        
        # Record for SEGMENT_DURATION seconds
        while (time.time() - start_time) < SEGMENT_DURATION:
            ret, frame = camera.read()
            
            if not ret:
                print("Failed to capture frame")
                break
            
            writer.write(frame)
            frame_count += 1
            
            # Progress indicator every second
            if frame_count % FPS == 0:
                elapsed = int(time.time() - start_time)
                remaining = SEGMENT_DURATION - elapsed
                print(f"Recording... {elapsed}s / {SEGMENT_DURATION}s (remaining: {remaining}s)", end='\r')
            
            time.sleep(1 / FPS)
        
        writer.release()
        print(f"\nSegment {segment_count} recorded: {frame_count} frames")
        
        # Read video file and encode to base64
        with open(temp_path, 'rb') as f:
            video_data = f.read()
        
        video_base64 = base64.b64encode(video_data).decode('utf-8')
        file_size_mb = len(video_data) / (1024 * 1024)
        print(f"Encoded video size: {file_size_mb:.2f} MB")
        
        # Create message payload
        message = {
            'segment_id': segment_count,
            'timestamp': start_time,
            'duration': SEGMENT_DURATION,
            'fps': FPS,
            'frame_count': frame_count,
            'video_data': video_base64
        }
        
        # Send to Kafka
        try:
            print("Sending to Kafka...")
            producer.produce(TOPIC_NAME, value=json.dumps(message).encode('utf-8'), callback=delivery_report)
            producer.flush()  # Wait for all messages to be delivered
            print(f"✓ Segment {segment_count} sent to Kafka")
        except KafkaException as e:
            print(f"❌ Failed to send segment {segment_count}: {e}")
        
        # Clean up temp file
        os.unlink(temp_path)
        
except KeyboardInterrupt:
    print("\n\nStopping producer...")
finally:
    camera.release()
    producer.flush()
    print("Producer closed")