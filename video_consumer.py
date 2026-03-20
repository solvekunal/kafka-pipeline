# import json
# import base64
# from kafka import KafkaConsumer
# from minio import Minio
# from minio.error import S3Error
# from io import BytesIO
# from datetime import datetime

# # Kafka Configuration
# KAFKA_BROKER = 'localhost:29092'
# TOPIC_NAME = 'video-segments'
# GROUP_ID = 'video-consumer-group'

# # MinIO Configuration
# MINIO_ENDPOINT = 'localhost:9000'
# MINIO_ACCESS_KEY = 'minioadmin'
# MINIO_SECRET_KEY = 'minioadmin'
# BUCKET_NAME = 'camera-videos'

# # Initialize MinIO client
# minio_client = Minio(
#     MINIO_ENDPOINT,
#     access_key=MINIO_ACCESS_KEY,
#     secret_key=MINIO_SECRET_KEY,
#     secure=False
# )

# # Create bucket if it doesn't exist
# try:
#     if not minio_client.bucket_exists(BUCKET_NAME):
#         minio_client.make_bucket(BUCKET_NAME)
#         print(f"Bucket '{BUCKET_NAME}' created")
#     else:
#         print(f"Bucket '{BUCKET_NAME}' already exists")
# except S3Error as e:
#     print(f"Error creating bucket: {e}")

# # Initialize Kafka Consumer
# consumer = KafkaConsumer(
#     TOPIC_NAME,
#     bootstrap_servers=[KAFKA_BROKER],
#     group_id=GROUP_ID,
#     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#     auto_offset_reset='latest',
#     enable_auto_commit=True,
#     max_partition_fetch_bytes=52428800  # 50MB
# )

# print("Video consumer started. Waiting for video segments...")

# try:
#     for message in consumer:
#         video_data = message.value
        
#         segment_id = video_data['segment_id']
#         timestamp = video_data['timestamp']
#         duration = video_data['duration']
#         frame_count = video_data['frame_count']
#         video_base64 = video_data['video_data']
        
#         print(f"\nReceived segment {segment_id} ({frame_count} frames, {duration}s)")
        
#         # Decode base64 video
#         video_bytes = base64.b64decode(video_base64)
#         video_size_mb = len(video_bytes) / (1024 * 1024)
        
#         print(f"Video size: {video_size_mb:.2f} MB")
        
#         # Create object name with timestamp
#         dt = datetime.fromtimestamp(timestamp)
#         object_name = f"videos/{dt.strftime('%Y%m%d')}/segment_{segment_id}_{dt.strftime('%H%M%S')}.mp4"
        
#         # Upload to MinIO
#         try:
#             print("Uploading to MinIO...")
#             minio_client.put_object(
#                 BUCKET_NAME,
#                 object_name,
#                 BytesIO(video_bytes),
#                 length=len(video_bytes),
#                 content_type='video/mp4'
#             )
#             print(f"✓ Stored segment {segment_id} as {object_name}")
#         except S3Error as e:
#             print(f"Error storing segment {segment_id}: {e}")
            
# except KeyboardInterrupt:
#     print("\nStopping consumer...")
# finally:
#     consumer.close()
#     print("Consumer closed")









import json
import base64
from confluent_kafka import Consumer, KafkaException
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from datetime import datetime
import sys

# Kafka Configuration
KAFKA_BROKER = 'localhost:29092'
TOPIC_NAME = 'video-segments'
GROUP_ID = 'video-consumer-group'

# MinIO Configuration
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'camera-videos'

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create bucket if it doesn't exist
try:
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created")
    else:
        print(f"Bucket '{BUCKET_NAME}' already exists")
except S3Error as e:
    print(f"Error creating bucket: {e}")

# Initialize Kafka Consumer
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'latest',  # start from latest messages
    'enable.auto.commit': True,
    'max.partition.fetch.bytes': 52428800  # 50MB
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_NAME])

print("Video consumer started. Waiting for video segments...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # poll for messages
        if msg is None:
            continue
        if msg.error():
            # Handle error properly
            raise KafkaException(msg.error())

        try:
            # Deserialize JSON message
            video_data = json.loads(msg.value().decode('utf-8'))

            segment_id = video_data['segment_id']
            timestamp = video_data['timestamp']
            duration = video_data['duration']
            frame_count = video_data['frame_count']
            video_base64 = video_data['video_data']

            print(f"\nReceived segment {segment_id} ({frame_count} frames, {duration}s)")

            # Decode base64 video
            video_bytes = base64.b64decode(video_base64)
            video_size_mb = len(video_bytes) / (1024 * 1024)
            print(f"Video size: {video_size_mb:.2f} MB")

            # Create object name with timestamp
            dt = datetime.fromtimestamp(timestamp)
            object_name = f"videos/{dt.strftime('%Y%m%d')}/segment_{segment_id}_{dt.strftime('%H%M%S')}.mp4"

            # Upload to MinIO
            try:
                print("Uploading to MinIO...")
                minio_client.put_object(
                    BUCKET_NAME,
                    object_name,
                    BytesIO(video_bytes),
                    length=len(video_bytes),
                    content_type='video/mp4'
                )
                print(f"✓ Stored segment {segment_id} as {object_name}")
            except S3Error as e:
                print(f"Error storing segment {segment_id}: {e}")

        except Exception as e:
            print(f"Error processing message: {e}", file=sys.stderr)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    print("Consumer closed")