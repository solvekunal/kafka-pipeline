
from confluent_kafka.admin import AdminClient, NewTopic
import time

KAFKA_BROKER = 'localhost:29092'
TOPIC_NAME = 'video-segments'

# Admin client configuration
admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})

# Define topic
topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)

# Create topic
fs = admin_client.create_topics([topic])

for topic, f in fs.items():
    try:
        f.result()  # Wait for creation
        print(f"Topic '{topic}' created successfully")
    except Exception as e:
        print(f"Topic '{topic}' creation failed: {e}")