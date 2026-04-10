import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
import signal
import sys
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Kafka consumer settings
consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=30000  # Stop after 30s of no new messages
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)


def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')


def extract_record(event):
    """Handle both schema-envelope and plain formats."""
    if "payload" in event:
        return event["payload"].get("after")
    return event.get("after")


def flush_all():
    """Flush any remaining records in the buffer."""
    for topic, records in buffer.items():
        table_name = topic.split('.')[-1]
        write_to_minio(table_name, records)
    print("✅ Flushed all remaining records.")


# Graceful shutdown
def shutdown(sig, frame):
    flush_all()
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# Batch consume
batch_size = 50
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")

try:
    for message in consumer:
        topic = message.topic
        event = message.value
        record = extract_record(event)

        if record:
            buffer[topic].append(record)
            print(f"[{topic}] -> {record}")

        if len(buffer[topic]) >= batch_size:
            write_to_minio(topic.split('.')[-1], buffer[topic])
            buffer[topic] = []
except StopIteration:
    pass
finally:
    flush_all()
    consumer.close()
    print("✅ Consumer stopped.")