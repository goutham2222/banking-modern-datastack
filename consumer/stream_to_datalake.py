import os
from dotenv import load_dotenv
import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime

load_dotenv()

consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Establishing connection with MinIO
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

minio_bucket = os.getenv("MINIO_BUCKET")

# MinIO Bucket Creation
if minio_bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=minio_bucket)

# Writing changes to MinIO
def stream_to_minio(table_name, records):
    if not records:
        return
    
    df = pd.DataFrame(records)
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = f'{table_name}_{date_str}.parquet'
    df.to_parquet(filepath, engine='fastparquet', index=False)
    
    s3_key = f'{table_name}/{date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(filepath, minio_bucket, s3_key)
    os.remove(filepath)
    print(f'Uploaded {len(records)} records to s3://{minio_bucket}/{s3_key}')

# Batch consumption
batch_size = 75
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("Connected to Kafka. Listening for messages...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})

    # Capture 'after' for inserts/updates, or 'before' for deletes
    record = payload.get("after")

    if record:
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")  # For debugging purpose

    if len(buffer[topic]) >= batch_size:
        stream_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []