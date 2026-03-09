import os
import json
import time
import pandas as pd
import boto3
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
BATCH_SIZE = 75
TIME_THRESHOLD = 60  # Seconds

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

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")
# Initialize buffers for each topic
buffer = {t: [] for t in [
    'banking_server.public.customers', 
    'banking_server.public.accounts', 
    'banking_server.public.transactions'
]}
last_flush_time = time.time()

def flush_to_minio(topic, records):
    if not records:
        return
    
    table_name = topic.split('.')[-1]
    date_str = datetime.now().strftime('%Y-%m-%d')
    ts_str = datetime.now().strftime("%H%M%S%f")
    
    df = pd.DataFrame(records)
    filename = f"{table_name}_{ts_str}.parquet"
    
    # Save locally then upload to MinIO
    df.to_parquet(filename, index=False)
    s3_key = f"{table_name}/{date_str}/{filename}"
    s3.upload_file(filename, bucket, s3_key)
    os.remove(filename)
    print(f"✅ [FLUSH] {len(records)} records for {table_name} -> {s3_key}")

print("📡 Streamer is active. Listening for CDC events...")

try:
    for message in consumer:
        topic = message.topic
        event_value = message.value
        
        # Debezium structure: payload contains 'before', 'after', and 'op'
        payload = event_value.get("payload", {})
        operation = payload.get("op") # c=create, u=update, d=delete
        
        # We want the 'after' state for inserts/updates
        # If it's a delete, 'after' is null, so we take 'before'
        record_data = payload.get("after") if payload.get("after") else payload.get("before")
        
        if record_data:
            # Add metadata so Snowflake/dbt knows WHAT happened to this row
            record_data['cdc_operation'] = operation
            record_data['stream_timestamp'] = datetime.now().isoformat()
            buffer[topic].append(record_data)

        # Check if we should flush based on count or time
        current_time = time.time()
        if len(buffer[topic]) >= BATCH_SIZE or (current_time - last_flush_time) > TIME_THRESHOLD:
            for t in buffer:
                if buffer[t]:
                    flush_to_minio(t, buffer[t])
                    buffer[t] = []
            last_flush_time = current_time

except KeyboardInterrupt:
    print("\nStopping streamer...")