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
    # REMOVED date_str from path to match Airflow's expected landing structure
    ts_str = datetime.now().strftime("%H%M%S%f")
    
    df = pd.DataFrame(records)
    filename = f"{table_name}_{ts_str}.parquet"
    
    df.to_parquet(filename, index=False)
    # Landing path is now flat: table_name/filename.parquet
    s3_key = f"{table_name}/{filename}" 
    s3.upload_file(filename, bucket, s3_key)
    os.remove(filename)
    print(f"✅ [FLUSH] {len(records)} records for {table_name} -> {s3_key}")

try:
    # Use consumer.poll to avoid getting stuck when no messages are coming in
    while True:
        msg_pack = consumer.poll(timeout_ms=1000) # Check every second
        
        for tp, messages in msg_pack.items():
            for message in messages:
                topic = message.topic
                payload = message.value.get("payload", {})
                operation = payload.get("op")
                
                record_data = payload.get("after") if payload.get("after") else payload.get("before")
                
                if record_data:
                    record_data['cdc_operation'] = operation
                    record_data['stream_timestamp'] = datetime.now().isoformat()
                    buffer[topic].append(record_data)

        # Now this check runs even if no NEW messages arrived this second
        current_time = time.time()
        # Flush if ANY buffer is full OR time has passed
        should_flush = any(len(b) >= BATCH_SIZE for b in buffer.values()) or (current_time - last_flush_time) > TIME_THRESHOLD
        
        if should_flush:
            for t in buffer:
                if buffer[t]:
                    flush_to_minio(t, buffer[t])
                    buffer[t] = []
            last_flush_time = current_time

except KeyboardInterrupt:
    # FINAL FLUSH on shutdown so you don't lose data!
    print("\nShutting down... performing final flush.")
    for t in buffer:
        flush_to_minio(t, buffer[t])
    print("Done.")