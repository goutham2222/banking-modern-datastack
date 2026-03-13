import os
import json
import time
import pandas as pd
import boto3
import signal
import sys
from datetime import datetime, timezone
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
BATCH_SIZE = 300  
TIME_THRESHOLD = 30  

TOPICS = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    'banking_server.public.locations',
    'banking_server.public.merchants'
]

# Initialize global buffer
buffer = {t: [] for t in TOPICS}

def flush_to_minio(topic, records):
    if not records:
        return
    table_name = topic.split('.')[-1]
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S%f")
    df = pd.DataFrame(records)
    filename = f"{table_name}_{ts_str}.parquet"
    df.to_parquet(filename, index=False)
    s3_key = f"{table_name}/{filename}" 
    s3.upload_file(filename, os.getenv("MINIO_BUCKET"), s3_key)
    os.remove(filename)
    print(f"✅ [FLUSH] {len(records)} records for {table_name} -> {s3_key}")

def handle_exit(signum, frame):
    """Ensures data in buffer is saved before the process exits."""
    print(f"\n🛑 Signal {signum} received. Cleaning up...")
    for t in buffer:
        if buffer[t]:
            flush_to_minio(t, buffer[t])
    print("Done. Exiting.")
    sys.exit(0)

# Register shutdown signals
signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)

consumer = KafkaConsumer(
    *TOPICS,
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

last_flush_time = time.time()

try:
    print(f"🚀 Streaming started. Monitoring {len(TOPICS)} topics...")
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        
        for tp, messages in msg_pack.items():
            for message in messages:
                topic = message.topic
                payload = message.value.get("payload", {})
                operation = payload.get("op")
                record_data = payload.get("after") if payload.get("after") else payload.get("before")
                
                if record_data:
                    record_data['cdc_operation'] = operation
                    record_data['stream_timestamp'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
                    buffer[topic].append(record_data)               

        current_time = time.time()
        if any(len(b) >= BATCH_SIZE for b in buffer.values()) or (current_time - last_flush_time) > TIME_THRESHOLD:
            for t in buffer:
                if buffer[t]:
                    flush_to_minio(t, buffer[t])
                    buffer[t] = []
            last_flush_time = current_time

except Exception as e:
    print(f"❌ Error: {e}")
    handle_exit(None, None)