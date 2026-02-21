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
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset = 'earliest',
    enable_auto_commit = True,
    group_id = os.getenv("KAFKA_GROUP"),
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
)

# Establishing connection with MinIO
s3 = boto3.client(
    's3',
    minio_endpoint = os.getenv("MINIO_ENDPOINT"),
    minio_access_key = os.getenv("MINIO_ACCESS_KEY"),
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
)

minio_bucket = os.getenv("MINIO_BUCKET")

# Bucket Creation
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Writing changes to MinIO
def stream_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    
    s3_key = f'{table_name}/date = {date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(filepath, bucket, s3_key)
    os.remove(filepath)
    print(f'Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

