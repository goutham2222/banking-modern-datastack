import os
from dotenv import load_dotenv
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import snowflake.connector

load_dotenv()

# Establishing MinIO Connection
minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket = os.getenv("MINIO_BUCKET")
local_dir = os.getenv("MINIO_LOCAL_DIR")