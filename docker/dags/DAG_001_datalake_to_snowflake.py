import os
from dotenv import load_dotenv
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import snowflake.connector

# Load environment variables from .env
load_dotenv()

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/airflow_landing")

SNOWFLAKE_CONN = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

TABLES = ['customers', 'accounts', 'transactions', 'locations', 'merchants']

# --- Tasks ---

def init_snowflake_infrastructure():
    """Ensures Database, Schemas, and Variant tables exist before loading."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur = conn.cursor()
    
    try:
        # 1. Create Database and Schemas
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_CONN['database']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_CONN['database']}.RAW")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_CONN['database']}.ANALYTICS")
        
        # 2. Use the correct context
        cur.execute(f"USE SCHEMA {SNOWFLAKE_CONN['database']}.RAW")
        
        # 3. Create Tables with Variant column 'v'
        for table in TABLES:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (v variant)")
            
        print("✅ Snowflake Infrastructure Verified/Created")
    finally:
        cur.close()
        conn.close()

def download_and_archive_minio():
    """Downloads files from landing, then moves them to partitioned archives."""
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    partition_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    downloaded_files = {t: [] for t in TABLES}
    
    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix, Delimiter='/')
        
        if 'Contents' not in resp:
            continue

        for obj in resp['Contents']:
            key = obj['Key']
            if key == prefix: continue 
            
            filename = os.path.basename(key)
            local_path = os.path.join(LOCAL_DIR, filename)
            
            s3.download_file(MINIO_BUCKET, key, local_path)
            downloaded_files[table].append(local_path)
            
            archive_key = f"archives/{table}/{partition_date}/{filename}"
            s3.copy_object(
                Bucket=MINIO_BUCKET,
                CopySource={'Bucket': MINIO_BUCKET, 'Key': key},
                Key=archive_key
            )
            
            s3.delete_object(Bucket=MINIO_BUCKET, Key=key)
            print(f"✅ Processed and Archived: {key} -> {archive_key}")
            
    return downloaded_files

def load_to_snowflake(**kwargs):
    """Optimized Bulk Load into Snowflake Stage and Table."""
    ti = kwargs['ti']
    files_map = ti.xcom_pull(task_ids='download_minio')
    
    if not files_map or all(not f for f in files_map.values()):
        print("No new data to load.")
        return

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur = conn.cursor()

    try:
        # Ensure we are in the RAW schema for the load
        cur.execute(f"USE SCHEMA {SNOWFLAKE_CONN['database']}.RAW")

        for table, files in files_map.items():
            if not files:
                continue
            
            # Step A: Bulk PUT all files for this table into its named stage
            for f in files:
                cur.execute(f"PUT file://{f} @%{table} AUTO_COMPRESS=TRUE")
            
            # Step B: Single Bulk COPY for the whole table
            copy_sql = f"""
            COPY INTO {table} (v)
            FROM @%{table}
            FILE_FORMAT = (TYPE = PARQUET)
            PURGE = TRUE;
            """
            cur.execute(copy_sql)
            print(f"🚀 Bulk Load Complete for {table}")

            # Step C: Cleanup Local Airflow Storage
            for f in files:
                if os.path.exists(f):
                    os.remove(f)

    finally:
        cur.close()
        conn.close()

# --- DAG Definition ---
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    description="Automated Snowflake Infra Setup and MinIO to Snowflake Parquet Loading",
    schedule_interval="*/2 * * * *", 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    task_init = PythonOperator(
        task_id="initialize_snowflake",
        python_callable=init_snowflake_infrastructure
    )

    task_download = PythonOperator(
        task_id="download_minio",
        python_callable=download_and_archive_minio
    )

    task_load = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake
    )

    # Dependency Chain
    task_init >> task_download >> task_load