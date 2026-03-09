import os
from dotenv import load_dotenv
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import snowflake.connector

load_dotenv()

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR")

SNOWFLAKE_CONN = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

TABLES = ['customers', 'accounts', 'transactions']

def download_from_minio():
    """Downloads new files from MinIO to Airflow local storage."""
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    downloaded_files = {t: [] for t in TABLES}
    
    for table in TABLES:
        # We only look for files in the table's specific folder
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        
        for obj in resp.get('Contents', []):
            key = obj['Key']
            local_path = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(MINIO_BUCKET, key, local_path)
            downloaded_files[table].append(local_path)
            
            # OPTIONAL: Move file to an 'archive' folder in MinIO here 
            # to prevent re-processing in the next DAG run.
            
    return downloaded_files

def load_to_snowflake(**kwargs):
    """Uploads local files to Snowflake stages and loads into VARIANT tables."""
    ti = kwargs['ti']
    files_to_load = ti.xcom_pull(task_ids='download_minio')
    
    if not files_to_load:
        print("No new files found to load.")
        return

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur = conn.cursor()

    try:
        for table, files in files_to_load.items():
            for f in files:
                # 1. PUT the file into the table's named stage
                cur.execute(f"PUT file://{f} @%{table} AUTO_COMPRESS=TRUE")
                print(f"Staged {f} to @%{table}")
                
                # 2. COPY INTO the variant column 'v'
                # We use MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE for Parquet flexibility
                copy_sql = f"""
                COPY INTO {table} (v)
                FROM @%{table}
                FILE_FORMAT = (TYPE = PARQUET)
                PURGE = TRUE;
                """
                cur.execute(copy_sql)
                print(f"Loaded {f} into {table} table.")
                
                # 3. Clean up local file after successful load
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
    schedule_interval="*/1 * * * *", # Runs every minute
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['MinIO', 'Snowflake', 'CDC']
) as dag:

    task_download = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio
    )

    task_load = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True
    )

    task_download >> task_load