import os
import shutil
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

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

def check_infra_exists():
    """Checks Snowflake for the existence of the RAW schema to decide if init is needed."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW SCHEMAS LIKE 'RAW' IN DATABASE {SNOWFLAKE_CONN['database']}")
        result = cur.fetchone()
        if result:
            return "skip_init"
        return "initialize_snowflake"
    finally:
        cur.close()
        conn.close()

def init_snowflake_infrastructure():
    """Runs DDL to create Database, Schemas, and Variant tables with dedicated Stages."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_CONN['database']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_CONN['database']}.RAW")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_CONN['database']}.ANALYTICS")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_CONN['database']}.RAW")
        
        for table in TABLES:
            t_upper = table.upper()
            # 1. Create the Raw Table
            cur.execute(f"CREATE TABLE IF NOT EXISTS {t_upper} (v variant)")
            # 2. Create a Named Stage (Avoids quoting errors with @% syntax)
            cur.execute(f"CREATE STAGE IF NOT EXISTS STAGE_{t_upper}")
        print("✅ Tables and Named Stages verified.")
    finally:
        cur.close()
        conn.close()

def download_and_archive_minio():
    """Handles Pre-Cleanup of local storage and moves data from Landing to Archive."""
    if os.path.exists(LOCAL_DIR):
        shutil.rmtree(LOCAL_DIR)
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, 
                      aws_access_key_id=MINIO_ACCESS_KEY, 
                      aws_secret_access_key=MINIO_SECRET_KEY)
    
    partition_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    downloaded_files = {t: [] for t in TABLES}
    
    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        
        if 'Contents' not in resp: continue

        for obj in resp['Contents']:
            key = obj['Key']
            if key == prefix: continue 
            
            filename = os.path.basename(key)
            local_path = os.path.join(LOCAL_DIR, filename)
            
            s3.download_file(MINIO_BUCKET, key, local_path)
            downloaded_files[table].append(local_path)
            
            archive_key = f"archives/{table}/{partition_date}/{filename}"
            s3.copy_object(Bucket=MINIO_BUCKET, CopySource={'Bucket': MINIO_BUCKET, 'Key': key}, Key=archive_key)
            s3.delete_object(Bucket=MINIO_BUCKET, Key=key)
            
    return downloaded_files

def load_to_snowflake(**kwargs):
    """Performs Bulk PUT and COPY INTO operations using the Named Stages."""
    ti = kwargs['ti']
    files_map = ti.xcom_pull(task_ids='download_minio')
    
    if not files_map or all(not f for f in files_map.values()):
        print("No new data to load.")
        return

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur = conn.cursor()
    try:
        cur.execute(f"USE SCHEMA {SNOWFLAKE_CONN['database']}.RAW")
        for table, files in files_map.items():
            if not files: 
                continue
            
            t_upper = table.upper()
            # We must use the name created in the init step
            stage_name = f"STAGE_{t_upper}"
            
            for f in files:
                # Use @STAGE_NAME (Named Stage) instead of @% (Table Stage)
                cur.execute(f"PUT file://{f} @{stage_name} AUTO_COMPRESS=TRUE")
            
            # COPY INTO from the Named Stage
            cur.execute(f"COPY INTO {t_upper} (v) FROM @{stage_name} FILE_FORMAT = (TYPE = PARQUET) PURGE = TRUE")
            
            print(f"🚀 Bulk Load Complete for {t_upper}")

            for f in files:
                if os.path.exists(f): 
                    os.remove(f)
    finally:
        cur.close()
        conn.close()

# --- DAG Definition ---

with DAG(
    dag_id="minio_to_snowflake_banking",
    schedule_interval="*/2 * * * *", 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}
) as dag:

    task_check_infra = BranchPythonOperator(task_id="check_infra", python_callable=check_infra_exists)
    task_init = PythonOperator(task_id="initialize_snowflake", python_callable=init_snowflake_infrastructure)
    task_skip_init = EmptyOperator(task_id="skip_init")

    task_download = PythonOperator(
        task_id="download_minio", 
        python_callable=download_and_archive_minio,
        trigger_rule="none_failed_min_one_success"
    )

    task_load = PythonOperator(task_id="load_snowflake", python_callable=load_to_snowflake)

    # Dependency Chain
    task_check_infra >> [task_init, task_skip_init]
    [task_init, task_skip_init] >> task_download >> task_load