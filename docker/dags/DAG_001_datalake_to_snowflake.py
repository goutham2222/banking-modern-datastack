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

# Establishing Snowflake Connection
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
snowflake_db = os.getenv("SNOWFLAKE_DB")
snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")

tables = ['customers', 'accounts', 'transactions']

# Download data from Data Lake
def download_from_data_lake():
    os.makedirs(local_dir, exist_ok=True)
    s3.boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    local_files = {}
    for table in tables:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=minio_bucke, Prefix=prefix)
        objects = resp.get('Contents', [])
        local_files[table] = []
        for obj in objects:
            key = obj['Key']
            local_file = os.path.join(local_dir, os.path.basename(key))
            s3.download_file(minio_bucket, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)
    return local_files

def upload_data_to_snowflake():
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    
    if not local_files:
        print("No files found in MinIO.")
        return
    
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    for table, files in local_files.items():
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        for f in files:
            cur.execute(f"PUT file://{f} @%{table}")
            print(f"Uploaded {f} -> @{table} stage")

        copy_sql = f"""
        COPY INTO {table}
        FROM @%{table}
        FILE_FORMAT=(TYPE=PARQUET)
        ON_ERROR='CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {table}")

    cur.close()
    conn.close()