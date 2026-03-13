from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Path configuration
DBT_PROJECT_DIR = "/opt/airflow/banking_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='DAG_002_dbt_weekly',
    default_args=default_args,
    description='Automated Seed, Staging, Snapshots, and Marts',
    schedule='0 0 * * 1',
    start_date=datetime(2026, 3, 8),
    catchup=False,
    tags=['dbt', 'Banking', 'SCD'],
) as dag:

    # 1. dbt seed (Crucial for your -1 and -2 IDs to work!)
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 2. dbt run staging
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 3. dbt snapshot
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 4. dbt run marts
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}'
    )

    # Dependency Chain
    dbt_seed >> dbt_run_staging >> dbt_snapshot >> dbt_run_marts