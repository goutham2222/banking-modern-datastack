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
    description='Weekly SCD Snapshots and Analytics Marts Refresh',
    schedule='0 0 * * 1', 
    start_date=datetime(2026, 3, 8),
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'Banking', 'SCD'],
) as dag:

    # 1. dbt run (Staging Models Only)
    # We build the staging layer in Snowflake first so the snapshot has something to look at.
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select staging.* --target snowflake --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 2. dbt snapshot
    # Now that staging is ready in Snowflake, we capture the SCD Type 2 history.
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt snapshot --target snowflake --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 3. dbt run (Marts & Dimensions)
    # Finally, build the final analytics tables using the newly updated snapshots.
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select marts.* --target snowflake --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 4. dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --select staging.* marts.* --target snowflake --profiles-dir {DBT_PROFILES_DIR}'
    )

    # New Dependency Chain
    dbt_run_staging >> dbt_snapshot >> dbt_run_marts >> dbt_test