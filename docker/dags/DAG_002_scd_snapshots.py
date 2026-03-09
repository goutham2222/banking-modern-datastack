from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
}

with DAG (
    dag_id = 'DAG_002_scd_snapshots',
    default_args = default_args,
    description = 'Run dbt snapshots for capturing Slowly Changing Dimensions',
    schedule = '@daily',
    start_date = datetime(2026, 3, 8),
    catchup= False,
    tags= ['dbt', 'Snapshots'],
) as dag:

    dbt_snapshot = BashOperator(
        task_id = 'dbt_snapshot',
        bash_command = 'cd /opt/airflow/banking_dbt && dbt snapshot --profiles-dir /home/airflow/.dbt'
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/banking_dbt && dbt run --select marts --profiles-dir /home/airflow/.dbt"
    )

    dbt_snapshot >> dbt_run_marts