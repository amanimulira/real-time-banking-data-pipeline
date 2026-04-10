from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

DBT_DIR = "/opt/airflow/banking_dbt"
PROFILES_DIR = "/home/airflow/.dbt"

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir {PROFILES_DIR}",
    )
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts --profiles-dir {PROFILES_DIR}",
    )
    dbt_snapshot >> dbt_run_marts