"""
Airflow DAG: Weekly Delta Lake Maintenance
Runs VACUUM, OPTIMIZE, and statistics computation.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator
)
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["alerts@company.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="delta_maintenance",
    default_args=default_args,
    description="Weekly Delta Lake VACUUM, OPTIMIZE, and stats",
    schedule_interval="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "maintenance", "delta-lake"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_maintenance = DatabricksRunNowOperator(
        task_id="run_delta_maintenance",
        databricks_conn_id="databricks",
        job_id="{{ var.value.delta_maintenance_job_id }}",
    )

    end = EmptyOperator(task_id="end")

    start >> run_maintenance >> end
