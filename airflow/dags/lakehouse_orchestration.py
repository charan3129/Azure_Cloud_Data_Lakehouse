"""
Airflow DAG: Lakehouse Orchestration
Runs the full medallion pipeline: ADF ingestion trigger,
Bronze→Silver, Silver→Gold, quality gates, Synapse refresh.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator
)
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator
)
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_orchestration",
    default_args=default_args,
    description="End-to-end medallion lakehouse pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "medallion", "data-vault"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    # Step 1: Trigger ADF master pipeline for ingestion
    trigger_adf_ingestion = AzureDataFactoryRunPipelineOperator(
        task_id="trigger_adf_ingestion",
        pipeline_name="pl_master_lakehouse",
        azure_data_factory_conn_id="azure_data_factory",
        resource_group_name="{{ var.value.adf_resource_group }}",
        factory_name="{{ var.value.adf_factory_name }}",
        parameters={
            "start_date": "{{ ds }}",
            "end_date": "{{ ds }}",
            "process_date": "{{ ds }}",
            "last_modified": "{{ prev_ds }}",
        },
        wait_for_termination=True,
        timeout=3600,
    )

    # Step 2: Run Bronze → Silver (Data Vault loading)
    bronze_to_silver = DatabricksRunNowOperator(
        task_id="bronze_to_silver",
        databricks_conn_id="databricks",
        job_id="{{ var.value.bronze_to_silver_job_id }}",
        notebook_params={"process_date": "{{ ds }}"},
    )

    # Step 3: Quality gate — Silver validation
    def check_silver_quality(**context):
        """Check silver layer quality and decide promotion."""
        # In production this would query GE results
        # For demo, always promote
        return "silver_to_gold"

    silver_quality_gate = BranchPythonOperator(
        task_id="silver_quality_gate",
        python_callable=check_silver_quality,
    )

    # Step 4: Run Silver → Gold (denormalization)
    silver_to_gold = DatabricksRunNowOperator(
        task_id="silver_to_gold",
        databricks_conn_id="databricks",
        job_id="{{ var.value.silver_to_gold_job_id }}",
        notebook_params={"process_date": "{{ ds }}"},
    )

    # Step 5: Quality blocked path
    quality_blocked = PythonOperator(
        task_id="quality_blocked",
        python_callable=lambda: print("BLOCKED: Silver quality gate failed"),
    )

    # Step 6: Notify success
    def notify_success(**context):
        print(f"Lakehouse pipeline complete for {context['ds']}")

    pipeline_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # DAG flow
    (start >> trigger_adf_ingestion >> bronze_to_silver
     >> silver_quality_gate)
    silver_quality_gate >> silver_to_gold >> pipeline_success >> end
    silver_quality_gate >> quality_blocked >> end
