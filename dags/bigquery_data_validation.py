from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bigquery_plugin import (
    BigQueryDataValidationOperator,
    BigQueryDatasetSensor,
)

default_arguments = {"owner": "Leo Arruda", "start_date": days_ago(1)}
PROJECT_ID = "leo-airflow-tutorial-demo"

with DAG(
    "bigquery_data_validation",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={"project": "leo-airflow-tutorial-demo"},
) as dag:

    is_table_empty = BigQueryDataValidationOperator(
        task_id="is_table_empty",
        sql="SELECT COUNT(*) FROM `{{ project }}.vehicle_analytics.history`",
        location="us-east1",
    )

    dataset_exists = BigQueryDatasetSensor(
        task_id="dataset_exists",
        project_id="{{ project }}",
        dataset_id="vehicle_analytics",
    )

dataset_exists >> is_table_empty
