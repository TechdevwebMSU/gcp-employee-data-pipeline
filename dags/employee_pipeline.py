
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.getenv(
    "GCP_PROJECT_ID",
    "project-67cb1491-36cb-437f-b95"
)
BUCKET = os.getenv(
    "EMPLOYEE_BUCKET",
    "employee-pipeline-neel-2026"
)
LOCATION = "us-central1"

with DAG(
    dag_id="employee_gcs_to_bigquery",
    start_date=datetime(2026, 10, 15),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2)
    },
    tags=["employee", "gcs", "bigquery"],
) as dag:

    load_raw = GCSToBigQueryOperator(
        task_id="load_gcs_to_raw",
        bucket=BUCKET,
        source_objects=["incoming/employees/*.csv"],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.employee_raw.employees"
        ),
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "employee_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "first_name", "type": "STRING"},
            {"name": "last_name", "type": "STRING"},
            {"name": "email", "type": "STRING"},
            {"name": "department", "type": "STRING"},
            {"name": "salary", "type": "NUMERIC"},
            {"name": "hire_date", "type": "DATE"},
            {"name": "updated_at", "type": "TIMESTAMP"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=LOCATION,
    )

    transform = BigQueryInsertJobOperator(
        task_id="raw_to_curated",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE
                    `{PROJECT_ID}.employee_curated.employees` AS

                    SELECT
                        employee_id,
                        INITCAP(TRIM(first_name)) AS first_name,
                        INITCAP(TRIM(last_name)) AS last_name,
                        LOWER(TRIM(email)) AS email,
                        UPPER(TRIM(department)) AS department,
                        salary,
                        hire_date,
                        updated_at

                    FROM `{PROJECT_ID}.employee_raw.employees`

                    WHERE employee_id IS NOT NULL

                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY employee_id
                        ORDER BY updated_at DESC
                    ) = 1
                """,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    validate = BigQueryInsertJobOperator(
        task_id="validate_curated_data",
        configuration={
            "query": {
                "query": f"""
                    ASSERT (
                        SELECT COUNT(*) > 0
                        FROM `{PROJECT_ID}.employee_curated.employees`
                    ) AS 'Curated employee table is empty';

                    ASSERT (
                        SELECT COUNT(*) = COUNT(DISTINCT employee_id)
                        FROM `{PROJECT_ID}.employee_curated.employees`
                    ) AS 'Duplicate employee IDs detected';
                """,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    load_raw >> transform >> validate
