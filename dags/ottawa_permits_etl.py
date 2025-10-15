"""
Global Airflow DAG for the Ottawa Permits ETL pipeline.

Steps:
1. Raw → S3 (LocalStack)
2. Staging → MySQL
3. Curated → MongoDB
4. Index → Elasticsearch
5. Reporting → Logs / JSON summary

Each step corresponds to a Python script in /src.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Import step functions from src
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from step1_raw_to_s3 import raw_to_s3 
from step2_to_mysql import to_mysql
# from step3_to_mongo import to_mongo
# from step4_to_elastic import to_elastic
# from step5_reporting import generate_report


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


# DAG definition
with DAG(
    dag_id="ottawa_permits_etl",
    description="End-to-end ETL pipeline: Ottawa Building Permits (S3 → MySQL → MongoDB → Elasticsearch → Logs)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ottawa", "etl", "airflow", "localstack"],
) as dag:

    # Step 1 – Raw → S3
    raw_to_s3_task = PythonOperator(
        task_id="raw_to_s3",
        python_callable=raw_to_s3,
        dag=dag,
    )

    # Step 2 – Staging → MySQL
    staging_mysql_task = PythonOperator(
        task_id="staging_mysql",
        python_callable=to_mysql,
        dag=dag,
    )

    # Step 3 – Curated → MongoDB
    # curated_mongo_task = PythonOperator(
    #     task_id="curated_mongo",
    #     python_callable=to_mongo,
    #     dag=dag,
    # )

    # Step 4 – Index → Elasticsearch
    # index_elasticsearch_task = PythonOperator(
    #     task_id="index_elasticsearch",
    #     python_callable=to_elastic,
    #     dag=dag,
    # )

    # Step 5 – Logging / Reporting
    # reporting_task = PythonOperator(
    #     task_id="reporting",
    #     python_callable=generate_report,
    #     dag=dag,
    # )

    # Define dependencies
    raw_to_s3_task >> staging_mysql_task
    # >> curated_mongo_task >> index_elasticsearch_task >> reporting_task
