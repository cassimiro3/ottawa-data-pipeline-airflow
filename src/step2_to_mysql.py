"""
Step 2 of the Ottawa Permits ETL pipeline.

Goal:
- Retrieve the raw JSON file from LocalStack S3.
- Clean and format the records.
- Insert the data into the MySQL table `permits_staging`.
"""

import os
import io
import json
import logging
import boto3
import pandas as pd
import sqlalchemy
from botocore.exceptions import ClientError


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Environment variables

LOCALSTACK_URL = os.getenv("LOCALSTACK_URL", "http://localstack:4566")
BUCKET_NAME = os.getenv("S3_BUCKET_RAW", "ottawa-raw")
OBJECT_NAME = os.getenv("S3_OBJECT_NAME", "permits_ottawa.json")

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "airflow")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "airflow")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "airflow")

TABLE_NAME = "permits_staging"



# Helper functions
def read_json_from_s3():
    """Read the JSON file stored in LocalStack S3."""
    logger.info("Connecting to LocalStack S3 at %s", LOCALSTACK_URL)
    s3_client = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_URL,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    try:
        logger.info("Downloading '%s' from bucket '%s'...", OBJECT_NAME, BUCKET_NAME)
        s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME)
        data = s3_object["Body"].read().decode("utf-8")
        json_data = [json.loads(line) for line in data.splitlines() if line.strip()]
        logger.info("Successfully loaded JSON from S3 (%d records)", len(json_data))
        return json_data
    except ClientError as e:
        logger.error("Error reading from S3: %s", e)
        raise


def clean_dataframe(records):
    """
    Clean and normalize the raw JSON data from the Ottawa permits dataset.
    Extract nested fields (permits, properties, geometry) and flatten them.
    """
    logger.info("Converting nested JSON to flat DataFrame for cleaning.")

    # Convert list of dicts to a flat structure (extract key subfields)
    flat_rows = []
    for r in records:
        permit = r.get("permits", {})
        props = r.get("properties", {})
        geom = r.get("geometry", {})

        row = {
            "PERMIT": permit.get("PERMIT"),
            "APPL_TYPE": permit.get("APPL_TYPE"),
            "BLG_TYPE": permit.get("BLG_TYPE"),
            "VALUE": permit.get("VALUE"),
            "WARD": permit.get("WARD"),
            "DESCRIPTION": permit.get("DESCRIPTION"),
            "ISSUED_DATE": permit.get("ISSUED_DATE"),
            "LOCATION": permit.get("location") or props.get("location"),
            "CONTRACTOR": permit.get("CONTRACTOR"),
            "GEOMETRY_TYPE": geom.get("type"),
            "COORDINATES": geom.get("coordinates"),
        }
        flat_rows.append(row)

    df = pd.DataFrame(flat_rows)

    # Basic cleaning
    df.dropna(subset=["PERMIT"], inplace=True)
    df.drop_duplicates(subset=["PERMIT"], inplace=True)

    # Convert VALUE to numeric
    df["VALUE"] = pd.to_numeric(df["VALUE"], errors="coerce").fillna(0)

    # Convert ISSUED_DATE to datetime
    df["ISSUED_DATE"] = pd.to_datetime(df["ISSUED_DATE"], errors="coerce")

    # Convert coordinates (list) to JSON string for SQL compatibility
    df["COORDINATES"] = df["COORDINATES"].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None)

    logger.info("Data cleaned and flattened: %d records, %d columns", *df.shape)

    return df



def insert_into_mysql(df):
    """Insert the cleaned DataFrame into MySQL."""
    connection_url = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )

    logger.info("Connecting to MySQL at %s:%s...", MYSQL_HOST, MYSQL_PORT)
    engine = sqlalchemy.create_engine(connection_url)

    with engine.connect() as conn:
        logger.info("Creating/replacing table '%s' in database '%s'...", TABLE_NAME, MYSQL_DATABASE)
        df.to_sql(TABLE_NAME, con=conn, if_exists="replace", index=False)
        logger.info("Successfully inserted %d rows into '%s'.", len(df), TABLE_NAME)


# Main step function (for Airflow)
def to_mysql():
    """Execute Step 2: Load and clean data from S3 → insert into MySQL."""
    logger.info("Starting Step 2: S3 → MySQL (Staging zone)")

    try:
        json_data = read_json_from_s3()
        cleaned_df = clean_dataframe(json_data)
        insert_into_mysql(cleaned_df)
        logger.info("Step 2 completed successfully.")
    except Exception as e:
        logger.error("Step 2 failed: %s", e)
        raise


if __name__ == "__main__":
    to_mysql()
