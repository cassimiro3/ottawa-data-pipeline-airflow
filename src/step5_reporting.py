"""
Step 5 of the Ottawa Permits ETL pipeline.

Goal:
Generate a summary report (analysis_report.json) with both
record counts and basic analytics from MySQL and MongoDB.
"""

import os
import json
import logging
import boto3
import pymysql
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from datetime import datetime
from dotenv import load_dotenv
from decimal import Decimal


# ---------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------
load_dotenv()

# LocalStack S3
LOCALSTACK_URL = os.getenv("LOCALSTACK_URL", "http://localstack:4566")
BUCKET_NAME = os.getenv("S3_BUCKET_RAW", "ottawa-raw")

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DB = os.getenv("MYSQL_DATABASE", "airflow")
MYSQL_USER = os.getenv("MYSQL_USER", "airflow")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "airflow")

# MongoDB
MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_INITDB_DATABASE", "ottawa")
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "root")
MONGO_PASS = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "example")

# Elasticsearch
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = int(os.getenv("ES_HTTP_PORT", 9200))
ES_INDEX = "ottawa_permits"

REPORT_PATH = "/opt/airflow/data/analysis_report.json"
os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------
def count_s3_objects():
    """Count objects in S3 bucket (LocalStack)."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_URL,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    count = len(response.get("Contents", []))
    logger.info("S3: %d object(s) in bucket '%s'.", count, BUCKET_NAME)
    return count


def get_mysql_stats():
    """Return row count and average VALUE from MySQL."""
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT,
    )
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*), AVG(VALUE) FROM permits_staging;")
        count, avg_val = cursor.fetchone()
    conn.close()
    avg_val = round(avg_val or 0, 2)
    logger.info("MySQL: %d rows, avg VALUE = %.2f", count, avg_val)
    return count, avg_val


def get_mongo_stats():
    """Return count, value category distribution, and top labels from MongoDB."""
    mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
    client = MongoClient(mongo_uri)
    db = client[MONGO_DB]
    collection = db["permits_curated"]

    count = collection.count_documents({})
    logger.info("MongoDB: %d documents in 'permits_curated'.", count)

    # Value category distribution
    value_dist = (
        collection.aggregate([{"$group": {"_id": "$VALUE_CATEGORY", "count": {"$sum": 1}}}])
    )
    value_distribution = {doc["_id"] or "Unknown": doc["count"] for doc in value_dist}

    # Top 5 labels
    label_cursor = collection.aggregate([
        {"$unwind": "$LABELS"},
        {"$group": {"_id": "$LABELS", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5},
    ])
    top_labels = [doc["_id"] for doc in label_cursor]

    return count, value_distribution, top_labels


def count_elastic_docs():
    """Count indexed documents in Elasticsearch."""
    es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
    if not es.indices.exists(index=ES_INDEX):
        logger.warning("Elasticsearch index '%s' not found.", ES_INDEX)
        return 0
    count = es.count(index=ES_INDEX)["count"]
    logger.info("Elasticsearch: %d document(s) in '%s'.", count, ES_INDEX)
    return count


# Convert Decimal values to float recursively
def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj


# Main step function
def generate_report():
    """Execute Step 5: Generate extended summary report."""
    logger.info("Starting Step 5: Reporting (analysis_report.json)")

    # Collect all stats
    s3_count = count_s3_objects()
    mysql_count, mysql_avg = get_mysql_stats()
    mongo_count, value_dist, top_labels = get_mongo_stats()
    es_count = count_elastic_docs()

    # Build final JSON
    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "data_zones": {
            "Raw_S3": s3_count,
            "Staging_MySQL": mysql_count,
            "Curated_MongoDB": mongo_count,
            "Index_Elasticsearch": es_count,
        },
        "analytics": {
            "avg_permit_value": mysql_avg,
            "value_category_distribution": value_dist,
            "top_labels": top_labels,
        },
    }



    # Save to file
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(convert_decimals(report), f, indent=4)
    logger.info("Report saved to %s", REPORT_PATH)
    logger.info("Step 5 completed successfully.")
    return True



if __name__ == "__main__":
    generate_report()
