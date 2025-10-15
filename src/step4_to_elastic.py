"""
Step 4 of the Ottawa Permits ETL pipeline.

Goal:
- Retrieve curated documents from MongoDB (collection `permits_curated`).
- Prepare and transform them for Elasticsearch indexing.
- Create an index `ottawa_permits` in Elasticsearch.
- Insert all documents in bulk for fast search.
"""

import os
import json
import logging
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Environment variables
load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_INITDB_DATABASE", "ottawa")
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "root")
MONGO_PASS = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "example")

ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = int(os.getenv("ES_HTTP_PORT", 9200))
ES_INDEX = "ottawa_permits"


# MongoDB → Extraction
def extract_from_mongo():
    """Fetch all curated documents from MongoDB."""
    logger.info("Connecting to MongoDB at %s:%d...", MONGO_HOST, MONGO_PORT)
    mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
    client = MongoClient(mongo_uri)
    db = client[MONGO_DB]
    collection = db["permits_curated"]

    docs = list(collection.find({}, {"_id": 0}))  # exclude MongoDB internal _id
    logger.info("Fetched %d documents from MongoDB (permits_curated).", len(docs))
    return docs


# Elasticsearch → Connection and Index Creation
def connect_elasticsearch():
    """Connect to Elasticsearch and return client."""
    es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
    if es.ping():
        logger.info("Connected to Elasticsearch at %s:%d", ES_HOST, ES_PORT)
    else:
        logger.error("Could not connect to Elasticsearch at %s:%d", ES_HOST, ES_PORT)
        raise ConnectionError("Elasticsearch connection failed.")
    return es


def create_index(es):
    """Create the ottawa_permits index with proper mappings."""
    if es.indices.exists(index=ES_INDEX):
        logger.info("Index '%s' already exists, deleting it first.", ES_INDEX)
        es.indices.delete(index=ES_INDEX)

    mapping = {
        "mappings": {
            "properties": {
                "PERMIT": {"type": "keyword"},
                "APPL_TYPE": {"type": "keyword"},
                "APPL_TYPE_2": {"type": "keyword"},
                "BLG_TYPE": {"type": "keyword"},
                "VALUE": {"type": "float"},
                "VALUE_CATEGORY": {"type": "keyword"},
                "WARD": {"type": "keyword"},
                "DESCRIPTION": {"type": "text"},
                "LABELS": {"type": "keyword"},
                "ISSUED_DATE": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "LOCATION": {"type": "text"},
                "CONTRACTOR": {"type": "keyword"},
                "GEO_POINT": {"type": "geo_point"},
            }
        }
    }

    es.indices.create(index=ES_INDEX, body=mapping)
    logger.info("Index '%s' created successfully.", ES_INDEX)


# Elasticsearch → Bulk Insert
def transform_for_elastic(doc):
    """Transform a MongoDB document to an Elasticsearch-friendly format."""
    if "GEO_POINT" in doc and isinstance(doc["GEO_POINT"], dict):
        coords = doc["GEO_POINT"].get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) == 2:
            doc["GEO_POINT"] = {"lat": coords[1], "lon": coords[0]}
        else:
            doc.pop("GEO_POINT", None)
    return doc


def load_to_elasticsearch(es, docs):
    """Insert all documents into Elasticsearch using bulk API."""
    logger.info("Preparing bulk insertion into Elasticsearch (%d docs)...", len(docs))
    actions = [
        {
            "_index": ES_INDEX,
            "_source": transform_for_elastic(doc),
        }
        for doc in docs
    ]

    helpers.bulk(es, actions)
    logger.info("Successfully indexed %d documents into '%s'.", len(actions), ES_INDEX)


# Main step function (for Airflow)
def to_elastic():
    """Execute Step 4: MongoDB → Elasticsearch (Index zone)."""
    logger.info("Starting Step 4: MongoDB → Elasticsearch (Index zone)")

    try:
        docs = extract_from_mongo()
        es = connect_elasticsearch()
        create_index(es)
        load_to_elasticsearch(es, docs)
        logger.info("Step 4 completed successfully.")
    except Exception as e:
        logger.error("Step 4 failed: %s", e)
        raise


if __name__ == "__main__":
    to_elastic()
