"""
Step 3 of the Ottawa Permits ETL pipeline.

Goal:
- Retrieve the staging data from MySQL (`permits_staging` table).
- Enrich the dataset with:
  - LABELS (keywords extracted from DESCRIPTION)
  - APPL_TYPE_2 (semantic classification of permits)
  - VALUE_CATEGORY (Low / Medium / High)
  - GEO_POINT (GeoJSON coordinates for spatial indexing)
    (same changes than my MongoDB project)
- Insert the curated documents into MongoDB (`permits_curated` collection).
"""

import os
import json
import logging
import pymysql
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Environment variables
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DB = os.getenv("MYSQL_DATABASE", "airflow")
MYSQL_USER = os.getenv("MYSQL_USER", "airflow")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "airflow")

MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_INITDB_DATABASE", "ottawa")
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "root")
MONGO_PASS = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "example")

STAGING_TABLE = "permits_staging"
CURATED_COLLECTION = "permits_curated"


# Helper functions
def extract_from_mysql():
    """Extract all records from the staging table in MySQL."""
    logger.info("Connecting to MySQL at %s:%d...", MYSQL_HOST, MYSQL_PORT)
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT,
    )
    query = f"SELECT * FROM {STAGING_TABLE};"
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info("Fetched %d records from table '%s'.", len(df), STAGING_TABLE)
    return df


def create_labels(description: str):
    """Extract labels from DESCRIPTION text using keyword matching."""
    if not isinstance(description, str):
        return []

    desc = description.lower()
    labels = []

    keywords = {
        "pool": ["pool", "spa", "hot tub"],
        "garage": ["garage"],
        "deck": ["deck"],
        "plumbing": ["plumbing"],
        "basement": ["basement"],
        "roofing": ["roof", "roofing"],
        "solar": ["solar"],
        "shed": ["shed"],
        "porch": ["porch"],
        "fireplace": ["fireplace"],
        "addition": ["addition", "extension"],
        "rowhouse": ["rowhouse"],
        "stacked_dwelling": ["stacked dwelling", "stacked"],
        "tenant_fitup": ["tenant fit", "fit-up", "fitup"],
    }

    for label, words in keywords.items():
        if any(word in desc for word in words):
            labels.append(label)

    return list(set(labels))


def create_appl_type_2(row):
    """Derive APPL_TYPE_2 based on APPL_TYPE and LABELS."""
    appl_type = str(row.get("APPL_TYPE", "")).lower()
    labels = row.get("LABELS", [])

    if "demolition" in appl_type or "demolition" in labels:
        return "Demolition"
    if "interior alteration" in appl_type or "renovation" in labels:
        return "Renovation"
    if "construction" in appl_type and "demolition" in labels:
        return "Destruct+Construct"
    return "Construction"


def create_value_category(value):
    """Categorize VALUE into Low / Medium / High ranges."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None

    if value < 50000:
        return "Low"
    elif value < 200000:
        return "Medium"
    else:
        return "High"


def create_geo_point(coord):
    """Convert a JSON string of coordinates into a valid GeoJSON Point."""
    if isinstance(coord, str):
        try:
            coord = json.loads(coord)
        except Exception:
            return None

    if not isinstance(coord, (list, tuple)) or len(coord) != 2:
        return None

    lon, lat = coord
    if lon is None or lat is None:
        return None

    return {"type": "Point", "coordinates": [lon, lat]}


def enrich_dataframe(df: pd.DataFrame):
    """Apply all enrichment transformations."""
    logger.info("Starting data enrichment process...")

    df["LABELS"] = df["DESCRIPTION"].apply(create_labels)
    df["APPL_TYPE_2"] = df.apply(create_appl_type_2, axis=1)
    df["VALUE_CATEGORY"] = df["VALUE"].apply(create_value_category)
    df["GEO_POINT"] = df["COORDINATES"].apply(create_geo_point)

    # Filter out invalid GEO_POINTs
    df = df[df["GEO_POINT"].notnull()].reset_index(drop=True)

    logger.info("Data enrichment completed. Final dataset size: %d records.", len(df))
    return df


def load_to_mongo(df: pd.DataFrame):
    """Insert curated documents into MongoDB."""
    mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
    client = MongoClient(mongo_uri)
    db = client[MONGO_DB]
    collection = db[CURATED_COLLECTION]

    logger.info("Clearing existing data in '%s' collection...", CURATED_COLLECTION)
    collection.delete_many({})

    records = df.to_dict(orient="records")
    if records:
        collection.insert_many(records)
        logger.info("Inserted %d documents into '%s'.", len(records), CURATED_COLLECTION)
    else:
        logger.warning("No records to insert into MongoDB.")

    # Create geospatial index
    collection.create_index([("GEO_POINT", "2dsphere")])
    logger.info("2dsphere index created on GEO_POINT.")



# Main step function (for Airflow)
def to_mongo():
    """Execute Step 3: MySQL → MongoDB (Curated zone)."""
    logger.info("Starting Step 3: MySQL → MongoDB (Curated zone)")

    try:
        df = extract_from_mysql()
        enriched_df = enrich_dataframe(df)
        load_to_mongo(enriched_df)
        logger.info("Step 3 completed successfully.")
    except Exception as e:
        logger.error("Step 3 failed: %s", e)
        raise


if __name__ == "__main__":
    to_mongo()
