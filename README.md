# Ottawa Data Pipeline (Airflow & Docker)

**Related Project:** Ottawa MongoDB Exploration (link to be added)

## 1. Introduction

This project demonstrates an end-to-end **Data Engineering pipeline** using a real-world public dataset: **Ottawa Building Permits**.
It simulates a modern data platform architecture using cloud-native tools (**S3, MySQL, MongoDB, Elasticsearch**) orchestrated with **Apache Airflow**, fully containerized with **Docker Compose**.

The objective is to replicate an enterprise-grade data ingestion and processing workflow:
**Raw Zone → Staging Zone → Curated Zone → Index Zone → Analytics**, ensuring data reliability, enrichment, and fast search capabilities.

This project was designed to:
- Demonstrate advanced Data Engineering skills
- Apply ETL/ELT best practices
- Build a cloud-ready architecture
- Integrate SQL, NoSQL, and search technologies

---

## 2. Architecture Overview

Data pipeline flow:

Local JSON → S3 (LocalStack) → MySQL (Staging) → MongoDB (Curated) → Elasticsearch (Index) → Analytics Report

Execution is automated with a single Airflow DAG composed of five Python tasks.

---

## 3. Pipeline Steps

| Step | Description                                   | Input                | Output                     | Technology                  |
|------|-----------------------------------------------|----------------------|----------------------------|-----------------------------|
| 1    | Upload raw dataset to S3                      | Local JSON file      | S3 object                  | Boto3, LocalStack          |
| 2    | Clean and load into MySQL (Staging zone)      | S3 object            | permits_staging table      | Pandas, SQLAlchemy         |
| 3    | Enrich and load into MongoDB (Curated zone)   | MySQL staging table  | permits_curated collection | PyMySQL, PyMongo           |
| 4    | Index curated records in Elasticsearch        | MongoDB collection   | ottawa_permits index       | Elasticsearch API          |
| 5    | Generate analytics report                     | All zones            | analysis_report.json       | Python, SQL, MongoDB, ES   |

---

## 4. Technology Stack

| Component        | Technology       | Purpose                           |
|------------------|------------------|-----------------------------------|
| Orchestration    | Apache Airflow   | Pipeline automation               |
| Containerization | Docker Compose   | Multi-service environment         |
| Cloud Storage    | LocalStack (S3)  | Raw object storage simulation     |
| Relational DB    | MySQL            | Staging structured data           |
| NoSQL DB         | MongoDB          | Curated enriched data             |
| Search Engine    | Elasticsearch    | Full-text and geospatial search   |
| Language         | Python 3.10+     | ETL logic implementation          |

---

## 5. Installation Guide

### 5.1 Prerequisites
- Docker & Docker Compose
- Git
- Python 3.10+ (optional)

### 5.2 Clone the Repository
git clone https://github.com/Adelllllllll/ottawa-data-pipeline-airflow.git
cd ottawa-data-pipeline-airflow

### 5.3 Configure Environment Variables (.env)
LOCALSTACK_URL=http://localstack:4566
S3_BUCKET_RAW=ottawa-raw
MYSQL_HOST=mysql
MYSQL_PORT=3306
MONGO_HOST=mongo
MONGO_PORT=27017
ES_HOST=elasticsearch
ES_HTTP_PORT=9200
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1

### 5.4 Start Infrastructure
docker-compose up -d

### 5.5 Access Airflow
URL: http://localhost:8080
Username: airflow
Password: airflow

---

## 6. Usage Instructions

1. Start the Docker containers
2. Open Airflow UI
3. Trigger the DAG: ottawa_permits_etl
4. Monitor task execution order

Outputs available in:
- S3 Raw Storage
- MySQL (permits_staging)
- MongoDB (permits_curated)
- Elasticsearch (ottawa_permits)
- Analytics report: /opt/airflow/data/analysis_report.json

---

## 7. Results

The pipeline generates:
- Record counts per data zone
- Average permit value
- Value category distribution (Low / Medium / High)
- Top labels extracted from descriptions


---

## 9. Author
Adel Zairi

Data Engineering Major - EFREI Paris
