# 🏗️ Ottawa Data Pipeline (Airflow)

🔗 Related Project: Ottawa MongoDB Exploration #ajouter le lien du repo

## 🎯 Objective

This project implements a **complete data processing pipeline** inspired by EFREI’s “Data Processing and Indexing” synthesis exercise, adapted to the public dataset **Ottawa Building Permits**.

The goal is to showcase a **professional Data Engineering architecture**:
**Raw (S3) → Staging (MySQL) → Curated (MongoDB) → Index (Elasticsearch)**  
orchestrated with **Apache Airflow** and **Docker Compose**.

---

## 🧱 Pipeline Architecture


Each step is implemented as a separate Airflow task:

1. **Raw → S3**: Upload raw dataset to LocalStack S3  
2. **S3 → MySQL (Staging)**: Clean and load structured data into a MySQL table  
3. **MySQL → MongoDB (Curated)**: Enrich data (semantic categories, geo-coordinates, value labels, etc.)  
4. **MongoDB → Elasticsearch (Index)**: Index the enriched documents for fast search  
5. **Reporting**: Generate a log file summarizing the processing pipeline

One single Airflow DAG: ottawa_permits_etl.py
Each step is a PythonOperator calling src/stepX_*.py


---

## ⚙️ Technologies

- **Orchestration:** Apache Airflow  
- **Raw storage:** LocalStack (S3 simulation)  
- **Relational DB:** MySQL  
- **NoSQL DB:** MongoDB  
- **Indexing Engine:** Elasticsearch  
- **Language:** Python 3.10+  
- **Containerization:** Docker Compose  
- **CI/CD:** GitHub Actions (flake8 + pytest)


---

## 🚀 Planned Pipeline Steps

| Step | Description | Output |
|------|--------------|--------|
| 1️⃣ Raw | Upload local file to S3 (LocalStack) | `raw/permits_ottawa.json` |
| 2️⃣ Staging | Clean and insert into MySQL | Table `permits_staging` |
| 3️⃣ Curated | Enrich and store in MongoDB | Collection `permits_curated` |
| 4️⃣ Index | Index data in Elasticsearch | Index `ottawa_permits` |
| 5️⃣ Reporting | Generate summary report | `logs/pipeline_log.log` or `analysis_report.json` |

---

## 📊 Dataset Overview

The dataset used in this project was originally provided for educational purposes.
It is derived from the Open Data Ottawa portal: [https://open.ottawa.ca](https://open.ottawa.ca).  

For licensing reasons, the **full dataset is not redistributed** in this repository.  
Instead, a small **sample file** (`data/raw/sample_permits_ottawa.json`) is included to allow local testing and demonstration of the pipeline.


**Dataset:** Ottawa Building Permits (Open Data Ottawa)  
**Format:** JSON (GeoJSON-like structure)  
Fields such as:
- `PERMIT`, `APPL_TYPE`, `BLG_TYPE`, `VALUE`, `WARD`
- `DESCRIPTION`, `ISSUED_DATE`, `LOCATION`, `GEO_POINT`

---

## 🧩 Author

**Adel Zairi**  
Data Engineering student at **EFREI Paris**  

