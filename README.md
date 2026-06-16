# FinFlow — Financial Transaction Intelligence Platform

An end-to-end data engineering and AI platform built on AWS, Databricks, and Snowflake,
processing financial transaction data for fraud detection, regulatory reporting, and
AI-powered compliance assistance.

**Built to demonstrate:** Python · PySpark · Databricks · Snowflake · AWS · Airflow · Kafka ·
Delta Lake · LangChain · LangGraph · RAG · Feature Engineering · Data Quality · CI/CD

---

## Architecture

```
PaySim CSV (6.3M rows)
        │
        ▼
[ingestion/ingest_bronze.py]
  Python + boto3 + PyArrow
  Chunked reading (500k rows/chunk)
  Partitioned Parquet → S3 Bronze
        │
        ▼
S3: bronze/transactions/year=YYYY/month=MM/*.parquet
        │
        ▼
[processing/silver_transform.py]         ← Week 2
  PySpark on Databricks
  Clean, type, deduplicate
  Delta Lake → S3 Silver
        │
        ▼
[processing/gold_aggregations.py]        ← Week 2
  Fraud signals, account metrics
  Delta Lake → S3 Gold
  Load → Snowflake
        │
        ▼
[orchestration/dags/]                    ← Week 3
  Apache Airflow DAGs
  Schedules Bronze→Gold pipeline
  Retry logic, SLA alerts
        │
        ├──▶ [quality/]                  ← Week 4
        │     Great Expectations suite
        │     6 validation checkpoints
        │
        ├──▶ [ml/feature_store.py]       ← Week 5
        │     10 fraud detection features
        │     MLflow experiment tracking
        │
        ├──▶ [rag/]                      ← Week 6–7
        │     LangChain + ChromaDB
        │     LangGraph agent
        │     Compliance Q&A assistant
        │
        └──▶ [governance/]              ← Week 9
              Snowflake RBAC
              Column masking
              Audit log
              FastAPI serving layer
```

---

## Modules & Resume Mapping

| Module | Folder | Resume Bullet | Key Metric |
|---|---|---|---|
| Bronze ingestion | `ingestion/` | Bullet 1 — scalable pipelines | 6.3M rows, partitioned Parquet |
| Silver/Gold transforms | `processing/` | Bullet 1 — 40% analytics improvement | Query time before vs after |
| Airflow orchestration | `orchestration/` | Bullet 2 — 75% less manual work | Steps before vs after automation |
| Data quality | `quality/` | Bullet 3 — AI-ready datasets | 6 validation checkpoints |
| Feature store + ML | `ml/` | Bullet 3 — 35% faster model dev | Feature prep time comparison |
| RAG + AI agent | `rag/` | Bullet 4 — 30% relevance gain | Benchmark: keyword vs RAG |
| Query optimization | `processing/optimize/` | Bullet 5 — 50% faster queries | Query time logs |
| RBAC + governance | `governance/` | Bullet 6 — 250+ users, security | Role design, audit coverage |

---

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/finflow
cd finflow
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # fill in your AWS credentials
```

## Run Bronze Ingestion

```bash
# Download PaySim dataset from Kaggle first, place in data/
python ingestion/ingest_bronze.py
```

## Run Tests

```bash
pytest ingestion/test_ingest_bronze.py -v
```

---

## Dataset

- **PaySim** — 6.3M synthetic mobile money transactions with fraud labels
  - Source: [Kaggle — PaySim1](https://www.kaggle.com/datasets/ealaxi/paysim1)
  - Columns: step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
    nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
  - Fraud rate: ~0.13% (8,213 fraudulent transactions)

---

## Measured Outcomes (updated as built)

| Component | Metric | Value |
|---|---|---|
| Bronze rows loaded | Row count | *(fill after running)* |
| Parquet vs CSV size | Compression | *(fill after running)* |
| Silver after dedup | Rows removed | *(fill after running)* |
| Query optimization | Time reduction | *(fill in Week 8)* |

