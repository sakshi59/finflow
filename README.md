# FinFlow

Fraud detection pipeline for financial transactions, built on Azure Data Lake, Databricks, and PySpark.

---

## Why this project

I wanted to get more hands-on with the modern lakehouse stack — Delta Lake, Unity Catalog, distributed processing with Spark — beyond what I'd worked with before. PaySim's transaction dataset gave me a realistic enough scenario to build a full pipeline around: ingest messy raw data, clean it properly, and pull out something an actual fraud team could use.

## What it does

Takes 6.3M+ synthetic transactions and runs them through a Bronze → Silver → Gold pipeline:

- **Bronze** — raw data landed in Azure Data Lake as partitioned Parquet
- **Silver** — cleaned and deduplicated with PySpark, stored as a Delta table
- **Gold** — aggregated into two views: fraud rate by transaction type, and a watchlist of flagged accounts

## What I found

- Fraud shows up almost entirely in **TRANSFER** (0.77%) and **CASH_OUT** (0.18%) transactions — the other three transaction types had zero fraud cases in this data
- **8,197** accounts are linked to at least one fraudulent transaction, and almost all of them only transacted once — looks like a one-shot pattern rather than repeat fraud

## Pipeline
grep -n "Week" README.md
git add .
git commit -m "docs: rewrite README"
git push
