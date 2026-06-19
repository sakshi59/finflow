# Databricks notebook source
# Databricks notebook source
# Gold Aggregations

from pyspark.sql.functions import count, sum as _sum, avg, col

# Read Silver table fresh — this makes this notebook fully standalone
df_silver = spark.read.table("workspace.default.transactions_silver")

print(f"Silver rows loaded: {df_silver.count():,}")

# Gold table 1 — fraud summary by transaction type
df_gold_fraud_summary = (
    df_silver
    .groupBy("type")
    .agg(
        count("*").alias("total_transactions"),
        _sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        _sum(col("isFraud")).alias("fraud_count"),
    )
    .withColumn(
        "fraud_rate_pct",
        (col("fraud_count") / col("total_transactions")) * 100
    )
    .orderBy(col("fraud_count").desc())
)

df_gold_fraud_summary.show()

df_gold_fraud_summary.write.format("delta").mode("overwrite").saveAsTable("workspace.default.fraud_summary_gold")
print("Gold table saved: workspace.default.fraud_summary_gold")

# Gold table 2 — high-risk account watchlist
df_gold_account_risk = (
    df_silver
    .groupBy("account_id_origin")
    .agg(
        count("*").alias("total_transactions"),
        _sum("amount").alias("total_sent"),
        _sum(col("isFraud")).alias("fraud_flags"),
    )
    .filter(col("fraud_flags") > 0)
    .orderBy(col("fraud_flags").desc())
)

df_gold_account_risk.show(10)

df_gold_account_risk.write.format("delta").mode("overwrite").saveAsTable("workspace.default.account_risk_gold")

print(f"High-risk accounts identified: {df_gold_account_risk.count():,}")
print("Gold table saved: workspace.default.account_risk_gold")