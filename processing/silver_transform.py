# Databricks notebook source
df = spark.read.parquet("/Volumes/workspace/default/finflow_bronze/")

print(f"Total rows: {df.count():,}")
df.printSchema()
df.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# Clean and standardize column names, drop duplicates, filter bad records
df_silver = (
    df
    .withColumnRenamed("nameOrig", "account_id_origin")
    .withColumnRenamed("nameDest", "account_id_dest")
    .withColumnRenamed("oldbalanceOrg", "old_balance_origin")
    .withColumnRenamed("newbalanceOrig", "new_balance_origin")
    .withColumnRenamed("oldbalanceDest", "old_balance_dest")
    .withColumnRenamed("newbalanceDest", "new_balance_dest")
    .dropDuplicates()
    .filter(col("amount") > 0)
    .withColumn("processed_at", current_timestamp())
)

print(f"Bronze rows: {df.count():,}")
print(f"Silver rows: {df_silver.count():,}")
print(f"Rows removed: {df.count() - df_silver.count():,}")

df_silver.printSchema()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable("workspace.default.transactions_silver")

print("Silver table saved successfully")

# COMMAND ----------

spark.sql("DESCRIBE HISTORY workspace.default.transactions_silver").show()