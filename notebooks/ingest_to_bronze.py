# Ingest to Bronze
# Reads raw data from source systems and lands in bronze lakehouse (append-only)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

source_path = spark.conf.get("spark.fabric.params.source_path", "/mnt/raw")

print(f"Ingesting from {source_path}")

# ---- Replace with your actual source ----
# Example: ADLS shortcut
# df = spark.read.format("delta").load(f"abfss://container@account.dfs.core.windows.net/raw/orders")

# Example: SQL source via JDBC
# df = spark.read.format("jdbc") \
#     .option("url", "jdbc:sqlserver://server;databaseName=db") \
#     .option("dbtable", "dbo.orders") \
#     .load()

# Demo: generate sample data
from datetime import datetime, timedelta
import random

dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]
data = [
    (i, f"customer_{random.randint(1, 100)}", dates[i % 30], round(random.uniform(10, 500), 2), f"product_{random.randint(1, 50)}")
    for i in range(1000)
]
df = spark.createDataFrame(data, ["order_id", "customer_id", "order_date", "amount", "product_id"])

# Add ingestion metadata
df_bronze = (
    df
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source", F.lit("demo"))
    .withColumn("_batch_id", F.lit(datetime.now().strftime("%Y%m%d%H%M%S")))
)

# Write to bronze (append-only)
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("bronze.raw_orders")

print(f"Ingested {df_bronze.count()} records to bronze.raw_orders")
