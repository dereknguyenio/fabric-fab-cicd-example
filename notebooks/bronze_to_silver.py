# Bronze to Silver
# Cleans, validates, deduplicates, and types raw data

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

print("Starting bronze → silver transformation")

# Read bronze
df_bronze = spark.read.format("delta").table("bronze.raw_orders")
bronze_count = df_bronze.count()
print(f"Read {bronze_count} records from bronze")

# Deduplicate by order_id (keep latest ingestion)
window = Window.partitionBy("order_id").orderBy(F.col("_ingestion_timestamp").desc())
df_dedup = (
    df_bronze
    .withColumn("_row_num", F.row_number().over(window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# Clean and validate
df_silver = (
    df_dedup
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("amount") > 0)
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("customer_id", F.trim(F.lower(F.col("customer_id"))))
    .withColumn("_silver_timestamp", F.current_timestamp())
    .drop("_ingestion_timestamp", "_source", "_batch_id")
)

# Write to silver
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.clean_orders")

silver_count = df_silver.count()
print(f"Wrote {silver_count} records to silver.clean_orders")
print(f"Dedup removed {bronze_count - silver_count} records")
