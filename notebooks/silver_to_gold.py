# Silver to Gold
# Aggregates silver data into business-ready datasets

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

print("Starting silver → gold aggregation")

df_silver = spark.read.format("delta").table("silver.clean_orders")
print(f"Read {df_silver.count()} records from silver")

# Gold: Daily revenue summary
df_daily = (
    df_silver
    .groupBy("order_date")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("revenue"),
        F.avg("amount").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("unique_products"),
    )
    .withColumn("_gold_timestamp", F.current_timestamp())
    .orderBy("order_date")
)

df_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.daily_revenue")

print(f"Wrote {df_daily.count()} daily summaries to gold.daily_revenue")

# Gold: Customer lifetime value
df_customer = (
    df_silver
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("lifetime_orders"),
        F.sum("amount").alias("lifetime_value"),
        F.avg("amount").alias("avg_order_value"),
        F.min("order_date").alias("first_order"),
        F.max("order_date").alias("last_order"),
    )
    .withColumn("days_active", F.datediff(F.col("last_order"), F.col("first_order")))
    .withColumn("_gold_timestamp", F.current_timestamp())
)

df_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.customer_ltv")

print(f"Wrote {df_customer.count()} customer records to gold.customer_ltv")

# Gold: Product performance
df_product = (
    df_silver
    .groupBy("product_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_buyers"),
    )
    .withColumn("_gold_timestamp", F.current_timestamp())
    .orderBy(F.desc("total_revenue"))
)

df_product.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.product_performance")

print(f"Wrote {df_product.count()} product records to gold.product_performance")
