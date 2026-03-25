# Data Quality Checks
# Validates data across all lakehouse layers

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

print("Running data quality checks...")

errors = []

# --- Bronze checks ---
try:
    bronze = spark.read.format("delta").table("bronze.raw_orders")
    bronze_count = bronze.count()
    print(f"  Bronze raw_orders: {bronze_count} records")
    if bronze_count == 0:
        errors.append("Bronze raw_orders is empty")
except Exception as e:
    errors.append(f"Bronze raw_orders not accessible: {e}")

# --- Silver checks ---
try:
    silver = spark.read.format("delta").table("silver.clean_orders")
    silver_count = silver.count()
    null_amounts = silver.filter(F.col("amount").isNull()).count()
    null_ids = silver.filter(F.col("order_id").isNull()).count()
    dup_count = silver.count() - silver.dropDuplicates(["order_id"]).count()

    print(f"  Silver clean_orders: {silver_count} records")
    print(f"    Null amounts: {null_amounts}")
    print(f"    Null order_ids: {null_ids}")
    print(f"    Duplicates: {dup_count}")

    if silver_count == 0:
        errors.append("Silver clean_orders is empty")
    if null_amounts > 0:
        errors.append(f"Silver has {null_amounts} null amounts")
    if null_ids > 0:
        errors.append(f"Silver has {null_ids} null order_ids")
    if dup_count > 0:
        errors.append(f"Silver has {dup_count} duplicate order_ids")
except Exception as e:
    errors.append(f"Silver clean_orders not accessible: {e}")

# --- Gold checks ---
try:
    daily = spark.read.format("delta").table("gold.daily_revenue")
    daily_count = daily.count()
    negative_revenue = daily.filter(F.col("revenue") < 0).count()

    print(f"  Gold daily_revenue: {daily_count} records")
    print(f"    Negative revenue days: {negative_revenue}")

    if daily_count == 0:
        errors.append("Gold daily_revenue is empty")
    if negative_revenue > 0:
        errors.append(f"Gold has {negative_revenue} days with negative revenue")
except Exception as e:
    errors.append(f"Gold daily_revenue not accessible: {e}")

try:
    customers = spark.read.format("delta").table("gold.customer_ltv")
    print(f"  Gold customer_ltv: {customers.count()} records")
except Exception as e:
    errors.append(f"Gold customer_ltv not accessible: {e}")

# --- Summary ---
print()
if errors:
    print(f"FAILED: {len(errors)} quality check(s) failed:")
    for err in errors:
        print(f"  - {err}")
    raise Exception(f"Data quality validation failed: {len(errors)} error(s)")
else:
    print("PASSED: All data quality checks passed")
