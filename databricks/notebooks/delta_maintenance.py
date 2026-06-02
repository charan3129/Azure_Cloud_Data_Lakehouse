# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Maintenance
# MAGIC Weekly optimization: VACUUM, OPTIMIZE, and table statistics.

# COMMAND ----------

STORAGE_ACCOUNT = spark.conf.get("spark.databricks.env.STORAGE_ACCOUNT",
                                  "stlakehousedev")
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

SILVER_TABLES = [
    "hub_customer", "hub_product", "hub_order",
    "lnk_order_customer", "lnk_order_product",
    "sat_customer_details", "sat_order_details",
    "sat_product_inventory", "sat_clickstream_events"
]

GOLD_TABLES = [
    "gold_orders_wide", "gold_customer_360",
    "gold_inventory_snapshot", "gold_daily_sales_agg"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM — Remove Old Files (7-day retention)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",
               "false")

for table in SILVER_TABLES:
    try:
        spark.sql(f"VACUUM delta.`{SILVER_PATH}/{table}` RETAIN 168 HOURS")
        print(f"VACUUM complete: {table}")
    except Exception as e:
        print(f"VACUUM skipped {table}: {e}")

for table in GOLD_TABLES:
    try:
        spark.sql(f"VACUUM delta.`{GOLD_PATH}/{table}` RETAIN 168 HOURS")
        print(f"VACUUM complete: {table}")
    except Exception as e:
        print(f"VACUUM skipped {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE — Compact Small Files

# COMMAND ----------

for table in SILVER_TABLES:
    try:
        spark.sql(f"OPTIMIZE delta.`{SILVER_PATH}/{table}`")
        print(f"OPTIMIZE complete: {table}")
    except Exception as e:
        print(f"OPTIMIZE skipped {table}: {e}")

for table in GOLD_TABLES:
    try:
        spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}/{table}`")
        print(f"OPTIMIZE complete: {table}")
    except Exception as e:
        print(f"OPTIMIZE skipped {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Statistics

# COMMAND ----------

for table in GOLD_TABLES:
    try:
        spark.sql(
            f"ANALYZE TABLE delta.`{GOLD_PATH}/{table}` COMPUTE STATISTICS")
        print(f"Statistics computed: {table}")
    except Exception as e:
        print(f"Stats skipped {table}: {e}")

# COMMAND ----------

print("\n=== Delta maintenance complete ===")
