# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: Business-Ready Denormalized Tables
# MAGIC Reads Data Vault structures (Hubs, Links, Satellites) from Silver
# MAGIC and builds wide, denormalized Gold tables optimized for BI and Synapse.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("process_date", "2024-01-15")
PROCESS_DATE = dbutils.widgets.get("process_date")

STORAGE_ACCOUNT = spark.conf.get("spark.databricks.env.STORAGE_ACCOUNT",
                                  "stlakehousedev")
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data Vault Tables

# COMMAND ----------

hub_customer = spark.read.format("delta").load(f"{SILVER_PATH}/hub_customer")
hub_product = spark.read.format("delta").load(f"{SILVER_PATH}/hub_product")
hub_order = spark.read.format("delta").load(f"{SILVER_PATH}/hub_order")

lnk_order_customer = spark.read.format("delta").load(
    f"{SILVER_PATH}/lnk_order_customer")
lnk_order_product = spark.read.format("delta").load(
    f"{SILVER_PATH}/lnk_order_product")

sat_customer = spark.read.format("delta").load(
    f"{SILVER_PATH}/sat_customer_details")
sat_order = spark.read.format("delta").load(
    f"{SILVER_PATH}/sat_order_details")
sat_inventory = spark.read.format("delta").load(
    f"{SILVER_PATH}/sat_product_inventory")
sat_clickstream = spark.read.format("delta").load(
    f"{SILVER_PATH}/sat_clickstream_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Satellite Point-in-Time: Get Latest Record per Hub Key
# MAGIC Data Vault satellites track history. For Gold we need the current
# MAGIC (latest) version of each satellite record.

# COMMAND ----------

def get_latest_satellite(df, hub_key_col):
    """Return only the most recent satellite record per hub key."""
    w = Window.partitionBy(hub_key_col).orderBy(
        F.col("_load_timestamp").desc())
    return (df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "hash_diff", "_load_timestamp", "_record_source"))


sat_customer_latest = get_latest_satellite(sat_customer, "hub_customer_hk")
sat_order_latest = get_latest_satellite(sat_order, "hub_order_hk")
sat_inventory_latest = get_latest_satellite(sat_inventory, "hub_product_hk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1: gold_orders_wide
# MAGIC Fully denormalized order table joining order, customer, and product
# MAGIC data. One row per order-product combination.

# COMMAND ----------

gold_orders = (
    hub_order.alias("ho")
    .join(lnk_order_customer.alias("loc"),
          F.col("ho.hub_order_hk") == F.col("loc.hub_order_hk"))
    .join(hub_customer.alias("hc"),
          F.col("loc.hub_customer_hk") == F.col("hc.hub_customer_hk"))
    .join(sat_customer_latest.alias("sc"),
          F.col("hc.hub_customer_hk") == F.col("sc.hub_customer_hk"))
    .join(sat_order_latest.alias("so"),
          F.col("ho.hub_order_hk") == F.col("so.hub_order_hk"))
    .join(lnk_order_product.alias("lop"),
          F.col("ho.hub_order_hk") == F.col("lop.hub_order_hk"))
    .join(hub_product.alias("hp"),
          F.col("lop.hub_product_hk") == F.col("hp.hub_product_hk"))
    .select(
        F.col("ho.hub_order_hk"),
        F.col("ho.order_id"),
        F.col("hc.customer_id"),
        F.col("sc.first_name"),
        F.col("sc.last_name"),
        F.col("sc.email"),
        F.col("sc.address_city"),
        F.col("sc.address_state"),
        F.col("sc.customer_segment"),
        F.col("sc.loyalty_tier"),
        F.col("hp.product_id"),
        F.col("so.order_date"),
        F.col("so.order_status"),
        F.col("so.payment_method"),
        F.col("so.shipping_method"),
        F.col("so.total_amount"),
        F.col("so.discount_amount"),
        F.col("so.tax_amount"),
        F.col("so.shipping_cost"),
        F.current_timestamp().alias("_gold_loaded_at")
    )
)

if DeltaTable.isDeltaTable(spark, f"{GOLD_PATH}/gold_orders_wide"):
    dt = DeltaTable.forPath(spark, f"{GOLD_PATH}/gold_orders_wide")
    dt.alias("tgt").merge(
        gold_orders.alias("src"),
        "tgt.hub_order_hk = src.hub_order_hk AND "
        "tgt.product_id = src.product_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    (gold_orders.write.format("delta")
     .mode("overwrite")
     .option("delta.autoOptimize.optimizeWrite", "true")
     .option("delta.autoOptimize.autoCompact", "true")
     .save(f"{GOLD_PATH}/gold_orders_wide"))

print(f"gold_orders_wide loaded: {gold_orders.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: gold_customer_360
# MAGIC Customer 360 view with lifetime metrics, latest profile, and
# MAGIC behavioral segmentation.

# COMMAND ----------

customer_order_metrics = (
    hub_order
    .join(lnk_order_customer,
          hub_order.hub_order_hk == lnk_order_customer.hub_order_hk)
    .join(sat_order_latest,
          hub_order.hub_order_hk == sat_order_latest.hub_order_hk)
    .groupBy("hub_customer_hk")
    .agg(
        F.count("*").alias("lifetime_orders"),
        F.sum("total_amount").alias("lifetime_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.sum("discount_amount").alias("total_discounts_used"),
        F.countDistinct(F.col("payment_method")).alias(
            "distinct_payment_methods")
    )
)

customer_click_metrics = (
    sat_clickstream
    .groupBy("hub_customer_hk")
    .agg(
        F.countDistinct("session_id").alias("total_sessions"),
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("event_type") == "purchase", 1)
              .otherwise(0)).alias("purchase_events"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1)
              .otherwise(0)).alias("cart_events"),
        F.countDistinct("device_type").alias("distinct_devices")
    )
)

gold_customer_360 = (
    hub_customer.alias("hc")
    .join(sat_customer_latest.alias("sc"),
          F.col("hc.hub_customer_hk") == F.col("sc.hub_customer_hk"))
    .join(customer_order_metrics.alias("om"),
          F.col("hc.hub_customer_hk") == F.col("om.hub_customer_hk"),
          "left")
    .join(customer_click_metrics.alias("cm"),
          F.col("hc.hub_customer_hk") == F.col("cm.hub_customer_hk"),
          "left")
    .select(
        F.col("hc.hub_customer_hk"),
        F.col("hc.customer_id"),
        F.col("sc.first_name"),
        F.col("sc.last_name"),
        F.col("sc.email"),
        F.col("sc.address_city"),
        F.col("sc.address_state"),
        F.col("sc.customer_segment"),
        F.col("sc.loyalty_tier"),
        F.coalesce(F.col("om.lifetime_orders"), F.lit(0)).alias(
            "lifetime_orders"),
        F.coalesce(F.col("om.lifetime_revenue"), F.lit(0.0)).alias(
            "lifetime_revenue"),
        F.coalesce(F.col("om.avg_order_value"), F.lit(0.0)).alias(
            "avg_order_value"),
        F.col("om.first_order_date"),
        F.col("om.last_order_date"),
        F.coalesce(F.col("om.total_discounts_used"), F.lit(0.0)).alias(
            "total_discounts_used"),
        F.coalesce(F.col("cm.total_sessions"), F.lit(0)).alias(
            "total_sessions"),
        F.coalesce(F.col("cm.total_events"), F.lit(0)).alias(
            "total_page_views"),
        F.coalesce(F.col("cm.purchase_events"), F.lit(0)).alias(
            "purchase_events"),
        F.when(F.col("om.lifetime_revenue") > 1000, "High Value")
         .when(F.col("om.lifetime_revenue") > 500, "Medium Value")
         .when(F.col("om.lifetime_revenue") > 0, "Low Value")
         .otherwise("No Purchase").alias("value_segment"),
        F.current_timestamp().alias("_gold_loaded_at")
    )
)

(gold_customer_360.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(f"{GOLD_PATH}/gold_customer_360"))

print(f"gold_customer_360 loaded: {gold_customer_360.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3: gold_inventory_snapshot
# MAGIC Current inventory with reorder alerts and stock health metrics.

# COMMAND ----------

gold_inventory = (
    hub_product.alias("hp")
    .join(sat_inventory_latest.alias("si"),
          F.col("hp.hub_product_hk") == F.col("si.hub_product_hk"))
    .select(
        F.col("hp.hub_product_hk"),
        F.col("hp.product_id"),
        F.col("si.quantity_on_hand"),
        F.col("si.reorder_point"),
        F.col("si.warehouse_location"),
        F.col("si.unit_cost"),
        F.col("si.last_restocked_date"),
        (F.col("si.quantity_on_hand") - F.col("si.reorder_point")).alias(
            "stock_above_reorder"),
        F.when(F.col("si.quantity_on_hand") <= F.col("si.reorder_point"),
               "REORDER_NOW")
         .when(F.col("si.quantity_on_hand") <=
               F.col("si.reorder_point") * 1.5, "LOW_STOCK")
         .otherwise("HEALTHY").alias("stock_status"),
        (F.col("si.quantity_on_hand") * F.col("si.unit_cost")).alias(
            "inventory_value"),
        F.current_timestamp().alias("_gold_loaded_at")
    )
)

(gold_inventory.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(f"{GOLD_PATH}/gold_inventory_snapshot"))

print(f"gold_inventory_snapshot loaded: {gold_inventory.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 4: gold_daily_sales_agg
# MAGIC Pre-aggregated daily sales metrics for fast BI dashboards.

# COMMAND ----------

gold_daily_sales = (
    hub_order
    .join(sat_order_latest,
          hub_order.hub_order_hk == sat_order_latest.hub_order_hk)
    .join(lnk_order_customer,
          hub_order.hub_order_hk == lnk_order_customer.hub_order_hk)
    .join(sat_customer_latest,
          lnk_order_customer.hub_customer_hk ==
          sat_customer_latest.hub_customer_hk)
    .withColumn("order_day", F.to_date("order_date"))
    .groupBy("order_day", "address_state", "customer_segment",
             "payment_method")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("total_amount").alias("gross_revenue"),
        F.sum("discount_amount").alias("total_discounts"),
        F.sum(F.col("total_amount") - F.col("discount_amount")).alias(
            "net_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum("tax_amount").alias("total_tax"),
        F.sum("shipping_cost").alias("total_shipping"),
        F.countDistinct(
            lnk_order_customer.hub_customer_hk).alias("unique_customers")
    )
    .withColumn("_gold_loaded_at", F.current_timestamp())
)

(gold_daily_sales.write.format("delta")
 .mode("overwrite")
 .partitionBy("order_day")
 .option("overwriteSchema", "true")
 .save(f"{GOLD_PATH}/gold_daily_sales_agg"))

print(f"gold_daily_sales_agg loaded: {gold_daily_sales.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-Order Optimization on Gold Tables

# COMMAND ----------

spark.sql(f"""
    OPTIMIZE delta.`{GOLD_PATH}/gold_orders_wide`
    ZORDER BY (order_date, address_state)
""")

spark.sql(f"""
    OPTIMIZE delta.`{GOLD_PATH}/gold_customer_360`
    ZORDER BY (customer_id, value_segment)
""")

print("Z-Order optimization complete on Gold tables")

# COMMAND ----------

print(f"\n=== Silver → Gold complete for {PROCESS_DATE} ===")
print("Loaded: gold_orders_wide, gold_customer_360")
print("Loaded: gold_inventory_snapshot, gold_daily_sales_agg")
print("Z-Ordered: gold_orders_wide, gold_customer_360")
