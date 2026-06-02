# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: Data Vault 2.0 Loading
# MAGIC Reads raw JSON/CSV from Bronze layer and loads into Data Vault
# MAGIC structures (Hubs, Links, Satellites) in Silver layer.

# COMMAND ----------

import hashlib
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("process_date", "2024-01-15")
PROCESS_DATE = dbutils.widgets.get("process_date")

STORAGE_ACCOUNT = spark.conf.get("spark.databricks.env.STORAGE_ACCOUNT",
                                  "stlakehousedev")
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

LOAD_TS = F.current_timestamp()
RECORD_SOURCE = F.lit("bronze_ingestion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hash Key Generation

# COMMAND ----------

def generate_hash_key(*cols):
    """Generate MD5 hash key from business key columns for Data Vault."""
    concat_expr = F.concat_ws("||", *[F.upper(F.trim(F.col(c).cast("string")))
                                       for c in cols])
    return F.md5(concat_expr)


def generate_hash_diff(*cols):
    """Generate hash diff from descriptive attributes for satellite
    change detection."""
    concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"),
                                                   F.lit(""))
                                       for c in cols])
    return F.md5(concat_expr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

df_orders_raw = (spark.read.json(
    f"{BRONZE_PATH}/orders/{PROCESS_DATE.replace('-', '/')}/"
).withColumn("_load_timestamp", LOAD_TS)
 .withColumn("_record_source", RECORD_SOURCE))

df_customers_raw = (spark.read.json(
    f"{BRONZE_PATH}/customers/{PROCESS_DATE.replace('-', '/')}/"
).withColumn("_load_timestamp", LOAD_TS)
 .withColumn("_record_source", RECORD_SOURCE))

df_clickstream_raw = (spark.read.json(
    f"{BRONZE_PATH}/clickstream/{PROCESS_DATE.replace('-', '/')}/"
).withColumn("_load_timestamp", LOAD_TS)
 .withColumn("_record_source", RECORD_SOURCE))

df_inventory_raw = (spark.read.json(
    f"{BRONZE_PATH}/inventory/{PROCESS_DATE.replace('-', '/')}/"
).withColumn("_load_timestamp", LOAD_TS)
 .withColumn("_record_source", RECORD_SOURCE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hub: hub_customer
# MAGIC Business key: customer_id

# COMMAND ----------

hub_customer = (df_customers_raw
    .select(
        generate_hash_key("customer_id").alias("hub_customer_hk"),
        F.col("customer_id"),
        F.col("_load_timestamp"),
        F.col("_record_source")
    )
    .dropDuplicates(["customer_id"])
)

if DeltaTable.isDeltaTable(spark, f"{SILVER_PATH}/hub_customer"):
    dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/hub_customer")
    dt.alias("tgt").merge(
        hub_customer.alias("src"),
        "tgt.hub_customer_hk = src.hub_customer_hk"
    ).whenNotMatchedInsertAll().execute()
else:
    hub_customer.write.format("delta").mode("overwrite").save(
        f"{SILVER_PATH}/hub_customer")

print(f"hub_customer loaded: {hub_customer.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hub: hub_product
# MAGIC Business key: product_id

# COMMAND ----------

hub_product = (df_orders_raw
    .select(F.explode("line_items").alias("item"))
    .select(
        generate_hash_key("item.product_id").alias("hub_product_hk"),
        F.col("item.product_id").alias("product_id"),
        LOAD_TS.alias("_load_timestamp"),
        RECORD_SOURCE.alias("_record_source")
    )
    .dropDuplicates(["product_id"])
)

if DeltaTable.isDeltaTable(spark, f"{SILVER_PATH}/hub_product"):
    dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/hub_product")
    dt.alias("tgt").merge(
        hub_product.alias("src"),
        "tgt.hub_product_hk = src.hub_product_hk"
    ).whenNotMatchedInsertAll().execute()
else:
    hub_product.write.format("delta").mode("overwrite").save(
        f"{SILVER_PATH}/hub_product")

print(f"hub_product loaded: {hub_product.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hub: hub_order
# MAGIC Business key: order_id

# COMMAND ----------

hub_order = (df_orders_raw
    .select(
        generate_hash_key("order_id").alias("hub_order_hk"),
        F.col("order_id"),
        F.col("_load_timestamp"),
        F.col("_record_source")
    )
    .dropDuplicates(["order_id"])
)

if DeltaTable.isDeltaTable(spark, f"{SILVER_PATH}/hub_order"):
    dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/hub_order")
    dt.alias("tgt").merge(
        hub_order.alias("src"),
        "tgt.hub_order_hk = src.hub_order_hk"
    ).whenNotMatchedInsertAll().execute()
else:
    hub_order.write.format("delta").mode("overwrite").save(
        f"{SILVER_PATH}/hub_order")

print(f"hub_order loaded: {hub_order.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Link: lnk_order_customer
# MAGIC Connects orders to customers

# COMMAND ----------

lnk_order_customer = (df_orders_raw
    .select(
        generate_hash_key("order_id", "customer_id").alias(
            "lnk_order_customer_hk"),
        generate_hash_key("order_id").alias("hub_order_hk"),
        generate_hash_key("customer_id").alias("hub_customer_hk"),
        F.col("_load_timestamp"),
        F.col("_record_source")
    )
    .dropDuplicates(["lnk_order_customer_hk"])
)

if DeltaTable.isDeltaTable(spark, f"{SILVER_PATH}/lnk_order_customer"):
    dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/lnk_order_customer")
    dt.alias("tgt").merge(
        lnk_order_customer.alias("src"),
        "tgt.lnk_order_customer_hk = src.lnk_order_customer_hk"
    ).whenNotMatchedInsertAll().execute()
else:
    lnk_order_customer.write.format("delta").mode("overwrite").save(
        f"{SILVER_PATH}/lnk_order_customer")

print(f"lnk_order_customer loaded: {lnk_order_customer.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Link: lnk_order_product
# MAGIC Connects orders to products (many-to-many via line items)

# COMMAND ----------

lnk_order_product = (df_orders_raw
    .select(F.col("order_id"), F.explode("line_items").alias("item"))
    .select(
        generate_hash_key("order_id",
                          "item.product_id").alias("lnk_order_product_hk"),
        generate_hash_key("order_id").alias("hub_order_hk"),
        generate_hash_key("item.product_id").alias("hub_product_hk"),
        LOAD_TS.alias("_load_timestamp"),
        RECORD_SOURCE.alias("_record_source")
    )
    .dropDuplicates(["lnk_order_product_hk"])
)

if DeltaTable.isDeltaTable(spark, f"{SILVER_PATH}/lnk_order_product"):
    dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/lnk_order_product")
    dt.alias("tgt").merge(
        lnk_order_product.alias("src"),
        "tgt.lnk_order_product_hk = src.lnk_order_product_hk"
    ).whenNotMatchedInsertAll().execute()
else:
    lnk_order_product.write.format("delta").mode("overwrite").save(
        f"{SILVER_PATH}/lnk_order_product")

print(f"lnk_order_product loaded: {lnk_order_product.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Satellite: sat_customer_details
# MAGIC Tracks changes in customer profile (SCD Type 2 via hash diff)

# COMMAND ----------

sat_customer = (df_customers_raw
    .select(
        generate_hash_key("customer_id").alias("hub_customer_hk"),
        generate_hash_diff("first_name", "last_name", "email", "phone",
                           "address_street", "address_city", "address_state",
                           "address_zip").alias("hash_diff"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email"),
        F.col("phone"),
        F.col("address_street"),
        F.col("address_city"),
        F.col("address_state"),
        F.col("address_zip"),
        F.col("customer_segment"),
        F.col("loyalty_tier"),
        F.col("_load_timestamp"),
        F.col("_record_source")
    )
)

if DeltaTable.isDeltaTable(spark, f"{SILVER_PATH}/sat_customer_details"):
    dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/sat_customer_details")
    dt.alias("tgt").merge(
        sat_customer.alias("src"),
        "tgt.hub_customer_hk = src.hub_customer_hk AND "
        "tgt.hash_diff = src.hash_diff"
    ).whenNotMatchedInsertAll().execute()
else:
    sat_customer.write.format("delta").mode("overwrite").save(
        f"{SILVER_PATH}/sat_customer_details")

print(f"sat_customer_details loaded: {sat_customer.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Satellite: sat_order_details
# MAGIC Order attributes that may be updated (status changes)

# COMMAND ----------

sat_order = (df_orders_raw
    .select(
        generate_hash_key("order_id").alias("hub_order_hk"),
        generate_hash_diff("order_status", "payment_method",
                           "shipping_method", "total_amount",
                           "discount_amount").alias("hash_diff"),
        F.col("order_status"),
        F.col("order_date"),
        F.col("payment_method"),
        F.col("shipping_method"),
        F.col("total_amount").cast("decimal(12,2)"),
        F.col("discount_amount").cast("decimal(12,2)"),
        F.col("tax_amount").cast("decimal(12,2)"),
        F.col("shipping_cost").cast("decimal(12,2)"),
        F.col("_load_timestamp"),
        F.col("_record_source")
    )
)

if DeltaTable.isDeltaTable(spark, f"{SILVER_PATH}/sat_order_details"):
    dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/sat_order_details")
    dt.alias("tgt").merge(
        sat_order.alias("src"),
        "tgt.hub_order_hk = src.hub_order_hk AND "
        "tgt.hash_diff = src.hash_diff"
    ).whenNotMatchedInsertAll().execute()
else:
    sat_order.write.format("delta").mode("overwrite").save(
        f"{SILVER_PATH}/sat_order_details")

print(f"sat_order_details loaded: {sat_order.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Satellite: sat_product_inventory
# MAGIC Inventory snapshots per product

# COMMAND ----------

sat_inventory = (df_inventory_raw
    .select(
        generate_hash_key("product_id").alias("hub_product_hk"),
        generate_hash_diff("quantity_on_hand", "reorder_point",
                           "warehouse_location",
                           "unit_cost").alias("hash_diff"),
        F.col("quantity_on_hand").cast("int"),
        F.col("reorder_point").cast("int"),
        F.col("warehouse_location"),
        F.col("unit_cost").cast("decimal(10,2)"),
        F.col("last_restocked_date"),
        F.col("_load_timestamp"),
        F.col("_record_source")
    )
)

if DeltaTable.isDeltaTable(spark, f"{SILVER_PATH}/sat_product_inventory"):
    dt = DeltaTable.forPath(spark, f"{SILVER_PATH}/sat_product_inventory")
    dt.alias("tgt").merge(
        sat_inventory.alias("src"),
        "tgt.hub_product_hk = src.hub_product_hk AND "
        "tgt.hash_diff = src.hash_diff"
    ).whenNotMatchedInsertAll().execute()
else:
    sat_inventory.write.format("delta").mode("overwrite").save(
        f"{SILVER_PATH}/sat_product_inventory")

print(f"sat_product_inventory loaded: {sat_inventory.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Satellite: sat_clickstream_events
# MAGIC Raw clickstream events linked to customers

# COMMAND ----------

sat_clickstream = (df_clickstream_raw
    .select(
        generate_hash_key("session_id").alias("hub_session_hk"),
        generate_hash_key("customer_id").alias("hub_customer_hk"),
        generate_hash_diff("event_type", "page_url", "product_id",
                           "referrer").alias("hash_diff"),
        F.col("session_id"),
        F.col("event_type"),
        F.col("page_url"),
        F.col("product_id"),
        F.col("referrer"),
        F.col("device_type"),
        F.col("browser"),
        F.col("event_timestamp"),
        F.col("_load_timestamp"),
        F.col("_record_source")
    )
)

sat_clickstream.write.format("delta").mode("append").save(
    f"{SILVER_PATH}/sat_clickstream_events")

print(f"sat_clickstream_events loaded: {sat_clickstream.count()} records")

# COMMAND ----------

print(f"\n=== Bronze → Silver complete for {PROCESS_DATE} ===")
print("Loaded: hub_customer, hub_product, hub_order")
print("Loaded: lnk_order_customer, lnk_order_product")
print("Loaded: sat_customer_details, sat_order_details")
print("Loaded: sat_product_inventory, sat_clickstream_events")
