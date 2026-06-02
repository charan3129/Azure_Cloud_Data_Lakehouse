-- ============================================================
-- Synapse Analytics: External Tables Reading Gold Delta Lake
-- ============================================================

-- Create master key for external data access
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$(MASTER_KEY_PASSWORD)';

-- Create credential for ADLS access
CREATE DATABASE SCOPED CREDENTIAL adls_credential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '$(SAS_TOKEN)';

-- Create external data source pointing to Gold layer
CREATE EXTERNAL DATA SOURCE gold_delta_lake
WITH (
    LOCATION = 'abfss://gold@$(STORAGE_ACCOUNT).dfs.core.windows.net',
    CREDENTIAL = adls_credential
);

-- Create external file format for Delta/Parquet
CREATE EXTERNAL FILE FORMAT parquet_format
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- ============================================================
-- External Tables
-- ============================================================

CREATE EXTERNAL TABLE ext_gold_orders_wide (
    hub_order_hk        VARCHAR(32),
    order_id            VARCHAR(50),
    customer_id         VARCHAR(50),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(200),
    address_city        VARCHAR(100),
    address_state       VARCHAR(10),
    customer_segment    VARCHAR(20),
    loyalty_tier        VARCHAR(20),
    product_id          VARCHAR(50),
    order_date          DATETIME2,
    order_status        VARCHAR(20),
    payment_method      VARCHAR(30),
    shipping_method     VARCHAR(30),
    total_amount        DECIMAL(12,2),
    discount_amount     DECIMAL(12,2),
    tax_amount          DECIMAL(12,2),
    shipping_cost       DECIMAL(12,2),
    _gold_loaded_at     DATETIME2
)
WITH (
    LOCATION = '/gold_orders_wide/',
    DATA_SOURCE = gold_delta_lake,
    FILE_FORMAT = parquet_format
);

CREATE EXTERNAL TABLE ext_gold_customer_360 (
    hub_customer_hk         VARCHAR(32),
    customer_id             VARCHAR(50),
    first_name              VARCHAR(100),
    last_name               VARCHAR(100),
    email                   VARCHAR(200),
    address_city            VARCHAR(100),
    address_state           VARCHAR(10),
    customer_segment        VARCHAR(20),
    loyalty_tier            VARCHAR(20),
    lifetime_orders         INT,
    lifetime_revenue        DECIMAL(14,2),
    avg_order_value         DECIMAL(10,2),
    first_order_date        DATETIME2,
    last_order_date         DATETIME2,
    total_discounts_used    DECIMAL(12,2),
    total_sessions          INT,
    total_page_views        INT,
    purchase_events         INT,
    value_segment           VARCHAR(20),
    _gold_loaded_at         DATETIME2
)
WITH (
    LOCATION = '/gold_customer_360/',
    DATA_SOURCE = gold_delta_lake,
    FILE_FORMAT = parquet_format
);

CREATE EXTERNAL TABLE ext_gold_inventory_snapshot (
    hub_product_hk      VARCHAR(32),
    product_id          VARCHAR(50),
    quantity_on_hand    INT,
    reorder_point       INT,
    warehouse_location  VARCHAR(50),
    unit_cost           DECIMAL(10,2),
    last_restocked_date DATE,
    stock_above_reorder INT,
    stock_status        VARCHAR(20),
    inventory_value     DECIMAL(14,2),
    _gold_loaded_at     DATETIME2
)
WITH (
    LOCATION = '/gold_inventory_snapshot/',
    DATA_SOURCE = gold_delta_lake,
    FILE_FORMAT = parquet_format
);

CREATE EXTERNAL TABLE ext_gold_daily_sales_agg (
    order_day           DATE,
    address_state       VARCHAR(10),
    customer_segment    VARCHAR(20),
    payment_method      VARCHAR(30),
    order_count         INT,
    gross_revenue       DECIMAL(14,2),
    total_discounts     DECIMAL(12,2),
    net_revenue         DECIMAL(14,2),
    avg_order_value     DECIMAL(10,2),
    total_tax           DECIMAL(12,2),
    total_shipping      DECIMAL(12,2),
    unique_customers    INT,
    _gold_loaded_at     DATETIME2
)
WITH (
    LOCATION = '/gold_daily_sales_agg/',
    DATA_SOURCE = gold_delta_lake,
    FILE_FORMAT = parquet_format
);
