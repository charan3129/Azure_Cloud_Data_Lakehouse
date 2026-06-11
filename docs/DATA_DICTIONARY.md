# Data Dictionary — Azure Cloud Data Lakehouse

## Bronze Layer (Raw)
Append-only raw data as-is from source systems.

| Source | Format | Schedule | Landing Path |
|--------|--------|----------|-------------|
| Orders API | JSON | Every 6 hours | bronze/orders/YYYY/MM/DD/ |
| Clickstream API | JSON | Hourly | bronze/clickstream/YYYY/MM/DD/ |
| Inventory SFTP | CSV→JSON | Daily 8 AM | bronze/inventory/YYYY/MM/DD/ |
| Customers API | JSON | Daily 6 AM | bronze/customers/YYYY/MM/DD/ |

## Silver Layer — Data Vault 2.0

### Hubs (Business Keys)
| Table | Business Key | Hash Key | Description |
|-------|-------------|----------|-------------|
| hub_customer | customer_id | hub_customer_hk (MD5) | Unique customers |
| hub_product | product_id | hub_product_hk (MD5) | Unique products |
| hub_order | order_id | hub_order_hk (MD5) | Unique orders |

### Links (Relationships)
| Table | Hub Keys | Hash Key | Description |
|-------|----------|----------|-------------|
| lnk_order_customer | hub_order_hk, hub_customer_hk | lnk_order_customer_hk | Order-Customer relationship |
| lnk_order_product | hub_order_hk, hub_product_hk | lnk_order_product_hk | Order-Product relationship (M:M via line items) |

### Satellites (Descriptive Attributes)
| Table | Parent Hub | Hash Diff Columns | Description |
|-------|-----------|-------------------|-------------|
| sat_customer_details | hub_customer | first_name, last_name, email, phone, address_*, segment, loyalty | Customer profile history (SCD2) |
| sat_order_details | hub_order | order_status, payment_method, shipping_method, amounts | Order state history |
| sat_product_inventory | hub_product | quantity_on_hand, reorder_point, warehouse_location, unit_cost | Inventory snapshots |
| sat_clickstream_events | hub_customer (via session) | event_type, page_url, product_id, referrer | Web behavior events |

## Gold Layer — Denormalized Business Tables

| Table | Grain | Key Columns | Description |
|-------|-------|-------------|-------------|
| gold_orders_wide | One row per order-product | hub_order_hk + product_id | Fully denormalized orders with customer + product data |
| gold_customer_360 | One row per customer | hub_customer_hk | Customer 360: profile + lifetime metrics + value segment |
| gold_inventory_snapshot | One row per product | hub_product_hk | Current inventory with stock status alerts |
| gold_daily_sales_agg | One row per day-state-segment-payment | order_day composite | Pre-aggregated daily sales for fast BI |

### Value Segment Logic (gold_customer_360)
| Segment | Criteria |
|---------|----------|
| High Value | lifetime_revenue > $1,000 |
| Medium Value | lifetime_revenue $500 - $1,000 |
| Low Value | lifetime_revenue > $0 |
| No Purchase | No orders |

### Stock Status Logic (gold_inventory_snapshot)
| Status | Criteria |
|--------|----------|
| REORDER_NOW | quantity_on_hand <= reorder_point |
| LOW_STOCK | quantity_on_hand <= reorder_point * 1.5 |
| HEALTHY | quantity_on_hand > reorder_point * 1.5 |

## Synapse Views

| View | Source Table | Purpose |
|------|------------|---------|
| vw_revenue_by_state | gold_daily_sales_agg | Revenue trends with cumulative totals by state |
| vw_customer_value_distribution | gold_customer_360 | Customer segmentation summary |
| vw_inventory_health | gold_inventory_snapshot | Warehouse inventory status |
| vw_payment_analysis | gold_orders_wide | Payment method breakdown by segment |
| vw_top_customers | gold_customer_360 | Top 100 customers by lifetime revenue |
