# Data Vault 2.0 — Design Decisions

## Why Data Vault for This Project?

Data Vault 2.0 is chosen for the Silver layer because:

1. **Auditability**: Every record is traceable via hash keys, load timestamps,
   and record source. This is critical for e-commerce data where order status
   changes, customer profiles update, and inventory fluctuates.

2. **Historical tracking**: Satellites store every version of descriptive data
   using hash_diff for change detection. When a customer changes address or
   loyalty tier, both versions are retained.

3. **Flexible integration**: Hubs and Links separate business keys from
   relationships. Adding a new source (e.g., returns, reviews) means adding
   new Hubs/Links/Satellites without restructuring existing ones.

4. **Parallel loading**: Hubs, Links, and Satellites can be loaded independently
   and in parallel, enabling efficient Spark-based processing.

## Hash Key Strategy

- **Hub hash keys**: MD5 of uppercase, trimmed business key
  - `hub_customer_hk = MD5(UPPER(TRIM(customer_id)))`
- **Link hash keys**: MD5 of concatenated business keys
  - `lnk_order_customer_hk = MD5(UPPER(TRIM(order_id)) || "||" || UPPER(TRIM(customer_id)))`
- **Hash diff**: MD5 of all descriptive attributes (null-safe)
  - Used in satellite MERGE to detect changes

## Loading Patterns

### Hub Loading (Insert-only)
```
MERGE INTO hub_customer USING source
ON hub_customer.hub_customer_hk = source.hub_customer_hk
WHEN NOT MATCHED THEN INSERT ALL
```

### Satellite Loading (Change-detected insert)
```
MERGE INTO sat_customer_details USING source
ON tgt.hub_customer_hk = src.hub_customer_hk
   AND tgt.hash_diff = src.hash_diff
WHEN NOT MATCHED THEN INSERT ALL
```
If hash_diff matches → no change → skip. If different → new version inserted.

### Point-in-Time for Gold
Gold queries use window functions to get the latest satellite record:
```python
Window.partitionBy(hub_key).orderBy(desc("_load_timestamp"))
row_number == 1 → latest version
```

## Data Vault vs Star Schema

| Aspect | Data Vault (Silver) | Star Schema (Kimball) |
|--------|-------------------|---------------------|
| Purpose | Integration, audit | BI performance |
| History | Full SCD2 via satellites | Typically SCD1 or limited SCD2 |
| Flexibility | Add sources without restructure | Schema changes propagate |
| Query complexity | Joins across Hub-Link-Sat | Simple fact-dim joins |
| Best for | Source-of-truth layer | Reporting layer |

In this project, Data Vault serves as the durable, auditable Silver layer,
while Gold tables flatten everything into wide tables for BI tools.
