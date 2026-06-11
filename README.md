# Azure Cloud Data Lakehouse

Enterprise-grade data lakehouse on Azure implementing the **medallion architecture (Bronze → Silver → Gold)** with **Data Vault 2.0** modeling in the Silver layer. Processes multi-source e-commerce data through Azure Data Factory, Databricks (PySpark + Delta Lake), and serves analytics via Azure Synapse.

## Architecture

📐 **[View Full Architecture Diagram (PDF)](docs/architecture_diagram.pdf)** — System architecture with all Azure components

📊 **[View Pipeline Flowchart (PDF)](docs/pipeline_flowchart.pdf)** — Data flow from sources through Bronze → Silver → Gold

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│   Orders API    Clickstream API    Inventory SFTP    Customers API   │
└──────┬──────────────┬──────────────────┬──────────────────┬─────────┘
       │              │                  │                  │
       ▼              ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    AZURE DATA FACTORY                                │
│   pl_ingest_orders   pl_ingest_clickstream   pl_ingest_inventory    │
│   pl_ingest_customers   pl_master_lakehouse (orchestration)         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
       ┌───────────────────────┼───────────────────────┐
       ▼                       ▼                       ▼
┌──────────────┐    ┌────────────────────┐    ┌──────────────────┐
│   BRONZE     │    │      SILVER        │    │      GOLD        │
│   (ADLS)     │───▶│   (Data Vault)     │───▶│  (Denormalized)  │
│              │    │                    │    │                  │
│ Raw JSON/CSV │    │ hub_customer       │    │ gold_orders_wide │
│ Append-only  │    │ hub_product        │    │ gold_customer_360│
│ Schema-on-   │    │ hub_order          │    │ gold_inventory   │
│ read         │    │ lnk_order_customer │    │ gold_daily_sales │
│              │    │ lnk_order_product  │    │                  │
│              │    │ sat_customer_*     │    │ Z-Ordered        │
│              │    │ sat_order_*        │    │ Auto-optimized   │
│              │    │ sat_inventory_*    │    │                  │
│              │    │ sat_clickstream_*  │    │                  │
└──────────────┘    └────────────────────┘    └────────┬─────────┘
                                                       │
                               ┌───────────────────────┤
                               ▼                       ▼
                    ┌──────────────────┐    ┌──────────────────┐
                    │  SYNAPSE         │    │  QUALITY GATES   │
                    │  External Tables │    │  Great Expect.   │
                    │  Analytics Views │    │  Layer Promotion  │
                    │  BI Serving      │    │  Bronze→Silver→  │
                    └──────────────────┘    │  Gold             │
                                           └──────────────────┘
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Infrastructure | Terraform (4 modules) | IaC for all Azure resources |
| Ingestion | Azure Data Factory | REST API + SFTP → ADLS Bronze |
| Storage | ADLS Gen2 | Hierarchical data lake (Bronze/Silver/Gold) |
| Processing | Databricks + PySpark | Bronze→Silver→Gold transformations |
| Table Format | Delta Lake | ACID transactions, time travel, Z-ordering |
| Silver Model | Data Vault 2.0 | Hubs, Links, Satellites with hash keys |
| Gold Model | Wide Tables | Denormalized for BI (orders, customer 360, inventory, sales) |
| Serving | Azure Synapse | External tables + views over Gold Delta |
| Orchestration | Airflow (2 DAGs) | Pipeline orchestration + maintenance |
| Quality | Great Expectations | 3 suites + layer promotion gate plugin |
| CI/CD | GitHub Actions | pytest + flake8 + JSON validation |
| Containers | Docker Compose | Local Airflow development |

## Data Vault 2.0 Model

```
Silver Layer Architecture:

  ┌─────────────┐         ┌─────────────────────┐         ┌─────────────┐
  │ hub_customer │◄───────►│ lnk_order_customer  │◄───────►│  hub_order  │
  │             │         └─────────────────────┘         │             │
  │ customer_id │                                         │  order_id   │
  └──────┬──────┘                                         └──────┬──────┘
         │                                                       │
    ┌────▼─────────────┐                                  ┌──────▼───────────┐
    │ sat_customer_    │         ┌─────────────────────┐  │ sat_order_       │
    │ details          │         │ lnk_order_product   │  │ details          │
    │                  │         └──────────┬──────────┘  │                  │
    │ SCD2 via         │                    │             │ Status tracking  │
    │ hash_diff        │              ┌─────▼──────┐     │ via hash_diff    │
    └──────────────────┘              │ hub_product │     └──────────────────┘
                                      │            │
    ┌──────────────────┐              │ product_id │
    │ sat_clickstream_ │              └─────┬──────┘
    │ events           │                    │
    │                  │         ┌──────────▼──────────┐
    │ Append-only      │         │ sat_product_        │
    │ event stream     │         │ inventory           │
    └──────────────────┘         │                     │
                                 │ Inventory snapshots │
                                 └─────────────────────┘
```

## Quality Gates

The pipeline uses a **gate-based promotion** system between layers:

| Gate | Min Success Rate | Max Null Keys | Min Rows |
|------|-----------------|---------------|----------|
| Bronze → Silver | 95% | 1% | 1 |
| Silver → Gold | 99% | 0% | 1 |

If a gate fails, data is **blocked** from promotion and alerts are triggered.

## Project Structure

```
Azure-Cloud-Data-Lakehouse/
├── terraform/                    # Infrastructure as Code
│   ├── main.tf                   # Root module
│   ├── variables.tf              # Input variables
│   ├── outputs.tf                # Output values
│   └── modules/
│       ├── storage/              # ADLS Gen2 + containers
│       ├── databricks/           # Workspace + cluster
│       ├── data_factory/         # ADF + RBAC
│       └── synapse/              # Workspace + SQL pool
├── adf/                          # Azure Data Factory
│   ├── linkedservices/           # ADLS, REST, SFTP, Databricks
│   ├── datasets/                 # Source + sink definitions
│   └── pipelines/                # Ingestion + master orchestration
├── databricks/notebooks/         # PySpark notebooks
│   ├── bronze_to_silver.py       # Data Vault loading
│   ├── silver_to_gold.py         # Denormalization + Z-ordering
│   └── delta_maintenance.py      # VACUUM + OPTIMIZE
├── synapse/                      # SQL scripts
│   ├── external_tables.sql       # Gold Delta Lake access
│   └── analytics_views.sql       # BI-ready views
├── airflow/dags/                 # Orchestration
│   ├── lakehouse_orchestration.py
│   └── delta_maintenance.py
├── great_expectations/           # Data quality
│   ├── expectations/             # Bronze, Silver, Gold suites
│   └── plugins/                  # Layer promotion gate
├── tests/                        # Test suite
│   ├── test_adf_configs.py       # ADF JSON validation
│   ├── test_data_vault.py        # Hash key + DV structure tests
│   ├── test_quality_gates.py     # Promotion gate logic tests
│   └── test_terraform.py         # Terraform structure tests
├── config/settings.yml           # Project configuration
├── data/samples/                 # Sample data files
├── docs/                         # Documentation
│   ├── SETUP_GUIDE.md
│   ├── DATA_DICTIONARY.md
│   └── DATA_VAULT_GUIDE.md
├── docker/Dockerfile.airflow
├── docker-compose.yml
└── .github/workflows/ci.yml     # CI/CD pipeline
```

## Quick Start

```bash
# Run tests
pip install pytest
pytest tests/ -v

# Start Airflow locally
docker-compose up -d

# Deploy infrastructure
cd terraform
terraform init && terraform apply
```

## Documentation

- [Setup Guide](docs/SETUP_GUIDE.md) — Full deployment instructions
- [Data Dictionary](docs/DATA_DICTIONARY.md) — Table and column reference
- [Data Vault Guide](docs/DATA_VAULT_GUIDE.md) — Modeling decisions and patterns
