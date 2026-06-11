# Setup Guide — Azure Cloud Data Lakehouse

## Prerequisites
- Azure subscription with Owner or Contributor access
- Terraform >= 1.5 installed
- Azure CLI installed and authenticated (`az login`)
- Databricks CLI configured
- Python 3.10+
- Docker and Docker Compose

## 1. Deploy Azure Infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your subscription ID

terraform init
terraform plan
terraform apply
```

This creates:
- Resource Group
- ADLS Gen2 Storage Account (with bronze/silver/gold/raw containers)
- Azure Data Factory (with managed identity + storage RBAC)
- Databricks Workspace (premium SKU, no public IP)
- Synapse Analytics Workspace + SQL Pool

## 2. Configure Databricks

1. Open Databricks workspace URL (from Terraform output)
2. Create a secret scope: `lakehouse-secrets`
3. Add secrets: storage account key, SFTP credentials
4. Import notebooks from `databricks/notebooks/`
5. Create jobs for each notebook

## 3. Deploy ADF Pipelines

1. Open Azure Data Factory Studio
2. Import linked services from `adf/linkedservices/`
3. Import datasets from `adf/datasets/`
4. Import pipelines from `adf/pipelines/`
5. Configure triggers for scheduled execution

## 4. Set Up Synapse

1. Open Synapse Studio
2. Run `synapse/external_tables.sql` (replace variables)
3. Run `synapse/analytics_views.sql`

## 5. Local Development

```bash
# Start Airflow locally
docker-compose up -d

# Run tests
pip install pytest
pytest tests/ -v

# Lint
flake8 --max-line-length=120 --ignore=E501,W503,E402 \
    tests/ great_expectations/plugins/ airflow/dags/
```

## 6. Connect BI Tools

Power BI / Tableau can connect to:
- **Synapse SQL Pool** for external table queries
- **Databricks SQL Warehouse** for direct Gold layer access
