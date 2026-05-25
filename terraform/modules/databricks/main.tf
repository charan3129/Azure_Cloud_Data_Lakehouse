resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${var.environment}-lakehouse"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "premium"

  custom_parameters {
    no_public_ip = true
  }

  tags = {
    layer = "compute"
  }
}

resource "databricks_cluster" "etl" {
  cluster_name            = "etl-cluster-${var.environment}"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 30
  num_workers             = 2

  spark_conf = {
    "spark.databricks.delta.optimizeWrite.enabled" = "true"
    "spark.databricks.delta.autoCompact.enabled"   = "true"
    "spark.sql.shuffle.partitions"                 = "8"
  }

  spark_env_vars = {
    "STORAGE_ACCOUNT" = var.storage_account_name
  }
}

resource "databricks_secret_scope" "lakehouse" {
  name = "lakehouse-secrets"
}
