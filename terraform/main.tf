terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-lakehouse-tfstate"
    storage_account_name = "stlakehousetfstate"
    container_name       = "tfstate"
    key                  = "lakehouse.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

provider "databricks" {
  host = azurerm_databricks_workspace.main.workspace_url
}

module "storage" {
  source              = "./modules/storage"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  environment         = var.environment
  project_name        = var.project_name
}

module "databricks" {
  source              = "./modules/databricks"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  environment         = var.environment
  storage_account_id  = module.storage.storage_account_id
  storage_account_name = module.storage.storage_account_name
}

module "data_factory" {
  source              = "./modules/data_factory"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  environment         = var.environment
  storage_account_id  = module.storage.storage_account_id
}

module "synapse" {
  source              = "./modules/synapse"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  environment         = var.environment
  storage_account_id  = module.storage.storage_account_id
  storage_account_name = module.storage.storage_account_name
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location
  tags     = local.tags
}

locals {
  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}
