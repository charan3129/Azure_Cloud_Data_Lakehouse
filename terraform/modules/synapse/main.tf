resource "azurerm_synapse_workspace" "main" {
  name                                 = "syn-${var.environment}-lakehouse"
  resource_group_name                  = var.resource_group_name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = var.sql_admin_password

  identity {
    type = "SystemAssigned"
  }

  tags = {
    layer = "serving"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  name               = "synapse"
  storage_account_id = var.storage_account_id
}

resource "azurerm_synapse_sql_pool" "serving" {
  name                 = "sqlpool_serving"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  sku_name             = "DW100c"
  create_mode          = "Default"
}
