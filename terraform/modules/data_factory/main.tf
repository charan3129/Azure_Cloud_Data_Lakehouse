resource "azurerm_data_factory" "main" {
  name                = "adf-${var.environment}-lakehouse"
  location            = var.location
  resource_group_name = var.resource_group_name

  identity {
    type = "SystemAssigned"
  }

  tags = {
    layer = "orchestration"
  }
}

resource "azurerm_role_assignment" "adf_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}
