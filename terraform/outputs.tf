output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = module.storage.storage_account_name
}

output "databricks_workspace_url" {
  value = module.databricks.workspace_url
}

output "synapse_endpoint" {
  value = module.synapse.sql_endpoint
}

output "data_factory_name" {
  value = module.data_factory.factory_name
}
