output "storage_account_id" {
  value = azurerm_storage_account.datalake.id
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "bronze_container" {
  value = azurerm_storage_container.bronze.name
}

output "silver_container" {
  value = azurerm_storage_container.silver.name
}

output "gold_container" {
  value = azurerm_storage_container.gold.name
}
