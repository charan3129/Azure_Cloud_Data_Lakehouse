output "sql_endpoint" {
  value = azurerm_synapse_workspace.main.connectivity_endpoints["sql"]
}

output "workspace_id" {
  value = azurerm_synapse_workspace.main.id
}
