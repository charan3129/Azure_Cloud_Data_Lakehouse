variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "environment" { type = string }
variable "storage_account_id" { type = string }
variable "storage_account_name" { type = string }
variable "sql_admin_password" {
  type      = string
  sensitive = true
  default   = "Ch@ngeMe123!"
}
