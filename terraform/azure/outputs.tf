output "resource_group_name" {
  description = "Azure resource group used by the Terraform deployment."
  value       = azurerm_resource_group.this.name
}

output "function_app_name" {
  description = "Flex Consumption Function App name."
  value       = azurerm_function_app_flex_consumption.bronze.name
}

output "function_app_url" {
  description = "Function App base URL used by the ADF linked service."
  value       = local.function_app_url
}

output "data_factory_name" {
  description = "ADF factory name."
  value       = azurerm_data_factory.this.name
}

output "pipeline_name" {
  description = "ADF Bronze CDC pipeline name."
  value       = var.pipeline_name
}

output "daily_trigger_name" {
  description = "ADF daily tumbling-window trigger name."
  value       = var.daily_trigger_name
}

output "monthly_trigger_name" {
  description = "ADF monthly full-refresh trigger name."
  value       = var.monthly_trigger_name
}

output "task_bundle_blob_path" {
  description = "ADLS path for the staged Azure Batch task bundle."
  value       = "${var.container_name}/${var.task_bundle_blob_name}"
}

output "function_package_blob_url" {
  description = "Blob URL for the Function App deployment package used by OneDeploy."
  value       = local.function_package_url
}

output "function_host_key_name" {
  description = "ADF uses this host-level Function key name."
  value       = var.function_host_key_name
}

output "function_host_key_value" {
  description = "Host-level Function key value wired into the ADF linked service."
  value       = local.function_host_key
  sensitive   = true
}
