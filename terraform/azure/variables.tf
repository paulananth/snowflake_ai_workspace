variable "subscription_id" {
  description = "Target Azure subscription ID. Leave empty to use the active Azure CLI / ARM context."
  type        = string
  default     = ""
}

variable "resource_group_name" {
  description = "Resource group for the SEC EDGAR Azure deployment."
  type        = string
  default     = "my-sec-edgar-rg"
}

variable "location" {
  description = "Azure region for the deployment."
  type        = string
  default     = "eastus"
}

variable "storage_account_name" {
  description = "ADLS Gen2 account used for Bronze outputs and ADF task staging."
  type        = string
  default     = "mysecedgarstorage"
}

variable "container_name" {
  description = "ADLS Gen2 filesystem / container for Bronze outputs and ADF staging."
  type        = string
  default     = "sec-edgar"
}

variable "storage_prefix" {
  description = "Logical root prefix under the data lake filesystem."
  type        = string
  default     = "sec-edgar"
}

variable "batch_account_name" {
  description = "Azure Batch account name."
  type        = string
  default     = "mysecedgarbatch"
}

variable "batch_pool_id" {
  description = "Azure Batch pool ID for the host-executed Custom Activities."
  type        = string
  default     = "sec-edgar-pool"
}

variable "batch_managed_identity_name" {
  description = "User-assigned managed identity attached to the Batch pool."
  type        = string
  default     = "sec-edgar-ingest-identity"
}

variable "batch_vm_size" {
  description = "VM size for the Azure Batch host pool."
  type        = string
  default     = "STANDARD_D2S_V3"
}

variable "data_factory_name" {
  description = "Azure Data Factory name."
  type        = string
  default     = "mysecedgaradf"
}

variable "pipeline_name" {
  description = "Canonical Bronze CDC pipeline name."
  type        = string
  default     = "sec-edgar-bronze-ingest"
}

variable "daily_trigger_name" {
  description = "Daily tumbling-window trigger name."
  type        = string
  default     = "DailyBronzeIngestTrigger"
}

variable "monthly_trigger_name" {
  description = "Monthly reconciliation trigger name."
  type        = string
  default     = "MonthlyBronzeFullRefreshTrigger"
}

variable "daily_trigger_start_time" {
  description = "UTC start time for the tumbling-window trigger. This value is immutable after the trigger exists."
  type        = string
  default     = "2026-04-09T06:00:00Z"
}

variable "monthly_trigger_start_time" {
  description = "UTC start time for the monthly full-refresh trigger."
  type        = string
  default     = "2026-05-01T06:00:00Z"
}

variable "function_app_name" {
  description = "Flex Consumption Function App name. Leave null to derive from the subscription ID."
  type        = string
  default     = null
}

variable "function_storage_account_name" {
  description = "Storage account for the Function host and deployment container. Leave null to derive from the subscription ID."
  type        = string
  default     = null
}

variable "function_plan_name" {
  description = "Azure Functions Flex Consumption plan name. Leave null to derive from the subscription ID."
  type        = string
  default     = null
}

variable "application_insights_name" {
  description = "Application Insights resource name for the Function App. Leave null to derive from the subscription ID."
  type        = string
  default     = null
}

variable "log_analytics_workspace_name" {
  description = "Log Analytics workspace name for Application Insights. Leave null to derive from the subscription ID."
  type        = string
  default     = null
}

variable "function_linked_service_name" {
  description = "ADF linked service name for the Bronze Azure Functions app."
  type        = string
  default     = "AzureFunctionBronzeLS"
}

variable "function_deployment_container_name" {
  description = "Blob container used by Flex Consumption for OneDeploy package storage."
  type        = string
  default     = "deploymentpackage"
}

variable "function_runtime_name" {
  description = "Azure Functions runtime name for the Flex Consumption app."
  type        = string
  default     = "python"
}

variable "function_runtime_version" {
  description = "Azure Functions runtime version for the Flex Consumption app."
  type        = string
  default     = "3.11"
}

variable "function_maximum_instance_count" {
  description = "Maximum Flex Consumption instances for the Function App."
  type        = number
  default     = 40
}

variable "function_instance_memory_in_mb" {
  description = "Per-instance memory for the Flex Consumption Function App."
  type        = number
  default     = 2048
}

variable "function_host_key_name" {
  description = "Host-level Function key name that ADF will use."
  type        = string
  default     = "adf"
}

variable "function_host_key_value" {
  description = "Optional explicit host key value. Leave null to have Terraform generate one."
  type        = string
  default     = null
  sensitive   = true
}

variable "task_bundle_blob_name" {
  description = "Blob path, relative to the ADLS container, for the staged Azure Batch task bundle."
  type        = string
  default     = "adf-resources/sec-edgar-task.zip"
}

variable "enable_legacy_acr" {
  description = "Create the legacy Azure Container Registry used by the old Docker path."
  type        = bool
  default     = false
}

variable "legacy_acr_name" {
  description = "Legacy ACR name for the old Docker-based path."
  type        = string
  default     = "mysecedgaracr"
}

variable "sec_user_agent" {
  description = "SEC EDGAR contact string required by the Python ingest runtime."
  type        = string
}

variable "function_app_settings" {
  description = "Additional Function App settings merged into the default Azure Bronze settings."
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Additional tags applied to Azure resources."
  type        = map(string)
  default     = {}
}
