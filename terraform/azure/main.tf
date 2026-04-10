resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.merged_tags
}

resource "azurerm_storage_account" "data_lake" {
  name                            = var.storage_account_name
  resource_group_name             = azurerm_resource_group.this.name
  location                        = azurerm_resource_group.this.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  access_tier                     = "Hot"
  is_hns_enabled                  = true
  https_traffic_only_enabled      = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = local.merged_tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = var.container_name
  storage_account_id = azurerm_storage_account.data_lake.id
}

resource "azurerm_storage_account" "function_host" {
  name                            = local.function_storage_name
  resource_group_name             = azurerm_resource_group.this.name
  location                        = azurerm_resource_group.this.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  https_traffic_only_enabled      = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = local.merged_tags
}

resource "azurerm_storage_container" "function_deployment" {
  name                  = var.function_deployment_container_name
  storage_account_id    = azurerm_storage_account.function_host.id
  container_access_type = "private"
}

resource "azurerm_user_assigned_identity" "batch_pool" {
  name                = var.batch_managed_identity_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.merged_tags
}

resource "azapi_resource" "batch_account" {
  type      = "Microsoft.Batch/batchAccounts@2025-06-01"
  name      = var.batch_account_name
  parent_id = azurerm_resource_group.this.id
  location  = azurerm_resource_group.this.location
  tags      = local.merged_tags

  body = {
    properties = {
      autoStorage = {
        authenticationMode = "StorageKeys"
        storageAccountId   = azurerm_storage_account.data_lake.id
      }
      poolAllocationMode  = "BatchService"
      publicNetworkAccess = "Enabled"
    }
  }

  response_export_values    = ["*"]
  schema_validation_enabled = false
}

resource "azapi_resource_action" "batch_account_keys" {
  type        = "Microsoft.Batch/batchAccounts@2025-06-01"
  resource_id = azapi_resource.batch_account.id
  action      = "listKeys"
  method      = "POST"

  response_export_values    = ["*"]
  schema_validation_enabled = false
}

resource "azapi_resource" "batch_pool" {
  type      = "Microsoft.Batch/batchAccounts/pools@2025-06-01"
  name      = var.batch_pool_id
  parent_id = azapi_resource.batch_account.id

  body = {
    identity = {
      type = "UserAssigned"
      userAssignedIdentities = {
        "${azurerm_user_assigned_identity.batch_pool.id}" = {}
      }
    }
    properties = {
      vmSize = var.batch_vm_size
      deploymentConfiguration = {
        virtualMachineConfiguration = {
          imageReference = {
            offer     = "ubuntu-hpc"
            publisher = "microsoft-dsvm"
            sku       = "2204"
            version   = "latest"
          }
          nodeAgentSkuId = "batch.node.ubuntu 22.04"
        }
      }
      scaleSettings = {
        autoScale = {
          evaluationInterval = "PT5M"
          formula            = local.batch_pool_formula
        }
      }
      taskSchedulingPolicy = {
        nodeFillType = "Spread"
      }
      taskSlotsPerNode = 1
    }
  }

  schema_validation_enabled = false
}

resource "azurerm_data_factory" "this" {
  name                = var.data_factory_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tags                = local.merged_tags

  identity {
    type = "SystemAssigned"
  }
}

resource "time_sleep" "adf_identity_ready" {
  depends_on      = [azurerm_data_factory.this]
  create_duration = "30s"
}

resource "azurerm_log_analytics_workspace" "function" {
  name                = local.log_analytics_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.merged_tags
}

resource "azurerm_application_insights" "function" {
  name                = local.app_insights_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  workspace_id        = azurerm_log_analytics_workspace.function.id
  application_type    = "web"
  tags                = local.merged_tags
}

resource "azurerm_service_plan" "function" {
  name                = local.function_plan_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  os_type             = "Linux"
  sku_name            = "FC1"
  tags                = local.merged_tags
}

resource "azurerm_function_app_flex_consumption" "bronze" {
  name                          = local.function_app_name
  resource_group_name           = azurerm_resource_group.this.name
  location                      = azurerm_resource_group.this.location
  service_plan_id               = azurerm_service_plan.function.id
  storage_container_type        = "blobContainer"
  storage_container_endpoint    = local.function_deploy_url
  storage_authentication_type   = "SystemAssignedIdentity"
  runtime_name                  = var.function_runtime_name
  runtime_version               = var.function_runtime_version
  maximum_instance_count        = var.function_maximum_instance_count
  instance_memory_in_mb         = var.function_instance_memory_in_mb
  public_network_access_enabled = true
  tags                          = local.merged_tags

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_insights_connection_string = azurerm_application_insights.function.connection_string
  }

  app_settings = merge(
    {
      AzureWebJobsStorage                        = ""
      AzureWebJobsStorage__accountName           = azurerm_storage_account.function_host.name
      APPLICATIONINSIGHTS_AUTHENTICATION_STRING = "Authorization=AAD"
      SEC_USER_AGENT                            = var.sec_user_agent
      CLOUD_PROVIDER                            = "azure"
      AZURE_STORAGE_ACCOUNT                     = azurerm_storage_account.data_lake.name
      AZURE_CONTAINER                           = azurerm_storage_data_lake_gen2_filesystem.bronze.name
      STORAGE_PREFIX                            = var.storage_prefix
    },
    var.function_app_settings
  )

  depends_on = [azurerm_storage_container.function_deployment]
}

resource "random_password" "function_host_key" {
  length  = 48
  lower   = true
  upper   = true
  numeric = true
  special = false
}

resource "azapi_resource" "function_host_key" {
  type      = "Microsoft.Web/sites/host/functionKeys@2022-09-01"
  name      = "default/${var.function_host_key_name}"
  parent_id = azurerm_function_app_flex_consumption.bronze.id

  body = {
    properties = {
      name  = var.function_host_key_name
      value = local.function_host_key
    }
  }

  schema_validation_enabled = false
}

resource "azurerm_role_assignment" "batch_pool_storage" {
  scope                            = azurerm_storage_account.data_lake.id
  role_definition_name             = "Storage Blob Data Contributor"
  principal_id                     = azurerm_user_assigned_identity.batch_pool.principal_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}

resource "azurerm_role_assignment" "function_data_lake" {
  scope                            = azurerm_storage_account.data_lake.id
  role_definition_name             = "Storage Blob Data Contributor"
  principal_id                     = azurerm_function_app_flex_consumption.bronze.identity[0].principal_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}

resource "azurerm_role_assignment" "function_host_storage" {
  scope                            = azurerm_storage_account.function_host.id
  role_definition_name             = "Storage Blob Data Owner"
  principal_id                     = azurerm_function_app_flex_consumption.bronze.identity[0].principal_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}

resource "azurerm_role_assignment" "adf_batch_contributor" {
  scope                            = azapi_resource.batch_account.id
  role_definition_name             = "Contributor"
  principal_id                     = azurerm_data_factory.this.identity[0].principal_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true

  depends_on = [time_sleep.adf_identity_ready]
}

resource "azurerm_container_registry" "legacy" {
  count = var.enable_legacy_acr ? 1 : 0

  name                = var.legacy_acr_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "Basic"
  admin_enabled       = false
  tags                = local.merged_tags
}

data "archive_file" "task_bundle" {
  type        = "zip"
  output_path = "${path.module}/.generated/sec-edgar-task.zip"

  dynamic "source" {
    for_each = local.task_bundle_file_map
    content {
      content  = file(source.value)
      filename = source.key
    }
  }
}

data "archive_file" "function_bundle" {
  type        = "zip"
  output_path = "${path.module}/.generated/sec-edgar-function.zip"

  dynamic "source" {
    for_each = local.function_bundle_file_map
    content {
      content  = file(source.value)
      filename = source.key
    }
  }
}

resource "azurerm_storage_blob" "task_bundle" {
  name                   = var.task_bundle_blob_name
  storage_account_name   = azurerm_storage_account.data_lake.name
  storage_container_name = azurerm_storage_data_lake_gen2_filesystem.bronze.name
  type                   = "Block"
  source                 = data.archive_file.task_bundle.output_path
  metadata = {
    sha256 = local.task_bundle_hash
  }

  depends_on = [azurerm_storage_data_lake_gen2_filesystem.bronze]
}

resource "azurerm_storage_blob" "function_package" {
  name                   = local.function_package_blob_name
  storage_account_name   = azurerm_storage_account.function_host.name
  storage_container_name = azurerm_storage_container.function_deployment.name
  type                   = "Block"
  source                 = data.archive_file.function_bundle.output_path
}

resource "azapi_resource" "function_onedeploy" {
  type      = "Microsoft.Web/sites/extensions@2022-09-01"
  name      = "onedeploy"
  parent_id = azurerm_function_app_flex_consumption.bronze.id

  body = {
    properties = {
      packageUri  = local.function_package_url
      remoteBuild = true
    }
  }

  schema_validation_enabled = false

  depends_on = [
    azapi_resource.function_host_key,
    azurerm_role_assignment.function_host_storage,
    azurerm_storage_blob.function_package
  ]
}

resource "azapi_resource" "adf_linked_services" {
  for_each = {
    for linked_service in local.adf_linked_services : linked_service.name => linked_service
  }

  type      = "Microsoft.DataFactory/factories/linkedservices@2018-06-01"
  name      = each.key
  parent_id = azurerm_data_factory.this.id

  body = {
    properties = each.value.properties
  }

  schema_validation_enabled = false

  depends_on = [
    azapi_resource.function_host_key,
    azapi_resource.function_onedeploy,
    azurerm_storage_blob.task_bundle,
    azapi_resource_action.batch_account_keys
  ]
}

resource "azapi_resource" "adf_pipeline" {
  type      = "Microsoft.DataFactory/factories/pipelines@2018-06-01"
  name      = var.pipeline_name
  parent_id = azurerm_data_factory.this.id

  body = {
    properties = local.adf_pipeline_properties
  }

  schema_validation_enabled = false

  depends_on = [for linked_service in azapi_resource.adf_linked_services : linked_service]
}

resource "azapi_resource" "adf_daily_trigger" {
  type      = "Microsoft.DataFactory/factories/triggers@2018-06-01"
  name      = var.daily_trigger_name
  parent_id = azurerm_data_factory.this.id

  body = {
    properties = local.adf_daily_trigger.properties
  }

  schema_validation_enabled = false

  depends_on = [azapi_resource.adf_pipeline]
}

resource "azapi_resource" "adf_monthly_trigger" {
  type      = "Microsoft.DataFactory/factories/triggers@2018-06-01"
  name      = var.monthly_trigger_name
  parent_id = azurerm_data_factory.this.id

  body = {
    properties = local.adf_monthly_trigger.properties
  }

  schema_validation_enabled = false

  depends_on = [azapi_resource.adf_pipeline]
}

resource "azapi_resource_action" "start_daily_trigger" {
  type        = "Microsoft.DataFactory/factories/triggers@2018-06-01"
  resource_id = azapi_resource.adf_daily_trigger.id
  action      = "start"
  method      = "POST"

  response_export_values    = []
  schema_validation_enabled = false

  lifecycle {
    replace_triggered_by = [azapi_resource.adf_daily_trigger.output]
  }
}

resource "azapi_resource_action" "start_monthly_trigger" {
  type        = "Microsoft.DataFactory/factories/triggers@2018-06-01"
  resource_id = azapi_resource.adf_monthly_trigger.id
  action      = "start"
  method      = "POST"

  response_export_values    = []
  schema_validation_enabled = false

  lifecycle {
    replace_triggered_by = [azapi_resource.adf_monthly_trigger.output]
  }
}
