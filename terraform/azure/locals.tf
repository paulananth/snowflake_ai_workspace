data "azurerm_client_config" "current" {}

locals {
  subscription_suffix = substr(replace(data.azurerm_client_config.current.subscription_id, "-", ""), 0, 8)

  repo_root             = abspath("${path.module}/../..")
  function_project_root = "${local.repo_root}/function_apps/adf_tickers_ingest"
  workflow_root         = "${local.repo_root}/workflows"

  function_app_name     = coalesce(var.function_app_name, "sec-edgar-flex-${local.subscription_suffix}")
  function_storage_name = coalesce(var.function_storage_account_name, "secedgarfn${local.subscription_suffix}")
  function_plan_name    = coalesce(var.function_plan_name, "sec-edgar-flex-plan-${local.subscription_suffix}")
  app_insights_name     = coalesce(var.application_insights_name, "sec-edgar-ai-${local.subscription_suffix}")
  log_analytics_name    = coalesce(var.log_analytics_workspace_name, "sec-edgar-law-${local.subscription_suffix}")

  task_bundle_folder  = "${var.container_name}/adf-resources"
  function_host_key   = coalesce(var.function_host_key_value, random_password.function_host_key.result)
  function_deploy_url = "${azurerm_storage_account.function_host.primary_blob_endpoint}${azurerm_storage_container.function_deployment.name}"
  function_package_url = "${azurerm_storage_account.function_host.primary_blob_endpoint}${azurerm_storage_container.function_deployment.name}/${azurerm_storage_blob.function_package.name}"
  function_app_url     = "https://${azurerm_function_app_flex_consumption.bronze.default_hostname}"

  merged_tags = merge({
    workload   = "sec-edgar"
    layer      = "bronze"
    managed_by = "terraform"
  }, var.tags)

  batch_pool_formula = "startingNumberOfVMs = 0; maxNumberofVMs = 1; pendingTaskSamplePercent = $PendingTasks.GetSamplePercent(180 * TimeInterval_Second); pendingTaskSamples = pendingTaskSamplePercent < 70 ? startingNumberOfVMs : avg($PendingTasks.GetSample(180 * TimeInterval_Second)); $TargetDedicatedNodes = min(maxNumberofVMs, pendingTaskSamples); $TargetLowPriorityNodes = 0; $NodeDeallocationOption = taskcompletion;"

  task_bundle_files = concat(
    [
      for rel in sort(fileset("${local.repo_root}/config", "**")) : {
        filename = "config/${rel}"
        path     = "${local.repo_root}/config/${rel}"
      }
    ],
    [
      for rel in sort(fileset("${local.repo_root}/scripts", "**")) : {
        filename = "scripts/${rel}"
        path     = "${local.repo_root}/scripts/${rel}"
      }
      if length(regexall("(^|/)__pycache__(/|$)", rel)) == 0 && !endswith(rel, ".pyc")
    ],
    [
      for rel in ["pyproject.toml", "uv.lock", ".python-version"] : {
        filename = rel
        path     = "${local.repo_root}/${rel}"
      }
      if fileexists("${local.repo_root}/${rel}")
    ]
  )

  function_bundle_files = concat(
    [
      for rel in sort(fileset(local.function_project_root, "**")) : {
        filename = rel
        path     = "${local.function_project_root}/${rel}"
      }
      if length(regexall("(^|/)__pycache__(/|$)", rel)) == 0 && !endswith(rel, ".pyc")
    ],
    [
      for rel in sort(fileset("${local.repo_root}/config", "**")) : {
        filename = "config/${rel}"
        path     = "${local.repo_root}/config/${rel}"
      }
    ],
    fileexists("${local.repo_root}/scripts/__init__.py") ? [
      {
        filename = "scripts/__init__.py"
        path     = "${local.repo_root}/scripts/__init__.py"
      }
    ] : [],
    [
      for rel in sort(fileset("${local.repo_root}/scripts/ingest", "**")) : {
        filename = "scripts/ingest/${rel}"
        path     = "${local.repo_root}/scripts/ingest/${rel}"
      }
      if length(regexall("(^|/)__pycache__(/|$)", rel)) == 0 && !endswith(rel, ".pyc") && rel != "test_sec_loader.py"
    ]
  )

  task_bundle_file_map = {
    for file_ref in local.task_bundle_files : file_ref.filename => file_ref.path
  }

  function_bundle_file_map = {
    for file_ref in local.function_bundle_files : file_ref.filename => file_ref.path
  }

  task_bundle_hash = sha256(join("\n", [
    for name in sort(keys(local.task_bundle_file_map)) :
    "${name}:${filesha256(local.task_bundle_file_map[name])}"
  ]))

  function_bundle_hash = sha256(join("\n", [
    for name in sort(keys(local.function_bundle_file_map)) :
    "${name}:${filesha256(local.function_bundle_file_map[name])}"
  ]))

  function_package_blob_name = "releases/sec-edgar-function-${local.function_bundle_hash}.zip"

  batch_account_json = try(jsondecode(azapi_resource.batch_account.output), {})
  batch_endpoint_raw = trimspace(try(local.batch_account_json.properties.accountEndpoint, ""))
  batch_endpoint     = startswith(local.batch_endpoint_raw, "https://") ? local.batch_endpoint_raw : "https://${local.batch_endpoint_raw}"

  batch_account_keys = try(jsondecode(azapi_resource_action.batch_account_keys.output), {})
  batch_primary_key  = try(local.batch_account_keys.primary, "")

  adf_linked_services_template = jsondecode(file("${local.workflow_root}/adf_linked_services.json")).linkedServices
  adf_pipeline_template        = jsondecode(file("${local.workflow_root}/adf_pipeline.json"))
  adf_daily_trigger_template   = jsondecode(file("${local.workflow_root}/adf_trigger.json"))
  adf_monthly_trigger_template = jsondecode(file("${local.workflow_root}/adf_trigger_monthly.json"))

  batch_command_prefix = "/bin/bash -lc 'set -euo pipefail; python3 -m pip --version >/dev/null 2>&1 || python3 -m ensurepip --upgrade; python3 -m pip install --user uv >/dev/null; export PATH=\"\\$HOME/.local/bin:\\$PATH\"; python3 -c \"import zipfile; zipfile.ZipFile('sec-edgar-task.zip').extractall('app')\"; cd app; uv sync --no-dev; SEC_USER_AGENT=\"${replace(var.sec_user_agent, "\"", "\\\"")}\" CLOUD_PROVIDER=azure AZURE_STORAGE_ACCOUNT=${var.storage_account_name} AZURE_CONTAINER=${var.container_name} STORAGE_PREFIX=${var.storage_prefix} AZURE_CLIENT_ID=${azurerm_user_assigned_identity.batch_pool.client_id} FULL_REFRESH="

  batch_command_middle = {
    IngestSubmissions  = " .venv/bin/python scripts/ingest/03_ingest_submissions.py --date "
    IngestCompanyFacts = " .venv/bin/python scripts/ingest/04_ingest_companyfacts.py --date "
  }

  batch_command_expressions = {
    for activity_name, middle in local.batch_command_middle :
    activity_name => "@concat('${replace(local.batch_command_prefix, "'", "''")}', if(pipeline().parameters.fullRefresh, 'true', 'false'), '${replace(middle, "'", "''")}', pipeline().parameters.ingestDate, '''')"
  }

  adf_linked_services = [
    for linked_service in local.adf_linked_services_template : merge(linked_service, {
      name = linked_service.name == "AzureFunctionBronzeLS" ? var.function_linked_service_name : linked_service.name
      properties = merge(linked_service.properties, {
        typeProperties = linked_service.name == "AzureStorageLS" ? merge(try(linked_service.properties.typeProperties, {}), {
          accountKind      = "StorageV2"
          connectionString = { type = "SecureString", value = azurerm_storage_account.data_lake.primary_connection_string }
        }) : linked_service.name == "AzureBatchLS" ? merge(try(linked_service.properties.typeProperties, {}), {
          accountName = var.batch_account_name
          accessKey   = { type = "SecureString", value = local.batch_primary_key }
          batchUri    = local.batch_endpoint
          poolName    = var.batch_pool_id
        }) : merge(try(linked_service.properties.typeProperties, {}), {
          functionAppUrl = local.function_app_url
          functionKey    = { type = "SecureString", value = local.function_host_key }
        })
      })
    })
  ]

  adf_pipeline_properties = merge(local.adf_pipeline_template.properties, {
    activities = [
      for activity in local.adf_pipeline_template.properties.activities : activity.type == "AzureFunctionActivity" ? merge(activity, {
        linkedServiceName = merge(activity.linkedServiceName, {
          referenceName = var.function_linked_service_name
        })
      }) : contains(keys(local.batch_command_expressions), activity.name) ? merge(activity, {
        typeProperties = merge(try(activity.typeProperties, {}), {
          command = {
            type  = "Expression"
            value = local.batch_command_expressions[activity.name]
          }
          resourceLinkedService = {
            referenceName = "AzureStorageLS"
            type          = "LinkedServiceReference"
          }
          folderPath = local.task_bundle_folder
          referenceObjects = {
            linkedServices = []
            datasets       = []
          }
        })
      }) : activity
    ]
  })

  adf_daily_trigger = merge(local.adf_daily_trigger_template, {
    name = var.daily_trigger_name
    properties = merge(local.adf_daily_trigger_template.properties, {
      typeProperties = merge(local.adf_daily_trigger_template.properties.typeProperties, {
        startTime = var.daily_trigger_start_time
      })
      pipeline = merge(local.adf_daily_trigger_template.properties.pipeline, {
        pipelineReference = merge(local.adf_daily_trigger_template.properties.pipeline.pipelineReference, {
          referenceName = var.pipeline_name
        })
      })
    })
  })

  adf_monthly_trigger = merge(local.adf_monthly_trigger_template, {
    name = var.monthly_trigger_name
    properties = merge(local.adf_monthly_trigger_template.properties, {
      typeProperties = merge(local.adf_monthly_trigger_template.properties.typeProperties, {
        recurrence = merge(local.adf_monthly_trigger_template.properties.typeProperties.recurrence, {
          startTime = var.monthly_trigger_start_time
        })
      })
      pipelines = [
        for index, pipeline_ref in local.adf_monthly_trigger_template.properties.pipelines : index == 0 ? merge(pipeline_ref, {
          pipelineReference = merge(pipeline_ref.pipelineReference, {
            referenceName = var.pipeline_name
          })
        }) : pipeline_ref
      ]
    })
  })
}
