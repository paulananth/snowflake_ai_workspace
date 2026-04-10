# Azure Terraform Conversion

This directory converts the current Azure CLI / PowerShell deployment into Terraform for the active SEC Bronze CDC design:

- Azure Data Factory orchestrates the run
- Azure Functions handles the lightweight `IngestTickersExchange` and `IngestDailyIndex` stages
- Azure Batch handles the heavier `IngestSubmissions` and `IngestCompanyFacts` stages
- ADLS Gen2 stores Bronze outputs and the staged Batch task bundle

## What This Terraform Manages

The Terraform configuration creates or updates:

- the resource group
- the ADLS Gen2 storage account and filesystem
- the separate Function host storage account and deployment container
- the Batch user-assigned managed identity
- the Azure Batch account and non-container host pool
- the Azure Data Factory factory
- the Flex Consumption Function App, App Insights, and Log Analytics workspace
- the ADF linked services, pipeline, and both triggers
- the Function deployment package and the ADF Batch task bundle
- the Function App host key used by ADF
- the required RBAC assignments

The ADF child resources stay sourced from the checked-in JSON files in:

- `c:\work\projects\snowflake_ai_workspace\workflows\adf_linked_services.json`
- `c:\work\projects\snowflake_ai_workspace\workflows\adf_pipeline.json`
- `c:\work\projects\snowflake_ai_workspace\workflows\adf_trigger.json`
- `c:\work\projects\snowflake_ai_workspace\workflows\adf_trigger_monthly.json`

Terraform mutates those templates at apply time in the same way the PowerShell deploy currently does:

- injects the storage connection string into `AzureStorageLS`
- injects the Batch account key and endpoint into `AzureBatchLS`
- injects the Function URL and host key into `AzureFunctionBronzeLS`
- injects the host-executed Batch command strings into the Custom activities
- rewires the trigger pipeline references and names

## Microsoft Azure Guidance Reflected Here

This implementation follows the current Azure guidance used during the conversion:

- Flex Consumption apps use a deployment container and OneDeploy rather than legacy zip deploy.
- Flex Consumption configuration is expressed through the Function resource plus deployment storage configuration.
- ADF linked services, pipelines, and triggers are ARM child resources.
- ADF tumbling-window triggers have an immutable `startTime` once created.

See:

- [Automate function app resource deployment to Azure](https://learn.microsoft.com/en-us/azure/azure-functions/functions-infrastructure-as-code)
- [Migrate Consumption plan apps to the Flex Consumption plan](https://learn.microsoft.com/en-us/azure/azure-functions/migration/migrate-plan-consumption-to-flex)
- [Create a trigger that runs a pipeline on a tumbling window](https://learn.microsoft.com/en-us/azure/data-factory/how-to-create-tumbling-window-trigger)
- [Microsoft.DataFactory/factories/pipelines ARM reference](https://learn.microsoft.com/en-us/azure/templates/microsoft.datafactory/factories/pipelines)
- [Microsoft.Batch/batchAccounts ARM reference](https://learn.microsoft.com/en-us/azure/templates/microsoft.batch/2018-12-01/batchaccounts)
- [Microsoft.Batch/batchAccounts/pools ARM reference](https://learn.microsoft.com/en-us/azure/templates/microsoft.batch/batchaccounts/pools)

## Prerequisites

You need:

- Terraform CLI
- Azure credentials that can create resources and assign RBAC
- a valid `sec_user_agent` value

The `sec_user_agent` input is required because the Python SEC downloader enforces a contact string.

## Usage

1. Copy the example tfvars file and edit it:

```powershell
Copy-Item `
  'c:\work\projects\snowflake_ai_workspace\terraform\azure\terraform.tfvars.example' `
  'c:\work\projects\snowflake_ai_workspace\terraform\azure\terraform.tfvars'
```

2. Initialize Terraform:

```powershell
Set-Location 'c:\work\projects\snowflake_ai_workspace\terraform\azure'
terraform init
```

3. Review the plan:

```powershell
terraform plan -out tfplan
```

4. Apply:

```powershell
terraform apply tfplan
```

## Notes

- The daily trigger start time is immutable after the first creation. If you need to change `daily_trigger_start_time` for an existing environment, recreate or import the trigger deliberately instead of expecting an in-place update.
- This Terraform keeps the current key-based ADF linked-service design, so the storage connection string, Batch account key, and Function host key are present in Terraform state.
- The Function App uses system-assigned identity for its deployment container and ADLS access. The Terraform creates the required storage role assignments.
- The Flex Consumption Function resource keeps `AzureWebJobsStorage = ""` and uses `AzureWebJobsStorage__accountName`, matching the current Microsoft workaround guidance for the AzureRM provider.
- The Batch pool remains a non-container Ubuntu host pool, matching the current ADF Custom Activity design.
- The optional legacy ACR resource is disabled by default and exists only for the old Docker-based path.

## Validation Status

This conversion was authored from the current repo scripts and Microsoft Azure documentation. In this environment, Terraform CLI was not available, so `terraform init`, `terraform validate`, and `terraform plan` were not executed here.
