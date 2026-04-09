# validate_azure.ps1 - SEC EDGAR platform Azure resource validation
# Run this in PowerShell where az is installed and you are logged in.
# Usage: .\validate_azure.ps1
# Requires: Azure CLI + Reader role on subscription

#Requires -Version 5.1

[CmdletBinding()]
param(
    [string]$StorageAccount = $(if ($env:AZURE_STORAGE_ACCOUNT) { $env:AZURE_STORAGE_ACCOUNT } else { "mysecedgarstorage" }),
    [string]$Container = $(if ($env:AZURE_CONTAINER) { $env:AZURE_CONTAINER } else { "sec-edgar" }),
    [string]$Prefix = $(if ($env:STORAGE_PREFIX) { $env:STORAGE_PREFIX } else { "sec-edgar" }),
    [string]$BatchAccount = $(if ($env:AZURE_BATCH_ACCOUNT) { $env:AZURE_BATCH_ACCOUNT } else { "mysecedgarbatch" }),
    [string]$BatchPoolId = $(if ($env:AZURE_BATCH_POOL_ID) { $env:AZURE_BATCH_POOL_ID } else { "sec-edgar-pool" }),
    [string]$AcrName = $(if ($env:AZURE_ACR_NAME) { $env:AZURE_ACR_NAME } else { "mysecedgaracr" }),
    [string]$AdfName = $(if ($env:AZURE_DATA_FACTORY_NAME) { $env:AZURE_DATA_FACTORY_NAME } else { "mysecedgaradf" }),
    [string]$ResourceGroup = $(if ($env:AZURE_RESOURCE_GROUP) { $env:AZURE_RESOURCE_GROUP } else { "my-sec-edgar-rg" }),
    [string]$ManagedIdentity = $(if ($env:AZURE_MANAGED_IDENTITY_NAME) { $env:AZURE_MANAGED_IDENTITY_NAME } else { "sec-edgar-ingest-identity" }),
    [string]$PipelineName = $(if ($env:ADF_PIPELINE_NAME) { $env:ADF_PIPELINE_NAME } else { "sec-edgar-bronze-ingest" }),
    [string]$TriggerName = $(if ($env:ADF_TRIGGER_NAME) { $env:ADF_TRIGGER_NAME } else { "DailyBronzeIngestTrigger" }),
    [string]$MonthlyTriggerName = $(if ($env:ADF_MONTHLY_TRIGGER_NAME) { $env:ADF_MONTHLY_TRIGGER_NAME } else { "MonthlyBronzeFullRefreshTrigger" }),
    [string]$FunctionAppName = $(if ($env:AZURE_FUNCTION_APP_NAME) { $env:AZURE_FUNCTION_APP_NAME } else { "" }),
    [string]$FunctionLinkedServiceName = $(if ($env:ADF_FUNCTION_LINKED_SERVICE_NAME) { $env:ADF_FUNCTION_LINKED_SERVICE_NAME } else { "AzureFunctionBronzeLS" })
)

& (Join-Path $PSScriptRoot "validate_azure_hardened.ps1") @PSBoundParameters
return

# -- CONFIGURE THESE ----------------------------------------------------------
$StorageAccount  = "mysecedgarstorage"    # e.g. mysecedgarstorage
$Container       = "sec-edgar"            # e.g. sec-edgar
$Prefix          = "sec-edgar"            # e.g. sec-edgar
$BatchAccount    = "mysecedgarbatch"      # e.g. mysecedgarbatch
$AcrName         = "mysecedgaracr"        # e.g. mysecedgaracr
$AdfName         = "mysecedgaradf"        # e.g. mysecedgaradf
$ResourceGroup   = "my-sec-edgar-rg"      # e.g. my-sec-edgar-rg
$ManagedIdentity = "sec-edgar-ingest-identity"    # e.g. sec-edgar-ingest-identity
# -----------------------------------------------------------------------------

$Pass = 0; $Fail = 0; $Warn = 0

function Write-Pass { param($Msg) Write-Host "  PASS  $Msg" -ForegroundColor Green;  $script:Pass++ }
function Write-Fail { param($Msg) Write-Host "  FAIL  $Msg" -ForegroundColor Red;    $script:Fail++ }
function Write-Warn { param($Msg) Write-Host "  WARN  $Msg" -ForegroundColor Yellow; $script:Warn++ }
function Write-Hdr  { param($Msg) Write-Host "`n-- $Msg --" -ForegroundColor Cyan }

# -- 0. LOGIN CHECK ------------------------------------------------------------
Write-Hdr "0. Login"
$AccountJson = az account show --output json 2>$null
if (-not $AccountJson) {
    Write-Host "Not logged in. Run: az login" -ForegroundColor Red
    exit 1
}
$Account      = $AccountJson | ConvertFrom-Json
$Subscription = $Account.id
$AccountName  = $Account.name
Write-Pass "Logged in - subscription: $AccountName ($Subscription)"

# -- 1. RESOURCE GROUP ---------------------------------------------------------
Write-Hdr "1. Resource Group"
$RgJson = az group show --name $ResourceGroup --output json 2>$null
if ($RgJson) {
    $Rg = $RgJson | ConvertFrom-Json
    Write-Pass "Resource group '$ResourceGroup' exists (location: $($Rg.location))"
} else {
    Write-Fail "Resource group '$ResourceGroup' not found"
}

# -- 2. STORAGE ACCOUNT -------------------------------------------------------
Write-Hdr "2. Storage Account"
$SaJson = az storage account show `
    --name $StorageAccount --resource-group $ResourceGroup `
    --output json 2>$null
if ($SaJson) {
    $Sa = $SaJson | ConvertFrom-Json
    Write-Pass "Storage account '$StorageAccount' exists (SKU: $($Sa.sku.name))"

    if ($Sa.isHnsEnabled -eq $true) {
        Write-Pass "Hierarchical Namespace enabled (ADLS Gen2)"
    } else {
        Write-Fail "Hierarchical Namespace NOT enabled - abfss:// will not work"
    }
    if ($Sa.minimumTlsVersion -eq "TLS1_2") {
        Write-Pass "Minimum TLS 1.2 enforced"
    } else {
        Write-Warn "TLS minimum is '$($Sa.minimumTlsVersion)' - recommend TLS1_2"
    }
    if ($Sa.allowBlobPublicAccess -eq $false) {
        Write-Pass "Public blob access blocked"
    } else {
        Write-Warn "Public blob access is not blocked"
    }
} else {
    Write-Fail "Storage account '$StorageAccount' not found in '$ResourceGroup'"
}

# -- 3. STORAGE CONTAINER -----------------------------------------------------
Write-Hdr "3. Storage Container"
$FsJson = az storage fs show `
    --name $Container --account-name $StorageAccount `
    --auth-mode login --output json 2>$null
if ($FsJson) {
    Write-Pass "Container '$Container' exists"
} else {
    Write-Fail "Container '$Container' not found - run: az storage fs create --name $Container --account-name $StorageAccount --auth-mode login"
}

$TaskBundleExists = az storage blob exists `
    --container-name $Container `
    --account-name $StorageAccount `
    --name "adf-resources/sec-edgar-task.zip" `
    --auth-mode login `
    --query exists `
    --output tsv 2>$null
if ($TaskBundleExists -eq "true") {
    Write-Pass "ADF task bundle exists at '$Container/adf-resources/sec-edgar-task.zip'"
} else {
    Write-Warn "ADF task bundle missing at '$Container/adf-resources/sec-edgar-task.zip' - deploy.ps1 uploads it during Step 3"
}

# -- 4. LIFECYCLE POLICY -------------------------------------------------------
Write-Hdr "4. Lifecycle Management Policy (cost optimisation)"
$PolicyJson = az storage account management-policy show `
    --account-name $StorageAccount --resource-group $ResourceGroup `
    --output json 2>$null
if ($PolicyJson) {
    $Policy  = $PolicyJson | ConvertFrom-Json
    $BronzeRule = $Policy.policy.rules | Where-Object { $_.name -eq "bronze-to-cool" }
    if ($BronzeRule -and $BronzeRule.enabled -eq $true) {
        Write-Pass "Lifecycle policy 'bronze-to-cool' is active (Bronze Parquet -> Cool after 30 days)"
    } elseif ($BronzeRule) {
        Write-Warn "Lifecycle policy 'bronze-to-cool' exists but is disabled"
    } else {
        Write-Warn "No 'bronze-to-cool' lifecycle policy - run Step 2a in azure_ad_setup.md to save storage cost"
    }
} else {
    Write-Warn "No lifecycle policy configured - run Step 2a in azure_ad_setup.md"
}

# -- 5. AZURE CONTAINER REGISTRY -----------------------------------------------
Write-Hdr "5. Azure Container Registry"
$AcrJson = az acr show `
    --name $AcrName --resource-group $ResourceGroup `
    --output json 2>$null
if ($AcrJson) {
    $Acr = $AcrJson | ConvertFrom-Json
    Write-Pass "ACR '$AcrName' exists - login server: $($Acr.loginServer) (SKU: $($Acr.sku.name))"

    if ($Acr.adminUserEnabled -eq $false) {
        Write-Pass "Admin user disabled (using managed identity - correct)"
    } else {
        Write-Warn "Admin user is enabled - disable: az acr update --name $AcrName --admin-enabled false"
    }

    $ImgJson = az acr repository show `
        --name $AcrName --repository sec-edgar-ingest --output json 2>$null
    if ($ImgJson) {
        Write-Pass "Image 'sec-edgar-ingest' exists in ACR (legacy build artifact)"
    } else {
        Write-Warn "Image 'sec-edgar-ingest' not yet pushed - deploy.ps1 can still build/push it, but the current ADF runtime no longer executes inside that container"
    }
} else {
    Write-Fail "ACR '$AcrName' not found in '$ResourceGroup'"
}

# -- 6. AZURE BATCH ACCOUNT ----------------------------------------------------
Write-Hdr "6. Azure Batch Account"
$BatchJson = az batch account show `
    --name $BatchAccount --resource-group $ResourceGroup `
    --output json 2>$null
if ($BatchJson) {
    $Batch = $BatchJson | ConvertFrom-Json
    Write-Pass "Batch account '$BatchAccount' exists (state: $($Batch.provisioningState))"

    az batch account login --name $BatchAccount --resource-group $ResourceGroup | Out-Null

    $PoolJson = az batch pool show `
        --pool-id sec-edgar-pool --account-name $BatchAccount `
        --output json 2>$null
    if ($PoolJson) {
        $Pool = $PoolJson | ConvertFrom-Json
        Write-Pass "Batch pool 'sec-edgar-pool' exists (VM: $($Pool.vmSize))"

        if ($Pool.vmSize -eq "standard_d2s_v3") {
            Write-Pass "VM size is Standard_D2s_v3 (cost-optimised)"
        } else {
            Write-Warn "VM size is '$($Pool.vmSize)' - Standard_D2s_v3 recommended for cost"
        }
        if ($Pool.enableAutoScale -eq $true) {
            Write-Pass "Auto-scale enabled - pool scales to 0 nodes when idle"
        } else {
            Write-Warn "Auto-scale not enabled - pool may incur idle compute cost"
        }
        if ($Pool.targetLowPriorityNodes -gt 0) {
            Write-Warn "Pool currently targets low-priority nodes ($($Pool.targetLowPriorityNodes)) - this can fail when spot quota is unavailable"
        } else {
            Write-Pass "Low-priority target nodes are disabled"
        }
        $VmConfig = $Pool.deploymentConfiguration.virtualMachineConfiguration
        if ($VmConfig.imageReference.publisher -eq "microsoft-dsvm" -and
            $VmConfig.imageReference.offer -eq "ubuntu-hpc" -and
            $VmConfig.imageReference.sku -eq "2204") {
            Write-Pass "Pool image is microsoft-dsvm/ubuntu-hpc/2204"
        } else {
            Write-Warn "Pool image is '$($VmConfig.imageReference.publisher)/$($VmConfig.imageReference.offer)/$($VmConfig.imageReference.sku)' - expected microsoft-dsvm/ubuntu-hpc/2204"
        }
        if ($null -eq $VmConfig.containerConfiguration) {
            Write-Pass "Pool has no containerConfiguration - host execution is enabled"
        } else {
            Write-Fail "Pool still has containerConfiguration - ADF Custom Activity host execution will fail on container-only pools"
        }
        Write-Host "       Current dedicated target: $($Pool.targetDedicatedNodes)" -ForegroundColor DarkGray
    } else {
        Write-Warn "Batch pool 'sec-edgar-pool' not yet created - run Step 9 in azure_ad_setup.md"
    }
} else {
    Write-Fail "Batch account '$BatchAccount' not found in '$ResourceGroup'"
}

# -- 7. AZURE DATA FACTORY (skipped) ------------------------------------------
Write-Hdr "7. Azure Data Factory"
Write-Warn "ADF validation skipped"

# -- 8. MANAGED IDENTITY RBAC --------------------------------------------------
Write-Hdr "8. Managed Identity RBAC"
$IdentityJson = az identity show `
    --name $ManagedIdentity --resource-group $ResourceGroup `
    --output json 2>$null
if ($IdentityJson) {
    $Identity    = $IdentityJson | ConvertFrom-Json
    $PrincipalId = $Identity.principalId
    Write-Pass "Managed identity '$ManagedIdentity' found (principalId: $PrincipalId)"

    $StorageScope = "/subscriptions/$Subscription/resourceGroups/$ResourceGroup/providers/Microsoft.Storage/storageAccounts/$StorageAccount"

    $ContribJson = az role assignment list `
        --assignee $PrincipalId `
        --role "Storage Blob Data Contributor" `
        --scope $StorageScope `
        --output json 2>$null
    if ($ContribJson) {
        $Contrib = $ContribJson | ConvertFrom-Json
        if ($Contrib.Count -ge 1) {
            Write-Pass "Storage Blob Data Contributor assigned on storage account"
        } else {
            Write-Fail "Storage Blob Data Contributor NOT assigned - Parquet writes will fail"
        }
    }

    Write-Warn "AcrPull is no longer required for the current host-executed ADF runtime"
} else {
    Write-Fail "Managed identity '$ManagedIdentity' not found in '$ResourceGroup'"
}

# -- SUMMARY -------------------------------------------------------------------
Write-Host "`n======================================" -ForegroundColor Cyan
Write-Host "  PASS: $Pass   FAIL: $Fail   WARN: $Warn" -ForegroundColor White
Write-Host "======================================" -ForegroundColor Cyan

if ($Fail -gt 0) { exit 1 } else { exit 0 }
