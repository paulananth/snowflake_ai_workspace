# validate_azure.ps1 - SEC EDGAR platform Azure resource validation
# Run this in PowerShell where az is installed and you are logged in.
# Usage: .\validate_azure.ps1
# Requires: Azure CLI + Reader role on subscription

#Requires -Version 5.1

# -- CONFIGURE THESE ----------------------------------------------------------
$StorageAccount  = ""    # e.g. mysecedgarstorage
$Container       = ""    # e.g. sec-edgar
$Prefix          = ""    # e.g. sec-edgar
$BatchAccount    = ""    # e.g. mysecedgarbatch
$AcrName         = ""    # e.g. mysecedgaracr
$AdfName         = ""    # e.g. mysecedgaradf
$ResourceGroup   = ""    # e.g. my-sec-edgar-rg
$ManagedIdentity = ""    # e.g. sec-edgar-ingest-identity
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
        Write-Pass "Image 'sec-edgar-ingest' exists in ACR"
    } else {
        Write-Warn "Image 'sec-edgar-ingest' not yet pushed - run Step 10 in azure_ad_setup.md"
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
        if ($Pool.targetDedicatedNodes -eq 0) {
            Write-Pass "No dedicated nodes (using low-priority only - cost-optimised)"
        } else {
            Write-Warn "Dedicated nodes: $($Pool.targetDedicatedNodes) - consider switching to low-priority"
        }
    } else {
        Write-Warn "Batch pool 'sec-edgar-pool' not yet created - run Step 9 in azure_ad_setup.md"
    }
} else {
    Write-Fail "Batch account '$BatchAccount' not found in '$ResourceGroup'"
}

# -- 7. AZURE DATA FACTORY -----------------------------------------------------
Write-Hdr "7. Azure Data Factory"
$AdfJson = az datafactory show `
    --factory-name $AdfName --resource-group $ResourceGroup `
    --output json 2>$null
if ($AdfJson) {
    $Adf = $AdfJson | ConvertFrom-Json
    Write-Pass "ADF '$AdfName' exists (state: $($Adf.provisioningState))"

    if ($Adf.identity.type -eq "SystemAssigned") {
        Write-Pass "ADF has system-assigned managed identity"
    } else {
        Write-Warn "ADF identity type is '$($Adf.identity.type)' - expected SystemAssigned"
    }
} else {
    Write-Fail "Data Factory '$AdfName' not found in '$ResourceGroup'"
}

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

    $AcrScopeJson = az acr show --name $AcrName --resource-group $ResourceGroup --query id --output tsv 2>$null
    if ($AcrScopeJson) {
        $AcrPullJson = az role assignment list `
            --assignee $PrincipalId `
            --role "AcrPull" `
            --scope $AcrScopeJson.Trim() `
            --output json 2>$null
        if ($AcrPullJson) {
            $AcrPull = $AcrPullJson | ConvertFrom-Json
            if ($AcrPull.Count -ge 1) {
                Write-Pass "AcrPull assigned on container registry"
            } else {
                Write-Fail "AcrPull NOT assigned - Batch pool cannot pull Docker image"
            }
        }
    }
} else {
    Write-Fail "Managed identity '$ManagedIdentity' not found in '$ResourceGroup'"
}

# -- SUMMARY -------------------------------------------------------------------
Write-Host "`n======================================" -ForegroundColor Cyan
Write-Host "  PASS: $Pass   FAIL: $Fail   WARN: $Warn" -ForegroundColor White
Write-Host "======================================" -ForegroundColor Cyan

if ($Fail -gt 0) { exit 1 } else { exit 0 }
