<#
.SYNOPSIS
Provision Azure resources for the SEC EDGAR platform (per docs/azure_ad_setup.md).

.DESCRIPTION
Creates the Azure components used by the pipeline:
  - Resource group
  - ADLS Gen2 storage account (HNS enabled) + filesystem (container)
  - Storage lifecycle management policy for bronze -> cool/archive
  - Azure Container Registry (ACR)
  - User-assigned managed identity for Batch nodes
  - RBAC for identity: Storage Blob Data Contributor + AcrPull
  - Azure Batch account
  - Azure Data Factory (ADF) + RBAC to Batch + Storage
  - Azure Batch pool with autoscale (scale-to-zero)

This script uses Azure CLI (`az`) and is designed to be re-runnable (best-effort idempotent).

Notes
  - It does NOT run EDGAR enrichment or Snowflake integration.
  - The Batch pool container configuration is called out in the docs as requiring portal/ARM; this script does not apply that JSON block automatically.

.PARAMETER SubscriptionId
Azure subscription ID. If omitted, uses `az account show`.

.PARAMETER ResourceGroup
Resource group name.

.PARAMETER Location
Azure region (e.g. eastus, westus2).

.PARAMETER StorageAccount
Storage account name (globally unique, lowercase, 3-24 chars).

.PARAMETER Container
ADLS Gen2 filesystem name (container).

.PARAMETER Prefix
Key prefix for blob paths (e.g. sec-edgar).

.PARAMETER BatchAccount
Azure Batch account name.

.PARAMETER AcrName
Azure Container Registry name.

.PARAMETER AdfName
Azure Data Factory name.

.PARAMETER IdentityName
User-assigned managed identity name.

.PARAMETER BatchPoolId
Batch pool id.

.PARAMETER VmSize
Batch VM size.

.PARAMETER BuildAndPushImage
If set, attempts to `az acr login`, `docker build`, and `docker push` the image URI `${ACR_SERVER}/sec-edgar-ingest:latest`.

.PARAMETER DockerImageTag
Docker tag to build/push (default: sec-edgar-ingest:latest).
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

[CmdletBinding()]
param(
  [Parameter(Mandatory = $false)]
  [string]$SubscriptionId,

  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$ResourceGroup,

  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$Location,

  [Parameter(Mandatory = $true)]
  [ValidatePattern('^[a-z0-9]{3,24}$')]
  [string]$StorageAccount,

  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$Container,

  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$Prefix,

  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$BatchAccount,

  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$AcrName,

  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]$AdfName,

  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]$IdentityName = 'sec-edgar-ingest-identity',

  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]$BatchPoolId = 'sec-edgar-pool',

  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]$VmSize = 'Standard_D2s_v3',

  [Parameter(Mandatory = $false)]
  [switch]$BuildAndPushImage,

  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]$DockerImageTag = 'sec-edgar-ingest:latest'
)

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Required command not found on PATH: $Name"
  }
}

function Invoke-Az {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args,
    [Parameter(Mandatory = $false)][switch]$Json
  )
  $cmd = @('az') + $Args
  Write-Host ("`n> " + ($cmd -join ' ')) -ForegroundColor DarkGray
  $out = & az @Args 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "Azure CLI command failed (exit $LASTEXITCODE): az $($Args -join ' ')`n$out"
  }
  if ($Json) {
    if ([string]::IsNullOrWhiteSpace($out)) { return $null }
    return ($out | ConvertFrom-Json)
  }
  return $out
}

function Ensure-RoleAssignment {
  param(
    [Parameter(Mandatory = $true)][string]$Role,
    [Parameter(Mandatory = $true)][string]$AssigneeObjectId,
    [Parameter(Mandatory = $true)][ValidateSet('ServicePrincipal','User')][string]$AssigneePrincipalType,
    [Parameter(Mandatory = $true)][string]$Scope
  )

  $existing = Invoke-Az -Args @(
    'role','assignment','list',
    '--assignee-object-id', $AssigneeObjectId,
    '--scope', $Scope,
    '--query', "[?roleDefinitionName=='$Role']",
    '--output','json'
  ) -Json

  if ($existing -and $existing.Count -gt 0) {
    Write-Host "Role assignment already exists: $Role on $Scope" -ForegroundColor DarkGreen
    return
  }

  Invoke-Az -Args @(
    'role','assignment','create',
    '--role', $Role,
    '--assignee-object-id', $AssigneeObjectId,
    '--assignee-principal-type', $AssigneePrincipalType,
    '--scope', $Scope
  ) | Out-Null
}

Assert-CommandExists -Name 'az'

if (-not $SubscriptionId) {
  $SubscriptionId = (Invoke-Az -Args @('account','show','--query','id','--output','tsv')).Trim()
}

Invoke-Az -Args @('account','set','--subscription', $SubscriptionId) | Out-Null

Write-Host "`n=== Inputs ===" -ForegroundColor Cyan
Write-Host "SubscriptionId : $SubscriptionId"
Write-Host "RG             : $ResourceGroup"
Write-Host "Location       : $Location"
Write-Host "StorageAccount : $StorageAccount"
Write-Host "Container      : $Container"
Write-Host "Prefix         : $Prefix"
Write-Host "BatchAccount   : $BatchAccount"
Write-Host "BatchPoolId    : $BatchPoolId"
Write-Host "ACR            : $AcrName"
Write-Host "ADF            : $AdfName"
Write-Host "Identity       : $IdentityName"

# ---------------------------------------------------------------------------
# Step 1 — Resource group
# ---------------------------------------------------------------------------
Invoke-Az -Args @('group','create','--name', $ResourceGroup,'--location', $Location) | Out-Null

# ---------------------------------------------------------------------------
# Step 2 — ADLS Gen2 storage + filesystem + lifecycle policy
# ---------------------------------------------------------------------------
Invoke-Az -Args @(
  'storage','account','create',
  '--name', $StorageAccount,
  '--resource-group', $ResourceGroup,
  '--location', $Location,
  '--sku','Standard_LRS',
  '--kind','StorageV2',
  '--enable-hierarchical-namespace','true',
  '--https-only','true',
  '--min-tls-version','TLS1_2',
  '--allow-blob-public-access','false',
  '--access-tier','Hot'
) | Out-Null

# Create ADLS filesystem (container)
Invoke-Az -Args @(
  'storage','fs','create',
  '--name', $Container,
  '--account-name', $StorageAccount,
  '--auth-mode','login'
) | Out-Null

# Verify HNS enabled
$hns = (Invoke-Az -Args @(
  'storage','account','show',
  '--name', $StorageAccount,
  '--resource-group', $ResourceGroup,
  '--query','isHnsEnabled',
  '--output','tsv'
)).Trim()
if ($hns -ne 'true') { throw "Storage account $StorageAccount does not have HNS enabled (isHnsEnabled=$hns)." }

# Lifecycle policy for bronze prefix
$policy = @{
  rules = @(
    @{
      name    = 'bronze-to-cool'
      enabled = $true
      type    = 'Lifecycle'
      definition = @{
        filters = @{
          blobTypes    = @('blockBlob')
          prefixMatch  = @("$Prefix/bronze/")
        }
        actions = @{
          baseBlob = @{
            tierToCool    = @{ daysAfterModificationGreaterThan = 30 }
            tierToArchive = @{ daysAfterModificationGreaterThan = 365 }
          }
        }
      }
    }
  )
}
$policyJson = ($policy | ConvertTo-Json -Depth 20 -Compress)
Invoke-Az -Args @(
  'storage','account','management-policy','create',
  '--account-name', $StorageAccount,
  '--resource-group', $ResourceGroup,
  '--policy', $policyJson
) | Out-Null

# ---------------------------------------------------------------------------
# Step 3 — ACR
# ---------------------------------------------------------------------------
Invoke-Az -Args @(
  'acr','create',
  '--name', $AcrName,
  '--resource-group', $ResourceGroup,
  '--location', $Location,
  '--sku','Basic',
  '--admin-enabled','false'
) | Out-Null

$acr = Invoke-Az -Args @('acr','show','--name', $AcrName,'--resource-group', $ResourceGroup,'--output','json') -Json
$acrId = $acr.id
$acrServer = $acr.loginServer

# ---------------------------------------------------------------------------
# Step 4 — User-assigned managed identity
# ---------------------------------------------------------------------------
Invoke-Az -Args @(
  'identity','create',
  '--name', $IdentityName,
  '--resource-group', $ResourceGroup,
  '--location', $Location
) | Out-Null

$identity = Invoke-Az -Args @(
  'identity','show',
  '--name', $IdentityName,
  '--resource-group', $ResourceGroup,
  '--output','json'
) -Json

$identityId = $identity.id
$clientId = $identity.clientId
$principalId = $identity.principalId

Write-Host "`n=== Identity outputs ===" -ForegroundColor Cyan
Write-Host "IDENTITY_ID   : $identityId"
Write-Host "CLIENT_ID     : $clientId (set as AZURE_CLIENT_ID on Batch pool nodes)"
Write-Host "PRINCIPAL_ID  : $principalId"

# ---------------------------------------------------------------------------
# Step 5 — RBAC for managed identity (Storage + ACR)
# ---------------------------------------------------------------------------
$storageScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.Storage/storageAccounts/$StorageAccount"
Ensure-RoleAssignment -Role 'Storage Blob Data Contributor' -AssigneeObjectId $principalId -AssigneePrincipalType ServicePrincipal -Scope $storageScope
Ensure-RoleAssignment -Role 'AcrPull' -AssigneeObjectId $principalId -AssigneePrincipalType ServicePrincipal -Scope $acrId

# ---------------------------------------------------------------------------
# Step 6 — Azure Batch account
# ---------------------------------------------------------------------------
Invoke-Az -Args @(
  'batch','account','create',
  '--name', $BatchAccount,
  '--resource-group', $ResourceGroup,
  '--location', $Location,
  '--storage-account', $StorageAccount
) | Out-Null

$batchScope = (Invoke-Az -Args @(
  'batch','account','show',
  '--name', $BatchAccount,
  '--resource-group', $ResourceGroup,
  '--query','id',
  '--output','tsv'
)).Trim()

# Batch CLI context login (required for pool operations)
Invoke-Az -Args @('batch','account','login','--name', $BatchAccount,'--resource-group', $ResourceGroup) | Out-Null

# ---------------------------------------------------------------------------
# Step 7 — Azure Data Factory + principal id
# ---------------------------------------------------------------------------
Invoke-Az -Args @(
  'datafactory','create',
  '--factory-name', $AdfName,
  '--resource-group', $ResourceGroup,
  '--location', $Location
) | Out-Null

$adfPrincipal = (Invoke-Az -Args @(
  'datafactory','show',
  '--factory-name', $AdfName,
  '--resource-group', $ResourceGroup,
  '--query','identity.principalId',
  '--output','tsv'
)).Trim()

Write-Host "`nADF principal ID: $adfPrincipal" -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# Step 8 — RBAC for ADF identity (Batch + Storage)
# ---------------------------------------------------------------------------
Ensure-RoleAssignment -Role 'Contributor' -AssigneeObjectId $adfPrincipal -AssigneePrincipalType ServicePrincipal -Scope $batchScope
Ensure-RoleAssignment -Role 'Storage Blob Data Reader' -AssigneeObjectId $adfPrincipal -AssigneePrincipalType ServicePrincipal -Scope $storageScope

# ---------------------------------------------------------------------------
# Step 9 — Batch pool (scale-to-zero)
# ---------------------------------------------------------------------------
$autoScaleFormula = @'
startingNumberOfVMs = 0;
maxNumberofVMs = 1;
pendingTaskSamplePercent = $PendingTasks.GetSamplePercent(180 * TimeInterval_Second);
pendingTaskSamples = pendingTaskSamplePercent < 70
  ? startingNumberOfVMs
  : avg($PendingTasks.GetSample(180 * TimeInterval_Second));
$TargetLowPriorityNodes = min(maxNumberofVMs, pendingTaskSamples);
$NodeDeallocationOption = taskcompletion;
'@ -replace "(\r?\n)+"," "

# Create (or update) pool. `az batch pool create` will fail if it already exists.
$poolExists = $false
try {
  Invoke-Az -Args @(
    'batch','pool','show',
    '--pool-id', $BatchPoolId,
    '--account-name', $BatchAccount,
    '--output','none'
  ) | Out-Null
  $poolExists = $true
  Write-Host "Batch pool already exists: $BatchPoolId (will not recreate)." -ForegroundColor DarkGreen
} catch {
  $poolExists = $false
}

if (-not $poolExists) {
  Invoke-Az -Args @(
    'batch','pool','create',
    '--id', $BatchPoolId,
    '--account-name', $BatchAccount,
    '--vm-size', $VmSize,
    '--target-dedicated-nodes','0',
    '--target-low-priority-nodes','0',
    '--image','canonical:0001-com-ubuntu-server-jammy:22_04-lts',
    '--node-agent-sku-id','batch.node.ubuntu 22.04',
    '--identity', $identityId,
    '--enable-auto-scale',
    '--auto-scale-evaluation-interval','PT5M',
    '--auto-scale-formula', $autoScaleFormula
  ) | Out-Null
}

Write-Host "`nNOTE: Per docs, configure Batch pool container settings via Portal/ARM." -ForegroundColor Yellow
Write-Host "      Also set AZURE_CLIENT_ID=$clientId on the pool environment settings." -ForegroundColor Yellow

# ---------------------------------------------------------------------------
# Step 10 — Optional: build/push Docker image to ACR
# ---------------------------------------------------------------------------
if ($BuildAndPushImage) {
  Assert-CommandExists -Name 'docker'

  Invoke-Az -Args @('acr','login','--name', $AcrName) | Out-Null
  $imageUri = "$acrServer/$DockerImageTag"

  Write-Host "`nBuilding Docker image: $imageUri" -ForegroundColor Cyan
  & docker build -t $imageUri .
  if ($LASTEXITCODE -ne 0) { throw "docker build failed (exit $LASTEXITCODE)" }

  Write-Host "Pushing Docker image: $imageUri" -ForegroundColor Cyan
  & docker push $imageUri
  if ($LASTEXITCODE -ne 0) { throw "docker push failed (exit $LASTEXITCODE)" }

  Write-Host "Image URI: $imageUri" -ForegroundColor Green
}

# ---------------------------------------------------------------------------
# Summary outputs
# ---------------------------------------------------------------------------
Write-Host "`n=== Summary (record these) ===" -ForegroundColor Cyan
Write-Host "Storage account: $StorageAccount"
Write-Host "Container      : $Container"
Write-Host "Prefix         : $Prefix"
Write-Host "ADLS URL       : abfss://$Container@$StorageAccount.dfs.core.windows.net/$Prefix"
Write-Host "Managed identity resource ID: $identityId"
Write-Host "Managed identity client ID  : $clientId  (AZURE_CLIENT_ID on Batch pool)"
Write-Host "ACR login server            : $acrServer"
Write-Host "Azure Data Factory name     : $AdfName"
Write-Host "Azure Batch account name    : $BatchAccount"
Write-Host "Batch pool id               : $BatchPoolId"

