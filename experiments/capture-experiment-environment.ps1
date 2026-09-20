[CmdletBinding()]
param(
    [string]$Protocol = "experiments/protocols/dissertation-main-v1.json",
    [string]$OutputPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-CapturedCommand {
    param([scriptblock]$Command)
    try { return ((& $Command 2>&1) -join "`n").Trim() } catch { return "ERROR: $($_.Exception.Message)" }
}

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$protocolPath = (Resolve-Path -LiteralPath (Join-Path $repoRoot $Protocol)).Path
$protocolData = Get-Content -LiteralPath $protocolPath -Raw | ConvertFrom-Json
$timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
$resolvedOutput = if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    Join-Path $repoRoot "results/environment-$timestamp.json"
} else {
    [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OutputPath))
}
$parent = Split-Path -Parent $resolvedOutput
New-Item -ItemType Directory -Force -Path $parent | Out-Null

$composePath = Join-Path $repoRoot "docker/docker-compose.yml"
$gitCommit = Invoke-CapturedCommand { git -C $repoRoot rev-parse HEAD }
$gitStatus = Invoke-CapturedCommand { git -C $repoRoot status --short }
$dockerContainers = @()
try {
    $containerJson = & docker ps --format "{{json .}}"
    foreach ($line in $containerJson) {
        if (-not [string]::IsNullOrWhiteSpace($line)) { $dockerContainers += ($line | ConvertFrom-Json) }
    }
} catch { }

$record = [pscustomobject]@{
    schemaVersion = 1
    capturedAt = (Get-Date).ToUniversalTime().ToString("o")
    protocol = [pscustomobject]@{
        path = $protocolPath
        version = $protocolData.protocolVersion
        status = $protocolData.status
        sha256 = (Get-FileHash -LiteralPath $protocolPath -Algorithm SHA256).Hash
    }
    repository = [pscustomobject]@{
        root = $repoRoot
        commit = $gitCommit
        clean = [string]::IsNullOrWhiteSpace($gitStatus)
        status = $gitStatus
    }
    host = [pscustomobject]@{
        computerName = $env:COMPUTERNAME
        operatingSystem = [System.Environment]::OSVersion.VersionString
        logicalProcessors = [System.Environment]::ProcessorCount
        powerShell = $PSVersionTable.PSVersion.ToString()
    }
    tools = [pscustomobject]@{
        java = Invoke-CapturedCommand { java -version }
        maven = Invoke-CapturedCommand { mvn -version }
        docker = Invoke-CapturedCommand { docker version --format "Client={{.Client.Version}} Server={{.Server.Version}}" }
        compose = Invoke-CapturedCommand { docker-compose --version }
    }
    compose = [pscustomobject]@{
        path = $composePath
        sha256 = (Get-FileHash -LiteralPath $composePath -Algorithm SHA256).Hash
    }
    runningContainers = $dockerContainers
    readiness = [pscustomobject]@{
        protocolFrozen = $protocolData.status -eq "frozen"
        repositoryClean = [string]::IsNullOrWhiteSpace($gitStatus)
        note = "Record power mode, Docker Desktop CPU/memory allocation, and background applications manually before the benchmark."
    }
}

$record | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $resolvedOutput -Encoding UTF8
Write-Output "ENVIRONMENT_RECORD $resolvedOutput"
Write-Output ($record.readiness | ConvertTo-Json -Compress)
