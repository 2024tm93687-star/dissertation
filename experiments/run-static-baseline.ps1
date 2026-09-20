[CmdletBinding()]
param(
    [string]$RunSetName = ("static-baseline-" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")),
    [string]$ResultsRoot = "results",
    [string]$ConsumerGroupPrefix = "",
    [string[]]$ConsumerCounts = @("1", "2", "4"),
    [ValidateRange(1, 20)]
    [int]$Replications = 1,
    [int]$ConsumerRunDurationSeconds = 60,
    [int]$ConsumerCpuIterations = 1000,
    [int]$ConsumerProcessingDelayMs = 0,
    [int]$ConsumerReportIntervalMs = 2000,
    [int]$SampleIntervalSeconds = 2,
    [int]$ProducerEventCount = 240,
    [int]$ProducerRatePerSecond = 80,
    [int]$ProducerPayloadSizeBytes = 256,
    [string]$ProducerScenarioFile = "",
    [switch]$SkipKafkaStart,
    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-UtcTimestamp {
    return (Get-Date).ToUniversalTime().ToString("o")
}

function Invoke-Experiment {
    param(
        [string]$RunName,
        [int]$ConsumerInstances,
        [string]$ConsumerGroup
    )

    $arguments = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", (Join-Path $PSScriptRoot "run-experiment.ps1"),
        "-ResultsRoot", (Join-Path $ResultsRoot $RunSetName),
        "-RunName", $RunName,
        "-ConsumerGroup", $ConsumerGroup,
        "-ConsumerInstances", $ConsumerInstances,
        "-ConsumerRunDurationSeconds", $ConsumerRunDurationSeconds,
        "-ConsumerCpuIterations", $ConsumerCpuIterations,
        "-ConsumerProcessingDelayMs", $ConsumerProcessingDelayMs,
        "-ConsumerReportIntervalMs", $ConsumerReportIntervalMs,
        "-SampleIntervalSeconds", $SampleIntervalSeconds,
        "-ProducerEventCount", $ProducerEventCount,
        "-ProducerRatePerSecond", $ProducerRatePerSecond,
        "-ProducerPayloadSizeBytes", $ProducerPayloadSizeBytes,
        "-SkipBuild",
        "-SkipKafkaStart"
    )

    if (-not [string]::IsNullOrWhiteSpace($ProducerScenarioFile)) {
        $arguments += "-ProducerScenarioFile"
        $arguments += $ProducerScenarioFile
    }

    & powershell @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Experiment failed: $RunName"
    }
}

function Convert-ToConsumerCounts {
    param([string[]]$Values)

    $counts = @()
    foreach ($value in $Values) {
        foreach ($part in ($value -split ",")) {
            $trimmed = $part.Trim()
            if ([string]::IsNullOrWhiteSpace($trimmed)) {
                continue
            }
            $count = 0
            if (-not [int]::TryParse($trimmed, [ref]$count) -or $count -lt 1) {
                throw "Invalid consumer count: $trimmed"
            }
            $counts += $count
        }
    }
    return @($counts | Select-Object -Unique)
}

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$resolvedConsumerGroupPrefix = if ([string]::IsNullOrWhiteSpace($ConsumerGroupPrefix)) { $RunSetName } else { $ConsumerGroupPrefix }
$parsedConsumerCounts = Convert-ToConsumerCounts -Values $ConsumerCounts
$runSetRoot = Join-Path (Join-Path $repoRoot $ResultsRoot) $RunSetName
if (Test-Path -LiteralPath $runSetRoot) {
    throw "Run-set directory already exists: $runSetRoot. Choose a new -RunSetName."
}
New-Item -ItemType Directory -Force -Path $runSetRoot | Out-Null

Push-Location -LiteralPath $repoRoot
try {
    if (-not $SkipBuild) {
        & mvn -B -ntp -f producer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Producer package failed." }
        & mvn -B -ntp -f consumer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Consumer package failed." }
    }

    if (-not $SkipKafkaStart) {
        & docker-compose -f docker/docker-compose.yml up -d --wait kafka
        if ($LASTEXITCODE -ne 0) { throw "Kafka startup failed." }
        & docker-compose -f docker/docker-compose.yml run --rm kafka-init
        if ($LASTEXITCODE -ne 0) { throw "Kafka topic initialization failed." }
    }

    $runSummaries = @()
    foreach ($count in $parsedConsumerCounts) {
        if ($count -lt 1) {
            throw "Consumer count must be positive: $count"
        }
        for ($replication = 1; $replication -le $Replications; $replication++) {
            $runName = "static-c$count-r$replication"
            $group = "$resolvedConsumerGroupPrefix-c$count-r$replication"
            Invoke-Experiment -RunName $runName -ConsumerInstances $count -ConsumerGroup $group

            $summaryPath = Join-Path $runSetRoot (Join-Path $runName "summary.json")
            $summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
            $runSummaries += [pscustomobject]@{
                runName = $runName
                consumerInstances = $count
                replication = $replication
                status = $summary.status
                producerAcknowledged = $summary.producerSummary.acknowledged
                consumerProcessed = $summary.aggregateConsumerMetrics.processed
                consumerFailed = $summary.aggregateConsumerMetrics.failed
                finalLag = $summary.finalKafkaLag.lag
                acknowledgedRatePerSecond = $summary.producerSummary.acknowledgedRatePerSecond
                processingP95MillisByInstance = @($summary.consumerInstanceMetrics | ForEach-Object {
                    $_.finalMetrics.metrics.processingTime.p95Millis
                })
                latencyP95MillisByInstance = @($summary.consumerInstanceMetrics | ForEach-Object {
                    $_.finalMetrics.metrics.endToEndLatency.p95Millis
                })
                summary = $summaryPath
            }
        }
    }

    $manifest = [pscustomobject]@{
        schemaVersion = 1
        runSetName = $RunSetName
        completedAt = New-UtcTimestamp
        type = "static-baseline"
        consumerGroupPrefix = $resolvedConsumerGroupPrefix
        consumerCounts = $parsedConsumerCounts
        replications = $Replications
        workload = [pscustomobject]@{
            scenarioFile = if ([string]::IsNullOrWhiteSpace($ProducerScenarioFile)) { $null } else { $ProducerScenarioFile }
            eventCount = $ProducerEventCount
            ratePerSecond = $ProducerRatePerSecond
            payloadSizeBytes = $ProducerPayloadSizeBytes
        }
        consumer = [pscustomobject]@{
            runDurationSeconds = $ConsumerRunDurationSeconds
            cpuIterations = $ConsumerCpuIterations
            processingDelayMs = $ConsumerProcessingDelayMs
            reportIntervalMs = $ConsumerReportIntervalMs
        }
        runs = $runSummaries
    }

    $manifestPath = Join-Path $runSetRoot "baseline-manifest.json"
    $manifest | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $manifestPath -Encoding UTF8
    Write-Output "BASELINE_MANIFEST $manifestPath"
    Write-Output ($manifest | ConvertTo-Json -Compress -Depth 40)
} finally {
    Pop-Location
}
