[CmdletBinding()]
param(
    [string]$RunName = ("adaptive-" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")),
    [string]$ResultsRoot = "results",
    [string]$Topic = "telecom-events",
    [ValidateRange(1, 1000)]
    [int]$TopicPartitions = 6,
    [string]$ConsumerGroup = ("adaptive-" + [guid]::NewGuid().ToString("N")),
    [ValidateRange(1, 32)]
    [int]$MinConsumers = 1,
    [ValidateRange(1, 32)]
    [int]$MaxConsumers = 4,
    [double]$InitialCapacityPerConsumer = 10.0,
    [double]$CapacityHeadroom = 1.20,
    [double]$TargetLatencyP95Millis = 2000.0,
    [int]$LagTrendWindowSamples = 3,
    [double]$KafkaCpuGuardPercent = 300.0,
    [int]$CooldownSeconds = 8,
    [int]$SampleIntervalSeconds = 2,
    [int]$DrainTimeoutSeconds = 90,
    [int]$ConsumerRunDurationSeconds = 120,
    [int]$ConsumerCpuIterations = 1000,
    [int]$ConsumerProcessingDelayMs = 0,
    [int]$ConsumerReportIntervalMs = 2000,
    [string]$ProducerScenarioFile = "",
    [int]$ProducerEventCount = 240,
    [int]$ProducerRatePerSecond = 80,
    [int]$ProducerPayloadSizeBytes = 256,
    [switch]$SkipKafkaStart,
    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-UtcTimestamp { return (Get-Date).ToUniversalTime().ToString("o") }

function Convert-ToPowerShellLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Write-JavaWrapper {
    param([string]$Path, [string]$ExitCodePath, [string[]]$Arguments)
    $lines = @('$ErrorActionPreference = "Continue"', '$javaArgs = @(')
    foreach ($argument in $Arguments) { $lines += "    $(Convert-ToPowerShellLiteral $argument)" }
    $lines += ')'
    $lines += '& java @javaArgs'
    $lines += '$exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }'
    $lines += "Set-Content -LiteralPath $(Convert-ToPowerShellLiteral $ExitCodePath) -Value `$exitCode -Encoding ASCII"
    $lines += 'exit $exitCode'
    Set-Content -LiteralPath $Path -Value $lines -Encoding UTF8
}

function Read-ExitCode {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    $code = 0
    if ([int]::TryParse((Get-Content -LiteralPath $Path -TotalCount 1).Trim(), [ref]$code)) { return $code }
    return $null
}

function Read-PrefixedJson {
    param([string]$Path, [string]$Prefix)
    $items = @()
    if (-not (Test-Path -LiteralPath $Path)) { return $items }
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line.StartsWith($Prefix)) {
            try { $items += ($line.Substring($Prefix.Length).Trim() | ConvertFrom-Json) } catch { }
        }
    }
    return $items
}

function Read-JsonLines {
    param([string]$Path)
    $items = @()
    if (-not (Test-Path -LiteralPath $Path)) { return $items }
    foreach ($line in Get-Content -LiteralPath $Path) {
        if (-not [string]::IsNullOrWhiteSpace($line)) {
            try { $items += ($line | ConvertFrom-Json) } catch { }
        }
    }
    return $items
}

function Write-JsonLine {
    param([string]$Path, [object]$Value)
    Add-Content -LiteralPath $Path -Value ($Value | ConvertTo-Json -Compress -Depth 40)
}

function Get-ScenarioValue {
    param([string]$Path, [string]$Name)
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) { return $null }
    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) { continue }
        $parts = $trimmed.Split("=", 2)
        if ($parts[0].Trim() -eq $Name) { return $parts[1].Trim() }
    }
    return $null
}

function Resolve-WorkloadProfile {
    $rate = [double]$ProducerRatePerSecond
    $payload = $ProducerPayloadSizeBytes
    if (-not [string]::IsNullOrWhiteSpace($ProducerScenarioFile)) {
        $scenarioPath = (Resolve-Path -LiteralPath $ProducerScenarioFile).Path
        $baseRate = Get-ScenarioValue -Path $scenarioPath -Name "workload.rate-per-second"
        $peakRate = Get-ScenarioValue -Path $scenarioPath -Name "workload.peak-rate-per-second"
        $payloadValue = Get-ScenarioValue -Path $scenarioPath -Name "workload.payload-size-bytes"
        if ($null -ne $baseRate) { $rate = [double]$baseRate }
        if ($null -ne $peakRate) { $rate = [Math]::Max($rate, [double]$peakRate) }
        if ($null -ne $payloadValue) { $payload = [int]$payloadValue }
    }
    return [pscustomobject]@{ incomingRatePerSecond = $rate; payloadSizeBytes = $payload }
}

function Select-ConsumerConfiguration {
    param([int]$PayloadSizeBytes)
    if ($PayloadSizeBytes -le 512) {
        return [pscustomobject]@{ maxPollRecords = 500; fetchMinBytes = 1; maxPartitionFetchBytes = 1048576; reason = "small_payload_larger_batches" }
    }
    if ($PayloadSizeBytes -le 4096) {
        return [pscustomobject]@{ maxPollRecords = 200; fetchMinBytes = 1; maxPartitionFetchBytes = 4194304; reason = "medium_payload_balanced_batches" }
    }
    return [pscustomobject]@{ maxPollRecords = 50; fetchMinBytes = 1; maxPartitionFetchBytes = 8388608; reason = "large_payload_smaller_batches" }
}

function New-ConsumerSpec {
    param([int]$Index)
    $name = "consumer-$Index"
    return [pscustomobject]@{
        index = $Index
        name = $name
        out = Join-Path $logsDir "$name.out.log"
        err = Join-Path $logsDir "$name.err.log"
        wrapper = Join-Path $logsDir "$name.run.ps1"
        exitCodePath = Join-Path $logsDir "$name.exitcode"
    }
}

function Get-ActiveConsumerRuns {
    return @($script:consumerRuns | Where-Object { $null -ne $_.process -and -not $_.process.HasExited -and $null -eq $_.stoppedAt })
}

function Get-ActiveConsumerCount { return @(Get-ActiveConsumerRuns).Count }

function Start-ConsumerInstance {
    param([int]$Index, [string]$Reason)
    $spec = New-ConsumerSpec -Index $Index
    $args = @(
        "-jar", $consumerJar,
        "--consumer.topic=$Topic",
        "--consumer.run-duration=${ConsumerRunDurationSeconds}s",
        "--consumer.cpu-iterations=$ConsumerCpuIterations",
        "--consumer.processing-delay-ms=$ConsumerProcessingDelayMs",
        "--consumer.report-interval-ms=$ConsumerReportIntervalMs",
        "--spring.kafka.consumer.group-id=$ConsumerGroup",
        "--spring.kafka.consumer.auto-offset-reset=latest",
        "--spring.kafka.consumer.max-poll-records=$($selectedConsumerConfig.maxPollRecords)",
        "--spring.kafka.consumer.properties.fetch.min.bytes=$($selectedConsumerConfig.fetchMinBytes)",
        "--spring.kafka.consumer.properties.max.partition.fetch.bytes=$($selectedConsumerConfig.maxPartitionFetchBytes)",
        "--debug=false"
    )
    Write-JavaWrapper -Path $spec.wrapper -ExitCodePath $spec.exitCodePath -Arguments $args
    $process = Start-Process -FilePath "powershell.exe" `
        -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $spec.wrapper) `
        -RedirectStandardOutput $spec.out `
        -RedirectStandardError $spec.err `
        -PassThru `
        -WindowStyle Hidden
    $run = [pscustomobject]@{ index = $Index; spec = $spec; process = $process; startedAt = New-UtcTimestamp; stoppedAt = $null; stopReason = $null }
    $script:consumerRuns += $run
    Write-JsonLine -Path $decisionPath -Value ([pscustomobject]@{
        timestamp = New-UtcTimestamp
        action = "start_consumer"
        consumerIndex = $Index
        activeConsumers = Get-ActiveConsumerCount
        reason = $Reason
        selectedConfig = $selectedConsumerConfig
    })
}

function Stop-ConsumerInstance {
    param([object]$Run, [string]$Reason)
    if ($null -ne $Run.process -and -not $Run.process.HasExited) {
        Stop-Process -Id $Run.process.Id -Force -ErrorAction SilentlyContinue
    }
    $Run.stoppedAt = New-UtcTimestamp
    $Run.stopReason = $Reason
    Write-JsonLine -Path $decisionPath -Value ([pscustomobject]@{
        timestamp = New-UtcTimestamp
        action = "stop_consumer"
        consumerIndex = $Run.index
        activeConsumers = Get-ActiveConsumerCount
        reason = $Reason
    })
}

function Read-LagSample {
    $rows = @()
    $state = "ok"
    $message = $null
    try {
        $output = & docker-compose -f docker/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group $ConsumerGroup 2>&1
        if ($LASTEXITCODE -ne 0) { $state = "command_failed"; $message = ($output -join "`n") }
        foreach ($line in $output) {
            $trimmed = $line.Trim()
            if ($trimmed.Length -eq 0 -or $trimmed.StartsWith("GROUP ")) { continue }
            $parts = $trimmed -split "\s+"
            if ($parts.Count -lt 6 -or $parts[1] -ne $Topic) { continue }
            $partition = 0; $logEndOffset = 0L; $lag = 0L; $current = 0L
            if (-not [int]::TryParse($parts[2], [ref]$partition)) { continue }
            [void][long]::TryParse($parts[3], [ref]$current)
            if (-not [long]::TryParse($parts[4], [ref]$logEndOffset)) { continue }
            if (-not [long]::TryParse($parts[5], [ref]$lag)) { continue }
            $rows += [pscustomobject]@{ partition = $partition; currentOffset = $current; logEndOffset = $logEndOffset; lag = $lag }
        }
    } catch {
        $state = "exception"
        $message = $_.Exception.Message
    }
    $sample = [pscustomobject]@{
        schemaVersion = 1
        timestamp = New-UtcTimestamp
        groupId = $ConsumerGroup
        topic = $Topic
        state = $state
        message = $message
        activeConsumers = Get-ActiveConsumerCount
        partitions = @($rows).Count
        lag = if (@($rows).Count -gt 0) { ($rows | Measure-Object -Property lag -Sum).Sum } else { $null }
        rows = $rows
    }
    Write-JsonLine -Path $lagPath -Value $sample
    return $sample
}

function Read-KafkaDockerStats {
    try {
        $lines = & docker stats --no-stream --format "{{json .}}" 2>$null
        foreach ($line in $lines) {
            if ([string]::IsNullOrWhiteSpace($line)) { continue }
            $item = $line | ConvertFrom-Json
            if ($item.Name -eq "dissertation-kafka-1") {
                return [pscustomobject]@{
                    cpuPercent = [double](($item.CPUPerc -replace "%", ""))
                    memoryPercent = [double](($item.MemPerc -replace "%", ""))
                }
            }
        }
    } catch { }
    return [pscustomobject]@{ cpuPercent = $null; memoryPercent = $null }
}

function Get-ConsumerMetricSummaries {
    $items = @()
    foreach ($run in $script:consumerRuns) {
        $reports = Read-PrefixedJson -Path $run.spec.out -Prefix "CONSUMER_METRICS "
        $latest = @($reports) | Select-Object -Last 1
        $items += [pscustomobject]@{
            index = $run.index
            startedAt = $run.startedAt
            stoppedAt = $run.stoppedAt
            stopReason = $run.stopReason
            exitCode = Read-ExitCode -Path $run.spec.exitCodePath
            reports = @($reports).Count
            latestMetrics = $latest
            out = $run.spec.out
            err = $run.spec.err
        }
    }
    return $items
}

function Get-LatestActiveMetrics {
    $latest = @()
    foreach ($run in Get-ActiveConsumerRuns) {
        $reports = Read-PrefixedJson -Path $run.spec.out -Prefix "CONSUMER_METRICS "
        $item = @($reports) | Select-Object -Last 1
        if ($null -ne $item -and $item.listenerRunning) { $latest += $item }
    }
    return $latest
}

function Get-ObservedProcessingRate {
    param([object[]]$LatestMetrics)
    $rates = @($LatestMetrics | ForEach-Object { $_.metrics.intervalThroughputPerSecond } | Where-Object { $null -ne $_ -and $_ -gt 0 })
    if ($rates.Count -eq 0) { return $null }
    return ($rates | Measure-Object -Sum).Sum
}

function Get-MaxLatencyP95 {
    param([object[]]$LatestMetrics)
    $values = @($LatestMetrics | ForEach-Object { $_.metrics.endToEndLatency.p95Millis } | Where-Object { $null -ne $_ })
    if ($values.Count -eq 0) { return $null }
    return ($values | Measure-Object -Maximum).Maximum
}

function Resolve-DesiredConsumers {
    param(
        [long]$Lag,
        [long]$LagTrend,
        [double]$CapacityPerConsumer,
        [double]$LatencyP95Millis,
        [double]$KafkaCpuPercent,
        [bool]$ProducerExited
    )

    $capacity = [Math]::Max(1.0, $CapacityPerConsumer)
    $desired = [int][Math]::Ceiling(($workload.incomingRatePerSecond * $CapacityHeadroom) / $capacity)
    $reason = "capacity_estimate"
    if ($LagTrend -gt 0) {
        $desired = [Math]::Max($desired, (Get-ActiveConsumerCount) + 1)
        $reason = "lag_trend_positive"
    }
    if ($LatencyP95Millis -gt $TargetLatencyP95Millis -and $Lag -gt 0) {
        $desired = [Math]::Max($desired, (Get-ActiveConsumerCount) + 1)
        $reason = "latency_above_target"
    }
    if ($ProducerExited -and $Lag -eq 0) {
        $desired = $MinConsumers
        $reason = "drained_after_producer"
    }
    if ($null -ne $KafkaCpuPercent -and $KafkaCpuPercent -gt $KafkaCpuGuardPercent) {
        $desired = [Math]::Min($desired, (Get-ActiveConsumerCount))
        $reason = "resource_guard_kafka_cpu"
    }
    $desired = [Math]::Max($MinConsumers, [Math]::Min([Math]::Min($MaxConsumers, $TopicPartitions), $desired))
    return [pscustomobject]@{ desired = $desired; reason = $reason }
}

function Start-Producer {
    if ([string]::IsNullOrWhiteSpace($ProducerScenarioFile)) {
        $args = @("-jar", $producerJar, "--workload.topic=$Topic", "--workload.event-count=$ProducerEventCount",
            "--workload.rate-per-second=$ProducerRatePerSecond", "--workload.payload-size-bytes=$ProducerPayloadSizeBytes", "--debug=false")
    } else {
        $scenarioPath = (Resolve-Path -LiteralPath $ProducerScenarioFile).Path
        $args = @("-jar", $producerJar, "--workload.topic=$Topic", "--spring.config.additional-location=file:$scenarioPath", "--debug=false")
    }
    Write-JavaWrapper -Path $producerWrapper -ExitCodePath $producerExitCodePath -Arguments $args
    return Start-Process -FilePath "powershell.exe" `
        -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $producerWrapper) `
        -RedirectStandardOutput $producerOut `
        -RedirectStandardError $producerErr `
        -PassThru `
        -WindowStyle Hidden
}

function New-ConsumerAggregate {
    param([object[]]$Metrics)
    $metricObjects = @($Metrics | Where-Object { $null -ne $_.latestMetrics } | ForEach-Object { $_.latestMetrics.metrics })
    return [pscustomobject]@{
        instancesStarted = @($Metrics).Count
        processed = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processed -Sum).Sum } else { 0 }
        failed = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property failed -Sum).Sum } else { 0 }
        processedPayloadBytes = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processedPayloadBytes -Sum).Sum } else { 0 }
        processedValueBytes = if ($metricObjects.Count -gt 0) { ($metricObjects | Measure-Object -Property processedValueBytes -Sum).Sum } else { 0 }
    }
}

if ($MaxConsumers -lt $MinConsumers) { throw "MaxConsumers must be greater than or equal to MinConsumers." }
if ($MaxConsumers -gt $TopicPartitions) { throw "MaxConsumers cannot exceed TopicPartitions for this experiment profile." }

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$runDir = Join-Path (Join-Path $repoRoot $ResultsRoot) $RunName
$logsDir = Join-Path $runDir "logs"
$samplesDir = Join-Path $runDir "samples"
if (Test-Path -LiteralPath $runDir) { throw "Run directory already exists: $runDir. Choose a new -RunName." }
New-Item -ItemType Directory -Force -Path $logsDir, $samplesDir | Out-Null

$consumerJar = Join-Path $repoRoot "consumer/target/telecom-consumer-0.1.0-SNAPSHOT.jar"
$producerJar = Join-Path $repoRoot "producer/target/telecom-producer-0.1.0-SNAPSHOT.jar"
$producerOut = Join-Path $logsDir "producer.out.log"
$producerErr = Join-Path $logsDir "producer.err.log"
$producerWrapper = Join-Path $logsDir "producer.run.ps1"
$producerExitCodePath = Join-Path $logsDir "producer.exitcode"
$lagPath = Join-Path $samplesDir "kafka-lag.jsonl"
$decisionPath = Join-Path $samplesDir "adaptive-decisions.jsonl"
$metadataPath = Join-Path $runDir "metadata.json"
$summaryPath = Join-Path $runDir "adaptive-summary.json"
$script:consumerRuns = @()
$producerProcess = $null
$nextConsumerIndex = 1
$lagHistory = New-Object System.Collections.Generic.List[long]
$lastScaleAt = (Get-Date).AddSeconds(-1 * $CooldownSeconds)
$capacityEstimate = $InitialCapacityPerConsumer
$workload = Resolve-WorkloadProfile
$selectedConsumerConfig = Select-ConsumerConfiguration -PayloadSizeBytes $workload.payloadSizeBytes

Push-Location -LiteralPath $repoRoot
try {
    if (-not $SkipBuild) {
        & mvn -B -ntp -f producer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Producer package failed." }
        & mvn -B -ntp -f consumer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Consumer package failed." }
    }
    if (-not (Test-Path -LiteralPath $producerJar)) { throw "Producer jar not found at $producerJar." }
    if (-not (Test-Path -LiteralPath $consumerJar)) { throw "Consumer jar not found at $consumerJar." }
    if (-not $SkipKafkaStart) {
        & docker-compose -f docker/docker-compose.yml up -d --wait kafka
        if ($LASTEXITCODE -ne 0) { throw "Kafka startup failed." }
        & docker-compose -f docker/docker-compose.yml run --rm kafka-init
        if ($LASTEXITCODE -ne 0) { throw "Kafka topic initialization failed." }
    }

    $metadata = [pscustomobject]@{
        schemaVersion = 1
        runName = $RunName
        type = "adaptive-workload-aware"
        createdAt = New-UtcTimestamp
        topic = $Topic
        topicPartitions = $TopicPartitions
        consumerGroup = $ConsumerGroup
        workload = $workload
        selectedConsumerConfig = $selectedConsumerConfig
        controller = [pscustomobject]@{
            minConsumers = $MinConsumers
            maxConsumers = $MaxConsumers
            initialCapacityPerConsumer = $InitialCapacityPerConsumer
            capacityHeadroom = $CapacityHeadroom
            targetLatencyP95Millis = $TargetLatencyP95Millis
            lagTrendWindowSamples = $LagTrendWindowSamples
            kafkaCpuGuardPercent = $KafkaCpuGuardPercent
            cooldownSeconds = $CooldownSeconds
            sampleIntervalSeconds = $SampleIntervalSeconds
        }
        consumer = [pscustomobject]@{
            runDurationSeconds = $ConsumerRunDurationSeconds
            cpuIterations = $ConsumerCpuIterations
            processingDelayMs = $ConsumerProcessingDelayMs
            reportIntervalMs = $ConsumerReportIntervalMs
        }
    }
    $metadata | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $metadataPath -Encoding UTF8

    for ($i = 1; $i -le $MinConsumers; $i++) {
        Start-ConsumerInstance -Index $nextConsumerIndex -Reason "minimum_capacity"
        $nextConsumerIndex++
    }

    $initialDeadline = (Get-Date).AddSeconds(90)
    do {
        $sample = Read-LagSample
        if ($sample.state -eq "ok" -and $sample.partitions -ge [Math]::Min($TopicPartitions, $MinConsumers)) { break }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $initialDeadline)

    $initialRequiredCapacity = $workload.incomingRatePerSecond * $CapacityHeadroom
    $initialCapacityPerConsumer = [Math]::Max(1.0, $InitialCapacityPerConsumer)
    $initialDesired = [int][Math]::Ceiling($initialRequiredCapacity / $initialCapacityPerConsumer)
    $initialDesired = [Math]::Max($MinConsumers, [Math]::Min([Math]::Min($MaxConsumers, $TopicPartitions), $initialDesired))
    while ((Get-ActiveConsumerCount) -lt $initialDesired) {
        Start-ConsumerInstance -Index $nextConsumerIndex -Reason "initial_workload_capacity_plan"
        $nextConsumerIndex++
    }
    if ($initialDesired -gt $MinConsumers) {
        Start-Sleep -Seconds ([Math]::Max(3, $SampleIntervalSeconds * 2))
        [void](Read-LagSample)
    }

    $producerProcess = Start-Producer
    $producerExitedAt = $null
    $drainDeadline = $null

    while ($true) {
        $sample = Read-LagSample
        $lag = if ($null -eq $sample.lag) { 0L } else { [int64]$sample.lag }
        $lagHistory.Add($lag)
        if ($lagHistory.Count -gt 50) { $lagHistory.RemoveAt(0) }
        $trendBaseIndex = [Math]::Max(0, $lagHistory.Count - 1 - $LagTrendWindowSamples)
        $lagTrend = $lag - $lagHistory[$trendBaseIndex]
        $active = Get-ActiveConsumerCount
        $latestMetrics = Get-LatestActiveMetrics
        $observedProcessingRate = Get-ObservedProcessingRate -LatestMetrics $latestMetrics
        if ($null -ne $observedProcessingRate -and $active -gt 0) {
            $observedPerConsumer = [double]$observedProcessingRate / [double]$active
            $capacityEstimate = (0.65 * $capacityEstimate) + (0.35 * [Math]::Max(1.0, $observedPerConsumer))
        }
        $latencyP95 = Get-MaxLatencyP95 -LatestMetrics $latestMetrics
        if ($null -eq $latencyP95) { $latencyP95 = 0.0 }
        $resource = Read-KafkaDockerStats
        $producerExited = $producerProcess.HasExited
        if ($producerExited -and $null -eq $producerExitedAt) { $producerExitedAt = Get-Date }
        $decision = Resolve-DesiredConsumers -Lag $lag -LagTrend $lagTrend -CapacityPerConsumer $capacityEstimate `
            -LatencyP95Millis $latencyP95 -KafkaCpuPercent $resource.cpuPercent -ProducerExited $producerExited
        $cooldownElapsed = ((Get-Date) - $lastScaleAt).TotalSeconds -ge $CooldownSeconds

        Write-JsonLine -Path $decisionPath -Value ([pscustomobject]@{
            timestamp = New-UtcTimestamp
            action = "evaluate"
            activeConsumers = $active
            desiredConsumers = $decision.desired
            reason = $decision.reason
            incomingRatePerSecond = $workload.incomingRatePerSecond
            payloadSizeBytes = $workload.payloadSizeBytes
            selectedMaxPollRecords = $selectedConsumerConfig.maxPollRecords
            observedProcessingRatePerSecond = $observedProcessingRate
            estimatedCapacityPerConsumer = $capacityEstimate
            lag = $lag
            lagTrend = $lagTrend
            latencyP95Millis = $latencyP95
            kafkaCpuPercent = $resource.cpuPercent
            kafkaMemoryPercent = $resource.memoryPercent
            producerExited = $producerExited
        })

        if ($decision.desired -gt $active -and $cooldownElapsed) {
            Start-ConsumerInstance -Index $nextConsumerIndex -Reason $decision.reason
            $nextConsumerIndex++
            $lastScaleAt = Get-Date
        } elseif ($decision.desired -lt $active -and $cooldownElapsed) {
            $candidate = @(Get-ActiveConsumerRuns | Sort-Object index -Descending | Select-Object -First 1)[0]
            Stop-ConsumerInstance -Run $candidate -Reason $decision.reason
            $lastScaleAt = Get-Date
        }

        if ($producerExited -and $lag -eq 0) {
            if ($null -eq $drainDeadline) {
                $drainDeadline = (Get-Date).AddSeconds($SampleIntervalSeconds * 2)
            } elseif ((Get-Date) -gt $drainDeadline) {
                break
            }
        } else {
            $drainDeadline = $null
        }
        if ($producerExited -and $null -ne $producerExitedAt -and ((Get-Date) - $producerExitedAt).TotalSeconds -gt $DrainTimeoutSeconds) { break }
        Start-Sleep -Seconds $SampleIntervalSeconds
    }

    $producerProcess.WaitForExit()
    $producerExitCode = Read-ExitCode -Path $producerExitCodePath
    foreach ($run in @(Get-ActiveConsumerRuns)) { Stop-ConsumerInstance -Run $run -Reason "run_complete" }
    Start-Sleep -Seconds 1

    $producerSummaries = Read-PrefixedJson -Path $producerOut -Prefix "PRODUCER_SUMMARY "
    $lagSamples = Read-JsonLines -Path $lagPath
    $decisions = Read-JsonLines -Path $decisionPath
    $consumerMetrics = Get-ConsumerMetricSummaries
    $aggregateConsumer = New-ConsumerAggregate -Metrics $consumerMetrics
    $validLagSamples = @($lagSamples | Where-Object { $_.state -eq "ok" -and $null -ne $_.lag })
    $peakLag = if ($validLagSamples.Count -gt 0) { ($validLagSamples | Measure-Object -Property lag -Maximum).Maximum } else { $null }
    $finalLag = @($validLagSamples | Select-Object -Last 1)[0]
    $maxObservedConsumers = if ($validLagSamples.Count -gt 0) { ($validLagSamples | Measure-Object -Property activeConsumers -Maximum).Maximum } else { $MinConsumers }

    $summary = [pscustomobject]@{
        schemaVersion = 1
        runName = $RunName
        completedAt = New-UtcTimestamp
        status = if ($producerExitCode -eq 0) { "completed" } else { "completed_with_nonzero_exit" }
        topic = $Topic
        consumerGroup = $ConsumerGroup
        producerExitCode = $producerExitCode
        producerSummary = @($producerSummaries) | Select-Object -Last 1
        workload = $workload
        selectedConsumerConfig = $selectedConsumerConfig
        controller = $metadata.controller
        decisions = $decisions
        consumerInstanceMetrics = $consumerMetrics
        aggregateConsumerMetrics = $aggregateConsumer
        finalKafkaLag = $finalLag
        peakKafkaLag = $peakLag
        maxObservedConsumers = $maxObservedConsumers
        files = [pscustomobject]@{
            metadata = $metadataPath
            producerOut = $producerOut
            kafkaLag = $lagPath
            adaptiveDecisions = $decisionPath
            summary = $summaryPath
        }
    }
    $summary | ConvertTo-Json -Depth 60 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
    Write-Output "ADAPTIVE_SUMMARY $summaryPath"
    Write-Output ($summary | ConvertTo-Json -Compress -Depth 60)
} finally {
    if ($null -ne $producerProcess -and -not $producerProcess.HasExited) { Stop-Process -Id $producerProcess.Id -Force -ErrorAction SilentlyContinue }
    if ($null -ne (Get-Variable -Name consumerRuns -Scope Script -ErrorAction SilentlyContinue).Value) {
        foreach ($run in $script:consumerRuns) {
            if ($null -ne $run.process -and -not $run.process.HasExited) {
                Stop-Process -Id $run.process.Id -Force -ErrorAction SilentlyContinue
            }
        }
    }
    Pop-Location
}
