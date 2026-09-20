[CmdletBinding()]
param(
    [string]$RunName = ("reactive-" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")),
    [string]$ResultsRoot = "results",
    [string]$Topic = "telecom-events",
    [ValidateRange(1, 1000)]
    [int]$TopicPartitions = 6,
    [string]$ConsumerGroup = ("reactive-" + [guid]::NewGuid().ToString("N")),
    [ValidateRange(1, 32)]
    [int]$MinConsumers = 1,
    [ValidateRange(1, 32)]
    [int]$MaxConsumers = 4,
    [int]$ScaleUpLagThreshold = 20,
    [int]$ScaleDownLagThreshold = 0,
    [ValidateRange(1, 20)]
    [int]$ScaleUpConsecutiveSamples = 2,
    [ValidateRange(1, 20)]
    [int]$ScaleDownConsecutiveSamples = 3,
    [int]$CooldownSeconds = 10,
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

function New-UtcTimestamp {
    return (Get-Date).ToUniversalTime().ToString("o")
}

function Convert-ToPowerShellLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Write-JavaWrapper {
    param(
        [string]$Path,
        [string]$ExitCodePath,
        [string[]]$Arguments
    )

    $lines = @()
    $lines += '$ErrorActionPreference = "Continue"'
    $lines += '$javaArgs = @('
    foreach ($argument in $Arguments) {
        $lines += "    $(Convert-ToPowerShellLiteral $argument)"
    }
    $lines += ')'
    $lines += '& java @javaArgs'
    $lines += '$exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }'
    $lines += "Set-Content -LiteralPath $(Convert-ToPowerShellLiteral $ExitCodePath) -Value `$exitCode -Encoding ASCII"
    $lines += 'exit $exitCode'
    Set-Content -LiteralPath $Path -Value $lines -Encoding UTF8
}

function Read-ExitCode {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    $value = (Get-Content -LiteralPath $Path -TotalCount 1).Trim()
    $code = 0
    if ([int]::TryParse($value, [ref]$code)) {
        return $code
    }
    return $null
}

function Read-PrefixedJson {
    param(
        [string]$Path,
        [string]$Prefix
    )

    $items = @()
    if (-not (Test-Path -LiteralPath $Path)) {
        return $items
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line.StartsWith($Prefix)) {
            $json = $line.Substring($Prefix.Length).Trim()
            try {
                $items += ($json | ConvertFrom-Json)
            } catch {
                # Raw logs are preserved for investigation.
            }
        }
    }
    return $items
}

function Read-JsonLines {
    param([string]$Path)

    $items = @()
    if (-not (Test-Path -LiteralPath $Path)) {
        return $items
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        if (-not [string]::IsNullOrWhiteSpace($line)) {
            try {
                $items += ($line | ConvertFrom-Json)
            } catch {
                # Raw JSONL is preserved for investigation.
            }
        }
    }
    return $items
}

function Write-JsonLine {
    param(
        [string]$Path,
        [object]$Value
    )
    Add-Content -LiteralPath $Path -Value ($Value | ConvertTo-Json -Compress -Depth 30)
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

function Start-ConsumerInstance {
    param([int]$Index)

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
        "--debug=false"
    )
    Write-JavaWrapper -Path $spec.wrapper -ExitCodePath $spec.exitCodePath -Arguments $args
    $process = Start-Process -FilePath "powershell.exe" `
        -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $spec.wrapper) `
        -RedirectStandardOutput $spec.out `
        -RedirectStandardError $spec.err `
        -PassThru `
        -WindowStyle Hidden

    $run = [pscustomobject]@{
        index = $Index
        spec = $spec
        process = $process
        startedAt = New-UtcTimestamp
        stoppedAt = $null
        stopReason = $null
    }
    $script:consumerRuns += $run
    Write-JsonLine -Path $scaleEventsPath -Value ([pscustomobject]@{
        timestamp = New-UtcTimestamp
        action = "start_consumer"
        consumerIndex = $Index
        activeConsumers = Get-ActiveConsumerCount
        reason = "scale_target"
    })
    return $run
}

function Stop-ConsumerInstance {
    param(
        [object]$Run,
        [string]$Reason
    )

    if ($null -ne $Run.process -and -not $Run.process.HasExited) {
        Stop-Process -Id $Run.process.Id -Force -ErrorAction SilentlyContinue
    }
    $Run.stoppedAt = New-UtcTimestamp
    $Run.stopReason = $Reason
    Write-JsonLine -Path $scaleEventsPath -Value ([pscustomobject]@{
        timestamp = New-UtcTimestamp
        action = "stop_consumer"
        consumerIndex = $Run.index
        activeConsumers = Get-ActiveConsumerCount
        reason = $Reason
    })
}

function Get-ActiveConsumerRuns {
    return @($script:consumerRuns | Where-Object {
        $null -ne $_.process -and -not $_.process.HasExited -and $null -eq $_.stoppedAt
    })
}

function Get-ActiveConsumerCount {
    return @(Get-ActiveConsumerRuns).Count
}

function Read-LagSample {
    $timestamp = New-UtcTimestamp
    $rows = @()
    $state = "ok"
    $message = $null

    try {
        $output = & docker-compose -f docker/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group $ConsumerGroup 2>&1
        if ($LASTEXITCODE -ne 0) {
            $state = "command_failed"
            $message = ($output -join "`n")
        }

        foreach ($line in $output) {
            $trimmed = $line.Trim()
            if ($trimmed.Length -eq 0 -or $trimmed.StartsWith("GROUP ")) {
                continue
            }

            $parts = $trimmed -split "\s+"
            if ($parts.Count -lt 6 -or $parts[1] -ne $Topic) {
                continue
            }

            $partition = 0
            if (-not [int]::TryParse($parts[2], [ref]$partition)) {
                continue
            }

            $currentOffset = $null
            $parsedCurrent = 0L
            if ([long]::TryParse($parts[3], [ref]$parsedCurrent)) {
                $currentOffset = $parsedCurrent
            }

            $logEndOffset = 0L
            $lag = 0L
            if (-not [long]::TryParse($parts[4], [ref]$logEndOffset)) {
                continue
            }
            if (-not [long]::TryParse($parts[5], [ref]$lag)) {
                continue
            }

            $rows += [pscustomobject]@{
                partition = $partition
                currentOffset = $currentOffset
                logEndOffset = $logEndOffset
                lag = $lag
                consumerId = if ($parts.Count -gt 6) { $parts[6] } else { $null }
                host = if ($parts.Count -gt 7) { $parts[7] } else { $null }
                clientId = if ($parts.Count -gt 8) { $parts[8] } else { $null }
            }
        }
    } catch {
        $state = "exception"
        $message = $_.Exception.Message
    }

    $sample = [pscustomobject]@{
        schemaVersion = 1
        timestamp = $timestamp
        groupId = $ConsumerGroup
        topic = $Topic
        state = $state
        message = $message
        activeConsumers = Get-ActiveConsumerCount
        partitions = @($rows).Count
        lag = if (@($rows).Count -gt 0) { ($rows | Measure-Object -Property lag -Sum).Sum } else { $null }
        rows = $rows
    }
    Write-JsonLine -Path $lagSamplesPath -Value $sample
    return $sample
}

function Wait-ForInitialAssignment {
    param([int]$TimeoutSeconds)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $sample = Read-LagSample
        if ($sample.state -eq "ok" -and $sample.partitions -ge [Math]::Min($TopicPartitions, $MinConsumers)) {
            return $true
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Start-Producer {
    if ([string]::IsNullOrWhiteSpace($ProducerScenarioFile)) {
        $producerArgs = @(
            "-jar", $producerJar,
            "--workload.topic=$Topic",
            "--workload.event-count=$ProducerEventCount",
            "--workload.rate-per-second=$ProducerRatePerSecond",
            "--workload.payload-size-bytes=$ProducerPayloadSizeBytes",
            "--debug=false"
        )
    } else {
        $scenarioPath = (Resolve-Path -LiteralPath $ProducerScenarioFile).Path
        $producerArgs = @(
            "-jar", $producerJar,
            "--workload.topic=$Topic",
            "--spring.config.additional-location=file:$scenarioPath",
            "--debug=false"
        )
    }

    Write-JavaWrapper -Path $producerWrapper -ExitCodePath $producerExitCodePath -Arguments $producerArgs
    return Start-Process -FilePath "powershell.exe" `
        -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $producerWrapper) `
        -RedirectStandardOutput $producerOut `
        -RedirectStandardError $producerErr `
        -PassThru `
        -WindowStyle Hidden
}

function Get-ConsumerMetricSummaries {
    $items = @()
    foreach ($run in $script:consumerRuns) {
        $reports = Read-PrefixedJson -Path $run.spec.out -Prefix "CONSUMER_METRICS "
        $final = @($reports | Where-Object { $_.finalSnapshot }) | Select-Object -Last 1
        if ($null -eq $final) {
            $final = @($reports) | Select-Object -Last 1
        }
        $items += [pscustomobject]@{
            index = $run.index
            startedAt = $run.startedAt
            stoppedAt = $run.stoppedAt
            stopReason = $run.stopReason
            exitCode = Read-ExitCode -Path $run.spec.exitCodePath
            reports = @($reports).Count
            latestMetrics = $final
            out = $run.spec.out
            err = $run.spec.err
        }
    }
    return $items
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

if ($MaxConsumers -lt $MinConsumers) {
    throw "MaxConsumers must be greater than or equal to MinConsumers."
}
if ($MaxConsumers -gt $TopicPartitions) {
    throw "MaxConsumers cannot exceed TopicPartitions for this experiment profile."
}

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$runDir = Join-Path (Join-Path $repoRoot $ResultsRoot) $RunName
$logsDir = Join-Path $runDir "logs"
$samplesDir = Join-Path $runDir "samples"
if (Test-Path -LiteralPath $runDir) {
    throw "Run directory already exists: $runDir. Choose a new -RunName."
}
New-Item -ItemType Directory -Force -Path $logsDir, $samplesDir | Out-Null

$consumerJar = Join-Path $repoRoot "consumer/target/telecom-consumer-0.1.0-SNAPSHOT.jar"
$producerJar = Join-Path $repoRoot "producer/target/telecom-producer-0.1.0-SNAPSHOT.jar"
$producerOut = Join-Path $logsDir "producer.out.log"
$producerErr = Join-Path $logsDir "producer.err.log"
$producerWrapper = Join-Path $logsDir "producer.run.ps1"
$producerExitCodePath = Join-Path $logsDir "producer.exitcode"
$lagSamplesPath = Join-Path $samplesDir "kafka-lag.jsonl"
$scaleEventsPath = Join-Path $samplesDir "scaling-events.jsonl"
$metadataPath = Join-Path $runDir "metadata.json"
$summaryPath = Join-Path $runDir "reactive-summary.json"
$script:consumerRuns = @()
$nextConsumerIndex = 1
$producerProcess = $null

Push-Location -LiteralPath $repoRoot
try {
    if (-not $SkipBuild) {
        & mvn -B -ntp -f producer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Producer package failed." }
        & mvn -B -ntp -f consumer/pom.xml package
        if ($LASTEXITCODE -ne 0) { throw "Consumer package failed." }
    }

    if (-not (Test-Path -LiteralPath $producerJar)) {
        throw "Producer jar not found at $producerJar. Run without -SkipBuild or build the producer first."
    }
    if (-not (Test-Path -LiteralPath $consumerJar)) {
        throw "Consumer jar not found at $consumerJar. Run without -SkipBuild or build the consumer first."
    }

    if (-not $SkipKafkaStart) {
        & docker-compose -f docker/docker-compose.yml up -d --wait kafka
        if ($LASTEXITCODE -ne 0) { throw "Kafka startup failed." }
        & docker-compose -f docker/docker-compose.yml run --rm kafka-init
        if ($LASTEXITCODE -ne 0) { throw "Kafka topic initialization failed." }
    }

    $metadata = [pscustomobject]@{
        schemaVersion = 1
        runName = $RunName
        createdAt = New-UtcTimestamp
        type = "reactive-threshold"
        topic = $Topic
        topicPartitions = $TopicPartitions
        consumerGroup = $ConsumerGroup
        controller = [pscustomobject]@{
            minConsumers = $MinConsumers
            maxConsumers = $MaxConsumers
            scaleUpLagThreshold = $ScaleUpLagThreshold
            scaleDownLagThreshold = $ScaleDownLagThreshold
            scaleUpConsecutiveSamples = $ScaleUpConsecutiveSamples
            scaleDownConsecutiveSamples = $ScaleDownConsecutiveSamples
            cooldownSeconds = $CooldownSeconds
            sampleIntervalSeconds = $SampleIntervalSeconds
        }
        consumer = [pscustomobject]@{
            runDurationSeconds = $ConsumerRunDurationSeconds
            cpuIterations = $ConsumerCpuIterations
            processingDelayMs = $ConsumerProcessingDelayMs
            reportIntervalMs = $ConsumerReportIntervalMs
        }
        producer = [pscustomobject]@{
            scenarioFile = if ([string]::IsNullOrWhiteSpace($ProducerScenarioFile)) { $null } else { $ProducerScenarioFile }
            eventCount = $ProducerEventCount
            ratePerSecond = $ProducerRatePerSecond
            payloadSizeBytes = $ProducerPayloadSizeBytes
        }
    }
    $metadata | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $metadataPath -Encoding UTF8

    for ($i = 1; $i -le $MinConsumers; $i++) {
        [void](Start-ConsumerInstance -Index $nextConsumerIndex)
        $nextConsumerIndex++
    }

    if (-not (Wait-ForInitialAssignment -TimeoutSeconds 90)) {
        throw "Initial reactive consumer group did not become ready within 90 seconds."
    }

    $producerProcess = Start-Producer
    $highLagSamples = 0
    $lowLagSamples = 0
    $lastScaleAt = (Get-Date).AddSeconds(-1 * $CooldownSeconds)
    $drainDeadline = $null
    $producerExitedAt = $null

    while ($true) {
        $sample = Read-LagSample
        $lag = if ($null -eq $sample.lag) { 0 } else { [int64]$sample.lag }
        $active = Get-ActiveConsumerCount
        $producerExited = $producerProcess.HasExited
        if ($producerExited -and $null -eq $producerExitedAt) {
            $producerExitedAt = Get-Date
        }
        $cooldownElapsed = ((Get-Date) - $lastScaleAt).TotalSeconds -ge $CooldownSeconds

        if ($lag -gt $ScaleUpLagThreshold) {
            $highLagSamples++
            $lowLagSamples = 0
        } elseif ($lag -le $ScaleDownLagThreshold) {
            $lowLagSamples++
            $highLagSamples = 0
        } else {
            $highLagSamples = 0
            $lowLagSamples = 0
        }

        if ($highLagSamples -ge $ScaleUpConsecutiveSamples -and $active -lt $MaxConsumers -and $cooldownElapsed) {
            [void](Start-ConsumerInstance -Index $nextConsumerIndex)
            $nextConsumerIndex++
            $lastScaleAt = Get-Date
            $highLagSamples = 0
        } elseif ($producerExited -and $lowLagSamples -ge $ScaleDownConsecutiveSamples -and $active -gt $MinConsumers -and $cooldownElapsed) {
            $candidate = @(Get-ActiveConsumerRuns | Sort-Object index -Descending | Select-Object -First 1)[0]
            Stop-ConsumerInstance -Run $candidate -Reason "lag_below_threshold_after_producer"
            $lastScaleAt = Get-Date
            $lowLagSamples = 0
        }

        if ($producerExited -and $lag -le $ScaleDownLagThreshold) {
            if ($null -eq $drainDeadline) {
                $drainDeadline = (Get-Date).AddSeconds($ScaleDownConsecutiveSamples * $SampleIntervalSeconds)
            } elseif ((Get-Date) -gt $drainDeadline) {
                break
            }
        } else {
            $drainDeadline = $null
        }

        if ($producerExited -and $null -ne $producerExitedAt -and ((Get-Date) - $producerExitedAt).TotalSeconds -gt $DrainTimeoutSeconds) {
            break
        }

        Start-Sleep -Seconds $SampleIntervalSeconds
    }

    $producerProcess.WaitForExit()
    $producerProcess.Refresh()
    $producerExitCode = Read-ExitCode -Path $producerExitCodePath

    foreach ($run in @(Get-ActiveConsumerRuns)) {
        Stop-ConsumerInstance -Run $run -Reason "run_complete"
    }

    Start-Sleep -Seconds 1

    $producerSummaries = Read-PrefixedJson -Path $producerOut -Prefix "PRODUCER_SUMMARY "
    $lagSamples = Read-JsonLines -Path $lagSamplesPath
    $scaleEvents = Read-JsonLines -Path $scaleEventsPath
    $consumerMetrics = Get-ConsumerMetricSummaries
    $aggregateConsumer = New-ConsumerAggregate -Metrics $consumerMetrics
    $validLagSamples = @($lagSamples | Where-Object { $_.state -eq "ok" -and $null -ne $_.lag })
    $finalLag = @($validLagSamples | Select-Object -Last 1)[0]
    $peakLag = if ($validLagSamples.Count -gt 0) { ($validLagSamples | Measure-Object -Property lag -Maximum).Maximum } else { $null }
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
        controller = $metadata.controller
        scaleEvents = $scaleEvents
        consumerInstanceMetrics = $consumerMetrics
        aggregateConsumerMetrics = $aggregateConsumer
        finalKafkaLag = $finalLag
        peakKafkaLag = $peakLag
        maxObservedConsumers = $maxObservedConsumers
        files = [pscustomobject]@{
            metadata = $metadataPath
            producerOut = $producerOut
            kafkaLag = $lagSamplesPath
            scalingEvents = $scaleEventsPath
            summary = $summaryPath
        }
    }
    $summary | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $summaryPath -Encoding UTF8

    Write-Output "REACTIVE_SUMMARY $summaryPath"
    Write-Output ($summary | ConvertTo-Json -Compress -Depth 50)
} finally {
    if ($null -ne $producerProcess -and -not $producerProcess.HasExited) {
        Stop-Process -Id $producerProcess.Id -Force -ErrorAction SilentlyContinue
    }
    if ($null -ne (Get-Variable -Name consumerRuns -Scope Script -ErrorAction SilentlyContinue).Value) {
        foreach ($run in $script:consumerRuns) {
            if ($null -ne $run.process -and -not $run.process.HasExited) {
                Stop-Process -Id $run.process.Id -Force -ErrorAction SilentlyContinue
            }
        }
    }
    Pop-Location
}
