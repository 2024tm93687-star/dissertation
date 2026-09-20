[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$MatrixManifest,
    [string]$OutputDirectory = "",
    [ValidateRange(0.01, 64.0)]
    [double]$CpuCoresPerConsumer = 1.0,
    [switch]$IncludeInvalidRuns
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-PropertyValue {
    param([object]$Object, [string]$Name, [object]$Default = $null)
    if ($null -eq $Object) { return $Default }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { return $Default }
    return $Object.$Name
}

function Read-JsonLines {
    param([string]$Path)
    $items = @()
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) { return $items }
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        try { $items += ($line | ConvertFrom-Json) } catch { }
    }
    return @($items)
}

function Get-Mean {
    param([object[]]$Values)
    $numbers = @($Values | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ })
    if ($numbers.Count -eq 0) { return $null }
    return ($numbers | Measure-Object -Average).Average
}

function Get-StandardDeviation {
    param([object[]]$Values)
    $numbers = @($Values | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ })
    if ($numbers.Count -lt 2) { return 0.0 }
    $mean = Get-Mean $numbers
    $sum = 0.0
    foreach ($number in $numbers) { $sum += [Math]::Pow($number - $mean, 2) }
    return [Math]::Sqrt($sum / ($numbers.Count - 1))
}

function Get-MetricSnapshots {
    param([object]$Summary)
    $snapshots = @()
    foreach ($item in @(Get-PropertyValue $Summary "consumerInstanceMetrics" @())) {
        $snapshot = Get-PropertyValue $item "finalMetrics"
        if ($null -eq $snapshot) { $snapshot = Get-PropertyValue $item "latestMetrics" }
        if ($null -ne $snapshot) { $snapshots += $snapshot }
    }
    foreach ($item in @(Get-PropertyValue $Summary "consumerContainerMetrics" @())) {
        $snapshot = Get-PropertyValue $item "latestMetrics"
        if ($null -ne $snapshot) { $snapshots += $snapshot }
    }
    return @($snapshots)
}

function Get-LagSamplesPath {
    param([object]$Summary)
    $files = Get-PropertyValue $Summary "files"
    if ($null -eq $files) { return "" }
    return [string](Get-PropertyValue $files "kafkaLag" "")
}

function Convert-ToInvariantNumber {
    param([object]$Value, [int]$Decimals = 3)
    if ($null -eq $Value) { return "" }
    return ([Math]::Round([double]$Value, $Decimals)).ToString([System.Globalization.CultureInfo]::InvariantCulture)
}

function Write-BarChart {
    param([object[]]$Rows, [string]$Property, [string]$Title, [string]$Unit, [string]$Path)
    $chartRows = @($Rows | Where-Object { $null -ne $_.$Property })
    if ($chartRows.Count -eq 0) { return }
    $width = 960
    $left = 260
    $right = 90
    $top = 70
    $rowHeight = 34
    $height = $top + 45 + ($chartRows.Count * $rowHeight)
    $plotWidth = $width - $left - $right
    $maxValue = ($chartRows | Measure-Object -Property $Property -Maximum).Maximum
    if ($maxValue -le 0) { $maxValue = 1 }
    $colors = @{ static = "#456990"; reactive = "#e07a5f"; adaptive = "#2a9d8f"; docker = "#7a5195" }
    $lines = @(
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<svg xmlns=`"http://www.w3.org/2000/svg`" width=`"$width`" height=`"$height`" viewBox=`"0 0 $width $height`">",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        "<text x=`"24`" y=`"36`" font-family=`"Arial, sans-serif`" font-size=`"22`" fill=`"#202124`">$([System.Security.SecurityElement]::Escape($Title))</text>",
        "<text x=`"24`" y=`"58`" font-family=`"Arial, sans-serif`" font-size=`"12`" fill=`"#5f6368`">$([System.Security.SecurityElement]::Escape($Unit))</text>"
    )
    for ($index = 0; $index -lt $chartRows.Count; $index++) {
        $row = $chartRows[$index]
        $value = [double]$row.$Property
        $barWidth = [Math]::Max(1, ($value / $maxValue) * $plotWidth)
        $y = $top + ($index * $rowHeight)
        $approachFamily = if ($row.approach.StartsWith("static")) { "static" } else { $row.approach }
        $color = if ($colors.ContainsKey($approachFamily)) { $colors[$approachFamily] } else { "#6c757d" }
        $label = [System.Security.SecurityElement]::Escape("$($row.scenario) | $($row.approach)")
        $displayValue = Convert-ToInvariantNumber $value 2
        $lines += "<text x=`"24`" y=`"$($y + 17)`" font-family=`"Arial, sans-serif`" font-size=`"12`" fill=`"#202124`">$label</text>"
        $lines += "<rect x=`"$left`" y=`"$y`" width=`"$barWidth`" height=`"22`" fill=`"$color`"/>"
        $lines += "<text x=`"$([Math]::Min($left + $barWidth + 8, $width - 70))`" y=`"$($y + 16)`" font-family=`"Arial, sans-serif`" font-size=`"12`" fill=`"#202124`">$displayValue</text>"
    }
    $lines += '</svg>'
    Set-Content -LiteralPath $Path -Value $lines -Encoding UTF8
}

$manifestPath = (Resolve-Path -LiteralPath $MatrixManifest).Path
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
$matrixRoot = Split-Path -Parent $manifestPath
$analysisRoot = if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    Join-Path $matrixRoot "analysis"
} else {
    [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $OutputDirectory))
}
New-Item -ItemType Directory -Force -Path $analysisRoot | Out-Null

$rows = @()
foreach ($run in @($manifest.runs)) {
    $runEvidenceReady = [bool](Get-PropertyValue $run "evidenceReady" $false)
    if (-not $IncludeInvalidRuns -and -not $runEvidenceReady) { continue }
    $summaryPath = [string]$run.summary
    if (-not (Test-Path -LiteralPath $summaryPath)) { throw "Run summary not found: $summaryPath" }
    $summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
    $snapshots = Get-MetricSnapshots $summary
    $latencyP95Values = @($snapshots | ForEach-Object { Get-PropertyValue (Get-PropertyValue $_ "metrics") "endToEndLatency" } | ForEach-Object { Get-PropertyValue $_ "p95Millis" })
    $processingP95Values = @($snapshots | ForEach-Object { Get-PropertyValue (Get-PropertyValue $_ "metrics") "processingTime" } | ForEach-Object { Get-PropertyValue $_ "p95Millis" })
    $lagSamples = @(Read-JsonLines (Get-LagSamplesPath $summary) | Where-Object { $_.state -eq "ok" -and $null -ne $_.lag })
    $sampledConsumers = @($lagSamples | ForEach-Object { Get-PropertyValue $_ "activeConsumers" })
    $fixedConsumers = [double](Get-PropertyValue $run "maxObservedConsumers" 1)
    $averageConsumers = Get-Mean $sampledConsumers
    if ($null -eq $averageConsumers -or $averageConsumers -le 0) { $averageConsumers = $fixedConsumers }
    $peakLag = if ($lagSamples.Count -gt 0) { ($lagSamples | Measure-Object -Property lag -Maximum).Maximum } else { Get-PropertyValue $run "peakLag" }
    $throughput = [double]$run.producerRatePerSecond
    $rows += [pscustomobject]@{
        matrixName = $manifest.matrixName
        scenario = $run.scenario
        approach = $run.approach
        replication = $run.replication
        evidenceReady = $runEvidenceReady
        acknowledged = $run.producerAcknowledged
        processed = $run.consumerProcessed
        failed = $run.consumerFailed
        throughputPerSecond = $throughput
        latencyP95MaxMillis = if ($latencyP95Values.Count -gt 0) { ($latencyP95Values | Measure-Object -Maximum).Maximum } else { $null }
        processingP95MaxMillis = if ($processingP95Values.Count -gt 0) { ($processingP95Values | Measure-Object -Maximum).Maximum } else { $null }
        peakLag = $peakLag
        finalLag = $run.finalLag
        averageActiveConsumers = $averageConsumers
        maxObservedConsumers = $run.maxObservedConsumers
        throughputPerActiveConsumer = $throughput / $averageConsumers
        assumedConsumerCpuCores = $averageConsumers * $CpuCoresPerConsumer
        throughputPerCpuCore = $throughput / ($averageConsumers * $CpuCoresPerConsumer)
        scalingEfficiency = $null
        summaryPath = $summaryPath
    }
}

foreach ($row in $rows) {
    $baseline = @($rows | Where-Object {
        $_.scenario -eq $row.scenario -and $_.replication -eq $row.replication -and $_.approach -eq "static-c1"
    } | Select-Object -First 1)
    if ($baseline.Count -gt 0 -and $baseline[0].throughputPerActiveConsumer -gt 0) {
        $row.scalingEfficiency = $row.throughputPerActiveConsumer / $baseline[0].throughputPerActiveConsumer
    }
}

$runCsv = Join-Path $analysisRoot "run-metrics.csv"
$rows | Export-Csv -LiteralPath $runCsv -NoTypeInformation -Encoding UTF8

$aggregateRows = @()
foreach ($group in @($rows | Group-Object scenario, approach)) {
    $first = $group.Group[0]
    $aggregateRows += [pscustomobject]@{
        scenario = $first.scenario
        approach = $first.approach
        replications = $group.Count
        evidenceReadyRuns = @($group.Group | Where-Object { $_.evidenceReady }).Count
        throughputMean = Get-Mean @($group.Group.throughputPerSecond)
        throughputStdDev = Get-StandardDeviation @($group.Group.throughputPerSecond)
        latencyP95MaxMeanMillis = Get-Mean @($group.Group.latencyP95MaxMillis)
        peakLagMean = Get-Mean @($group.Group.peakLag)
        averageActiveConsumersMean = Get-Mean @($group.Group.averageActiveConsumers)
        throughputPerCpuCoreMean = Get-Mean @($group.Group.throughputPerCpuCore)
        scalingEfficiencyMean = Get-Mean @($group.Group.scalingEfficiency)
    }
}
$aggregateCsv = Join-Path $analysisRoot "aggregate-metrics.csv"
$aggregateRows | Export-Csv -LiteralPath $aggregateCsv -NoTypeInformation -Encoding UTF8

$report = @(
    "# Experiment Analysis: $($manifest.matrixName)", "",
    "Generated: $((Get-Date).ToUniversalTime().ToString('o'))", "",
    "Analyzed runs: $($rows.Count) of $(@($manifest.runs).Count). Invalid runs included: $([bool]$IncludeInvalidRuns).", "",
    "## Metric Definitions", "",
    "- Throughput is the producer acknowledged event rate.",
    "- P95 latency and processing time are the maximum reported per-instance p95 values, avoiding an invalid average of percentiles.",
    "- Average active consumers is the arithmetic mean of sampled controller counts; static runs use their fixed count.",
    "- Throughput per CPU core divides throughput by average active consumers and the assumed $CpuCoresPerConsumer core allocation per consumer.",
    "- Scaling efficiency compares throughput per active consumer with the matching scenario/replication static one-consumer baseline; 1.0 means equal efficiency.",
    "- Only evidence-ready runs are included by default.", "",
    "## Aggregated Results", "",
    "| Scenario | Approach | N | Throughput mean | P95 latency mean (ms) | Peak lag mean | Avg consumers | Throughput/core | Scaling efficiency |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|"
)
foreach ($row in $aggregateRows | Sort-Object scenario, approach) {
    $report += "| $($row.scenario) | $($row.approach) | $($row.replications) | $(Convert-ToInvariantNumber $row.throughputMean 2) | $(Convert-ToInvariantNumber $row.latencyP95MaxMeanMillis 2) | $(Convert-ToInvariantNumber $row.peakLagMean 2) | $(Convert-ToInvariantNumber $row.averageActiveConsumersMean 2) | $(Convert-ToInvariantNumber $row.throughputPerCpuCoreMean 2) | $(Convert-ToInvariantNumber $row.scalingEfficiencyMean 3) |"
}
$report += @(
    "", "## Files", "",
    '- `run-metrics.csv`: one row per experiment run.',
    '- `aggregate-metrics.csv`: mean and sample standard deviation by scenario and approach.',
    "- SVG charts are generated from the aggregate table.", "",
    "Do not treat smoke-run values as dissertation findings."
)
$reportPath = Join-Path $analysisRoot "analysis-report.md"
Set-Content -LiteralPath $reportPath -Value $report -Encoding UTF8

Write-BarChart -Rows $aggregateRows -Property "throughputPerCpuCoreMean" -Title "Normalized throughput" -Unit "Acknowledged events/s per assumed consumer CPU core" -Path (Join-Path $analysisRoot "throughput-per-core.svg")
Write-BarChart -Rows $aggregateRows -Property "latencyP95MaxMeanMillis" -Title "End-to-end latency" -Unit "Mean of maximum per-instance p95 latency (ms)" -Path (Join-Path $analysisRoot "latency-p95.svg")
Write-BarChart -Rows $aggregateRows -Property "peakLagMean" -Title "Peak consumer lag" -Unit "Mean peak lag (events)" -Path (Join-Path $analysisRoot "peak-lag.svg")

$summary = [pscustomobject]@{
    schemaVersion = 1
    matrixName = $manifest.matrixName
    generatedAt = (Get-Date).ToUniversalTime().ToString("o")
    includedRuns = $rows.Count
    excludedRuns = @($manifest.runs).Count - $rows.Count
    cpuCoresPerConsumerAssumption = $CpuCoresPerConsumer
    outputDirectory = $analysisRoot
}
$summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $analysisRoot "analysis-summary.json") -Encoding UTF8
Write-Output "ANALYSIS_REPORT $reportPath"
Write-Output ($summary | ConvertTo-Json -Compress)
