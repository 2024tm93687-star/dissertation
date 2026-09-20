# Prometheus And Grafana Monitoring

This project now supports live monitoring with Micrometer, Prometheus, Grafana,
and a Kafka consumer group exporter.

## Start The Monitoring Stack

From the repository root:

```powershell
docker-compose -f docker/docker-compose.yml up -d kafka kafka-init kafka-exporter prometheus grafana
```

Open:

- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000>

Grafana credentials are `admin` / `admin`. The Prometheus datasource and the
`Dissertation Kafka Consumer` dashboard are provisioned automatically.

## Run A Consumer With Micrometer Enabled

The consumer remains a command-line app by default. Enable its HTTP actuator
endpoint only for monitoring runs:

```powershell
java -jar consumer/target/telecom-consumer-0.1.0-SNAPSHOT.jar `
  --spring.main.web-application-type=servlet `
  --server.port=18081 `
  --management.server.port=18081 `
  --debug=false
```

Prometheus scrapes the consumer at:

```text
http://host.docker.internal:18081/actuator/prometheus
```

From Windows PowerShell, you can check the endpoint directly:

```powershell
(Invoke-WebRequest http://localhost:18081/actuator/prometheus).Content |
  Select-String dissertation_consumer
```

## Run An Experiment With Live Metrics

The experiment runner can enable the consumer's Prometheus endpoint:

```powershell
powershell -ExecutionPolicy Bypass -File experiments/run-experiment.ps1 `
  -SkipBuild `
  -EnableConsumerMetricsHttp `
  -ConsumerHttpPort 18081 `
  -ProducerEventCount 120 `
  -ProducerRatePerSecond 40 `
  -ConsumerRunDurationSeconds 60
```

Watch Grafana while the run is active. The same run still writes result files
under `results/<run-name>/`.

## Metrics

The custom Micrometer metrics use the `dissertation.consumer.*` names in Java.
Prometheus converts them to underscore-separated names such as:

- `dissertation_consumer_processed`
- `dissertation_consumer_failed`
- `dissertation_consumer_payload_bytes`
- `dissertation_consumer_value_bytes`
- `dissertation_consumer_assigned_partitions`
- `dissertation_consumer_processing_p95_millis`
- `dissertation_consumer_processing_p99_millis`
- `dissertation_consumer_latency_p95_millis`
- `dissertation_consumer_latency_p99_millis`

Kafka lag comes from the Kafka exporter. Useful Prometheus queries:

```promql
rate(dissertation_consumer_processed[1m])
dissertation_consumer_latency_p95_millis
sum(kafka_consumergroup_lag{consumergroup=~"telecom-consumers|experiment-.*"})
dissertation_consumer_assigned_partitions
```

## Boundary

Prometheus and Grafana are for live observation. The file-based result collector
still remains the evidence source for repeatable dissertation experiments. The
finite producer emits `PRODUCER_SUMMARY` JSON and is not scraped continuously.
