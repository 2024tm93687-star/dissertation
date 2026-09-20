# Step 1: Local Kafka Environment

## Design

One Kafka 4.2.0 JVM container runs both the broker and KRaft controller.
KRaft manages Kafka cluster metadata without ZooKeeper. This Kafka controller
is separate from the adaptive Spring Boot controller we will build later.

The image version is fixed for repeatability; it is not a claim to be the latest
release. Reference: [Apache Kafka Docker documentation](https://kafka.apache.org/42/getting-started/docker/).

| Setting | Initial value |
| --- | --- |
| Compose project | `dissertation` |
| Windows bootstrap address | `localhost:9092` |
| Bootstrap address for services on the Compose network | `kafka:19092` |
| Topic | `telecom-events` |
| Partitions | 6 |
| Replication factor | 1 |
| Broker CPU limit | 2 CPU cores |
| Broker memory limit | 2 GiB |
| Broker Java heap | 512 MiB initial, 1 GiB maximum |
| Data volume | `dissertation_kafka-data` |

The six partitions allow up to six consumers with assigned partitions in one
ordinary consumer group. That is a partition ceiling, not a guarantee that the
computer can run six consumers efficiently. Resource limits and consumer capacity
will be calibrated before experiments. One broker provides no broker redundancy.

## Start

Run from the repository root in PowerShell:

```powershell
docker-compose -f docker/docker-compose.yml up -d --wait kafka
docker-compose -f docker/docker-compose.yml run --rm kafka-init
docker-compose -f docker/docker-compose.yml ps
```

The first command downloads the image if needed and waits for broker health.
The second creates the topic; repeating it preserves an existing topic. It does
not change the partition count of an existing topic.

## Verify

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File docker/kafka-smoke-test.ps1
```

The script creates a uniquely named test topic, sends a synthetic JSON message,
checks that the consumed message matches exactly, describes `telecom-events`, and
deletes only its temporary test topic. Expect a `PASS` line and six partitions.
It tests the Docker network listener. The Windows listener will also be exercised
by the Spring Boot producer in Step 2.

`-ExecutionPolicy Bypass` applies only to this PowerShell process; it does not
persistently change the machine's execution policy.

Inspect topic details or logs:

```powershell
docker-compose -f docker/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic telecom-events
docker-compose -f docker/docker-compose.yml logs --tail 80 kafka
```

## Stop And Resume

```powershell
docker-compose -f docker/docker-compose.yml stop
docker-compose -f docker/docker-compose.yml up -d --wait kafka
```

The named volume preserves Kafka data across container recreation. Avoid adding
`--volumes` to `down` unless intentionally deleting the dissertation's Kafka data.
The commands are scoped to the dissertation Compose project.

## Experiment Notes

Verified on 2026-09-16:

- Compose configuration validation passed.
- Broker reached `healthy` status.
- Docker inspection confirmed `NanoCPUs=2000000000` and `Memory=2147483648`.
- `telecom-events` has six partitions, each with broker 1 as leader and in-sync replica.
- Smoke test passed with exact message equality and successful temporary-topic cleanup.
- Pulled image digest: `apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9`.

Kafka 4.2.0 emits a deprecation warning for the smoke test's
`--producer-property` option; the command still succeeds.

This is a functional setup check, not a performance result. The health check and
Kafka CLI processes consume some resources inside the broker container. Account
for monitoring overhead when defining the final experiment protocol.

Docker CPU limits are ceilings, not dedicated CPU reservations. Record Docker VM
resources, background containers, host load, image digest, and resource settings
for experiments. Before measured runs, arrange a consistent background workload;
existing unrelated containers have not been stopped by this setup.

Next: build the Spring Boot synthetic telecom producer with configurable event
rate and payload size, using `localhost:9092` when running from the IDE.
