# kafka-lag-lab

A Kafka consumer-lag bottleneck lab: reproduce real causes of growing
consumer lag on purpose, watch them happen, then fix them — all against a
real, running Kafka cluster.

> The question this project answers: a consumer group's lag is climbing
> fast, consumers are already scaled horizontally, and the instinct is
> "just add more consumers." But that only helps if consumer-vs-partition
> count is actually the constraint. This project makes the other possible
> causes — key skew, slow processing, retry storms, rebalance thrashing —
> concrete and reproducible, so the difference is something you can see
> rather than just reason about.

## Architecture

```
kafka-lag-lab/
├── docker-compose.yml     # Kafka (KRaft mode) + topic-init
├── pom.xml                # Maven multi-module parent
├── producer/              # configurable-rate, configurable-key-strategy producer
├── consumer/              # configurable-processing-mode consumer
├── diagnose/              # auto-diagnosis of which bottleneck is active
└── scripts/
    ├── run-scenario.ps1   # drives producer/consumer/diagnose per named scenario
    ├── watch-lag.sh       # live per-partition lag table
    └── watch-lag-skew.sh  # live lag-skew ratio (max/avg)
```

- **Kafka**: single-broker KRaft mode (no ZooKeeper), `confluentinc/cp-kafka:8.0.0`
- **Topic**: `lag-lab-events`, explicitly created with **6 partitions**
  (auto-create is disabled — partition count is a deliberate variable)
- **Producer / Consumer / Diagnose**: plain Java 21 + `kafka-clients`, no
  Spring — kept lean since the point is the Kafka mechanics
- Producer and consumer each use a small **strategy pattern**: which key
  strategy or message-processing behavior runs is selected by an
  environment variable at startup, so each scenario lives in its own class
  (`producer/strategy/`, `consumer/processing/`)

## Requirements

- Docker + Docker Compose
- Java 21, Maven

## Quick start

```bash
# 1. Start Kafka and create the topic
docker compose up -d

# 2. Build everything
mvn clean install

# 3. Run the baseline consumer
java -jar consumer/target/consumer-1.0-SNAPSHOT.jar

# 4. In another terminal, run the baseline producer
java -jar producer/target/producer-1.0-SNAPSHOT.jar

# 5. In a third terminal, watch lag live
./scripts/watch-lag.sh
```

With no environment variables set, this is the **baseline**: even key
distribution, fast processing, lag should stay flat and near-zero across
all 6 partitions.

## Running scenarios

`scripts/run-scenario.ps1` sets the right environment variables for a
named scenario and launches the given role. Run the **same scenario name**
as `-Role consumer` and `-Role producer` in separate terminals; add
`-Role diagnose` in a third for a live verdict.

```powershell
# one-time per PowerShell session
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# Terminal 1
.\scripts\run-scenario.ps1 -Role consumer -Scenario retry-storm

# Terminal 2
.\scripts\run-scenario.ps1 -Role producer -Scenario retry-storm

# Terminal 3 (optional)
.\scripts\run-scenario.ps1 -Role diagnose -Scenario retry-storm
```

| Scenario | Root cause | What you should see |
|---|---|---|
| `baseline` | — | Flat, low lag on all 6 partitions |
| `hot-partition` | One key gets disproportionate traffic, pinning it to one partition | One partition climbs steadily, others stay flat |
| `hot-partition-fixed` | Same entity, key salted across buckets | All 6 partitions flat again |
| `slow-processing` | Consumer blocks synchronously per message | All 6 partitions climb evenly |
| `slow-processing-fixed` | Same work, fanned out across a thread pool | Lag stabilizes and drains |
| `retry-storm` | Unbounded retries, no backoff cap | Lag climbs, one bad message can stall a batch |
| `retry-storm-fixed` | Capped attempts + exponential backoff + simulated DLQ | Lag stays bounded; occasional `DLQ: giving up...` lines |
| `rebalance-thrashing` | Batch processing time exceeds `max.poll.interval.ms` | Repeated `[REBALANCE]` log lines |
| `rebalance-thrashing-fixed` | Smaller batches or concurrent processing | `[REBALANCE]` lines stop appearing |

For the last three pairs, the producer config never changes — only the
consumer needs restarting between problem and fix. Only `hot-partition*`
requires restarting the producer too.

## Auto-diagnosis

The `diagnose` module samples lag over a short rolling window and
classifies the active bottleneck using the same signals in the table
above: rebalance events (checked first, since thrashing can masquerade as
a processing slowdown), skew ratio between the worst and average
partition, and how erratic lag growth is step-to-step.

```bash
java -jar consumer/target/consumer-1.0-SNAPSHOT.jar | Tee-Object -FilePath consumer.log
java -jar diagnose/target/diagnose-1.0-SNAPSHOT.jar
```

Tunable via env vars: `GROUP_ID`, `KAFKA_CONTAINER`, `BOOTSTRAP_SERVER`,
`INTERVAL_SECONDS` (default 3), `SAMPLES` (default 5), `REBALANCE_LOG`.

## Known limitations

- The diagnosis thresholds (skew ratio, step-delta erraticness) are
  hand-tuned against this project's own traffic rates — a demonstration
  of the approach, not a production-grade classifier. In particular, at
  default parameters a mild retry storm can average out to look
  statistically similar to a smooth slow-processing bottleneck once
  batched — worth trying a higher `RETRY_FAILURE_RATE` to see the
  distinction sharpen.
- Everything runs against a single-broker cluster; multi-broker
  replication and rebalancing add failure modes this project doesn't cover.
- Lag observability is deliberately simple (polling
  `kafka-consumer-groups --describe`); a production setup would use
  Prometheus/Grafana or Kafka's own JMX metrics instead.

## What this project demonstrates

"Add more consumers" is only correct if consumer-vs-partition count is
actually the constraint. Key skew, slow downstream calls, unbounded
retries, and rebalance churn all produce the same visible symptom — rising
lag — from different causes, and each needs a different fix. The goal
here was to make that difference something you can reproduce and observe,
not just reason about.
