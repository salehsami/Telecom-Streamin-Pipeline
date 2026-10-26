# Real-Time Telecom Fraud & Usage Intelligence Pipeline

> A personal project demonstrating event-driven ingestion, stateful stream processing, late-data handling, real-time analytics, observability, and ML-ready feature engineering.

---

## 1. Project vision

Telecom systems continuously produce calls, SMS, mobile-data usage, account logins, top-ups, SIM swaps, device changes, and payment events. Valuable signals are hidden across these event streams: abnormal usage, account takeover, top-up abuse, impossible travel, and activity following a SIM swap.
This project builds a local, end-to-end streaming pipeline that generates realistic telecom events, transports them through Kafka, processes them with Apache Flink using event time and keyed state, persists analytics-ready results in ClickHouse, and presents operational and fraud insights in Grafana.
The project is intentionally designed as a **credible two-week MVP**, not a miniature telecom company. Every component must demonstrate a specific data engineering capability.

## 2. End goal

By completion, one command should launch the stack and a second command should start a reproducible scenario in which:

1. A Python producer emits normal traffic plus controlled duplicates, late events, out-of-order events, and fraud scenarios.
2. Kafka durably transports raw and processed events through versioned topics.
3. Flink validates, deduplicates, applies event-time windows and stateful detection rules, and routes invalid records to a dead-letter topic.
4. ClickHouse receives curated events, fraud alerts, and customer-level streaming features.
5. Grafana displays live usage, alert, throughput, latency, and data-quality views.
6. An automated benchmark produces honest measurements for throughput, latency, late events, duplicates, and detected scenarios.
7. The repository explains the architecture, trade-offs, failure handling, and measured results well enough to discuss in a technical interview.

The final result should prove more than “Kafka runs on my laptop.” It should show that the pipeline behaves correctly when streaming data is imperfect.

---

## 3. Architecture

```
flowchart TB
    A["Python event generator"] --> B["Kafka: raw events"]
    B --> C["Apache Flink"]
    C --> D["Kafka: curated events"]
    C --> E["Kafka: fraud alerts"]
    C --> F["Kafka: dead-letter events"]
    D --> G["ClickHouse"]
    E --> G
    G --> H["Grafana dashboards"]
    C -. metrics .-> I["Prometheus"]
    B -. metrics .-> I
    I --> H
```

### Data path

| StageResponsibilityEvidence produced |                                                            |                                                           |
| ------------------------------------ | ---------------------------------------------------------- | --------------------------------------------------------- |
| Event generator                      | Emit deterministic normal and suspicious scenarios         | Reproducible scenario IDs and configurable rates          |
| Kafka                                | Buffer, partition, retain, and replay events               | Topics, partitions, offsets, consumer lag                 |
| Flink                                | Validate, deduplicate, enrich, window, and detect patterns | Checkpoints, watermarks, side outputs, stateful rules     |
| ClickHouse                           | Serve low-latency analytical queries                       | Curated event, alert, and feature tables                  |
| Grafana                              | Communicate business and system behavior                   | Provisioned, screenshot-ready dashboards                  |
| Prometheus                           | Collect infrastructure and processing metrics              | Throughput, lag, failures, checkpoint and latency metrics |

---

## 4. Core technology choices

| ToolRoleWhy it belongs                |                                              |                                                                                                    |
| ------------------------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Python                                | Generator, test harness, and utility scripts | Fast synthetic-data development and reproducible test scenarios                                    |
| Apache Kafka in KRaft mode            | Distributed event log                        | Demonstrates topics, partitions, offsets, consumer groups, retention, and replay without ZooKeeper |
| Apache Flink / PyFlink DataStream API | Stateful stream processing                   | Demonstrates event time, watermarks, windows, keyed state, timers, checkpoints, and side outputs   |
| ClickHouse                            | Real-time analytical store                   | Fast aggregation over high-volume event data and a natural source for live dashboards              |
| Grafana                               | Visualization                                | Provides recruiter-friendly live and screenshot views without building a custom frontend           |
| Prometheus                            | Operational metrics                          | Separates pipeline observability from business analytics                                           |
| Docker Compose                        | Local runtime                                | Reproducible, zero-cloud-cost environment with one-command startup                                 |
| JSON Schema or Pydantic models        | Data contract and validation                 | Makes event shape, required fields, and schema versions explicit                                   |
| pytest                                | Unit and integration testing                 | Verifies generator logic, transformations, rules, and edge cases                                   |
| GitHub Actions                        | CI                                           | Runs formatting, linting, unit tests, and configuration validation on every change                 |
| Ruff + mypy                           | Python quality                               | Keeps the codebase consistent and type-aware                                                       |

### Deliberate exclusions from the MVP

No Kubernetes, Terraform, cloud deployment, Redis, Iceberg, custom React application, paid API, or heavy ML model is required. These tools do not improve the core streaming story enough to justify the extra time.

---

## 5. Event design

### Initial event types

- `DATA_USAGE`
- `CALL_COMPLETED`
- `SMS_SENT`
- `TOP_UP_ATTEMPT`
- `LOGIN`
- `SIM_SWAP`
- `DEVICE_CHANGE`
- `PAYMENT_ATTEMPT`

### Common event envelope

```
{
  "schema_version": 1,
  "event_id": "01J...",
  "scenario_id": "sim-swap-risk-0042",
  "event_type": "TOP_UP_ATTEMPT",
  "event_time": "2026-09-18T10:41:32.381Z",
  "ingested_at": "2026-09-18T10:41:33.014Z",
  "customer_id": "C0010291",
  "country_code": "BE",
  "device_id": "D008392",
  "payload": {
    "amount": 250.0,
    "currency": "EUR",
    "status": "APPROVED"
  }
}
```

The generator must support a seed so the same test run can be reproduced. Event time and ingestion time remain separate so lateness and end-to-end latency can be measured honestly.

### Kafka topics

| TopicPurposeSuggested key      |                                     |               |
| ------------------------------ | ----------------------------------- | ------------- |
| `telecom.raw.events.v1`        | Immutable source events             | `customer_id` |
| `telecom.curated.events.v1`    | Validated and deduplicated events   | `customer_id` |
| `telecom.fraud.alerts.v1`      | Detected fraud/risk alerts          | `customer_id` |
| `telecom.customer.features.v1` | Windowed ML-ready features          | `customer_id` |
| `telecom.dlq.events.v1`        | Invalid records with failure reason | `event_id`    |

Keying by customer preserves per-customer ordering within a partition and lets Flink maintain keyed customer state.

---

## 6. Streaming behavior to demonstrate

### Data-quality path

- Validate required fields, timestamps, enums, numeric ranges, and schema version.
- Route invalid records to the DLQ with the original payload, reason, pipeline stage, and failure timestamp.
- Deduplicate by `event_id` using keyed state with a time-to-live policy.
- Keep invalid and duplicate counters visible as operational metrics.

### Event-time handling

- Assign timestamps from `event_time`, not ingestion time.
- Use bounded-out-of-orderness watermarks.
- Configure an allowed-lateness policy appropriate to the demo.
- Record events that arrive beyond the accepted lateness boundary rather than silently hiding them.
- Demonstrate the difference between event time and processing time in the README.

### Initial fraud/risk rules

| RuleStream logicOutput |                                                                                            |                     |
| ---------------------- | ------------------------------------------------------------------------------------------ | ------------------- |
| Top-up velocity        | At least 5 top-up attempts for a customer within 60 seconds                                | `TOP_UP_VELOCITY`   |
| SIM-swap risk          | SIM swap followed by a device change and a high-value top-up within 10 minutes             | `SIM_SWAP_RISK`     |
| Impossible travel      | Login from two distant countries inside a deliberately impossible interval                 | `IMPOSSIBLE_TRAVEL` |
| Usage spike            | Data usage inside a 10-minute window exceeds a configurable threshold or baseline multiple | `USAGE_SPIKE`       |

Each alert should contain `alert_id`, rule name and version, severity, customer, detection time, event-time window, contributing event IDs, and human-readable evidence.
Thresholds belong in configuration, not scattered through application code.

### ML-ready streaming features

The pipeline should publish a compact customer feature record such as:

- `topup_attempts_1m`
- `topup_value_10m`
- `failed_payments_30m`
- `data_usage_mb_10m`
- `unique_devices_24h`
- `countries_seen_1h`
- `sim_swaps_7d`
- `seconds_since_device_change`

This is the Data Science bridge. The MVP does not need a trained model; it needs reliable, well-defined features that a later supervised fraud project can consume.

---

## 7. ClickHouse data model

Use a small model with clear responsibilities:

| TableGrainPurpose            |                                        |                                         |
| ---------------------------- | -------------------------------------- | --------------------------------------- |
| `curated_events`             | One accepted event                     | Audit and usage analytics               |
| `fraud_alerts`               | One emitted alert                      | Investigation and alert reporting       |
| `customer_feature_snapshots` | One customer per feature window/update | Offline ML dataset and feature analysis |
| `dlq_events`                 | One rejected event                     | Data-quality diagnosis                  |

Kafka engine tables and materialized views can move records from Kafka into durable MergeTree-family tables. Exact engines, partitioning, order keys, retention, and deduplication behavior should be documented as explicit design choices.

---

## 8. Dashboards that make the project visible

### Dashboard A — Telecom operations

- Events per second
- Total curated events
- Active customers in the selected interval
- Data usage by country
- Event-type distribution
- Top customers by data consumption

### Dashboard B — Fraud intelligence

- Alerts per minute
- Alerts by rule and severity
- Top suspicious customers
- Recent alert evidence table
- Fraud scenario detection rate

### Dashboard C — Pipeline health

- Kafka consumer lag
- End-to-end latency: p50, p95, and p99
- Invalid and duplicate event counts
- Late and beyond-lateness event counts
- Flink checkpoint duration and failures
- Processing throughput and restart count

Provision dashboards as code so a clean clone does not require manual panel creation.

---

## 9. Two-week execution plan

| DaysMilestoneDefinition of done |                        |                                                                                                     |
| ------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------- |
| 1–2                             | Foundation             | Repository, Docker Compose, Kafka, topic bootstrap, Python producer, health checks                  |
| 3–4                             | Contracts and bad data | Versioned event model, deterministic scenarios, duplicates, malformed, late and out-of-order events |
| 5–7                             | Flink core             | Kafka source/sinks, validation, DLQ, event timestamps, watermarks, deduplication, checkpoints       |
| 8–9                             | Stateful rules         | Four detection rules, alert evidence, configurable thresholds, feature outputs                      |
| 10–11                           | Serving layer          | ClickHouse ingestion, durable tables, analytical queries, retention decisions                       |
| 12                              | Observability          | Prometheus metrics and three provisioned Grafana dashboards                                         |
| 13                              | Verification           | Unit/integration tests, restart/replay test, benchmark script, measured results                     |
| 14                              | Portfolio finish       | README, architecture diagram, demo recording, screenshots, resume bullets, cleanup                  |

### MVP completion gate

The project is complete when all of the following are true:

- A new user can run it from documented commands.
- At least 1 million events can be generated in benchmark mode.
- All four planted fraud scenarios are detected in deterministic acceptance tests.
- Duplicates do not create duplicate curated outcomes or alerts.
- Invalid events reach the DLQ with understandable reasons.
- Out-of-order and late-event behavior is observable and documented.
- ClickHouse tables and Grafana dashboards update continuously.
- A controlled Flink restart demonstrates recovery from a checkpoint without silently losing data.
- Throughput and latency claims are generated from measurements, not invented.

### Stretch goals — only after the MVP

- Schema Registry with Avro or Protobuf compatibility checks
- Savepoint-driven job upgrade demonstration
- Property-based testing for event streams
- Short comparison of rule-based detection versus an offline anomaly model
- Local object-storage archive for replay

---

## 10. Repository shape

```
telecom-streaming-pipeline/
├── README.md
├── docker-compose.yml
├── Makefile
├── .env.example
├── configs/
│   ├── rules.yaml
│   └── prometheus.yml
├── generator/
│   ├── src/
│   └── tests/
├── flink_job/
│   ├── src/
│   └── tests/
├── clickhouse/
│   └── init/
├── grafana/
│   ├── dashboards/
│   └── provisioning/
├── scripts/
│   ├── bootstrap.sh
│   ├── run_scenario.sh
│   └── benchmark.sh
├── docs/
│   ├── architecture.md
│   ├── data-contract.md
│   ├── design-decisions.md
│   └── results.md
└── .github/workflows/ci.yml
```

Useful developer commands should be memorable:

```
make up
make topics
make run-demo
make test
make benchmark
make down
```

---

## 11. Testing and evidence

### Tests

- Unit tests for event generation, validation, and each fraud rule
- Contract tests for supported and rejected schema versions
- Deterministic end-to-end scenario tests
- Duplicate, late-event, and out-of-order tests
- ClickHouse row-count and alert-evidence assertions
- Recovery test after stopping and restarting the Flink job

### Measurements to report

- Total events processed
- Sustained events per second
- End-to-end p50/p95/p99 latency
- Duplicate suppression count and rate
- Invalid/DLQ count and rate
- Late-event count and handling outcome
- Planted scenarios detected versus missed
- False alerts in the controlled normal dataset
- Checkpoint duration and recovery result

Always state the laptop hardware, event rate, number of Kafka partitions, Flink parallelism, run duration, and measurement method beside performance numbers.

---

## 12. Portfolio presentation standard

The repository should lead with outcomes, not installation steps.

### README opening order

1. One-sentence business problem
2. Animated GIF or dashboard hero image
3. Three to five measured results
4. Architecture diagram
5. Fraud scenarios and streaming concepts demonstrated
6. Reproduction commands
7. Design decisions, limitations, and trade-offs

### Visual assets to publish

- Clean architecture diagram
- Grafana operations dashboard screenshot
- Grafana fraud dashboard screenshot
- Flink job graph and checkpoint screenshot
- Short 60–90 second demo video
- Small table proving planted scenarios and outcomes

### Honest positioning

Call it a **locally deployed production-style pipeline**, not a production telecom system. Distinguish implemented guarantees from conceptual discussion. Report observed benchmark numbers with the test conditions
