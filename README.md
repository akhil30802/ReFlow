# ReFlow
ReFlow is an event reliability platform for Kafka pipelines, focused on validation, failure isolation, replay, and recovery.

## Status

The repository currently contains the Kafka laboratory used for the first
implementation milestone. It provides one-broker development and three-broker
KRaft cluster profiles.

## Run locally

```bash
make kafka-dev
make kafka-cluster
make kafka-down
```

Use `make kafka-dev` for application development. The dev broker exposes
`localhost:49092`. Use `make kafka-cluster` for replication and broker-failure
experiments; that profile exposes brokers on `localhost:19092`,
`localhost:29092`, and `localhost:39092`.

## Project direction

ReFlow will provide reliable event ingestion, schema validation, retry and
dead-letter handling, safe replay, idempotent sinks, and operational
observability for Kafka-backed data pipelines.
