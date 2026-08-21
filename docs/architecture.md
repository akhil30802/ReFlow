# ReFlow Architecture

## Current milestone: Kafka laboratory

ReFlow starts with a reproducible Kafka KRaft laboratory. The `dev` profile
runs one broker for application development. The `cluster` profile runs three
combined broker/controller nodes for replication and failure experiments.

Clients running on the host use the external listeners:

| Broker | Host port | Container listener |
| --- | ---: | --- |
| kafka-dev | 49092 | `kafka-dev:9092` |
| kafka-1 | 19092 | `kafka-1:9092` |
| kafka-2 | 29092 | `kafka-2:9092` |
| kafka-3 | 39092 | `kafka-3:9092` |

The cluster profile intentionally uses `min.insync.replicas=2` for replicated
internal topics. Topic-specific durability settings will be added with the
first event contracts milestone.

## Learning objective

Before adding Go services, verify broker leadership, replication, ISR changes,
and recovery behavior under a broker failure. Every experiment should produce
an observation and a short runbook entry.
