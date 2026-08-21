# Experiment 001: Broker Failure Recovery

## Purpose

Verify leader failover, ISR shrinkage, continued writes, and replica recovery
after one Kafka broker fails.

## Setup

- Kafka: Apache Kafka 3.9.0
- Mode: KRaft
- Brokers: 3
- Topic: `reflow.orders.v1`
- Partitions: 6
- Replication factor: 3
- `min.insync.replicas`: 2
- Producer setting: `acks=all`

## Verification Steps

### 1. Verify the KRaft quorum

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 \
  describe --status
```

Expected:

```text
CurrentVoters: 3
MaxFollowerLag: 0
```

Observed:

```text
LeaderId: 2
CurrentVoters: 1,2,3
MaxFollowerLag: 0
```

### 2. Create the replicated topic

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create \
  --topic reflow.orders.v1 \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000
```

### 3. Verify the initial topic state

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic reflow.orders.v1
```

Expected for every partition:

```text
ReplicationFactor: 3
ISR: 1,2,3
```

### 4. Stop broker 2

```bash
docker compose --profile cluster stop kafka-2
```

### 5. Verify failover and ISR shrinkage

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic reflow.orders.v1
```

Expected:

```text
ISR: 1,3
```

Partitions previously led by broker 2 should have leaders on broker 1 or 3.

Verify the KRaft leader:

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 \
  describe --status
```

Expected: the metadata leader changes from broker 2 to broker 1 or 3.

### 6. Verify writes with two ISR replicas

```bash
printf 'order-1:created-1\norder-1:paid-1\norder-2:created-1\norder-2:paid-1\norder-3:created-1\norder-3:paid-1\n' | \
docker compose --profile cluster exec -T kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic reflow.orders.v1 \
  --producer-property acks=all \
  --property parse.key=true \
  --property key.separator=:
```

Expected: the producer exits successfully because two ISR replicas satisfy
`min.insync.replicas=2`.

### 7. Verify key partitioning and per-partition ordering

```bash
docker compose --profile cluster exec -T kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic reflow.orders.v1 \
  --from-beginning \
  --max-messages 6 \
  --property print.partition=true \
  --property print.key=true
```

Expected:

- Events with the same key use the same partition.
- `order-1:created-1` appears before `order-1:paid-1`.
- Different partitions have no global ordering guarantee.

### 8. Restart broker 2 and verify recovery

```bash
docker compose --profile cluster start kafka-2
```

After the broker starts, describe the topic again:

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic reflow.orders.v1
```

Expected for every partition:

```text
ISR: 1,2,3
```

Observed: broker 2 rejoined the ISR for all six partitions.
