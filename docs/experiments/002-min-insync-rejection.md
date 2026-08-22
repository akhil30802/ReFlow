# Experiment 002: Minimum ISR Write Rejection

## Purpose

Verify that Kafka rejects `acks=all` writes when the number of in-sync
replicas falls below `min.insync.replicas`.

## Setup

- Kafka: Apache Kafka 3.9.0
- Mode: KRaft
- Brokers: 3
- Topic: `reflow.insync-test.v1`
- Partitions: 1
- Replication factor: 2
- `min.insync.replicas`: 2
- Producer setting: `acks=all`

## Verification Steps

### 1. Create the test topic

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create \
  --topic reflow.insync-test.v1 \
  --partitions 1 \
  --replication-factor 2 \
  --config min.insync.replicas=2
```

### 2. Verify the initial replica state

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic reflow.insync-test.v1
```

Observed:

```text
Leader: 2
Replicas: 2,3
Isr: 2,3
```

### 3. Stop one replica

```bash
docker compose --profile cluster stop kafka-2
```

Verify the topic:

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic reflow.insync-test.v1
```

Observed:

```text
Leader: 3
Replicas: 2,3
Isr: 3
```

### 4. Attempt a durable write

```bash
printf 'payment-1:authorized\n' | \
docker compose --profile cluster exec -T kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic reflow.insync-test.v1 \
  --producer-property acks=all \
  --producer-property request.timeout.ms=3000 \
  --producer-property delivery.timeout.ms=10000 \
  --property parse.key=true \
  --property key.separator=:
```

Observed:

```text
NotEnoughReplicasException
```

The producer retried and then failed because one ISR replica was below the
required minimum of two.

### 5. Restore the replica

```bash
docker compose --profile cluster start kafka-2
```

Verify recovery:

```bash
docker compose --profile cluster exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic reflow.insync-test.v1
```

Observed:

```text
Leader: 2
Replicas: 2,3
Isr: 3,2
```

A subsequent `acks=all` write succeeded after broker 2 rejoined the ISR.
