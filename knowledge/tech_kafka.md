# Technology: Apache Kafka, the message broker between ingestion and processing

## What is Apache Kafka?
Apache Kafka is a distributed event streaming platform: a durable, ordered log of messages.
Producers append messages to named topics; consumers read them at their own pace and remember
their position (offset). Messages are kept for a retention period, so a consumer that stops can
resume or even replay from earlier offsets. It decouples the systems that create data from the
systems that process it.

## What Kafka does in this pipeline
Kafka sits between the producer (which polls NOAA and USGS every minute) and the two Spark
streaming jobs. The producer writes each weather reading to topic `weather` (keyed by
station_id) and each earthquake to topic `seismic` (keyed by event_id), as JSON. If Spark is
down or slow, messages wait in Kafka instead of being lost.

## How Kafka is configured here
- Apache Kafka 3.8 in KRaft mode: one container acts as both broker and controller (no ZooKeeper).
- Topics `weather` and `seismic`: 1 partition each, replication factor 1 (single node).
- Retention: 7 days, stored on the Docker volume `kafka-data`.
- Listeners: `kafka:9092` for other containers, `localhost:9094` on the VM only.
- Kafka UI runs on the VM for inspection, reachable only through an SSH tunnel.

## Delivery guarantees: at-least-once
The producer can resend the latest readings after a restart, so delivery is at-least-once:
duplicates are possible but nothing is lost. Duplicates are removed downstream: Spark drops them
with a 3-hour watermark, and the nightly Glue job keeps one row per key.

## Kafka's role when things break
- If the producer stops, no new messages arrive and the streaming lake goes stale
  (check_freshness fails; see runbook: check_freshness failed).
- If a Spark job stops, messages accumulate in Kafka for up to 7 days and are processed when the
  job restarts from its checkpoint, so data is delayed rather than lost.
- If Kafka itself is down, both datasets go stale at once.
