#!/bin/bash

# Script to create dead letter queue topics for Kafka Connect error handling

set -e

KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}

echo "Creating dead letter queue topics..."

# Main DLQ topic for general connector errors
kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create --if-not-exists \
    --topic connect-dlq \
    --partitions 3 \
    --replication-factor 1 \
    --config cleanup.policy=compact,delete \
    --config retention.ms=604800000

# Bronze layer DLQ
kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create --if-not-exists \
    --topic bronze-dlq \
    --partitions 3 \
    --replication-factor 1 \
    --config cleanup.policy=compact,delete \
    --config retention.ms=604800000

# Silver layer DLQ
kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create --if-not-exists \
    --topic silver-dlq \
    --partitions 3 \
    --replication-factor 1 \
    --config cleanup.policy=compact,delete \
    --config retention.ms=604800000

# Gold layer DLQ
kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create --if-not-exists \
    --topic gold-dlq \
    --partitions 3 \
    --replication-factor 1 \
    --config cleanup.policy=compact,delete \
    --config retention.ms=604800000

echo "Dead letter queue topics created successfully!"