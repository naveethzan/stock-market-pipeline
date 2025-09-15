"""
Deserialization utilities for Kafka Avro payloads using JVM-side from_avro.

This module keeps StreamConsumer lean by handling:
- Confluent header stripping
- Avro deserialization via from_avro
- Canonical projection to the schema expected by downstream transformations
"""

from typing import Dict

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


def ensure_from_avro_available() -> None:
    """Fail fast if from_avro is not available in the runtime."""
    try:
        from pyspark.sql.avro.functions import from_avro  # noqa: F401
    except Exception as exc:  # pragma: no cover - environment check
        raise ImportError(
            "Spark Avro functions are not available. Ensure spark-avro is on the classpath"
        ) from exc


def strip_confluent_header(df: DataFrame, value_col: str = "value", out_col: str = "avro_payload") -> DataFrame:
    """Strip the 5-byte Confluent header from a binary Kafka value column.

    Note: Uses substring on the column name; expects binary input.
    """
    # If payload shorter than header length, set to null
    return df.withColumn(
        out_col,
        F.when(F.length(F.col(value_col)) > F.lit(5), F.expr(f"substring({value_col}, 6)"))
         .otherwise(F.lit(None).cast("binary"))
    )


def deserialize_with_from_avro(df: DataFrame, payload_col: str, schema_json: str, out_col: str = "data") -> DataFrame:
    """Deserialize binary Avro payload into a struct column using JVM from_avro."""
    from pyspark.sql.avro.functions import from_avro

    return df.withColumn(out_col, from_avro(F.col(payload_col), schema_json, {"mode": "PERMISSIVE"}))


def project_canonical(df: DataFrame) -> DataFrame:
    """Project a canonical set of columns required by downstream transformations.

    Expects columns:
      - topic, partition, offset, kafka_timestamp
      - data (struct) containing symbol, current_price, open_price, high_price, low_price,
        previous_close, change, volume, timestamp (millis)
    """
    projected = (
        df.select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            F.col("data.symbol").alias("symbol"),
            F.col("data.current_price").cast("double").alias("current_price"),
            F.col("data.open_price").cast("double").alias("open_price"),
            F.col("data.high_price").cast("double").alias("high_price"),
            F.col("data.low_price").cast("double").alias("low_price"),
            F.col("data.previous_close").cast("double").alias("previous_close"),
            F.col("data.change").cast("double").alias("change"),
            F.col("data.volume").cast("long").alias("volume"),
            F.col("data.timestamp").cast("long").alias("payload_timestamp_ms"),
        )
        .withColumn(
            "processing_timestamp",
            F.coalesce(
                F.to_timestamp(F.from_unixtime((F.col("payload_timestamp_ms") / F.lit(1000)).cast("double"))),
                F.col("kafka_timestamp")
            )
        )
        # Apply simple, robust defaults to keep downstream simple
        .withColumn("current_price", F.coalesce(F.col("current_price"), F.col("low_price"), F.lit(0.0)))
        .withColumn("open_price", F.coalesce(F.col("open_price"), F.col("current_price")))
        .withColumn("high_price", F.coalesce(F.col("high_price"), F.col("current_price")))
        .withColumn("low_price", F.coalesce(F.col("low_price"), F.col("current_price")))
        .withColumn("previous_close", F.coalesce(F.col("previous_close"), F.col("current_price")))
        .withColumn("change", F.coalesce(F.col("change"), F.lit(0.0)))
        .withColumn("volume", F.coalesce(F.col("volume"), F.lit(0)))
    )

    # Drop records that still miss critical identifiers
    projected = projected.filter(F.col("symbol").isNotNull() & F.col("processing_timestamp").isNotNull())

    return projected





