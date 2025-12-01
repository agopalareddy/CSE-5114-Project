"""
Simple Spark Streaming Demo - Run Locally
==========================================
This is a simplified version you can run immediately to test Spark Streaming.

Prerequisites:
1. Java 8 or 11 installed (check with: java -version)
2. JAVA_HOME environment variable set
3. Install PySpark: pip install pyspark

Run:
    python spark_streaming_simple.py

This demo:
- Generates synthetic pitch data using Spark's rate source
- Demonstrates streaming transformations
- Outputs aggregated stats to console
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    window,
    count,
    avg,
    max as spark_max,
    expr,
    current_timestamp,
    rand,
)


def main():
    print("=" * 60)
    print("Spark Structured Streaming - Local Demo")
    print("=" * 60)

    # Check Java
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        print("WARNING: JAVA_HOME not set. Spark may not work correctly.")
        print("Set JAVA_HOME to your Java installation directory.")
    else:
        print(f"JAVA_HOME: {java_home}")

    # Create Spark Session
    print("\nInitializing Spark...")
    spark = (
        SparkSession.builder.appName("StatcastStreamingDemo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark version: {spark.version}")
    print(f"Spark UI: http://localhost:4040")

    # Generate streaming data using rate source
    # This creates a stream with 'timestamp' and 'value' columns
    print("\nStarting streaming source (rate generator)...")

    rate_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

    # Transform to simulate pitch data
    pitch_types = ["FF", "SL", "CH", "CU", "SI", "FC", "KC", "FS"]

    # Create synthetic pitch data
    pitch_df = rate_df.select(
        # Simulate game_pk (alternate between 2 "games")
        (717435 + (col("value") % 2)).cast("int").alias("game_pk"),
        # Simulate pitcher IDs (3 pitchers)
        (660271 + (col("value") % 3) * 100).cast("int").alias("pitcher"),
        # Pitch type (cycle through types) - cast index to int
        expr(
            "element_at(array('FF','SL','CH','CU','SI','FC'), CAST((value % 6) + 1 AS INT))"
        ).alias("pitch_type"),
        # Velocity (85-100 mph with some randomness)
        (lit(92.0) + (col("value") % 8) - 4 + rand() * 3).alias("release_speed"),
        # Spin rate (1800-2800 rpm)
        (lit(2300) + (col("value") % 500) - 250 + rand() * 200).alias(
            "release_spin_rate"
        ),
        # Timestamps
        col("timestamp").alias("pitch_timestamp"),
        current_timestamp().alias("processed_at"),
    )

    # =========================================================================
    # STREAMING AGGREGATION 1: Running totals per pitcher
    # =========================================================================
    pitcher_stats = pitch_df.groupBy("pitcher", "pitch_type").agg(
        count("*").alias("pitch_count"),
        avg("release_speed").alias("avg_velocity"),
        avg("release_spin_rate").alias("avg_spin_rate"),
        spark_max("pitch_timestamp").alias("last_pitch_time"),
    )

    # =========================================================================
    # STREAMING AGGREGATION 2: Windowed aggregation (last 30 seconds)
    # =========================================================================
    windowed_stats = (
        pitch_df.withWatermark("pitch_timestamp", "10 seconds")
        .groupBy(window("pitch_timestamp", "30 seconds", "10 seconds"), "game_pk")
        .agg(
            count("*").alias("pitches_in_window"),
            avg("release_speed").alias("avg_velocity"),
        )
    )

    # =========================================================================
    # OUTPUT SINKS
    # =========================================================================

    # Output 1: Pitcher stats to console (complete mode - full table each time)
    print("\n" + "=" * 60)
    print("STREAM 1: Pitcher Statistics (updates every 10 seconds)")
    print("=" * 60)

    query1 = (
        pitcher_stats.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .queryName("pitcher_stats")
        .start()
    )

    # Output 2: Windowed stats (append mode for windows)
    print("\n" + "=" * 60)
    print("STREAM 2: Windowed Game Statistics (30-second windows)")
    print("=" * 60)

    query2 = (
        windowed_stats.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .queryName("windowed_stats")
        .start()
    )

    print("\n" + "=" * 60)
    print("Streaming started! Watch the console for updates.")
    print("Press Ctrl+C to stop.")
    print("=" * 60 + "\n")

    try:
        # Wait for both queries
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping streams...")
        query1.stop()
        query2.stop()
    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()
