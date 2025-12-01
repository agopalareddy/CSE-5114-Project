"""
Spark Structured Streaming - Kafka to Snowflake
================================================
Consumes pitch events from Kafka and writes to Snowflake LIVE_PITCHES table.

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        streaming/spark_kafka_consumer.py

Or run directly (requires SPARK_HOME or pyspark installed):
    python streaming/spark_kafka_consumer.py
"""

import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    when,
    lit,
    coalesce,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
    DateType,
)
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "statcast-live")

# Snowflake Configuration
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_USER = os.getenv("SF_USER")
SF_DATABASE = os.getenv("SF_DATABASE", "BLUEJAY_DB")
SF_SCHEMA = os.getenv("SF_SCHEMA", "BLUEJAY_SCHEMA")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_PRIVATE_KEY_FILE = os.getenv("SF_PRIVATE_KEY_FILE")

# Define schema for incoming pitch events
PITCH_SCHEMA = StructType(
    [
        StructField("game_pk", IntegerType(), True),
        StructField("game_date", StringType(), True),
        StructField("at_bat_number", IntegerType(), True),
        StructField("pitch_number", IntegerType(), True),
        StructField("inning", IntegerType(), True),
        StructField("inning_topbot", StringType(), True),
        StructField("pitcher", IntegerType(), True),
        StructField("pitcher_name", StringType(), True),
        StructField("batter", IntegerType(), True),
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("home_score", IntegerType(), True),
        StructField("away_score", IntegerType(), True),
        StructField("pitch_type", StringType(), True),
        StructField("pitch_name", StringType(), True),
        StructField("release_speed", FloatType(), True),
        StructField("release_spin_rate", FloatType(), True),
        StructField("pfx_x", FloatType(), True),
        StructField("pfx_z", FloatType(), True),
        StructField("plate_x", FloatType(), True),
        StructField("plate_z", FloatType(), True),
        StructField("zone", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("events", StringType(), True),
        StructField("description", StringType(), True),
        StructField("balls", IntegerType(), True),
        StructField("strikes", IntegerType(), True),
        StructField("sz_top", FloatType(), True),
        StructField("sz_bot", FloatType(), True),
        StructField("_streaming_timestamp", StringType(), True),
        StructField("_event_type", StringType(), True),
    ]
)


def create_spark_session():
    """Create Spark session with Kafka support."""
    # Note: Kafka packages should be passed via spark-submit --packages
    # Don't specify them here to avoid conflicts
    return (
        SparkSession.builder.appName("StatcastLiveStreaming")
        .config("spark.sql.streaming.checkpointLocation", "./streaming_checkpoint")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )


def get_snowflake_options():
    """Get Snowflake connection options for Spark."""
    # Read private key for Snowflake auth
    if SF_PRIVATE_KEY_FILE and os.path.exists(SF_PRIVATE_KEY_FILE):
        with open(SF_PRIVATE_KEY_FILE, "r") as f:
            private_key_content = f.read()
    else:
        raise ValueError("SF_PRIVATE_KEY_FILE not set or file not found")

    return {
        "sfURL": f"{SF_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SF_USER,
        "pem_private_key": private_key_content,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE,
        "dbtable": "LIVE_PITCHES",
    }


def write_to_snowflake_batch(batch_df, batch_id):
    """Write a micro-batch to Snowflake (foreachBatch sink)."""
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty batch, skipping")
        return

    count = batch_df.count()
    print(f"Batch {batch_id}: Writing {count} records to Snowflake...")

    try:
        sf_options = get_snowflake_options()

        batch_df.write.format("snowflake").options(**sf_options).mode("append").save()

        print(f"Batch {batch_id}: Successfully wrote {count} records")
    except Exception as e:
        print(f"Batch {batch_id}: Error writing to Snowflake: {e}")
        # Re-raise to trigger retry logic
        raise


def write_to_console_batch(batch_df, batch_id):
    """Write a micro-batch to console for debugging."""
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty")
        return

    count = batch_df.count()
    print(f"\n{'='*60}")
    print(f"Batch {batch_id}: {count} pitches")
    print(f"{'='*60}")

    # Show summary
    batch_df.select(
        "game_pk",
        "inning",
        "inning_topbot",
        "pitcher_name",
        "pitch_name",
        "release_speed",
        "description",
    ).show(truncate=False)


def run_streaming_job(sink="console", checkpoint_dir="./streaming_checkpoint"):
    """
    Run the Spark Structured Streaming job.

    Args:
        sink: "console" for debugging, "snowflake" for production
        checkpoint_dir: Directory for streaming checkpoints
    """
    print("[START] Starting Spark Structured Streaming...")

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[OK] Spark session created: {spark.sparkContext.appName}")
    print(f"     Spark version: {spark.version}")

    # Read from Kafka
    print(f"\n[KAFKA] Connecting to Kafka...")
    print(f"        Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"        Topic: {KAFKA_TOPIC}")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    print("[OK] Connected to Kafka stream")

    # Parse JSON messages
    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), PITCH_SCHEMA).alias("pitch"))
        .select("pitch.*")
    )

    # Add processing metadata
    enriched_df = parsed_df.withColumn("processed_at", current_timestamp()).withColumn(
        "batch_source", lit("kafka_stream")
    )

    # Choose sink
    if sink == "snowflake":
        print(f"\n[SINK] Writing to Snowflake: {SF_DATABASE}.{SF_SCHEMA}.LIVE_PITCHES")

        query = (
            enriched_df.writeStream.foreachBatch(write_to_snowflake_batch)
            .outputMode("append")
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="5 seconds")
            .start()
        )
    else:
        print("\n[SINK] Writing to console (debug mode)")

        query = (
            enriched_df.writeStream.foreachBatch(write_to_console_batch)
            .outputMode("append")
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="5 seconds")
            .start()
        )

    print(f"\n{'='*60}")
    print("STREAMING JOB RUNNING")
    print(f"   Query ID: {query.id}")
    print(f"   Checkpoint: {checkpoint_dir}")
    print(f"   Press Ctrl+C to stop")
    print(f"{'='*60}\n")

    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[STOP] Stopping streaming job...")
        query.stop()
        print("[OK] Streaming job stopped")
    finally:
        spark.stop()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Spark Kafka to Snowflake Streaming")
    parser.add_argument(
        "--sink",
        choices=["console", "snowflake"],
        default="console",
        help="Output sink (default: console)",
    )
    parser.add_argument(
        "--checkpoint", default="./streaming_checkpoint", help="Checkpoint directory"
    )
    parser.add_argument(
        "--clean-checkpoint",
        action="store_true",
        help="Clean checkpoint directory before starting",
    )

    args = parser.parse_args()

    # Clean checkpoint if requested
    if args.clean_checkpoint:
        import shutil

        if os.path.exists(args.checkpoint):
            shutil.rmtree(args.checkpoint)
            print(f"[CLEAN] Cleaned checkpoint directory: {args.checkpoint}")

    run_streaming_job(sink=args.sink, checkpoint_dir=args.checkpoint)


if __name__ == "__main__":
    main()
