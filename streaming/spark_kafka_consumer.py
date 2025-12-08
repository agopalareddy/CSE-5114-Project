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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "game_simulation")

# Snowflake Configuration
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_USER = os.getenv("SF_USER")
SF_DATABASE = os.getenv("SF_DATABASE", "BLUEJAY_DB")
SF_SCHEMA = os.getenv("SF_SCHEMA", "BLUEJAY_SCHEMA")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_PRIVATE_KEY_FILE = os.getenv("SF_PRIVATE_KEY_FILE")

# Define schema for incoming pitch events (UPPERCASE to match simulation_producer)
# Must match the columns from fetch_game_pitches() in simulation_producer.py
PITCH_SCHEMA = StructType(
    [
        StructField("GAME_PK", IntegerType(), True),
        StructField("GAME_DATE", StringType(), True),
        StructField("AT_BAT_NUMBER", IntegerType(), True),
        StructField("PITCH_NUMBER", IntegerType(), True),
        StructField("INNING", IntegerType(), True),
        StructField("INNING_TOPBOT", StringType(), True),
        StructField("HOME_TEAM", StringType(), True),
        StructField("AWAY_TEAM", StringType(), True),
        StructField("HOME_SCORE", IntegerType(), True),
        StructField("AWAY_SCORE", IntegerType(), True),
        StructField("BATTER", IntegerType(), True),
        StructField("PITCHER", IntegerType(), True),
        StructField("PLAYER_NAME", StringType(), True),
        StructField("PITCH_TYPE", StringType(), True),
        StructField("PITCH_NAME", StringType(), True),
        StructField("RELEASE_SPEED", FloatType(), True),
        StructField("RELEASE_SPIN_RATE", FloatType(), True),
        StructField("PFX_X", FloatType(), True),
        StructField("PFX_Z", FloatType(), True),
        StructField("PLATE_X", FloatType(), True),
        StructField("PLATE_Z", FloatType(), True),
        StructField("ZONE", IntegerType(), True),
        StructField("SZ_TOP", FloatType(), True),
        StructField("SZ_BOT", FloatType(), True),
        StructField("BALLS", IntegerType(), True),
        StructField("STRIKES", IntegerType(), True),
        StructField("OUTS_WHEN_UP", IntegerType(), True),
        StructField("TYPE", StringType(), True),
        StructField("EVENTS", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("ON_1B", IntegerType(), True),
        StructField("ON_2B", IntegerType(), True),
        StructField("ON_3B", IntegerType(), True),
        StructField("_simulation_timestamp", StringType(), True),
        StructField("_is_simulation", StringType(), True),
    ]
)


def create_spark_session():
    """Create Spark session with Kafka support."""
    # Only need Kafka package - using Python connector for Snowflake
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    return (
        SparkSession.builder.appName("StatcastLiveStreaming")
        .config("spark.jars.packages", kafka_package)
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

        # The Snowflake Spark connector expects the private key as a single line
        # without the PEM headers (-----BEGIN/END PRIVATE KEY-----)
        # Strip headers and join lines
        lines = private_key_content.strip().split("\n")
        # Remove header and footer lines
        key_lines = [line for line in lines if not line.startswith("-----")]
        # Join into single line
        private_key_b64 = "".join(key_lines)
    else:
        raise ValueError("SF_PRIVATE_KEY_FILE not set or file not found")

    return {
        "sfURL": f"{SF_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SF_USER,
        "pem_private_key": private_key_b64,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE,
        "dbtable": "LIVE_PITCHES",
    }


def get_snowflake_connection():
    """Get Snowflake connection using Python connector (more reliable than Spark connector)."""
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    # Read and parse private key
    with open(SF_PRIVATE_KEY_FILE, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    return snowflake.connector.connect(
        user=SF_USER,
        account=SF_ACCOUNT,
        private_key=private_key_bytes,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )


def write_to_snowflake_batch(batch_df, batch_id):
    """Write a micro-batch to Snowflake using Python connector."""
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty batch, skipping")
        return

    count = batch_df.count()
    print(f"Batch {batch_id}: Writing {count} records to Snowflake...")

    try:
        # Get column names and collect rows (avoid toPandas() which requires distutils)
        columns = batch_df.columns
        rows = batch_df.collect()

        # Get Snowflake connection
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Build INSERT statement
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)

        insert_sql = f"INSERT INTO LIVE_PITCHES ({columns_str}) VALUES ({placeholders})"

        # Insert rows
        rows_inserted = 0
        for row in rows:
            # Convert Row to tuple, handling None values
            values = tuple(
                None if v is None or (isinstance(v, float) and v != v) else v
                for v in row
            )
            cursor.execute(insert_sql, values)
            rows_inserted += 1

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Batch {batch_id}: Successfully wrote {rows_inserted} records")
    except Exception as e:
        print(f"Batch {batch_id}: Error writing to Snowflake: {e}")
        import traceback

        traceback.print_exc()
        # Don't re-raise to allow stream to continue
        # raise


def write_to_console_batch(batch_df, batch_id):
    """Write a micro-batch to console for debugging."""
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty")
        return

    count = batch_df.count()
    print(f"\n{'='*60}")
    print(f"Batch {batch_id}: {count} pitches")
    print(f"{'='*60}")

    # Show summary (UPPERCASE column names to match schema)
    batch_df.select(
        "GAME_PK",
        "INNING",
        "INNING_TOPBOT",
        "PLAYER_NAME",
        "PITCH_NAME",
        "RELEASE_SPEED",
        "DESCRIPTION",
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
