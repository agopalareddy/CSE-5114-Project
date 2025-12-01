"""
Spark Structured Streaming - Game Simulation Consumer
=====================================================
Consumes pitch events from the game_simulation Kafka topic and writes
to the SIMULATION_PITCHES table in Snowflake.

This consumer works with the simulation_producer.py to power
the live game simulation feature in the dashboard.

Usage:
    # Console mode (for debugging)
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        streaming/simulation_consumer.py --mode console

    # Snowflake mode (production)
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 \
        streaming/simulation_consumer.py --mode snowflake
"""

import os
import json
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    when,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
)
from dotenv import load_dotenv

# Load environment variables from project root
env_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
)
load_dotenv(env_path)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_SIMULATION_TOPIC = os.getenv("KAFKA_SIMULATION_TOPIC", "game_simulation")

# Snowflake Configuration (matching .env file)
SF_ACCOUNT = os.getenv("SF_ACCOUNT", "sfedu02-nmb12256")
SF_USER = os.getenv("SF_USER", "BLUEJAY")
SF_DATABASE = os.getenv("SF_DATABASE", "BLUEJAY_DB")
SF_SCHEMA = os.getenv("SF_SCHEMA", "BLUEJAY_SCHEMA")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "BLUEJAY_WH")
SF_PRIVATE_KEY_FILE = os.getenv("SF_PRIVATE_KEY_FILE", "rsa_key.p8")

# Schema for simulation pitch events
# Note: Producer sends uppercase keys, so schema uses uppercase
SIMULATION_PITCH_SCHEMA = StructType(
    [
        # Core identifiers
        StructField("GAME_PK", IntegerType(), True),
        StructField("GAME_DATE", StringType(), True),
        StructField("AT_BAT_NUMBER", IntegerType(), True),
        StructField("PITCH_NUMBER", IntegerType(), True),
        # Game context
        StructField("INNING", IntegerType(), True),
        StructField("INNING_TOPBOT", StringType(), True),
        StructField("HOME_TEAM", StringType(), True),
        StructField("AWAY_TEAM", StringType(), True),
        StructField("HOME_SCORE", IntegerType(), True),
        StructField("AWAY_SCORE", IntegerType(), True),
        # Players
        StructField("BATTER", IntegerType(), True),
        StructField("PITCHER", IntegerType(), True),
        StructField("PLAYER_NAME", StringType(), True),
        # Pitch data
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
        # Count
        StructField("BALLS", IntegerType(), True),
        StructField("STRIKES", IntegerType(), True),
        StructField("OUTS_WHEN_UP", IntegerType(), True),
        # Result
        StructField("TYPE", StringType(), True),
        StructField("EVENTS", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        # Runners
        StructField("ON_1B", IntegerType(), True),
        StructField("ON_2B", IntegerType(), True),
        StructField("ON_3B", IntegerType(), True),
        # Simulation metadata
        StructField("_simulation_timestamp", StringType(), True),
        StructField("_is_simulation", BooleanType(), True),
        StructField("_control_message", BooleanType(), True),
        StructField("_action", StringType(), True),
    ]
)


def create_spark_session():
    """Create Spark session with Kafka support."""
    return (
        SparkSession.builder.appName("GameSimulationConsumer")
        .config("spark.sql.streaming.checkpointLocation", "./simulation_checkpoint")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )


def get_snowflake_options():
    """Get Snowflake connection options for Spark."""
    private_key_path = SF_PRIVATE_KEY_FILE

    # Handle relative paths
    if not os.path.isabs(private_key_path):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        private_key_path = os.path.join(base_dir, private_key_path)

    if not os.path.exists(private_key_path):
        raise FileNotFoundError(f"Private key not found: {private_key_path}")

    with open(private_key_path, "r") as f:
        private_key_content = f.read()

    return {
        "sfURL": f"{SF_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SF_USER,
        "pem_private_key": private_key_content,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE,
    }


def write_to_snowflake_batch(batch_df, batch_id):
    """Write a micro-batch to Snowflake SIMULATION_PITCHES table."""
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty batch, skipping")
        return

    # Filter out control messages - only write actual pitch data
    pitch_df = batch_df.filter(
        (col("_control_message").isNull()) | (col("_control_message") == False)
    )

    if pitch_df.isEmpty():
        print(f"Batch {batch_id}: Only control messages, skipping DB write")
        return

    count = pitch_df.count()
    print(f"Batch {batch_id}: Writing {count} pitches to Snowflake...")

    try:
        sf_options = get_snowflake_options()
        sf_options["dbtable"] = "SIMULATION_PITCHES"

        # Select only the columns that match SIMULATION_PITCHES table
        output_df = pitch_df.select(
            col("GAME_PK"),
            col("AT_BAT_NUMBER"),
            col("PITCH_NUMBER"),
            col("INNING"),
            col("INNING_TOPBOT"),
            col("HOME_TEAM"),
            col("AWAY_TEAM"),
            col("HOME_SCORE"),
            col("AWAY_SCORE"),
            col("BATTER"),
            col("PITCHER"),
            col("PLAYER_NAME"),
            col("PITCH_TYPE"),
            col("PITCH_NAME"),
            col("RELEASE_SPEED"),
            col("PLATE_X"),
            col("PLATE_Z"),
            col("BALLS"),
            col("STRIKES"),
            col("OUTS_WHEN_UP"),
            col("TYPE"),
            col("EVENTS"),
            col("DESCRIPTION"),
            col("ON_1B"),
            col("ON_2B"),
            col("ON_3B"),
            col("_simulation_timestamp").alias("SIMULATION_TIMESTAMP"),
            current_timestamp().alias("LOADED_AT"),
        )

        output_df.write.format("snowflake").options(**sf_options).mode("append").save()

        print(f"Batch {batch_id}: Successfully wrote {count} records")

    except Exception as e:
        print(f"Batch {batch_id}: Error writing to Snowflake: {e}")
        raise


def write_to_console_batch(batch_df, batch_id):
    """Write a micro-batch to console for debugging."""
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty")
        return

    count = batch_df.count()
    print(f"\n{'='*60}")
    print(f"Batch {batch_id}: {count} records")
    print(f"{'='*60}")

    # Check for control messages
    control_df = batch_df.filter(col("_control_message") == True)
    if not control_df.isEmpty():
        print("CONTROL MESSAGES:")
        control_df.select("_action", "GAME_PK").show(truncate=False)

    # Show pitch data
    pitch_df = batch_df.filter(
        (col("_control_message").isNull()) | (col("_control_message") == False)
    )
    if not pitch_df.isEmpty():
        print("PITCH DATA:")
        pitch_df.select(
            "GAME_PK",
            "INNING",
            "INNING_TOPBOT",
            "PITCHER",
            "BATTER",
            "PITCH_TYPE",
            "RELEASE_SPEED",
            "DESCRIPTION",
        ).show(truncate=False)


def run_streaming_consumer(output_mode: str = "console"):
    """
    Run the Spark Structured Streaming consumer.

    Args:
        output_mode: 'console' for debugging, 'snowflake' for production
    """
    print(f"Starting Simulation Consumer (output: {output_mode})")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}, Topic: {KAFKA_SIMULATION_TOPIC}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_SIMULATION_TOPIC)
        .option("startingOffsets", "latest")  # Only read new messages
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON from Kafka value
    parsed_df = (
        kafka_df.select(
            col("key").cast("string").alias("pitch_key"),
            col("value").cast("string").alias("json_value"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "pitch_key",
            "kafka_timestamp",
            from_json(col("json_value"), SIMULATION_PITCH_SCHEMA).alias("data"),
        )
        .select("pitch_key", "kafka_timestamp", "data.*")
    )

    # Choose output sink
    if output_mode == "snowflake":
        query = (
            parsed_df.writeStream.foreachBatch(write_to_snowflake_batch)
            .outputMode("append")
            .trigger(processingTime="2 seconds")
            .start()
        )
    else:
        # Console mode for debugging
        query = (
            parsed_df.writeStream.foreachBatch(write_to_console_batch)
            .outputMode("append")
            .trigger(processingTime="2 seconds")
            .start()
        )

    print("Streaming consumer started. Press Ctrl+C to stop...")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        query.stop()
        spark.stop()


def main():
    global KAFKA_BOOTSTRAP_SERVERS, KAFKA_SIMULATION_TOPIC

    parser = argparse.ArgumentParser(description="Game Simulation Spark Consumer")
    parser.add_argument(
        "--mode",
        choices=["console", "snowflake"],
        default="console",
        help="Output mode: console (debug) or snowflake (production)",
    )
    parser.add_argument("--servers", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=None, help="Kafka topic to consume from")

    args = parser.parse_args()

    # Use args if provided, otherwise use defaults from environment
    if args.servers:
        KAFKA_BOOTSTRAP_SERVERS = args.servers
    if args.topic:
        KAFKA_SIMULATION_TOPIC = args.topic

    run_streaming_consumer(args.mode)


if __name__ == "__main__":
    main()
