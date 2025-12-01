"""
Spark Structured Streaming ETL for MLB Statcast Data
=====================================================
This script sets up a Spark Structured Streaming job that:
1. Polls for new Statcast data at regular intervals
2. Processes/transforms the data
3. Writes to Snowflake in micro-batches

Prerequisites:
- Java 8 or 11 installed (JAVA_HOME set)
- PySpark installed: pip install pyspark
- Snowflake Spark Connector JAR

Run with:
    spark-submit --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.14.4 spark_streaming_etl.py

Or simply:
    python spark_streaming_etl.py  (if running with local Spark)
"""

import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    concat_ws,
    current_timestamp,
    when,
    year,
    month,
    dayofmonth,
    dayofweek,
    avg,
    count,
    max as spark_max,
    min as spark_min,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

# Snowflake Configuration
SNOWFLAKE_OPTIONS = {
    "sfURL": f"{os.getenv('SF_ACCOUNT')}.snowflakecomputing.com",
    "sfUser": os.getenv("SF_USER"),
    "sfPassword": os.getenv("SF_PASSWORD"),  # For Spark connector, use password auth
    "sfDatabase": os.getenv("SF_DATABASE", "BLUEJAY_DB"),
    "sfSchema": os.getenv("SF_SCHEMA", "BLUEJAY_SCHEMA"),
    "sfWarehouse": os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
}

# Streaming Configuration
BATCH_INTERVAL_SECONDS = 60  # Process every 60 seconds
CHECKPOINT_LOCATION = "./checkpoints/statcast_stream"


def create_spark_session():
    """
    Create a Spark session with Snowflake connector.
    """
    # For local development, you may need to download JARs manually
    # or use --packages flag with spark-submit

    spark = (
        SparkSession.builder.appName("StatcastLiveStreaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,"
            "net.snowflake:snowflake-jdbc:3.14.4",
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_statcast_schema():
    """
    Define schema for Statcast data.
    This ensures type consistency in streaming.
    """
    return StructType(
        [
            StructField("pitch_type", StringType(), True),
            StructField("game_date", DateType(), True),
            StructField("release_speed", DoubleType(), True),
            StructField("release_pos_x", DoubleType(), True),
            StructField("release_pos_z", DoubleType(), True),
            StructField("player_name", StringType(), True),
            StructField("batter", IntegerType(), True),
            StructField("pitcher", IntegerType(), True),
            StructField("events", StringType(), True),
            StructField("description", StringType(), True),
            StructField("zone", IntegerType(), True),
            StructField("des", StringType(), True),
            StructField("game_type", StringType(), True),
            StructField("stand", StringType(), True),
            StructField("p_throws", StringType(), True),
            StructField("home_team", StringType(), True),
            StructField("away_team", StringType(), True),
            StructField("type", StringType(), True),
            StructField("hit_location", IntegerType(), True),
            StructField("bb_type", StringType(), True),
            StructField("balls", IntegerType(), True),
            StructField("strikes", IntegerType(), True),
            StructField("game_year", IntegerType(), True),
            StructField("pfx_x", DoubleType(), True),
            StructField("pfx_z", DoubleType(), True),
            StructField("plate_x", DoubleType(), True),
            StructField("plate_z", DoubleType(), True),
            StructField("on_3b", IntegerType(), True),
            StructField("on_2b", IntegerType(), True),
            StructField("on_1b", IntegerType(), True),
            StructField("outs_when_up", IntegerType(), True),
            StructField("inning", IntegerType(), True),
            StructField("inning_topbot", StringType(), True),
            StructField("hc_x", DoubleType(), True),
            StructField("hc_y", DoubleType(), True),
            StructField("sv_id", StringType(), True),
            StructField("vx0", DoubleType(), True),
            StructField("vy0", DoubleType(), True),
            StructField("vz0", DoubleType(), True),
            StructField("ax", DoubleType(), True),
            StructField("ay", DoubleType(), True),
            StructField("az", DoubleType(), True),
            StructField("sz_top", DoubleType(), True),
            StructField("sz_bot", DoubleType(), True),
            StructField("hit_distance_sc", DoubleType(), True),
            StructField("launch_speed", DoubleType(), True),
            StructField("launch_angle", DoubleType(), True),
            StructField("effective_speed", DoubleType(), True),
            StructField("release_spin_rate", DoubleType(), True),
            StructField("release_extension", DoubleType(), True),
            StructField("game_pk", IntegerType(), True),
            StructField("at_bat_number", IntegerType(), True),
            StructField("pitch_number", IntegerType(), True),
            StructField("pitch_name", StringType(), True),
            StructField("home_score", IntegerType(), True),
            StructField("away_score", IntegerType(), True),
            StructField("bat_score", IntegerType(), True),
            StructField("fld_score", IntegerType(), True),
            StructField("post_home_score", IntegerType(), True),
            StructField("post_away_score", IntegerType(), True),
            StructField("post_bat_score", IntegerType(), True),
            StructField("post_fld_score", IntegerType(), True),
            StructField("if_fielding_alignment", StringType(), True),
            StructField("of_fielding_alignment", StringType(), True),
            StructField("spin_axis", DoubleType(), True),
            StructField("delta_home_win_exp", DoubleType(), True),
            StructField("delta_run_exp", DoubleType(), True),
        ]
    )


def fetch_statcast_batch():
    """
    Fetch the latest Statcast data.
    In a real streaming scenario, this would be replaced by Kafka consumer.
    """
    from pybaseball import statcast
    import pandas as pd

    # Get today's date (or yesterday if no games today)
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        df = statcast(start_dt=today, end_dt=today)
        if df.empty:
            # Try yesterday
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            df = statcast(start_dt=yesterday, end_dt=yesterday)
        return df
    except Exception as e:
        print(f"Error fetching Statcast data: {e}")
        return pd.DataFrame()


def process_batch(df, epoch_id):
    """
    Process each micro-batch of streaming data.
    This is called for each batch in foreachBatch sink.
    """
    if df.count() == 0:
        print(f"Batch {epoch_id}: No data to process")
        return

    print(f"Batch {epoch_id}: Processing {df.count()} records")

    # Add processing timestamp
    df = df.withColumn("processed_at", current_timestamp())

    # Add composite key
    df = df.withColumn(
        "pitch_uid",
        concat_ws("_", col("game_pk"), col("at_bat_number"), col("pitch_number")),
    )

    # Write to Snowflake
    try:
        df.write.format("snowflake").options(**SNOWFLAKE_OPTIONS).option(
            "dbtable", "STATCAST_STREAMING"
        ).mode("append").save()
        print(f"Batch {epoch_id}: Successfully wrote to Snowflake")
    except Exception as e:
        print(f"Batch {epoch_id}: Error writing to Snowflake: {e}")


def run_file_streaming(spark, input_path="./streaming_input"):
    """
    Run Spark Structured Streaming from a file source.

    This monitors a directory for new CSV/Parquet files and processes them.
    Useful for testing without Kafka.
    """
    os.makedirs(input_path, exist_ok=True)
    os.makedirs(CHECKPOINT_LOCATION, exist_ok=True)

    schema = get_statcast_schema()

    # Read stream from CSV files dropped into input directory
    stream_df = (
        spark.readStream.schema(schema)
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .csv(input_path)
    )

    # Process and write using foreachBatch
    query = (
        stream_df.writeStream.foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime=f"{BATCH_INTERVAL_SECONDS} seconds")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    print(f"Streaming started. Drop CSV files into: {input_path}")
    print("Press Ctrl+C to stop...")

    query.awaitTermination()


def run_rate_streaming_demo(spark):
    """
    Demo streaming using Spark's rate source.
    Generates synthetic data for testing the pipeline.
    """
    import random

    # Generate test data using rate source
    rate_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

    # Transform rate data to look like pitch data (for demo)
    pitch_types = ["FF", "SL", "CH", "CU", "SI", "FC"]

    demo_df = rate_df.select(
        lit(717435).alias("game_pk"),
        (col("value") % 50 + 1).alias("at_bat_number"),
        (col("value") % 10 + 1).alias("pitch_number"),
        lit("FF").alias("pitch_type"),
        (90 + (col("value") % 10)).cast("double").alias("release_speed"),
        col("timestamp").alias("processed_at"),
    )

    # Write to console for demo
    query = (
        demo_df.writeStream.outputMode("append")
        .format("console")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Demo streaming started (writing to console)...")
    print("Press Ctrl+C to stop...")

    query.awaitTermination()


def run_kafka_streaming(
    spark, kafka_bootstrap_servers="localhost:9092", topic="statcast"
):
    """
    Run Spark Structured Streaming from Kafka.

    Prerequisites:
    - Kafka running locally (use Docker)
    - Topic created: kafka-topics --create --topic statcast --bootstrap-server localhost:9092
    """
    schema = get_statcast_schema()

    # Read from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON value from Kafka message
    from pyspark.sql.functions import from_json

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Process and write
    query = (
        parsed_df.writeStream.foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime=f"{BATCH_INTERVAL_SECONDS} seconds")
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/kafka")
        .start()
    )

    print(f"Kafka streaming started. Listening to topic: {topic}")
    print("Press Ctrl+C to stop...")

    query.awaitTermination()


def run_polling_streaming(spark, poll_interval_seconds=300):
    """
    Polling-based "streaming" that fetches new data periodically.

    This is a practical approach when you don't have a true streaming source.
    Fetches from pybaseball API at regular intervals.
    """
    from pybaseball import statcast
    import pandas as pd

    print(f"Starting polling-based streaming (interval: {poll_interval_seconds}s)")

    processed_keys = set()  # Track processed pitch UIDs

    while True:
        try:
            # Fetch today's data
            today = datetime.now().strftime("%Y-%m-%d")
            print(f"\n[{datetime.now()}] Fetching data for {today}...")

            pdf = statcast(start_dt=today, end_dt=today)

            if pdf.empty:
                print("No data available. Waiting...")
                time.sleep(poll_interval_seconds)
                continue

            # Create composite key
            pdf["pitch_uid"] = (
                pdf["game_pk"].astype(str)
                + "_"
                + pdf["at_bat_number"].astype(str)
                + "_"
                + pdf["pitch_number"].astype(str)
            )

            # Filter to only new records
            new_keys = set(pdf["pitch_uid"].unique()) - processed_keys
            if not new_keys:
                print(f"No new records. Total tracked: {len(processed_keys)}")
                time.sleep(poll_interval_seconds)
                continue

            new_df = pdf[pdf["pitch_uid"].isin(new_keys)]
            print(f"Found {len(new_df)} new records")

            # Convert to Spark DataFrame and process
            spark_df = spark.createDataFrame(new_df)
            process_batch(spark_df, epoch_id=int(time.time()))

            # Update processed keys
            processed_keys.update(new_keys)
            print(f"Total processed: {len(processed_keys)} pitches")

        except Exception as e:
            print(f"Error in polling loop: {e}")

        time.sleep(poll_interval_seconds)


# =============================================================================
# AGGREGATION QUERIES FOR DASHBOARD
# =============================================================================


def create_live_aggregations(spark):
    """
    Create streaming aggregations for dashboard metrics.
    These can be written to Snowflake or served via API.
    """
    schema = get_statcast_schema()

    # Assuming file-based input for this example
    stream_df = (
        spark.readStream.schema(schema)
        .option("header", "true")
        .csv("./streaming_input")
    )

    # Aggregation 1: Pitcher performance (running averages)
    pitcher_stats = stream_df.groupBy("pitcher", "pitch_type").agg(
        count("*").alias("pitch_count"),
        avg("release_speed").alias("avg_velocity"),
        avg("release_spin_rate").alias("avg_spin_rate"),
        avg("pfx_x").alias("avg_horizontal_break"),
        avg("pfx_z").alias("avg_vertical_break"),
    )

    # Aggregation 2: Game score updates
    game_scores = stream_df.groupBy("game_pk").agg(
        spark_max("home_score").alias("home_score"),
        spark_max("away_score").alias("away_score"),
        spark_max("inning").alias("current_inning"),
        count("*").alias("total_pitches"),
    )

    # Write pitcher stats to console (for demo) or Snowflake
    pitcher_query = (
        pitcher_stats.writeStream.outputMode("complete")
        .format("console")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Write game scores to console
    game_query = (
        game_scores.writeStream.outputMode("complete")
        .format("console")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return [pitcher_query, game_query]


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Statcast Spark Streaming ETL")
    parser.add_argument(
        "--mode",
        choices=["file", "kafka", "poll", "demo"],
        default="demo",
        help="Streaming mode: file (watch directory), kafka, poll (API polling), demo (rate source)",
    )
    parser.add_argument(
        "--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument("--topic", default="statcast", help="Kafka topic name")
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=300,
        help="Polling interval in seconds (for poll mode)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Statcast Spark Structured Streaming")
    print("=" * 60)

    # Create Spark session
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")
    print(f"Mode: {args.mode}")

    try:
        if args.mode == "demo":
            run_rate_streaming_demo(spark)
        elif args.mode == "file":
            run_file_streaming(spark)
        elif args.mode == "kafka":
            run_kafka_streaming(spark, args.kafka_servers, args.topic)
        elif args.mode == "poll":
            run_polling_streaming(spark, args.poll_interval)
    except KeyboardInterrupt:
        print("\nStopping streaming...")
    finally:
        spark.stop()
        print("Spark session stopped.")
