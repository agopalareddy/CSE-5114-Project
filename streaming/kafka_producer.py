"""
Kafka Producer for Statcast Data
=================================
Fetches MLB Statcast data and publishes to Kafka topic.

This acts as the data source for the Spark Streaming consumer.

Prerequisites:
- Kafka running (see docker-compose.yml)
- pip install kafka-python pybaseball

Usage:
    python kafka_producer.py --interval 60
"""

import json
import time
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import warnings

# Suppress pybaseball warnings
warnings.filterwarnings("ignore", category=FutureWarning)


class StatcastKafkaProducer:
    """
    Produces Statcast pitch data to Kafka.
    """

    def __init__(self, bootstrap_servers="localhost:9092", topic="statcast"):
        self.topic = topic
        self.producer = None
        self.bootstrap_servers = bootstrap_servers
        self.processed_keys = set()  # Track sent pitches to avoid duplicates

    def connect(self):
        """Connect to Kafka broker."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"Connected to Kafka at {self.bootstrap_servers}")
            return True
        except NoBrokersAvailable:
            print(f"ERROR: Cannot connect to Kafka at {self.bootstrap_servers}")
            print("Make sure Kafka is running (see docker-compose.yml)")
            return False

    def fetch_and_publish(self, date_str=None):
        """
        Fetch Statcast data and publish new records to Kafka.
        """
        from pybaseball import statcast
        import pandas as pd

        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        print(f"[{datetime.now()}] Fetching Statcast data for {date_str}...")

        try:
            df = statcast(start_dt=date_str, end_dt=date_str)

            if df.empty:
                print("No data available")
                return 0

            # Create composite key
            df["pitch_uid"] = (
                df["game_pk"].astype(str)
                + "_"
                + df["at_bat_number"].astype(str)
                + "_"
                + df["pitch_number"].astype(str)
            )

            # Filter to only new records
            new_records = df[~df["pitch_uid"].isin(self.processed_keys)]

            if new_records.empty:
                print(f"No new records (total tracked: {len(self.processed_keys)})")
                return 0

            # Publish each record to Kafka
            count = 0
            for _, row in new_records.iterrows():
                pitch_uid = row["pitch_uid"]

                # Convert row to dict, handling NaN values
                record = row.to_dict()
                record = {k: (None if pd.isna(v) else v) for k, v in record.items()}

                # Add metadata
                record["_published_at"] = datetime.now().isoformat()

                # Publish to Kafka with pitch_uid as key
                self.producer.send(self.topic, key=pitch_uid, value=record)

                self.processed_keys.add(pitch_uid)
                count += 1

            self.producer.flush()
            print(f"Published {count} new records to topic '{self.topic}'")
            return count

        except Exception as e:
            print(f"Error fetching/publishing data: {e}")
            return 0

    def run_continuous(self, interval_seconds=60):
        """
        Continuously fetch and publish data.
        """
        print(f"Starting continuous producer (interval: {interval_seconds}s)")
        print("Press Ctrl+C to stop...")

        while True:
            self.fetch_and_publish()
            time.sleep(interval_seconds)

    def close(self):
        """Close Kafka producer."""
        if self.producer:
            self.producer.close()
            print("Kafka producer closed")


def main():
    parser = argparse.ArgumentParser(description="Statcast Kafka Producer")
    parser.add_argument(
        "--servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument("--topic", default="statcast", help="Kafka topic name")
    parser.add_argument(
        "--interval", type=int, default=60, help="Fetch interval in seconds"
    )
    parser.add_argument(
        "--date", default=None, help="Specific date to fetch (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch once and exit (don't run continuously)",
    )

    args = parser.parse_args()

    producer = StatcastKafkaProducer(bootstrap_servers=args.servers, topic=args.topic)

    if not producer.connect():
        return 1

    try:
        if args.once:
            producer.fetch_and_publish(args.date)
        else:
            producer.run_continuous(args.interval)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

    return 0


if __name__ == "__main__":
    exit(main())
