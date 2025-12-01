"""
Kafka Pitch Producer
====================
Reads historical pitch data from Snowflake and publishes to Kafka topic
to simulate a live game feed.

Usage:
    python streaming/kafka_pitch_producer.py --game-pk 745546
    python streaming/kafka_pitch_producer.py --date 2024-07-01

This reads pitch data and sends each pitch to Kafka with realistic timing,
simulating a live baseball game broadcast.
"""

import json
import time
import argparse
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
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


def get_snowflake_connection():
    """Create Snowflake connection using key-pair auth."""
    if SF_PRIVATE_KEY_FILE and os.path.exists(SF_PRIVATE_KEY_FILE):
        with open(SF_PRIVATE_KEY_FILE, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
    else:
        raise ValueError("SF_PRIVATE_KEY_FILE not set or file not found")

    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=private_key,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
    )


def get_game_pitches(conn, game_pk):
    """Fetch all pitches for a game in chronological order."""
    query = f"""
    SELECT 
        "game_pk" as game_pk,
        "game_date" as game_date,
        "at_bat_number" as at_bat_number,
        "pitch_number" as pitch_number,
        "inning" as inning,
        "inning_topbot" as inning_topbot,
        "pitcher" as pitcher,
        "player_name" as pitcher_name,
        "batter" as batter,
        "home_team" as home_team,
        "away_team" as away_team,
        "home_score" as home_score,
        "away_score" as away_score,
        "pitch_type" as pitch_type,
        "pitch_name" as pitch_name,
        "release_speed" as release_speed,
        "release_spin_rate" as release_spin_rate,
        "pfx_x" as pfx_x,
        "pfx_z" as pfx_z,
        "plate_x" as plate_x,
        "plate_z" as plate_z,
        "zone" as zone,
        "type" as type,
        "events" as events,
        "description" as description,
        "balls" as balls,
        "strikes" as strikes,
        "sz_top" as sz_top,
        "sz_bot" as sz_bot
    FROM STATCAST
    WHERE "game_pk" = {game_pk}
    ORDER BY "inning", 
             CASE WHEN "inning_topbot" = 'Top' THEN 0 ELSE 1 END,
             "at_bat_number", 
             "pitch_number"
    """
    cursor = conn.cursor()
    cursor.execute(query)
    # Convert column names to lowercase for consistency
    columns = [desc[0].lower() for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def get_games_for_date(conn, date_str):
    """Get list of games for a given date."""
    query = f"""
    SELECT DISTINCT 
        "game_pk" as GAME_PK,
        "home_team" as HOME_TEAM,
        "away_team" as AWAY_TEAM
    FROM STATCAST
    WHERE "game_date" = '{date_str}'
    ORDER BY "game_pk"
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def create_kafka_producer():
    """Create Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def serialize_pitch(pitch):
    """Convert pitch data to JSON-serializable format."""
    serialized = {}
    for key, value in pitch.items():
        if value is None:
            serialized[key] = None
        elif isinstance(value, (datetime,)):
            serialized[key] = value.isoformat()
        elif hasattr(value, "item"):  # numpy types
            serialized[key] = value.item()
        else:
            serialized[key] = value

    # Add streaming metadata
    serialized["_streaming_timestamp"] = datetime.now().isoformat()
    serialized["_event_type"] = "pitch"

    return serialized


def publish_pitch(producer, pitch, topic=KAFKA_TOPIC):
    """Publish a single pitch to Kafka."""
    key = f"{pitch['game_pk']}_{pitch['at_bat_number']}_{pitch['pitch_number']}"
    serialized = serialize_pitch(pitch)

    future = producer.send(topic, key=key, value=serialized)
    try:
        record_metadata = future.get(timeout=10)
        return True
    except KafkaError as e:
        print(f"Failed to send pitch: {e}")
        return False


def simulate_game(producer, pitches, speed_multiplier=10.0, verbose=True):
    """
    Simulate a live game by publishing pitches with realistic timing.

    Args:
        producer: Kafka producer
        pitches: List of pitch dictionaries
        speed_multiplier: How fast to replay (10.0 = 10x faster than real-time)
        verbose: Print progress messages
    """
    if not pitches:
        print("No pitches to simulate!")
        return

    # Average time between pitches in MLB is ~20 seconds
    # With speed_multiplier=10, we send every 2 seconds
    base_delay = 20.0 / speed_multiplier

    game_pk = pitches[0]["game_pk"]
    home_team = pitches[0]["home_team"]
    away_team = pitches[0]["away_team"]

    print(f"\n{'='*60}")
    print(f"🎬 SIMULATING GAME: {away_team} @ {home_team}")
    print(f"   Game PK: {game_pk}")
    print(f"   Total Pitches: {len(pitches)}")
    print(f"   Speed: {speed_multiplier}x real-time")
    print(f"   Estimated duration: {len(pitches) * base_delay / 60:.1f} minutes")
    print(f"{'='*60}\n")

    current_inning = None
    current_topbot = None
    pitch_count = 0

    for pitch in pitches:
        # Check for inning change
        if (
            pitch["inning"] != current_inning
            or pitch["inning_topbot"] != current_topbot
        ):
            current_inning = pitch["inning"]
            current_topbot = pitch["inning_topbot"]
            half = "Top" if current_topbot == "Top" else "Bottom"
            print(f"\n⚾ {half} of Inning {current_inning}")
            print("-" * 40)

        # Publish to Kafka
        success = publish_pitch(producer, pitch)
        pitch_count += 1

        if verbose and success:
            # Format pitch info
            pitcher = pitch["pitcher_name"] or f"Pitcher {pitch['pitcher']}"
            pitch_type = pitch["pitch_name"] or pitch["pitch_type"] or "Unknown"
            velocity = pitch["release_speed"]
            result = pitch["description"] or ""

            velo_str = f"{velocity:.1f} mph" if velocity else "-- mph"
            print(
                f"  [{pitch_count:3d}] {pitcher[:20]:<20} | {pitch_type:<15} | {velo_str:<10} | {result[:30]}"
            )

        # Delay between pitches (with some randomness)
        import random

        delay = base_delay * random.uniform(0.5, 1.5)

        # Longer delay between at-bats
        if pitch_count < len(pitches) - 1:
            next_pitch = pitches[pitch_count]
            if next_pitch["at_bat_number"] != pitch["at_bat_number"]:
                delay *= 2  # Double delay for new at-bat

        time.sleep(delay)

    print(f"\n{'='*60}")
    print(f"✅ GAME COMPLETE!")
    print(f"   Total pitches published: {pitch_count}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Simulate live MLB game via Kafka")
    parser.add_argument("--game-pk", type=int, help="Specific game PK to replay")
    parser.add_argument("--date", type=str, help="Date to list games (YYYY-MM-DD)")
    parser.add_argument(
        "--speed",
        type=float,
        default=10.0,
        help="Speed multiplier (default: 10x real-time)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=KAFKA_TOPIC,
        help=f"Kafka topic (default: {KAFKA_TOPIC})",
    )
    parser.add_argument("--quiet", action="store_true", help="Reduce output verbosity")

    args = parser.parse_args()

    print("🔌 Connecting to Snowflake...")
    conn = get_snowflake_connection()
    print("✅ Connected to Snowflake")

    # If date provided, list games and let user choose
    if args.date and not args.game_pk:
        games = get_games_for_date(conn, args.date)
        if not games:
            print(f"No games found for {args.date}")
            return

        print(f"\nGames on {args.date}:")
        for i, (game_pk, home, away) in enumerate(games, 1):
            print(f"  {i}. {away} @ {home} (game_pk: {game_pk})")

        choice = input("\nEnter game number (or 'all' for all games): ").strip()
        if choice.lower() == "all":
            game_pks = [g[0] for g in games]
        else:
            try:
                game_pks = [games[int(choice) - 1][0]]
            except (ValueError, IndexError):
                print("Invalid selection")
                return
    elif args.game_pk:
        game_pks = [args.game_pk]
    else:
        # Default: get most recent game
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT "game_pk" 
            FROM STATCAST 
            ORDER BY "game_date" DESC, "game_pk" DESC 
            LIMIT 1
        """
        )
        result = cursor.fetchone()
        if result:
            game_pks = [result[0]]
            print(f"Using most recent game: {game_pks[0]}")
        else:
            print("No games found in database")
            return

    print("🔌 Connecting to Kafka...")
    try:
        producer = create_kafka_producer()
        print(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"   Topic: {args.topic}")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        print("\nMake sure Kafka is running:")
        print("  cd streaming && docker-compose up -d")
        return

    # Simulate each game
    for game_pk in game_pks:
        print(f"\n📥 Fetching pitches for game {game_pk}...")
        pitches = get_game_pitches(conn, game_pk)
        print(f"   Found {len(pitches)} pitches")

        if pitches:
            simulate_game(
                producer, pitches, speed_multiplier=args.speed, verbose=not args.quiet
            )

    producer.close()
    conn.close()
    print("\n👋 Producer shutdown complete")


if __name__ == "__main__":
    main()
