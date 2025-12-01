"""
Game Simulation Kafka Producer
==============================
Replays historical game data pitch-by-pitch to Kafka for live simulation.

This producer reads pitches from Snowflake for a specific game and publishes
them to Kafka at a configurable speed, simulating a live game experience.

Prerequisites:
- Kafka running (see docker-compose.yml)
- Snowflake connection configured
- pip install kafka-python fastapi uvicorn

Usage:
    # Start simulation server (FastAPI)
    python -m uvicorn streaming.simulation_producer:app --port 8000

    # Or run directly for a specific game
    python streaming/simulation_producer.py --game-pk 747066 --speed 1.0
"""

import json
import time
import os
import sys
import threading
import argparse
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file in project root
env_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
)
load_dotenv(env_path)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Import the project's Snowflake connection
try:
    from snowflake_connect import setup_snowflake_connection

    USE_PROJECT_SNOWFLAKE = True
except ImportError:
    USE_PROJECT_SNOWFLAKE = False
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

# FastAPI imports for API server mode
try:
    from fastapi import FastAPI, HTTPException, BackgroundTasks
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False


class SimulationState:
    """Thread-safe simulation state manager."""

    def __init__(self):
        self.is_running = False
        self.is_paused = False
        self.current_game_pk: Optional[int] = None
        self.current_pitch_idx: int = 0
        self.total_pitches: int = 0
        self.speed: float = 1.0
        self.pitches: list = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

    def reset(self):
        with self.lock:
            self.is_running = False
            self.is_paused = False
            self.current_pitch_idx = 0
            self.stop_event.set()

    def get_status(self) -> dict:
        with self.lock:
            return {
                "is_running": self.is_running,
                "is_paused": self.is_paused,
                "game_pk": self.current_game_pk,
                "current_pitch": self.current_pitch_idx,
                "total_pitches": self.total_pitches,
                "speed": self.speed,
                "progress_pct": (
                    (self.current_pitch_idx / self.total_pitches * 100)
                    if self.total_pitches > 0
                    else 0
                ),
            }


# Global simulation state
simulation_state = SimulationState()


def get_snowflake_connection():
    """Create Snowflake connection using the project's standard method."""
    if USE_PROJECT_SNOWFLAKE:
        return setup_snowflake_connection()

    # Fallback: create connection manually
    private_key_path = os.getenv("SF_PRIVATE_KEY_FILE", "rsa_key.p8")

    if not os.path.isabs(private_key_path):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        private_key_path = os.path.join(base_dir, private_key_path)

    if not os.path.exists(private_key_path):
        raise FileNotFoundError(f"Private key not found: {private_key_path}")

    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(), password=None, backend=default_backend()
        )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        account=os.getenv("SF_ACCOUNT"),
        private_key=private_key_bytes,
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE"),
        schema=os.getenv("SF_SCHEMA"),
    )


def fetch_game_pitches(game_pk: int) -> list:
    """
    Fetch all pitches for a game from Snowflake, ordered chronologically.

    Note: STATCAST table has lowercase column names, so we need to quote them.
    We convert to uppercase keys when returning so the rest of the system
    (Kafka, Spark, SIMULATION_PITCHES) uses consistent uppercase.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        # STATCAST has lowercase columns - must quote them
        query = f"""
        SELECT 
            "game_pk",
            "game_date",
            "at_bat_number",
            "pitch_number",
            "inning",
            "inning_topbot",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "batter",
            "pitcher",
            "player_name",
            "pitch_type",
            "pitch_name",
            "release_speed",
            "release_spin_rate",
            "pfx_x",
            "pfx_z",
            "plate_x",
            "plate_z",
            "zone",
            "sz_top",
            "sz_bot",
            "balls",
            "strikes",
            "outs_when_up",
            "type",
            "events",
            "description",
            "on_1b",
            "on_2b",
            "on_3b"
        FROM STATCAST
        WHERE "game_pk" = {game_pk}
        ORDER BY "at_bat_number" ASC, "pitch_number" ASC
        """
        cursor.execute(query)

        # Get column names from cursor (will be lowercase)
        columns = [desc[0] for desc in cursor.description]
        pitches = []

        for row in cursor.fetchall():
            # Create dict with UPPERCASE keys for consistency with SIMULATION_PITCHES
            pitch = {}
            for col, val in zip(columns, row):
                # Convert column name to uppercase
                upper_col = col.upper()
                # Convert datetime objects to ISO format strings
                if hasattr(val, "isoformat"):
                    pitch[upper_col] = val.isoformat()
                else:
                    pitch[upper_col] = val
            pitches.append(pitch)

        print(f"Fetched {len(pitches)} pitches for game {game_pk}")
        return pitches

    finally:
        cursor.close()
        conn.close()


class GameSimulationProducer:
    """Kafka producer for game simulation."""

    def __init__(
        self, bootstrap_servers: str = "localhost:9092", topic: str = "game_simulation"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: Optional[KafkaProducer] = None

    def connect(self) -> bool:
        """Connect to Kafka broker."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"✓ Connected to Kafka at {self.bootstrap_servers}")
            return True
        except NoBrokersAvailable:
            print(f"✗ Cannot connect to Kafka at {self.bootstrap_servers}")
            return False

    def publish_pitch(self, pitch: dict, game_pk: int) -> bool:
        """Publish a single pitch to Kafka."""
        if not self.producer:
            return False

        try:
            # Create unique key for the pitch (uppercase keys)
            pitch_key = f"{game_pk}_{pitch.get('AT_BAT_NUMBER', 0)}_{pitch.get('PITCH_NUMBER', 0)}"

            # Add simulation metadata
            pitch_data = pitch.copy()
            pitch_data["_simulation_timestamp"] = datetime.now().isoformat()
            pitch_data["_is_simulation"] = True

            self.producer.send(self.topic, key=pitch_key, value=pitch_data)
            self.producer.flush()
            return True

        except Exception as e:
            print(f"Error publishing pitch: {e}")
            return False

    def publish_control_message(self, action: str, game_pk: int, metadata: dict = None):
        """Publish a control message (start, pause, stop, reset)."""
        if not self.producer:
            return False

        message = {
            "_control_message": True,
            "_action": action,
            "_game_pk": game_pk,
            "_timestamp": datetime.now().isoformat(),
            **(metadata or {}),
        }

        self.producer.send(self.topic, key=f"control_{game_pk}", value=message)
        self.producer.flush()
        return True

    def close(self):
        """Close Kafka producer."""
        if self.producer:
            self.producer.close()


def run_simulation(producer: GameSimulationProducer, game_pk: int, speed: float = 1.0):
    """
    Run the game simulation, publishing pitches at the specified speed.

    Args:
        producer: Kafka producer instance
        game_pk: Game ID to simulate
        speed: Playback speed (1.0 = ~3 seconds between pitches)
    """
    global simulation_state

    # Fetch pitches if not already loaded
    if simulation_state.current_game_pk != game_pk or not simulation_state.pitches:
        print(f"Fetching pitches for game {game_pk}...")
        try:
            simulation_state.pitches = fetch_game_pitches(game_pk)
        except Exception as e:
            print(f"Error fetching pitches: {e}")
            simulation_state.is_running = False
            return

        simulation_state.current_game_pk = game_pk
        simulation_state.total_pitches = len(simulation_state.pitches)
        simulation_state.current_pitch_idx = 0
        print(f"Loaded {simulation_state.total_pitches} pitches")

    if not simulation_state.pitches:
        print("No pitches found for this game")
        simulation_state.is_running = False
        return

    # Send start control message
    producer.publish_control_message(
        "start",
        game_pk,
        {"total_pitches": simulation_state.total_pitches, "speed": speed},
    )

    simulation_state.is_running = True
    simulation_state.is_paused = False
    simulation_state.speed = speed
    simulation_state.stop_event.clear()

    # Speed is now pitches per second
    # delay = 1.0 / speed (e.g., 2 pitches/sec = 0.5s delay)
    print(f"Starting simulation at {speed} pitches/second...")

    while simulation_state.current_pitch_idx < simulation_state.total_pitches:
        # Check for stop signal
        if simulation_state.stop_event.is_set():
            print("Simulation stopped")
            break

        # Check for pause
        while simulation_state.is_paused and not simulation_state.stop_event.is_set():
            time.sleep(0.1)

        if simulation_state.stop_event.is_set():
            break

        # Get current pitch
        pitch = simulation_state.pitches[simulation_state.current_pitch_idx]

        # Publish pitch
        if producer.publish_pitch(pitch, game_pk):
            simulation_state.current_pitch_idx += 1

            # Log progress every 10 pitches
            if simulation_state.current_pitch_idx % 10 == 0:
                print(
                    f"Progress: {simulation_state.current_pitch_idx}/{simulation_state.total_pitches}"
                )

        # Wait based on speed (pitches per second)
        # delay = 1 / pitches_per_second
        actual_delay = 1.0 / simulation_state.speed
        time.sleep(actual_delay)

    # Send completion message
    if simulation_state.current_pitch_idx >= simulation_state.total_pitches:
        producer.publish_control_message("complete", game_pk)
        print("Simulation complete!")
    else:
        producer.publish_control_message("stopped", game_pk)

    simulation_state.is_running = False


# ============================================================================
# FastAPI Server for controlling simulation from dashboard
# ============================================================================

if FASTAPI_AVAILABLE:
    app = FastAPI(title="Game Simulation API", version="1.0.0")

    # Enable CORS for dashboard access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Global producer instance
    kafka_producer: Optional[GameSimulationProducer] = None
    simulation_thread: Optional[threading.Thread] = None

    class SimulationRequest(BaseModel):
        game_pk: int
        speed: float = 1.0

    class SpeedUpdate(BaseModel):
        speed: float

    @app.on_event("startup")
    async def startup():
        global kafka_producer
        kafka_producer = GameSimulationProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_SIMULATION_TOPIC", "game_simulation"),
        )
        if not kafka_producer.connect():
            print("Warning: Could not connect to Kafka")

    @app.on_event("shutdown")
    async def shutdown():
        global kafka_producer
        simulation_state.reset()
        if kafka_producer:
            kafka_producer.close()

    @app.get("/status")
    async def get_status():
        """Get current simulation status."""
        return simulation_state.get_status()

    @app.post("/start")
    async def start_simulation(
        request: SimulationRequest, background_tasks: BackgroundTasks
    ):
        """Start a new game simulation."""
        global simulation_thread

        if simulation_state.is_running:
            raise HTTPException(status_code=400, detail="Simulation already running")

        if not kafka_producer:
            raise HTTPException(status_code=500, detail="Kafka not connected")

        # Start simulation in background thread
        simulation_thread = threading.Thread(
            target=run_simulation, args=(kafka_producer, request.game_pk, request.speed)
        )
        simulation_thread.start()

        return {"status": "started", "game_pk": request.game_pk, "speed": request.speed}

    @app.post("/pause")
    async def pause_simulation():
        """Pause the current simulation."""
        if not simulation_state.is_running:
            raise HTTPException(status_code=400, detail="No simulation running")

        simulation_state.is_paused = True
        if kafka_producer:
            kafka_producer.publish_control_message(
                "pause", simulation_state.current_game_pk
            )

        return {"status": "paused"}

    @app.post("/resume")
    async def resume_simulation():
        """Resume a paused simulation."""
        if not simulation_state.is_running:
            raise HTTPException(status_code=400, detail="No simulation running")

        simulation_state.is_paused = False
        if kafka_producer:
            kafka_producer.publish_control_message(
                "resume", simulation_state.current_game_pk
            )

        return {"status": "resumed"}

    @app.post("/stop")
    async def stop_simulation():
        """Stop the current simulation."""
        simulation_state.reset()
        return {"status": "stopped"}

    @app.post("/speed")
    async def update_speed(update: SpeedUpdate):
        """Update simulation speed."""
        if update.speed <= 0:
            raise HTTPException(status_code=400, detail="Speed must be positive")

        simulation_state.speed = update.speed
        return {"status": "speed_updated", "speed": update.speed}

    @app.post("/jump/{pitch_index}")
    async def jump_to_pitch(pitch_index: int):
        """Jump to a specific pitch index."""
        if not simulation_state.is_running:
            raise HTTPException(status_code=400, detail="No simulation running")

        if pitch_index < 0 or pitch_index >= simulation_state.total_pitches:
            raise HTTPException(status_code=400, detail="Invalid pitch index")

        simulation_state.current_pitch_idx = pitch_index
        return {"status": "jumped", "pitch_index": pitch_index}

    @app.get("/games/{date}")
    async def get_games_for_date(date: str):
        """Get available games for a date."""
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        try:
            # STATCAST has lowercase columns
            query = f"""
            SELECT DISTINCT 
                "game_pk",
                "home_team",
                "away_team",
                COUNT(*) as PITCH_COUNT
            FROM STATCAST
            WHERE "game_date" = '{date}'
            GROUP BY "game_pk", "home_team", "away_team"
            ORDER BY "game_pk"
            """
            cursor.execute(query)

            games = []
            for row in cursor.fetchall():
                games.append(
                    {
                        "game_pk": row[0],
                        "home_team": row[1],
                        "away_team": row[2],
                        "pitch_count": row[3],
                        "label": f"{row[2]} @ {row[1]} ({row[3]} pitches)",
                    }
                )

            return {"date": date, "games": games}

        finally:
            cursor.close()
            conn.close()


# ============================================================================
# CLI Mode
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="Game Simulation Kafka Producer")
    parser.add_argument(
        "--game-pk", type=int, required=True, help="Game PK to simulate"
    )
    parser.add_argument(
        "--speed", type=float, default=1.0, help="Playback speed (1.0 = real-time)"
    )
    parser.add_argument(
        "--servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic", default="game_simulation", help="Kafka topic for simulation"
    )

    args = parser.parse_args()

    producer = GameSimulationProducer(bootstrap_servers=args.servers, topic=args.topic)

    if not producer.connect():
        return 1

    try:
        run_simulation(producer, args.game_pk, args.speed)
    except KeyboardInterrupt:
        print("\nSimulation interrupted")
        simulation_state.reset()
    finally:
        producer.close()

    return 0


if __name__ == "__main__":
    exit(main())
