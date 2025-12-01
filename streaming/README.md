# MLB Statcast Real-Time Streaming Pipeline

This module implements a **real-time streaming pipeline** that simulates live MLB pitch data using historical Statcast data from Snowflake, streams it through Apache Kafka, and processes it with Spark Structured Streaming.

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌────────────┐
│   Snowflake     │────▶│   Kafka     │────▶│  Spark Streaming │────▶│ Snowflake  │
│ (Historical DB) │     │  Producer   │     │    Consumer      │     │ (Live DB)  │
└─────────────────┘     └─────────────┘     └──────────────────┘     └────────────┘
                              │
                              ▼
                        ┌───────────┐
                        │ Kafka UI  │
                        │ :8080     │
                        └───────────┘
```

### Components

| Component | Description | Port |
|-----------|-------------|------|
| **Kafka Producer** | Reads historical pitches from Snowflake, publishes to Kafka with realistic timing | - |
| **Kafka Broker** | Message broker for pitch events | 9092 |
| **Kafka UI** | Web interface for monitoring topics | 8080 |
| **Spark Consumer** | Processes pitch streams, writes to console or Snowflake | 4040 (UI) |
| **Zookeeper** | Kafka coordination service | 2181 |

---

## Quick Start

### Prerequisites

- **Docker Desktop** - [Download](https://www.docker.com/products/docker-desktop/)
- **Java 17** - Required for Spark
- **Apache Spark 3.5.x** - [Download](https://spark.apache.org/downloads.html)
- **Python 3.10+** with virtual environment

### Step 1: Start Kafka Infrastructure

```powershell
cd streaming
docker-compose up -d
```

Verify containers are running:
```powershell
docker ps
# Should show: zookeeper, kafka, kafka-ui
```

### Step 2: Start the Spark Consumer

```powershell
# Set Python environment for PySpark
$env:PYSPARK_PYTHON = ".\.venv\Scripts\python.exe"

# Start Spark Structured Streaming consumer
spark-submit `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    --master "local[2]" `
    streaming\spark_kafka_consumer.py --sink console
```

### Step 3: Start the Kafka Producer

In a **new terminal**:
```powershell
.\.venv\Scripts\python.exe streaming\kafka_pitch_producer.py --speed 100
```

### Step 4: Watch the Magic!

- **Producer terminal**: Shows pitch-by-pitch simulation
- **Consumer terminal**: Shows Spark processing micro-batches
- **Kafka UI**: http://localhost:8080 - Monitor topics and messages
- **Spark UI**: http://localhost:4040 - Monitor streaming jobs

---

## Kafka Producer (`kafka_pitch_producer.py`)

Simulates a live game by reading historical pitch data from Snowflake and publishing to Kafka with realistic timing.

### Usage

```powershell
# Simulate most recent game at 100x speed
python streaming/kafka_pitch_producer.py --speed 100

# Simulate a specific game
python streaming/kafka_pitch_producer.py --game-pk 775296 --speed 50

# Simulate games from a specific date
python streaming/kafka_pitch_producer.py --date 2024-10-30

# Real-time simulation (1x speed)
python streaming/kafka_pitch_producer.py --speed 1
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--game-pk` | Specific game ID to simulate | Most recent |
| `--date` | Date to fetch games from (YYYY-MM-DD) | - |
| `--speed` | Playback speed multiplier (1=real-time) | 10 |
| `--quiet` | Suppress pitch-by-pitch output | False |

### Output Fields

Each pitch event published to Kafka contains:

```json
{
    "game_pk": 775296,
    "game_date": "2024-10-30",
    "inning": 5,
    "inning_topbot": "Top",
    "pitcher": 543037,
    "pitcher_name": "Cole, Gerrit",
    "batter": 665742,
    "pitch_type": "FF",
    "pitch_name": "4-Seam Fastball",
    "release_speed": 98.6,
    "release_spin_rate": 2412,
    "description": "swinging_strike",
    "_streaming_timestamp": "2024-11-30T22:54:25.123Z",
    "_event_type": "pitch"
}
```

---

## Spark Consumer (`spark_kafka_consumer.py`)

Spark Structured Streaming application that consumes pitch events from Kafka and processes them in micro-batches.

### Usage

```powershell
# Console output (for testing)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    streaming\spark_kafka_consumer.py --sink console

# Write to Snowflake
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    streaming\spark_kafka_consumer.py --sink snowflake
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--sink` | Output destination: `console` or `snowflake` | console |
| `--checkpoint` | Checkpoint directory for fault tolerance | ./streaming_checkpoint |

### Console Output

When using `--sink console`, you'll see formatted tables:

```
============================================================
Batch 7: 20 pitches
============================================================
+-------+------+-------------+---------------+---------------+-------------+---------------+
|game_pk|inning|inning_topbot|pitcher_name   |pitch_name     |release_speed|description    |
+-------+------+-------------+---------------+---------------+-------------+---------------+
|775296 |4     |Bot          |Kopech, Michael|4-Seam Fastball|97.8         |ball           |
|775296 |4     |Bot          |Kopech, Michael|Cutter         |91.1         |ball           |
|775296 |4     |Bot          |Kopech, Michael|4-Seam Fastball|100.4        |ball           |
+-------+------+-------------+---------------+---------------+-------------+---------------+
```

---

## Docker Services

### Start Services
```powershell
cd streaming
docker-compose up -d
```

### Stop Services
```powershell
docker-compose down
```

### View Logs
```powershell
docker-compose logs -f kafka
```

### Kafka Topic Management

```powershell
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create topic manually
docker exec kafka kafka-topics --bootstrap-server localhost:9092 `
    --create --topic statcast-live --partitions 1 --replication-factor 1

# Delete topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 `
    --delete --topic statcast-live

# Describe topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 `
    --describe --topic statcast-live
```

### Test Kafka Connectivity

```powershell
python streaming/test_kafka.py
```

---

## Environment Variables

Add to your `.env` file:

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=statcast-live

# Snowflake Configuration (for producer)
SF_ACCOUNT=your_account
SF_USER=your_user
SF_DATABASE=BLUEJAY_DB
SF_SCHEMA=BLUEJAY_SCHEMA
SF_WAREHOUSE=COMPUTE_WH
SF_PRIVATE_KEY_FILE=rsa_key.p8
```

---

## Snowflake Integration

### Create Live Pitches Table

```sql
-- Table for streaming pitch data
CREATE OR REPLACE TABLE LIVE_PITCHES (
    game_pk             INTEGER,
    game_date           DATE,
    at_bat_number       INTEGER,
    pitch_number        INTEGER,
    inning              INTEGER,
    inning_topbot       VARCHAR(10),
    pitcher             INTEGER,
    pitcher_name        VARCHAR(100),
    batter              INTEGER,
    pitch_type          VARCHAR(10),
    pitch_name          VARCHAR(50),
    release_speed       FLOAT,
    release_spin_rate   FLOAT,
    description         VARCHAR(100),
    streaming_timestamp TIMESTAMP_NTZ,
    processed_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Real-time aggregation view
CREATE OR REPLACE VIEW LIVE_PITCHER_SUMMARY AS
SELECT 
    pitcher_name,
    pitch_name,
    COUNT(*) as pitch_count,
    ROUND(AVG(release_speed), 1) as avg_velocity,
    ROUND(AVG(release_spin_rate), 0) as avg_spin_rate
FROM LIVE_PITCHES
WHERE streaming_timestamp > DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY pitcher_name, pitch_name
ORDER BY pitcher_name, pitch_count DESC;
```

---

## Troubleshooting

### Kafka Connection Issues

```powershell
# Check if Kafka is running
docker ps | Select-String kafka

# Check Kafka logs
docker-compose logs kafka

# Test connectivity
python streaming/test_kafka.py
```

### Spark Issues

```powershell
# Verify Java version (need 17+)
java -version

# Verify Spark installation
spark-submit --version

# Check SPARK_HOME
echo $env:SPARK_HOME
```

### Common Errors

| Error | Solution |
|-------|----------|
| `UnknownTopicOrPartitionException` | Start the producer first to create the topic, then start consumer |
| `UnicodeEncodeError` on Windows | Fixed in latest code - removed emoji characters |
| `NullPointerException` in BlockManager | Add `--master "local[2]"` to spark-submit |
| `game_pk` shows NULL | Type mismatch - working on fix |

### Spark UI
- Access at http://localhost:4040 while Spark is running
- Shows streaming jobs, stages, and micro-batch progress

### Kafka UI
- Access at http://localhost:8080
- View topics, partitions, messages, and consumer groups

---

## File Structure

```
streaming/
├── docker-compose.yml          # Kafka + Zookeeper + UI setup
├── kafka_pitch_producer.py     # Snowflake → Kafka producer
├── spark_kafka_consumer.py     # Kafka → Spark → Snowflake consumer
├── test_kafka.py               # Kafka connectivity test
├── requirements-streaming.txt  # Python dependencies
├── README.md                   # This documentation
│
├── kafka_producer.py           # (Legacy) Basic Kafka producer
├── spark_streaming_simple.py   # (Legacy) Simple demo without Kafka
└── spark_streaming_etl.py      # (Legacy) File-based ETL
```

---

## Performance Tuning

### Spark Configuration

```python
SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoint")
```

### Kafka Producer

```python
KafkaProducer(
    bootstrap_servers="localhost:9092",
    batch_size=16384,
    linger_ms=10,
    compression_type="gzip"
)
```

---

## Example Session

Complete walkthrough of running the streaming pipeline:

```powershell
# Terminal 1: Start Kafka
cd "c:\Users\adurs\OneDrive\Documents\repos\WashU\CSE 5114 Project\streaming"
docker-compose up -d

# Terminal 2: Start Spark Consumer (wait for "STREAMING JOB RUNNING")
cd "c:\Users\adurs\OneDrive\Documents\repos\WashU\CSE 5114 Project"
$env:PYSPARK_PYTHON = ".\.venv\Scripts\python.exe"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --master "local[2]" streaming\spark_kafka_consumer.py --sink console

# Terminal 3: Start Producer (watch pitches fly!)
cd "c:\Users\adurs\OneDrive\Documents\repos\WashU\CSE 5114 Project"
.\.venv\Scripts\python.exe streaming\kafka_pitch_producer.py --speed 100

# Open browser to http://localhost:8080 to see Kafka UI
# Open browser to http://localhost:4040 to see Spark UI
```

---

## Demo Game Data

The producer defaults to simulating the most recent game in Snowflake. For demonstration:

**2024 World Series Game 5: LAD @ NYY**
- Game PK: 775296
- Total Pitches: 342
- Pitchers: Cole, Flaherty, Treinen, Buehler, and more
- Duration at 100x: ~1.1 minutes

---

## Dashboard Live Simulation (Kafka-Powered)

The Streamlit dashboard includes a **Live Simulation** page that can be powered by Kafka streaming, providing a more realistic game replay experience.

### Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Dashboard  │────▶│  FastAPI    │────▶│     Kafka       │────▶│    Spark     │
│ (Streamlit) │     │ (Control)   │     │   (game_sim)    │     │  Consumer    │
└─────────────┘     └─────────────┘     └─────────────────┘     └──────────────┘
       │                                                               │
       │                                                               ▼
       └─────────────────────────── Polls ──────────────────────▶ Snowflake
                                                               (SIMULATION_PITCHES)
```

### Components

| Component | File | Description |
|-----------|------|-------------|
| **Simulation Producer** | `simulation_producer.py` | FastAPI server + Kafka producer |
| **Simulation Consumer** | `simulation_consumer.py` | Spark consumer for simulation topic |
| **Dashboard Helper** | `dashboard/kafka_simulation.py` | API client for dashboard |
| **Snowflake Table** | `sql/simulation_table_ddl.sql` | DDL for SIMULATION_PITCHES table |

### Quick Start

```powershell
# Terminal 1: Start Kafka (if not already running)
cd streaming
docker-compose up -d

# Terminal 2: Start the Simulation API Server
cd "c:\Users\adurs\OneDrive\Documents\repos\WashU\CSE 5114 Project"
.\.venv\Scripts\python.exe -m uvicorn streaming.simulation_producer:app --host 0.0.0.0 --port 8000

# Terminal 3: Start Spark Simulation Consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    streaming\simulation_consumer.py --mode console

# Terminal 4: Start Dashboard
streamlit run dashboard/app.py
```

### API Endpoints

The FastAPI simulation server provides these endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/status` | GET | Get current simulation status |
| `/start` | POST | Start simulation for a game |
| `/pause` | POST | Pause current simulation |
| `/resume` | POST | Resume paused simulation |
| `/stop` | POST | Stop current simulation |
| `/speed` | POST | Update playback speed |
| `/games/{date}` | GET | Get available games for a date |

### Example API Usage

```python
import requests

# Start a simulation
response = requests.post("http://localhost:8000/start", json={
    "game_pk": 775296,
    "speed": 2.0  # 2x speed
})

# Check status
status = requests.get("http://localhost:8000/status").json()
print(f"Progress: {status['progress_pct']:.1f}%")

# Pause
requests.post("http://localhost:8000/pause")

# Change speed
requests.post("http://localhost:8000/speed", json={"speed": 5.0})
```

### Dashboard Modes

The Live Simulation page operates in two modes:

1. **Kafka Mode** (🔴): When API is available
   - Pitches stream through Kafka → Spark → Snowflake
   - Dashboard polls SIMULATION_PITCHES table
   - Real-time progress shown in sidebar
   
2. **Local Mode** (💻): Fallback when API unavailable
   - Uses Streamlit session state
   - Direct playback from in-memory data
   - Faster for testing

### Create Simulation Table

Before using Kafka mode, create the simulation table in Snowflake:

```sql
-- Run this in Snowflake
SOURCE 'sql/simulation_table_ddl.sql';
```
