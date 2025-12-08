"""
Create LIVE_PITCHES table in Snowflake for streaming data.
"""

import snowflake.connector
from dotenv import load_dotenv
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

load_dotenv()

# Read private key
with open(os.getenv("SF_PRIVATE_KEY_FILE"), "rb") as f:
    private_key = serialization.load_pem_private_key(
        f.read(), password=None, backend=default_backend()
    )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

db = os.getenv("SF_DATABASE", "BLUEJAY_DB")
schema = os.getenv("SF_SCHEMA", "BLUEJAY_SCHEMA")

conn = snowflake.connector.connect(
    account=os.getenv("SF_ACCOUNT"),
    user=os.getenv("SF_USER"),
    private_key=private_key_bytes,
    database=db,
    schema=schema,
    warehouse=os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
)

cursor = conn.cursor()
print(f"Connected to Snowflake: {db}.{schema}")

# Check if table exists
cursor.execute("SHOW TABLES LIKE 'LIVE_PITCHES'")
existing = cursor.fetchall()

if existing:
    print("Table LIVE_PITCHES already exists")
else:
    print("Creating LIVE_PITCHES table...")

    create_sql = """
    CREATE TABLE LIVE_PITCHES (
        GAME_PK INTEGER,
        GAME_DATE VARCHAR(20),
        AT_BAT_NUMBER INTEGER,
        PITCH_NUMBER INTEGER,
        INNING INTEGER,
        INNING_TOPBOT VARCHAR(10),
        HOME_TEAM VARCHAR(10),
        AWAY_TEAM VARCHAR(10),
        HOME_SCORE INTEGER,
        AWAY_SCORE INTEGER,
        BATTER INTEGER,
        PITCHER INTEGER,
        PLAYER_NAME VARCHAR(100),
        PITCH_TYPE VARCHAR(10),
        PITCH_NAME VARCHAR(50),
        RELEASE_SPEED FLOAT,
        RELEASE_SPIN_RATE FLOAT,
        PFX_X FLOAT,
        PFX_Z FLOAT,
        PLATE_X FLOAT,
        PLATE_Z FLOAT,
        ZONE INTEGER,
        SZ_TOP FLOAT,
        SZ_BOT FLOAT,
        BALLS INTEGER,
        STRIKES INTEGER,
        OUTS_WHEN_UP INTEGER,
        TYPE VARCHAR(5),
        EVENTS VARCHAR(50),
        DESCRIPTION VARCHAR(100),
        ON_1B INTEGER,
        ON_2B INTEGER,
        ON_3B INTEGER,
        _SIMULATION_TIMESTAMP VARCHAR(50),
        _IS_SIMULATION VARCHAR(10),
        PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        BATCH_SOURCE VARCHAR(50)
    )
    """
    cursor.execute(create_sql)
    print("LIVE_PITCHES table created successfully!")

# Verify table exists
cursor.execute("SHOW TABLES LIKE 'LIVE_PITCHES'")
result = cursor.fetchone()
if result:
    print(f"Verified: Table {result[1]} exists in {result[3]}.{result[4]}")

# Show table structure
print("\nTable columns:")
cursor.execute("DESCRIBE TABLE LIVE_PITCHES")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

cursor.close()
conn.close()
print("\nDone!")
