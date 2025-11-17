import os
import warnings
import pybaseball as pyb
import pandas as pd
from snowflake_connect import setup_snowflake_connection, upload_data_to_snowflake
from dotenv import load_dotenv

con = setup_snowflake_connection()  # Capture the returned connection


# Suppress FutureWarnings from pybaseball's postprocessing
warnings.filterwarnings("ignore", category=FutureWarning, module="pybaseball")

# Set pandas to show all columns
pd.set_option("display.max_columns", None)

# CONFIGURATIONS
export_dir = "exports"
eda_dir = "eda"
docs_dir = "docs"

# --- RE-RUN YOUR CODE TO GET DATA ---
data = pyb.statcast(start_dt="2024-07-01", end_dt="2024-07-02")

# Drop these columns as they are empty
data = data.drop(
    columns=[
        "spin_dir",
        "spin_rate_deprecated",
        "break_angle_deprecated",
        "break_length_deprecated",
        "tfs_deprecated",
        "tfs_zulu_deprecated",
        "umpire",
        "sv_id",
    ]
)

# ------------------------------------------------------------------------------------------------ #
# 1. Game Context: Info that is static for the *entire game*.
static_context = [
    'game_pk', # game id
    'game_date',
    'game_year',
    'game_type',
    'home_team',
    'away_team',
]

# 2. Dynamic State: The "situation" before this specific pitch is thrown
dynamic_state = [
    # Player Matchup
    'player_name',  # Batter's Name
    'batter',       # Batter's ID
    'pitcher',      # Pitcher's ID
    'stand',        # Batter's Stance
    'p_throws',     # Pitcher's Hand
    
    # Game Situation
    'inning',
    'inning_topbot',
    'at_bat_number',
    'pitch_number',
    'balls',
    'strikes',
    'outs_when_up',
    'on_1b', 'on_2b', 'on_3b', # Runner IDs
    
    # Score & Win Probability (before the pitch)
    'home_score',
    'away_score',
    'home_score_diff',
    'bat_score_diff',
    'home_win_exp',
    'bat_win_exp',
    
    # Player State
    'n_thruorder_pitcher',
    'n_priorpa_thisgame_player_at_bat',
    'age_pit',
    'age_bat',
    
    # Defensive Alignment
    'if_fielding_alignment',
    'of_fielding_alignment',
]

# 3. 🚀 Pitch Event: Info about the specific pitch including its spin, movement, etc.
pitch_event = [
    'pitch_type', # pitch type
    'pitch_name',
    'release_speed',
    'release_spin_rate',
    'pfx_x',  # Horizontal movement
    'pfx_z',  # Vertical movement
    'sz_top', # Top of strike zone (simple metric)
    'sz_bot', # Bottom of strike zone (simple metric)
    'zone',   # Numbered zone
    'release_extension', # Simple metric, not a coordinate
    'spin_axis',
]

# 4. 💥 Batted Ball Event: Info about the batted ball including its speed, angle, etc.
batted_ball_event = [
    'launch_speed',
    'launch_angle',
    'bb_type',
    'hit_distance_sc',
    'launch_speed_angle', # Combo-category for launch_speed/angle
    
    # New (2024+) Bat Tracking Stats
    'bat_speed',
    'swing_length',
    'attack_angle',
    'arm_angle',
    'attack_direction',
    'swing_path_tilt',
]

# 5. Play Outcome: The final, discrete result of this pitch/at-bat.
# This is your main group of targets (y) for prediction.
play_outcome = [
    'type',        # Simple type (S, B, X)
    'description', # e.g., 'swinging_strike'
    'events',      # The final play result (e.g., 'single', 'strikeout')
    'des',         # Long-form description
    'hit_location',# The fielder number who handled the ball
    'post_home_score', # Score *after* the play
    'post_away_score',
]

# 6. Advanced/Modeled Stats: Some more advanced stats. We can omit these for now and use them later if we want to conduct advanced analytics
# advanced_stats = [
#     'woba_value',
#     'woba_denom',
#     'babip_value',
#     'iso_value',
#     'estimated_woba_using_speedangle', # xWOBA
#     'estimated_ba_using_speedangle',   # xBA
#     'estimated_slg_using_speedangle',  # xSLG
#     'delta_home_win_exp', # Win Probability Added (WPA)
#     'delta_run_exp',
#     'delta_pitcher_run_exp',
# ]


# Final List of Columns
all_relevant_columns = (
    static_context +
    dynamic_state +
    pitch_event +
    batted_ball_event +
    play_outcome
)

# Getting a composite key for the data

composite_key = ['game_pk', 'at_bat_number', 'pitch_number']

# Assuming 'data' is your DataFrame with all the statcast info

# 1. Define the columns for the composite key
key_columns = ['game_pk', 'at_bat_number', 'pitch_number']

# 2. Convert key columns to strings (to ensure they join properly)
data[key_columns] = data[key_columns].astype(str)

# 3. Create the new composite key column
# This joins them with an underscore, e.g., "717435_1_1"
data['pitch_uid'] = (
    data['game_pk'] + '_' + 
    data['at_bat_number'] + '_' + 
    data['pitch_number']
)

# 4. (Optional) Set this new column as the DataFrame's index
# This makes lookups very fast
data = data.set_index('pitch_uid')

# 5. Check your work
print("DataFrame with new composite key ('pitch_uid'):")
print(data.head())

# ------------------------------------------------------------------------------------------------ #

# export column names to statcast_columns_from_fetch.txt
with open(os.path.join(docs_dir, "statcast_columns_from_fetch.txt"), "w") as f:
    f.write("\n".join(data.columns))

# Exploratory Data Analysis (EDA)
os.makedirs(eda_dir, exist_ok=True)
with open(os.path.join(eda_dir, "zzz.txt"), "w") as f:
    f.write(str(data.info()))

data.describe().to_csv(os.path.join(eda_dir, "describe.csv"))
data.head().to_csv(os.path.join(eda_dir, "head.csv"))
data.tail().to_csv(os.path.join(eda_dir, "tail.csv"))
# pd.DataFrame(data.columns, columns=["columns"]).to_csv(
#     os.path.join(eda_dir, "columns.csv")
# ) # Not needed since columns are already documented

# count na values per column and export to na_counts.csv
na_counts = data.isna().sum()
na_counts = na_counts[na_counts > 0]
na_counts.to_csv(os.path.join(eda_dir, "na_counts.csv"))

# Reset index to avoid Pandas index warnings
data = data.reset_index(drop=True)

# Upload data to Snowflake with smart deduplication
# (checks what's already in Snowflake and only uploads new rows)
print("\n" + "=" * 80)
print("UPLOADING DATA TO SNOWFLAKE")
print("=" * 80)

load_dotenv()
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
STATCAST_TABLE = "STATCAST"

upload_stats = upload_data_to_snowflake(
    connection=con,
    dataframe=data,
    table_name=STATCAST_TABLE,
    database=SF_DATABASE,
    schema=SF_SCHEMA,
    max_retries=3,
    retry_delay=2,
)

print("\n" + "=" * 80)
print("UPLOAD SUMMARY")
print("=" * 80)
print(f"Success: {upload_stats['success']}")
print(f"Total Rows Fetched: {len(data)}")
print(f"Rows Skipped (Already in Snowflake): {upload_stats['rows_skipped']}")
print(f"Rows Uploaded: {upload_stats['rows_uploaded']}")
print(f"Rows Inserted: {upload_stats['rows_inserted']}")
print(f"Timestamp: {upload_stats['timestamp']}")
print(f"Message: {upload_stats['message']}")
if upload_stats["errors"]:
    print(f"Errors: {upload_stats['errors']}")

# Show tables in snowflake
print("\n" + "=" * 80)
print("TABLES IN SNOWFLAKE")
print("=" * 80)
cs = con.cursor()
try:
    cs.execute("SHOW TABLES;")
    tables = cs.fetchall()
    print("Tables in Snowflake:")
    for table in tables:
        print(f"  - {table[1]}")
finally:
    cs.close()
    con.close()  # Close the connection
