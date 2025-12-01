"""
Statcast Historical Backfill DAG (2020-2024)
=============================================
Automatically backfills MLB seasons from 2020 to 2024, month by month.
Runs every 3 minutes and processes one month per run.

Features:
- Tracks progress using Airflow Variables (current month index)
- Checks if data already exists in Snowflake before fetching
- Uses MERGE (upsert) to update changed data
- Automatically advances to next month after completion
- Stops itself when all seasons are complete

MLB Seasons Covered:
- 2020: July 23 - October 27 (COVID-shortened 60-game season)
- 2021: April 1 - November 2 (full season)
- 2022: April 7 - November 5 (full season)
- 2023: March 30 - November 1 (full season)
- 2024: March 28 - November 2 (full season)

Total: ~35 months to process (~105 minutes at 3-min intervals)

Usage:
    Enable the DAG and it will automatically process one month every 3 minutes.
    Monitor progress in Airflow Variables: statcast_historical_current_month
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
import logging

# =============================================================================
# CONFIGURATION
# =============================================================================
SNOWFLAKE_CONN_ID = "snowflake"
DATABASE = "BLUEJAY_DB"
SCHEMA = "BLUEJAY_SCHEMA"

# All seasons from 2020-2024 broken down by month
# Format: (start_date, end_date, description)
SEASON_MONTHS = [
    # 2020 Season (COVID-shortened, started July 23)
    ("2020-07-23", "2020-07-31", "2020 July (season start)"),
    ("2020-08-01", "2020-08-31", "2020 August"),
    ("2020-09-01", "2020-09-30", "2020 September"),
    ("2020-10-01", "2020-10-27", "2020 October (World Series)"),
    # 2021 Season (full season)
    ("2021-04-01", "2021-04-30", "2021 April (Opening Day)"),
    ("2021-05-01", "2021-05-31", "2021 May"),
    ("2021-06-01", "2021-06-30", "2021 June"),
    ("2021-07-01", "2021-07-31", "2021 July (All-Star)"),
    ("2021-08-01", "2021-08-31", "2021 August"),
    ("2021-09-01", "2021-09-30", "2021 September"),
    ("2021-10-01", "2021-10-31", "2021 October (Postseason)"),
    ("2021-11-01", "2021-11-02", "2021 November (World Series)"),
    # 2022 Season (full season)
    ("2022-04-07", "2022-04-30", "2022 April (Opening Day)"),
    ("2022-05-01", "2022-05-31", "2022 May"),
    ("2022-06-01", "2022-06-30", "2022 June"),
    ("2022-07-01", "2022-07-31", "2022 July (All-Star)"),
    ("2022-08-01", "2022-08-31", "2022 August"),
    ("2022-09-01", "2022-09-30", "2022 September"),
    ("2022-10-01", "2022-10-31", "2022 October (Postseason)"),
    ("2022-11-01", "2022-11-05", "2022 November (World Series)"),
    # 2023 Season (full season)
    ("2023-03-30", "2023-03-31", "2023 March (Opening Day)"),
    ("2023-04-01", "2023-04-30", "2023 April"),
    ("2023-05-01", "2023-05-31", "2023 May"),
    ("2023-06-01", "2023-06-30", "2023 June"),
    ("2023-07-01", "2023-07-31", "2023 July (All-Star)"),
    ("2023-08-01", "2023-08-31", "2023 August"),
    ("2023-09-01", "2023-09-30", "2023 September"),
    ("2023-10-01", "2023-10-31", "2023 October (Postseason)"),
    ("2023-11-01", "2023-11-01", "2023 November (World Series)"),
    # 2024 Season (full season)
    ("2024-03-28", "2024-03-31", "2024 March (Opening Day)"),
    ("2024-04-01", "2024-04-30", "2024 April"),
    ("2024-05-01", "2024-05-31", "2024 May"),
    ("2024-06-01", "2024-06-30", "2024 June"),
    ("2024-07-01", "2024-07-31", "2024 July (All-Star)"),
    ("2024-08-01", "2024-08-31", "2024 August"),
    ("2024-09-01", "2024-09-30", "2024 September"),
    ("2024-10-01", "2024-10-31", "2024 October (Postseason)"),
    ("2024-11-01", "2024-11-02", "2024 November (World Series)"),
]

# Airflow Variable to track progress
PROGRESS_VAR = "statcast_historical_current_month"

# =============================================================================
# DEFAULT ARGUMENTS
# =============================================================================
default_args = {
    "owner": "agopalareddy",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
dag = DAG(
    dag_id="statcast_historical_backfill",
    default_args=default_args,
    description="Automated 2020-2024 seasons backfill - one month every 3 minutes",
    schedule_interval="*/3 * * * *",  # Every 3 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Only one run at a time
    tags=["mlb", "statcast", "backfill", "historical", "automated"],
)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def get_current_month_index():
    """Get the current month index from Airflow Variables."""
    try:
        return int(Variable.get(PROGRESS_VAR, default_var="0"))
    except Exception:
        return 0


def set_current_month_index(index):
    """Set the current month index in Airflow Variables."""
    Variable.set(PROGRESS_VAR, str(index))


# =============================================================================
# TASK FUNCTIONS
# =============================================================================


def check_progress(**context):
    """
    Check if we should process, skip (data exists), or stop (all done).
    Returns the task_id to branch to.
    """
    month_index = get_current_month_index()

    # Check if all months are complete
    if month_index >= len(SEASON_MONTHS):
        logging.info("✅ All seasons (2020-2024) have been processed!")
        logging.info("Setting month index back to 0 for potential re-runs")
        # Optionally reset to allow re-runs, or keep at max to stay stopped
        # set_current_month_index(0)  # Uncomment to allow re-runs
        return "all_complete"

    start_date, end_date, description = SEASON_MONTHS[month_index]
    logging.info(f"Current month index: {month_index}/{len(SEASON_MONTHS)-1}")
    logging.info(f"Processing: {description} ({start_date} to {end_date})")

    # Check if data already exists in Snowflake for this period
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        # Count existing rows for this date range
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM STATCAST 
            WHERE TO_DATE("game_date") >= '{start_date}' 
              AND TO_DATE("game_date") <= '{end_date}'
        """
        )
        existing_count = cursor.fetchone()[0]

        logging.info(
            f"Existing rows in Snowflake for {start_date} to {end_date}: {existing_count:,}"
        )

        # Store info for downstream tasks
        context["ti"].xcom_push(key="month_index", value=month_index)
        context["ti"].xcom_push(key="start_date", value=start_date)
        context["ti"].xcom_push(key="end_date", value=end_date)
        context["ti"].xcom_push(key="description", value=description)
        context["ti"].xcom_push(key="existing_count", value=existing_count)

        # Always process - MERGE will handle updates vs inserts
        # Even if data exists, we want to check for updates
        return "fetch_data"

    finally:
        cursor.close()
        conn.close()


def fetch_data(**context):
    """
    Fetch Statcast data for the current month.
    """
    import pandas as pd
    from pybaseball import statcast
    import warnings
    import tempfile
    import os

    warnings.filterwarnings("ignore", category=FutureWarning, module="pybaseball")

    start_date = context["ti"].xcom_pull(key="start_date", task_ids="check_progress")
    end_date = context["ti"].xcom_pull(key="end_date", task_ids="check_progress")
    existing_count = context["ti"].xcom_pull(
        key="existing_count", task_ids="check_progress"
    )

    logging.info(f"Fetching data: {start_date} to {end_date}")
    logging.info(
        f"Existing rows in Snowflake: {existing_count:,} (will MERGE to update if changed)"
    )

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    all_data = []
    current_dt = start_dt

    while current_dt <= end_dt:
        day_str = current_dt.strftime("%Y-%m-%d")
        logging.info(f"Fetching: {day_str}")

        try:
            daily_data = statcast(start_dt=day_str, end_dt=day_str)

            if daily_data is not None and not daily_data.empty:
                all_data.append(daily_data)
                logging.info(f"  -> {len(daily_data)} pitches")
            else:
                logging.info(f"  -> No games")

        except Exception as e:
            logging.error(f"  -> ERROR: {str(e)}")

        current_dt += timedelta(days=1)

    if not all_data:
        logging.warning("No data fetched for this period")
        context["ti"].xcom_push(key="has_data", value=False)
        return {"status": "no_data", "rows": 0}

    # Combine all days
    combined_df = pd.concat(all_data, ignore_index=True)
    logging.info(f"Total pitches fetched: {len(combined_df)}")

    # Drop deprecated columns
    columns_to_drop = [
        "spin_dir",
        "spin_rate_deprecated",
        "break_angle_deprecated",
        "break_length_deprecated",
        "tfs_deprecated",
        "tfs_zulu_deprecated",
        "umpire",
        "sv_id",
    ]
    cols_to_drop = [col for col in columns_to_drop if col in combined_df.columns]
    if cols_to_drop:
        combined_df = combined_df.drop(columns=cols_to_drop)
        logging.info(f"Dropped deprecated columns: {cols_to_drop}")

    # Save to temp file
    temp_file = os.path.join(
        tempfile.gettempdir(),
        f"statcast_historical_{start_date}_{end_date}.parquet",
    )
    combined_df.to_parquet(temp_file, index=False)
    logging.info(f"Saved to temp file: {temp_file}")

    context["ti"].xcom_push(key="temp_file", value=temp_file)
    context["ti"].xcom_push(key="row_count", value=len(combined_df))
    context["ti"].xcom_push(key="has_data", value=True)

    return {"status": "success", "rows": len(combined_df)}


def upload_to_snowflake(**context):
    """
    Upload data to Snowflake using MERGE (upsert).
    Inserts new records, updates existing ones if data has changed.
    """
    import pandas as pd
    import os

    has_data = context["ti"].xcom_pull(key="has_data", task_ids="fetch_data")
    if not has_data:
        logging.info("No data to upload, skipping")
        context["ti"].xcom_push(key="upload_success", value=False)
        return {"status": "skipped"}

    temp_file = context["ti"].xcom_pull(key="temp_file", task_ids="fetch_data")

    if not temp_file or not os.path.exists(temp_file):
        logging.warning("No temp file found, skipping")
        context["ti"].xcom_push(key="upload_success", value=False)
        return {"status": "skipped"}

    # Load data
    data = pd.read_parquet(temp_file)
    logging.info(f"Loaded {len(data)} rows from temp file")

    # Create unique key
    data["pitch_uid"] = (
        data["game_pk"].astype(str)
        + "_"
        + data["at_bat_number"].astype(str)
        + "_"
        + data["pitch_number"].astype(str)
    )

    # Get Snowflake connection
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        # Upload to staging table
        staging_table = "STATCAST_STAGING"

        from snowflake.connector.pandas_tools import write_pandas

        cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=data,
            table_name=staging_table,
            database=DATABASE,
            schema=SCHEMA,
            auto_create_table=True,
            overwrite=True,
            use_logical_type=True,
        )

        if not success:
            logging.error("Failed to upload to staging table")
            context["ti"].xcom_push(key="upload_success", value=False)
            return {"status": "failed"}

        logging.info(f"Uploaded {nrows} rows to staging table")

        # Check existing records
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM {staging_table} s
            WHERE EXISTS (
                SELECT 1 FROM STATCAST t
                WHERE t."game_pk" = s."game_pk"
                  AND t."at_bat_number" = s."at_bat_number"
                  AND t."pitch_number" = s."pitch_number"
            )
        """
        )
        existing_count = cursor.fetchone()[0]
        logging.info(
            f"Found {existing_count} existing records (will be updated if changed)"
        )

        # Get column list
        cursor.execute(f"DESCRIBE TABLE {staging_table}")
        columns = [row[0] for row in cursor.fetchall() if row[0] != "pitch_uid"]

        # Build MERGE statement
        update_set = ", ".join([f't."{col}" = s."{col}"' for col in columns])
        insert_cols = ", ".join([f'"{col}"' for col in columns])
        insert_vals = ", ".join([f's."{col}"' for col in columns])

        merge_sql = f"""
            MERGE INTO STATCAST t
            USING {staging_table} s
            ON t."game_pk" = s."game_pk"
               AND t."at_bat_number" = s."at_bat_number"
               AND t."pitch_number" = s."pitch_number"
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
        """

        cursor.execute(merge_sql)

        # Get merge results
        cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        merge_result = cursor.fetchone()
        rows_inserted = merge_result[0] if merge_result else 0
        rows_updated = merge_result[1] if merge_result and len(merge_result) > 1 else 0

        logging.info(
            f"MERGE complete: {rows_inserted} inserted, {rows_updated} updated"
        )

        # Clean up
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

        context["ti"].xcom_push(key="upload_success", value=True)
        context["ti"].xcom_push(key="rows_inserted", value=rows_inserted)
        context["ti"].xcom_push(key="rows_updated", value=rows_updated)

        # Clean up temp file
        os.remove(temp_file)

        return {
            "status": "success",
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
        }

    finally:
        cursor.close()
        conn.close()


def update_star_schema(**context):
    """
    Update dimension and fact tables using MERGE.
    """
    upload_success = context["ti"].xcom_pull(
        key="upload_success", task_ids="upload_to_snowflake"
    )

    if not upload_success:
        logging.warning("Upload was not successful, skipping dimension update")
        return {"status": "skipped"}

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")

    results = {}

    try:
        # MERGE DIM_GAME
        logging.info("Merging DIM_GAME...")
        cursor.execute(
            """
            MERGE INTO DIM_GAME t
            USING (
                SELECT
                    "game_pk"::INTEGER AS game_pk,
                    TO_DATE("game_date") AS game_date,
                    "game_year" AS game_year,
                    "game_type" AS game_type,
                    "home_team" AS home_team,
                    "away_team" AS away_team,
                    "fielder_2" AS fielder_2,
                    "fielder_3" AS fielder_3,
                    "fielder_4" AS fielder_4,
                    "fielder_5" AS fielder_5,
                    "fielder_6" AS fielder_6,
                    "fielder_7" AS fielder_7,
                    "fielder_8" AS fielder_8,
                    "fielder_9" AS fielder_9
                FROM STATCAST
                QUALIFY ROW_NUMBER() OVER (PARTITION BY "game_pk" ORDER BY "game_date") = 1
            ) s
            ON t.game_pk = s.game_pk
            WHEN MATCHED THEN UPDATE SET
                t.game_date = s.game_date,
                t.game_year = s.game_year,
                t.game_type = s.game_type,
                t.home_team = s.home_team,
                t.away_team = s.away_team,
                t.fielder_2 = s.fielder_2,
                t.fielder_3 = s.fielder_3,
                t.fielder_4 = s.fielder_4,
                t.fielder_5 = s.fielder_5,
                t.fielder_6 = s.fielder_6,
                t.fielder_7 = s.fielder_7,
                t.fielder_8 = s.fielder_8,
                t.fielder_9 = s.fielder_9
            WHEN NOT MATCHED THEN INSERT (
                game_pk, game_date, game_year, game_type, home_team, away_team,
                fielder_2, fielder_3, fielder_4, fielder_5, fielder_6,
                fielder_7, fielder_8, fielder_9
            ) VALUES (
                s.game_pk, s.game_date, s.game_year, s.game_type, s.home_team, s.away_team,
                s.fielder_2, s.fielder_3, s.fielder_4, s.fielder_5, s.fielder_6,
                s.fielder_7, s.fielder_8, s.fielder_9
            )
        """
        )
        results["games_merged"] = cursor.rowcount
        logging.info(f"DIM_GAME: {cursor.rowcount} rows merged")

        # MERGE DIM_PLAYER (pitchers)
        logging.info("Merging DIM_PLAYER (pitchers)...")
        cursor.execute(
            """
            MERGE INTO DIM_PLAYER t
            USING (
                SELECT 
                    "pitcher" AS player_id,
                    "player_name" AS player_name,
                    "p_throws" AS throws,
                    MAX("age_pit") AS age
                FROM STATCAST
                WHERE "pitcher" IS NOT NULL
                GROUP BY "pitcher", "player_name", "p_throws"
                QUALIFY ROW_NUMBER() OVER (PARTITION BY "pitcher" ORDER BY MAX("age_pit") DESC NULLS LAST) = 1
            ) s
            ON t.player_id = s.player_id
            WHEN MATCHED THEN UPDATE SET
                t.player_name = COALESCE(s.player_name, t.player_name),
                t.throws = COALESCE(s.throws, t.throws),
                t.age = COALESCE(s.age, t.age)
            WHEN NOT MATCHED THEN INSERT (player_id, player_name, throws, age)
                VALUES (s.player_id, s.player_name, s.throws, s.age)
        """
        )
        results["pitchers_merged"] = cursor.rowcount
        logging.info(f"DIM_PLAYER (pitchers): {cursor.rowcount} rows merged")

        # MERGE DIM_PLAYER (batters)
        logging.info("Merging DIM_PLAYER (batters)...")
        cursor.execute(
            """
            MERGE INTO DIM_PLAYER t
            USING (
                SELECT 
                    "batter" AS player_id,
                    "stand" AS bats,
                    MAX("age_bat") AS age
                FROM STATCAST
                WHERE "batter" IS NOT NULL
                GROUP BY "batter", "stand"
                QUALIFY ROW_NUMBER() OVER (PARTITION BY "batter" ORDER BY MAX("age_bat") DESC NULLS LAST) = 1
            ) s
            ON t.player_id = s.player_id
            WHEN MATCHED THEN UPDATE SET
                t.bats = COALESCE(s.bats, t.bats),
                t.age = COALESCE(s.age, t.age)
            WHEN NOT MATCHED THEN INSERT (player_id, bats, age)
                VALUES (s.player_id, s.bats, s.age)
        """
        )
        results["batters_merged"] = cursor.rowcount
        logging.info(f"DIM_PLAYER (batters): {cursor.rowcount} rows merged")

        # MERGE FACT_PITCH
        logging.info("Merging FACT_PITCH...")
        cursor.execute(
            """
            MERGE INTO FACT_PITCH t
            USING (
                SELECT 
                    CONCAT("game_pk", '_', "at_bat_number", '_', "pitch_number") AS pitch_uid,
                    "game_pk"::INTEGER AS game_pk,
                    "at_bat_number"::INTEGER AS at_bat_number,
                    "pitch_number"::INTEGER AS pitch_number,
                    "pitcher" AS pitcher_id,
                    "batter" AS batter_id,
                    "pitch_type" AS pitch_type,
                    "pitch_name" AS pitch_name,
                    "inning" AS inning,
                    "inning_topbot" AS inning_topbot,
                    "balls" AS balls,
                    "strikes" AS strikes,
                    "outs_when_up" AS outs_when_up,
                    "on_1b" AS on_1b,
                    "on_2b" AS on_2b,
                    "on_3b" AS on_3b,
                    "home_score" AS home_score,
                    "away_score" AS away_score,
                    "home_score_diff" AS home_score_diff,
                    "bat_score_diff" AS bat_score_diff,
                    "home_win_exp" AS home_win_exp,
                    "bat_win_exp" AS bat_win_exp,
                    "stand" AS stand,
                    "p_throws" AS p_throws,
                    "age_pit" AS age_pit,
                    "age_bat" AS age_bat,
                    "n_thruorder_pitcher" AS n_thruorder_pitcher,
                    "n_priorpa_thisgame_player_at_bat" AS n_priorpa_thisgame_player_at_bat,
                    "if_fielding_alignment" AS if_fielding_alignment,
                    "of_fielding_alignment" AS of_fielding_alignment,
                    "release_speed" AS release_speed,
                    "release_spin_rate" AS release_spin_rate,
                    "release_extension" AS release_extension,
                    "spin_axis" AS spin_axis,
                    "pfx_x" AS pfx_x,
                    "pfx_z" AS pfx_z,
                    "zone" AS zone,
                    "sz_top" AS sz_top,
                    "sz_bot" AS sz_bot,
                    "plate_x" AS plate_x,
                    "plate_z" AS plate_z,
                    "launch_speed" AS launch_speed,
                    "launch_angle" AS launch_angle,
                    "bb_type" AS bb_type,
                    "hit_distance_sc" AS hit_distance_sc,
                    "launch_speed_angle" AS launch_speed_angle,
                    "bat_speed" AS bat_speed,
                    "swing_length" AS swing_length,
                    "attack_angle" AS attack_angle,
                    "arm_angle" AS arm_angle,
                    "attack_direction" AS attack_direction,
                    "swing_path_tilt" AS swing_path_tilt,
                    "type" AS type,
                    "description" AS description,
                    "events" AS events,
                    "des" AS des,
                    "hit_location" AS hit_location,
                    "post_home_score" AS post_home_score,
                    "post_away_score" AS post_away_score,
                    TO_NUMBER(TO_CHAR(TO_DATE("game_date"), 'YYYYMMDD')) AS date_key
                FROM STATCAST
                QUALIFY ROW_NUMBER() OVER (PARTITION BY "game_pk", "at_bat_number", "pitch_number" ORDER BY "game_date") = 1
            ) s
            ON t.pitch_uid = s.pitch_uid
            WHEN MATCHED THEN UPDATE SET
                t.release_speed = s.release_speed,
                t.release_spin_rate = s.release_spin_rate,
                t.launch_speed = s.launch_speed,
                t.launch_angle = s.launch_angle,
                t.hit_distance_sc = s.hit_distance_sc,
                t.bat_speed = s.bat_speed,
                t.swing_length = s.swing_length,
                t.events = s.events,
                t.description = s.description,
                t.des = s.des
            WHEN NOT MATCHED THEN INSERT (
                pitch_uid, game_pk, at_bat_number, pitch_number,
                pitcher_id, batter_id, pitch_type, pitch_name,
                inning, inning_topbot, balls, strikes, outs_when_up,
                on_1b, on_2b, on_3b,
                home_score, away_score, home_score_diff, bat_score_diff,
                home_win_exp, bat_win_exp, stand, p_throws, age_pit, age_bat,
                n_thruorder_pitcher, n_priorpa_thisgame_player_at_bat,
                if_fielding_alignment, of_fielding_alignment,
                release_speed, release_spin_rate, release_extension, spin_axis,
                pfx_x, pfx_z, zone, sz_top, sz_bot, plate_x, plate_z,
                launch_speed, launch_angle, bb_type, hit_distance_sc, launch_speed_angle,
                bat_speed, swing_length, attack_angle, arm_angle, attack_direction, swing_path_tilt,
                type, description, events, des, hit_location,
                post_home_score, post_away_score, date_key
            ) VALUES (
                s.pitch_uid, s.game_pk, s.at_bat_number, s.pitch_number,
                s.pitcher_id, s.batter_id, s.pitch_type, s.pitch_name,
                s.inning, s.inning_topbot, s.balls, s.strikes, s.outs_when_up,
                s.on_1b, s.on_2b, s.on_3b,
                s.home_score, s.away_score, s.home_score_diff, s.bat_score_diff,
                s.home_win_exp, s.bat_win_exp, s.stand, s.p_throws, s.age_pit, s.age_bat,
                s.n_thruorder_pitcher, s.n_priorpa_thisgame_player_at_bat,
                s.if_fielding_alignment, s.of_fielding_alignment,
                s.release_speed, s.release_spin_rate, s.release_extension, s.spin_axis,
                s.pfx_x, s.pfx_z, s.zone, s.sz_top, s.sz_bot, s.plate_x, s.plate_z,
                s.launch_speed, s.launch_angle, s.bb_type, s.hit_distance_sc, s.launch_speed_angle,
                s.bat_speed, s.swing_length, s.attack_angle, s.arm_angle, s.attack_direction, s.swing_path_tilt,
                s.type, s.description, s.events, s.des, s.hit_location,
                s.post_home_score, s.post_away_score, s.date_key
            )
        """
        )
        results["facts_merged"] = cursor.rowcount
        logging.info(f"FACT_PITCH: {cursor.rowcount} rows merged")

        results["status"] = "success"
        return results

    finally:
        cursor.close()
        conn.close()


def advance_to_next_month(**context):
    """
    Advance to the next month after successful processing.
    """
    upload_success = context["ti"].xcom_pull(
        key="upload_success", task_ids="upload_to_snowflake"
    )
    month_index = context["ti"].xcom_pull(key="month_index", task_ids="check_progress")
    start_date = context["ti"].xcom_pull(key="start_date", task_ids="check_progress")
    end_date = context["ti"].xcom_pull(key="end_date", task_ids="check_progress")

    if upload_success:
        rows_inserted = (
            context["ti"].xcom_pull(key="rows_inserted", task_ids="upload_to_snowflake")
            or 0
        )
        rows_updated = (
            context["ti"].xcom_pull(key="rows_updated", task_ids="upload_to_snowflake")
            or 0
        )

        logging.info(f"✅ Successfully processed {start_date} to {end_date}")
        logging.info(f"   Rows inserted: {rows_inserted:,}")
        logging.info(f"   Rows updated: {rows_updated:,}")

        # Advance to next month
        next_index = month_index + 1
        set_current_month_index(next_index)

        if next_index >= len(SEASON_MONTHS):
            logging.info("🎉 All seasons (2020-2024) have been processed!")
            logging.info("The DAG will detect this on next run and skip processing.")
        else:
            next_start, next_end, next_desc = SEASON_MONTHS[next_index]
            logging.info(f"📅 Next run will process: {next_desc} ({next_start} to {next_end})")
    else:
        logging.warning(
            f"⚠️ Processing failed or was skipped for {start_date} to {end_date}"
        )
        logging.warning("Will retry this month on next run")

    return {"next_month_index": get_current_month_index()}


def mark_all_complete(**context):
    """
    Called when all months are complete.
    """
    logging.info("🎉 Historical Backfill Complete (2020-2024)!")
    logging.info(f"All {len(SEASON_MONTHS)} months have been processed:")
    
    current_year = None
    for i, (start, end, desc) in enumerate(SEASON_MONTHS):
        year = start[:4]
        if year != current_year:
            logging.info(f"\n  {year} Season:")
            current_year = year
        logging.info(f"    {i+1}. {desc}")
    
    logging.info("")
    logging.info("To re-run the backfill, set the Airflow Variable:")
    logging.info(f"  Variable: {PROGRESS_VAR}")
    logging.info("  Value: 0")

    return {"status": "complete"}


# =============================================================================
# TASK DEFINITIONS
# =============================================================================

check_progress_task = BranchPythonOperator(
    task_id="check_progress",
    python_callable=check_progress,
    dag=dag,
)

fetch_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_snowflake",
    python_callable=upload_to_snowflake,
    dag=dag,
)

schema_task = PythonOperator(
    task_id="update_star_schema",
    python_callable=update_star_schema,
    dag=dag,
)

advance_task = PythonOperator(
    task_id="advance_to_next_month",
    python_callable=advance_to_next_month,
    dag=dag,
)

all_complete_task = PythonOperator(
    task_id="all_complete",
    python_callable=mark_all_complete,
    dag=dag,
)

# =============================================================================
# TASK DEPENDENCIES
# =============================================================================
# Branch: check_progress -> either fetch_data (continue) or all_complete (done)
check_progress_task >> [fetch_task, all_complete_task]
fetch_task >> upload_task >> schema_task >> advance_task
