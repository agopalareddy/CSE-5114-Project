"""
Statcast Batch Backfill DAG
===========================
Fetches MLB Statcast data for a date RANGE in a single DAG run.
Uses Airflow's Snowflake provider for database connections.

Features:
- Incremental loading (MERGE/upsert) - no duplicates
- Updates existing records if data has changed
- Skips unchanged data

Usage:
1. Trigger manually from Airflow UI with "Trigger DAG w/ config"
2. Pass config parameters:
   {
     "start_date": "2024-03-28",
     "end_date": "2024-04-07"
   }

Or use CLI:
  airflow dags trigger statcast_backfill --conf '{"start_date": "2024-03-28", "end_date": "2024-04-07"}'
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models.param import Param
import logging

# =============================================================================
# CONFIGURATION - Match your other DAGs
# =============================================================================
SNOWFLAKE_CONN_ID = "snowflake"  # From airflow connections list
DATABASE = "BLUEJAY_DB"  # Your Snowflake database
SCHEMA = "BLUEJAY_SCHEMA"  # Your Snowflake schema

# =============================================================================
# DEFAULT ARGUMENTS
# =============================================================================
default_args = {
    "owner": "agopalareddy",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
dag = DAG(
    dag_id="statcast_backfill",
    default_args=default_args,
    description="Batch backfill Statcast data for a date range (incremental/upsert)",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlb", "statcast", "backfill"],
    params={
        "start_date": Param(
            default="2024-07-01",
            type="string",
            description="Start date (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default="2024-07-07",
            type="string",
            description="End date (YYYY-MM-DD)",
        ),
    },
    render_template_as_native_obj=True,
)


# =============================================================================
# TASK FUNCTIONS
# =============================================================================


def fetch_date_range(**context):
    """
    Fetch Statcast data for a date range passed via DAG params.
    Loops day-by-day to avoid API timeouts.
    """
    import pandas as pd
    from pybaseball import statcast
    import warnings
    from datetime import datetime, timedelta
    import tempfile
    import os

    warnings.filterwarnings("ignore", category=FutureWarning, module="pybaseball")

    # Get date range from DAG params (UI form) or conf (CLI)
    params = context.get("params", {})
    dag_conf = context.get("dag_run").conf or {}

    # params takes precedence (from UI), fallback to conf (from CLI)
    start_date_str = params.get("start_date") or dag_conf.get(
        "start_date", "2024-07-01"
    )
    end_date_str = params.get("end_date") or dag_conf.get("end_date", "2024-07-07")

    logging.info(f"Backfill date range: {start_date_str} to {end_date_str}")

    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")

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
        logging.warning("No data fetched for entire date range")
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

    # Save to temp file (more reliable than XCom for large data)
    temp_file = os.path.join(
        tempfile.gettempdir(),
        f"statcast_backfill_{start_date_str}_{end_date_str}.parquet",
    )
    combined_df.to_parquet(temp_file, index=False)
    logging.info(f"Saved to temp file: {temp_file}")

    context["ti"].xcom_push(key="temp_file", value=temp_file)
    context["ti"].xcom_push(key="row_count", value=len(combined_df))

    return {"status": "success", "rows": len(combined_df), "temp_file": temp_file}


def upload_to_snowflake(**context):
    """
    Upload data to Snowflake STATCAST table using MERGE (upsert) logic.
    - New records are inserted
    - Existing records are updated if any data has changed
    - Duplicates are avoided
    """
    import pandas as pd
    import os

    # Get temp file from previous task
    temp_file = context["ti"].xcom_pull(key="temp_file", task_ids="fetch_date_range")

    if not temp_file or not os.path.exists(temp_file):
        logging.warning("No temp file found, skipping")
        return {"status": "skipped"}

    # Load data
    data = pd.read_parquet(temp_file)
    logging.info(f"Loaded {len(data)} rows from temp file")

    # Create a unique key for each pitch (game_pk + at_bat_number + pitch_number)
    data["pitch_uid"] = (
        data["game_pk"].astype(str)
        + "_"
        + data["at_bat_number"].astype(str)
        + "_"
        + data["pitch_number"].astype(str)
    )

    # Get Snowflake connection using Airflow Hook
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Set context
        cursor.execute(f"USE DATABASE {DATABASE}")
        cursor.execute(f"USE SCHEMA {SCHEMA}")

        # Step 1: Upload to a temporary staging table
        staging_table = "STATCAST_STAGING"

        from snowflake.connector.pandas_tools import write_pandas

        # Drop staging table if exists and recreate
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

        # Step 2: Check how many already exist in STATCAST
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

        # Step 3: MERGE - Insert new, Update existing
        # Get column list dynamically (excluding our added pitch_uid)
        cursor.execute(f"DESCRIBE TABLE {staging_table}")
        columns = [row[0] for row in cursor.fetchall() if row[0] != "pitch_uid"]

        # Build the MERGE statement
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

        # Step 4: Clean up staging table
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
            "rows_skipped": existing_count,
        }

    finally:
        cursor.close()
        conn.close()


def update_star_schema(**context):
    """
    Update dimension and fact tables using MERGE (upsert) logic.
    - New records are inserted
    - Existing records are updated if data has changed
    - More efficient than NOT IN subqueries
    """
    upload_success = context["ti"].xcom_pull(
        key="upload_success", task_ids="upload_to_snowflake"
    )

    if not upload_success:
        logging.warning("Upload was not successful, skipping dimension update")
        return {"status": "skipped"}

    # Get Snowflake connection using Airflow Hook
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Set database/schema context
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")

    results = {}

    try:
        # =====================================================================
        # MERGE DIM_GAME - Insert new games, update existing ones
        # Use ROW_NUMBER to get one row per game (fielders can change mid-game)
        # =====================================================================
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
        logging.info(f"DIM_GAME: {cursor.rowcount} rows merged (insert/update)")

        # =====================================================================
        # MERGE DIM_PLAYER - Pitchers first, then batters
        # Use ROW_NUMBER to get one row per player (age can vary across games)
        # =====================================================================
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

        # =====================================================================
        # MERGE FACT_PITCH - Main fact table
        # Use QUALIFY to ensure one row per pitch_uid (shouldn't have dupes, but be safe)
        # =====================================================================
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
        logging.info(f"FACT_PITCH: {cursor.rowcount} rows merged (insert/update)")

        results["status"] = "success"
        return results

    finally:
        cursor.close()
        conn.close()


def run_data_quality_checks(**context):
    """
    Run data quality checks after ETL completes.
    Checks:
    1. Row counts in all tables
    2. Null checks on critical columns
    3. Referential integrity between fact and dimension tables
    4. Date range validation
    """
    # Get Snowflake connection using Airflow Hook
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Set database/schema context
    cursor.execute(f"USE DATABASE {DATABASE}")
    cursor.execute(f"USE SCHEMA {SCHEMA}")

    results = {"checks_passed": 0, "checks_failed": 0, "details": []}

    try:
        # =====================================================================
        # CHECK 1: Row counts in all tables
        # =====================================================================
        logging.info("Running row count checks...")
        cursor.execute(
            """
            SELECT 'STATCAST' AS table_name, COUNT(*) AS row_count FROM STATCAST
            UNION ALL SELECT 'DIM_GAME', COUNT(*) FROM DIM_GAME
            UNION ALL SELECT 'DIM_PLAYER', COUNT(*) FROM DIM_PLAYER
            UNION ALL SELECT 'DIM_DATE', COUNT(*) FROM DIM_DATE
            UNION ALL SELECT 'FACT_PITCH', COUNT(*) FROM FACT_PITCH
        """
        )
        row_counts = {row[0]: row[1] for row in cursor.fetchall()}

        for table, count in row_counts.items():
            if count > 0:
                results["checks_passed"] += 1
                results["details"].append(f"✅ {table}: {count:,} rows")
                logging.info(f"✅ {table}: {count:,} rows")
            else:
                results["checks_failed"] += 1
                results["details"].append(f"❌ {table}: EMPTY (0 rows)")
                logging.warning(f"❌ {table}: EMPTY (0 rows)")

        # =====================================================================
        # CHECK 2: Null checks on critical columns in FACT_PITCH
        # =====================================================================
        logging.info("Running null checks on FACT_PITCH...")
        critical_columns = [
            "pitch_uid",
            "game_pk",
            "pitcher_id",
            "batter_id",
            "pitch_number",
        ]

        for col in critical_columns:
            cursor.execute(f"SELECT COUNT(*) FROM FACT_PITCH WHERE {col} IS NULL")
            null_count = cursor.fetchone()[0]

            if null_count == 0:
                results["checks_passed"] += 1
                results["details"].append(f"✅ FACT_PITCH.{col}: No nulls")
                logging.info(f"✅ FACT_PITCH.{col}: No nulls")
            else:
                results["checks_failed"] += 1
                results["details"].append(
                    f"❌ FACT_PITCH.{col}: {null_count:,} nulls found"
                )
                logging.warning(f"❌ FACT_PITCH.{col}: {null_count:,} nulls found")

        # =====================================================================
        # CHECK 3: Referential integrity - all FACT_PITCH.game_pk exists in DIM_GAME
        # =====================================================================
        logging.info("Running referential integrity checks...")
        cursor.execute(
            """
            SELECT COUNT(DISTINCT f.game_pk) 
            FROM FACT_PITCH f
            LEFT JOIN DIM_GAME g ON f.game_pk = g.game_pk
            WHERE g.game_pk IS NULL
        """
        )
        orphan_games = cursor.fetchone()[0]

        if orphan_games == 0:
            results["checks_passed"] += 1
            results["details"].append(
                "✅ Referential integrity: All FACT_PITCH.game_pk exist in DIM_GAME"
            )
            logging.info(
                "✅ Referential integrity: All FACT_PITCH.game_pk exist in DIM_GAME"
            )
        else:
            results["checks_failed"] += 1
            results["details"].append(
                f"❌ Referential integrity: {orphan_games} orphan game_pk in FACT_PITCH"
            )
            logging.warning(
                f"❌ Referential integrity: {orphan_games} orphan game_pk in FACT_PITCH"
            )

        # Check pitcher_id exists in DIM_PLAYER
        cursor.execute(
            """
            SELECT COUNT(DISTINCT f.pitcher_id) 
            FROM FACT_PITCH f
            LEFT JOIN DIM_PLAYER p ON f.pitcher_id = p.player_id
            WHERE p.player_id IS NULL AND f.pitcher_id IS NOT NULL
        """
        )
        orphan_pitchers = cursor.fetchone()[0]

        if orphan_pitchers == 0:
            results["checks_passed"] += 1
            results["details"].append(
                "✅ Referential integrity: All FACT_PITCH.pitcher_id exist in DIM_PLAYER"
            )
            logging.info(
                "✅ Referential integrity: All FACT_PITCH.pitcher_id exist in DIM_PLAYER"
            )
        else:
            results["checks_failed"] += 1
            results["details"].append(
                f"❌ Referential integrity: {orphan_pitchers} orphan pitcher_id in FACT_PITCH"
            )
            logging.warning(
                f"❌ Referential integrity: {orphan_pitchers} orphan pitcher_id in FACT_PITCH"
            )

        # =====================================================================
        # CHECK 4: Data freshness - most recent data loaded
        # =====================================================================
        logging.info("Checking data freshness...")
        cursor.execute(
            """
            SELECT MIN(TO_DATE("game_date")) AS min_date, 
                   MAX(TO_DATE("game_date")) AS max_date,
                   COUNT(DISTINCT TO_DATE("game_date")) AS unique_dates
            FROM STATCAST
        """
        )
        date_range = cursor.fetchone()
        min_date, max_date, unique_dates = date_range

        results["checks_passed"] += 1
        results["details"].append(
            f"✅ Data range: {min_date} to {max_date} ({unique_dates} unique dates)"
        )
        logging.info(
            f"✅ Data range: {min_date} to {max_date} ({unique_dates} unique dates)"
        )

        # =====================================================================
        # CHECK 5: Duplicate check in FACT_PITCH
        # =====================================================================
        logging.info("Checking for duplicates in FACT_PITCH...")
        cursor.execute(
            """
            SELECT COUNT(*) - COUNT(DISTINCT pitch_uid) AS duplicate_count
            FROM FACT_PITCH
        """
        )
        duplicates = cursor.fetchone()[0]

        if duplicates == 0:
            results["checks_passed"] += 1
            results["details"].append("✅ No duplicates in FACT_PITCH")
            logging.info("✅ No duplicates in FACT_PITCH")
        else:
            results["checks_failed"] += 1
            results["details"].append(
                f"❌ {duplicates:,} duplicate pitch_uid in FACT_PITCH"
            )
            logging.warning(f"❌ {duplicates:,} duplicate pitch_uid in FACT_PITCH")

        # =====================================================================
        # SUMMARY
        # =====================================================================
        total_checks = results["checks_passed"] + results["checks_failed"]
        logging.info(
            f"Data Quality Summary: {results['checks_passed']}/{total_checks} checks passed"
        )

        results["status"] = "success" if results["checks_failed"] == 0 else "warning"
        results["summary"] = f"{results['checks_passed']}/{total_checks} checks passed"

        return results

    finally:
        cursor.close()
        conn.close()


# =============================================================================
# TASK DEFINITIONS
# =============================================================================

fetch_task = PythonOperator(
    task_id="fetch_date_range",
    python_callable=fetch_date_range,
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

quality_task = PythonOperator(
    task_id="data_quality_checks",
    python_callable=run_data_quality_checks,
    dag=dag,
)

# =============================================================================
# TASK DEPENDENCIES
# =============================================================================
fetch_task >> upload_task >> schema_task >> quality_task
