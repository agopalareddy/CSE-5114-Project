-- =============================================================================
-- SNOWFLAKE STREAMING TABLES AND VIEWS
-- =============================================================================
-- Run these in Snowflake to support the streaming pipeline

USE DATABASE BLUEJAY_DB;
USE SCHEMA BLUEJAY_SCHEMA;

-- =============================================================================
-- STREAMING LANDING TABLE
-- =============================================================================
-- This table receives data from Spark Streaming

CREATE TABLE IF NOT EXISTS STATCAST_STREAMING (
    -- Composite key
    pitch_uid               VARCHAR(50) PRIMARY KEY,
    
    -- Core identifiers
    game_pk                 INTEGER,
    at_bat_number           INTEGER,
    pitch_number            INTEGER,
    
    -- Game info
    game_date               DATE,
    game_type               VARCHAR(10),
    home_team               VARCHAR(10),
    away_team               VARCHAR(10),
    
    -- Player info
    pitcher                 INTEGER,
    batter                  INTEGER,
    player_name             VARCHAR(100),
    
    -- Pitch characteristics
    pitch_type              VARCHAR(10),
    pitch_name              VARCHAR(50),
    release_speed           FLOAT,
    release_spin_rate       FLOAT,
    release_extension       FLOAT,
    release_pos_x           FLOAT,
    release_pos_z           FLOAT,
    
    -- Movement
    pfx_x                   FLOAT,
    pfx_z                   FLOAT,
    plate_x                 FLOAT,
    plate_z                 FLOAT,
    spin_axis               FLOAT,
    
    -- Pitch result
    type                    VARCHAR(10),
    events                  VARCHAR(50),
    description             VARCHAR(100),
    zone                    INTEGER,
    
    -- Game state
    balls                   INTEGER,
    strikes                 INTEGER,
    outs_when_up            INTEGER,
    inning                  INTEGER,
    inning_topbot           VARCHAR(10),
    
    -- Runners
    on_1b                   INTEGER,
    on_2b                   INTEGER,
    on_3b                   INTEGER,
    
    -- Scores
    home_score              INTEGER,
    away_score              INTEGER,
    
    -- Batted ball data
    launch_speed            FLOAT,
    launch_angle            FLOAT,
    hit_distance_sc         FLOAT,
    
    -- Metadata
    processed_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    kafka_offset            BIGINT,
    kafka_partition         INTEGER
);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_streaming_processed_at 
    ON STATCAST_STREAMING (processed_at);

-- Index for game queries
CREATE INDEX IF NOT EXISTS idx_streaming_game 
    ON STATCAST_STREAMING (game_pk, game_date);


-- =============================================================================
-- DYNAMIC TABLES (Auto-Refreshing Materialized Views)
-- =============================================================================
-- These automatically update as new data arrives

-- Live Pitcher Statistics (refreshes every 1 minute)
CREATE OR REPLACE DYNAMIC TABLE DT_LIVE_PITCHER_STATS
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
    AS
    SELECT 
        pitcher,
        pitch_type,
        COUNT(*) AS pitch_count,
        ROUND(AVG(release_speed), 1) AS avg_velocity,
        ROUND(MAX(release_speed), 1) AS max_velocity,
        ROUND(AVG(release_spin_rate), 0) AS avg_spin_rate,
        ROUND(AVG(pfx_x), 2) AS avg_h_break,
        ROUND(AVG(pfx_z), 2) AS avg_v_break,
        MAX(processed_at) AS last_update
    FROM STATCAST_STREAMING
    WHERE game_date = CURRENT_DATE()
    GROUP BY pitcher, pitch_type;


-- Live Game Scores (refreshes every 30 seconds)
CREATE OR REPLACE DYNAMIC TABLE DT_LIVE_GAMES
    TARGET_LAG = '30 seconds'
    WAREHOUSE = COMPUTE_WH
    AS
    SELECT 
        game_pk,
        home_team,
        away_team,
        MAX(home_score) AS home_score,
        MAX(away_score) AS away_score,
        MAX(inning) AS current_inning,
        MAX(inning_topbot) AS half_inning,
        COUNT(*) AS total_pitches,
        MAX(processed_at) AS last_update
    FROM STATCAST_STREAMING
    WHERE game_date = CURRENT_DATE()
    GROUP BY game_pk, home_team, away_team;


-- Pitch Type Distribution (refreshes every 2 minutes)
CREATE OR REPLACE DYNAMIC TABLE DT_PITCH_TYPE_DIST
    TARGET_LAG = '2 minutes'
    WAREHOUSE = COMPUTE_WH
    AS
    SELECT 
        game_date,
        pitch_type,
        pitch_name,
        COUNT(*) AS total_pitches,
        ROUND(AVG(release_speed), 1) AS avg_velocity,
        ROUND(AVG(release_spin_rate), 0) AS avg_spin,
        ROUND(100.0 * SUM(CASE WHEN type = 'S' THEN 1 ELSE 0 END) / COUNT(*), 1) AS strike_pct,
        ROUND(100.0 * SUM(CASE WHEN description LIKE '%swinging%' THEN 1 ELSE 0 END) / COUNT(*), 1) AS whiff_pct
    FROM STATCAST_STREAMING
    WHERE game_date >= CURRENT_DATE() - 7
    GROUP BY game_date, pitch_type, pitch_name;


-- =============================================================================
-- STREAMS (Change Data Capture)
-- =============================================================================
-- Capture changes to the streaming table

CREATE OR REPLACE STREAM STREAM_STATCAST_CHANGES
    ON TABLE STATCAST_STREAMING
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE;


-- =============================================================================
-- TASKS (Scheduled Processing)
-- =============================================================================
-- Process stream data on a schedule

-- Task to merge streaming data into fact table
CREATE OR REPLACE TASK TASK_MERGE_STREAMING_TO_FACT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_STATCAST_CHANGES')
    AS
    MERGE INTO FACT_PITCH f
    USING STREAM_STATCAST_CHANGES s
    ON f.pitch_uid = s.pitch_uid
    WHEN NOT MATCHED THEN
        INSERT (
            pitch_uid, game_key, date_key, pitcher_key, batter_key,
            pitch_type_key, pitch_number, at_bat_number,
            release_speed, release_spin_rate, release_extension,
            pfx_x, pfx_z, plate_x, plate_z,
            zone, balls, strikes, outs_when_up,
            inning, inning_topbot, pitch_result, events
        )
        VALUES (
            s.pitch_uid,
            (SELECT game_key FROM DIM_GAME WHERE game_pk = s.game_pk),
            TO_NUMBER(TO_CHAR(s.game_date, 'YYYYMMDD')),
            (SELECT player_key FROM DIM_PLAYER WHERE player_id = s.pitcher),
            (SELECT player_key FROM DIM_PLAYER WHERE player_id = s.batter),
            (SELECT pitch_type_key FROM DIM_PITCH_TYPE WHERE pitch_type_code = s.pitch_type),
            s.pitch_number, s.at_bat_number,
            s.release_speed, s.release_spin_rate, s.release_extension,
            s.pfx_x, s.pfx_z, s.plate_x, s.plate_z,
            s.zone, s.balls, s.strikes, s.outs_when_up,
            s.inning, s.inning_topbot, s.type, s.events
        );

-- Resume the task (tasks are created in suspended state)
-- ALTER TASK TASK_MERGE_STREAMING_TO_FACT RESUME;


-- =============================================================================
-- VIEWS FOR DASHBOARD QUERIES
-- =============================================================================

-- Real-time pitch velocity by pitcher (last 2 hours)
CREATE OR REPLACE VIEW V_LIVE_VELOCITY AS
SELECT 
    pitcher,
    player_name,
    pitch_type,
    processed_at,
    release_speed,
    AVG(release_speed) OVER (
        PARTITION BY pitcher, pitch_type 
        ORDER BY processed_at 
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_velocity
FROM STATCAST_STREAMING
WHERE processed_at > DATEADD(hour, -2, CURRENT_TIMESTAMP())
ORDER BY processed_at DESC;


-- Current at-bat info
CREATE OR REPLACE VIEW V_CURRENT_AT_BAT AS
SELECT *
FROM STATCAST_STREAMING
WHERE (game_pk, at_bat_number) IN (
    SELECT game_pk, MAX(at_bat_number)
    FROM STATCAST_STREAMING
    WHERE game_date = CURRENT_DATE()
    GROUP BY game_pk
)
ORDER BY pitch_number DESC;


-- Strike zone heatmap data
CREATE OR REPLACE VIEW V_STRIKE_ZONE_HEATMAP AS
SELECT 
    pitcher,
    pitch_type,
    ROUND(plate_x, 1) AS zone_x,
    ROUND(plate_z, 1) AS zone_z,
    COUNT(*) AS pitch_count,
    ROUND(AVG(release_speed), 1) AS avg_velocity,
    SUM(CASE WHEN type = 'S' THEN 1 ELSE 0 END) AS strikes,
    SUM(CASE WHEN type = 'B' THEN 1 ELSE 0 END) AS balls
FROM STATCAST_STREAMING
WHERE game_date = CURRENT_DATE()
  AND plate_x IS NOT NULL
  AND plate_z IS NOT NULL
GROUP BY pitcher, pitch_type, ROUND(plate_x, 1), ROUND(plate_z, 1);


-- =============================================================================
-- GRANT PERMISSIONS (if using roles)
-- =============================================================================
-- GRANT SELECT ON ALL TABLES IN SCHEMA BLUEJAY_SCHEMA TO ROLE DASHBOARD_ROLE;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA BLUEJAY_SCHEMA TO ROLE DASHBOARD_ROLE;
-- GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA BLUEJAY_SCHEMA TO ROLE DASHBOARD_ROLE;
