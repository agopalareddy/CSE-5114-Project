-- =============================================================================
-- SIMULATION_PITCHES TABLE DDL
-- =============================================================================
-- This table stores pitch data from live game simulations.
-- Data is streamed from Kafka via Spark Structured Streaming.
-- Dashboard reads from this table for live simulation display.
-- =============================================================================

-- Create table for simulation pitch data
CREATE OR REPLACE TABLE MLB_STATCAST.RAW.SIMULATION_PITCHES (
    -- Identifiers
    GAME_PK             INTEGER NOT NULL,
    AT_BAT_NUMBER       INTEGER,
    PITCH_NUMBER        INTEGER,
    
    -- Game context
    INNING              INTEGER,
    INNING_TOPBOT       VARCHAR(10),
    HOME_TEAM           VARCHAR(10),
    AWAY_TEAM           VARCHAR(10),
    HOME_SCORE          INTEGER,
    AWAY_SCORE          INTEGER,
    
    -- Players
    BATTER              INTEGER,
    PITCHER             INTEGER,
    PLAYER_NAME         VARCHAR(100),
    
    -- Pitch data
    PITCH_TYPE          VARCHAR(10),
    PITCH_NAME          VARCHAR(50),
    RELEASE_SPEED       FLOAT,
    PLATE_X             FLOAT,
    PLATE_Z             FLOAT,
    
    -- Count
    BALLS               INTEGER,
    STRIKES             INTEGER,
    OUTS_WHEN_UP        INTEGER,
    
    -- Result
    TYPE                VARCHAR(5),
    EVENTS              VARCHAR(100),
    DESCRIPTION         VARCHAR(200),
    
    -- Runners
    ON_1B               INTEGER,
    ON_2B               INTEGER,
    ON_3B               INTEGER,
    
    -- Metadata
    SIMULATION_TIMESTAMP TIMESTAMP_NTZ,
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Primary key
    PRIMARY KEY (GAME_PK, AT_BAT_NUMBER, PITCH_NUMBER, SIMULATION_TIMESTAMP)
);

-- Create index for fast lookups by game
-- CREATE INDEX IF NOT EXISTS idx_simulation_game ON SIMULATION_PITCHES(GAME_PK);

-- Create a view for the latest simulation state
CREATE OR REPLACE VIEW MLB_STATCAST.RAW.V_CURRENT_SIMULATION AS
WITH latest_game AS (
    SELECT MAX(GAME_PK) as current_game_pk
    FROM MLB_STATCAST.RAW.SIMULATION_PITCHES
    WHERE LOADED_AT > DATEADD(hour, -1, CURRENT_TIMESTAMP())
)
SELECT sp.*
FROM MLB_STATCAST.RAW.SIMULATION_PITCHES sp
JOIN latest_game lg ON sp.GAME_PK = lg.current_game_pk
ORDER BY sp.AT_BAT_NUMBER, sp.PITCH_NUMBER;

-- Procedure to clear simulation data (start fresh)
CREATE OR REPLACE PROCEDURE MLB_STATCAST.RAW.CLEAR_SIMULATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM MLB_STATCAST.RAW.SIMULATION_PITCHES
    WHERE LOADED_AT < DATEADD(hour, -24, CURRENT_TIMESTAMP());
    RETURN 'Cleared old simulation data';
END;
$$;

-- Grant permissions (adjust as needed)
-- GRANT SELECT ON MLB_STATCAST.RAW.SIMULATION_PITCHES TO ROLE PUBLIC;
-- GRANT SELECT ON MLB_STATCAST.RAW.V_CURRENT_SIMULATION TO ROLE PUBLIC;
