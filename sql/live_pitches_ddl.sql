-- ============================================
-- LIVE_PITCHES Table for Streaming Data
-- ============================================
-- This table stores real-time pitch data from the streaming pipeline.
-- Structure mirrors the source STATCAST table for dashboard compatibility.

CREATE TABLE IF NOT EXISTS LIVE_PITCHES (
    -- Game identifiers
    game_pk INTEGER,
    game_date VARCHAR(20),
    
    -- At-bat identifiers
    at_bat_number INTEGER,
    pitch_number INTEGER,
    inning INTEGER,
    inning_topbot VARCHAR(10),
    
    -- Player identifiers
    pitcher INTEGER,
    pitcher_name VARCHAR(100),
    batter INTEGER,
    
    -- Team info
    home_team VARCHAR(10),
    away_team VARCHAR(10),
    home_score INTEGER,
    away_score INTEGER,
    
    -- Pitch characteristics
    pitch_type VARCHAR(10),
    pitch_name VARCHAR(50),
    release_speed FLOAT,
    release_spin_rate FLOAT,
    pfx_x FLOAT,
    pfx_z FLOAT,
    
    -- Pitch location
    plate_x FLOAT,
    plate_z FLOAT,
    zone INTEGER,
    sz_top FLOAT,
    sz_bot FLOAT,
    
    -- Outcome
    type VARCHAR(5),
    events VARCHAR(50),
    description VARCHAR(100),
    balls INTEGER,
    strikes INTEGER,
    
    -- Streaming metadata
    _streaming_timestamp TIMESTAMP_NTZ,
    _event_type VARCHAR(20),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    batch_source VARCHAR(50),
    
    -- Primary key for deduplication
    CONSTRAINT pk_live_pitches PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);

-- Create index for common query patterns
CREATE INDEX IF NOT EXISTS idx_live_pitches_game_date ON LIVE_PITCHES(game_date);
CREATE INDEX IF NOT EXISTS idx_live_pitches_game_pk ON LIVE_PITCHES(game_pk);
CREATE INDEX IF NOT EXISTS idx_live_pitches_processed ON LIVE_PITCHES(processed_at);

-- View to get the most recent pitches (last 5 minutes)
CREATE OR REPLACE VIEW LIVE_PITCHES_RECENT AS
SELECT *
FROM LIVE_PITCHES
WHERE processed_at > DATEADD('minute', -5, CURRENT_TIMESTAMP())
ORDER BY processed_at DESC;

-- View for current game state (most recent pitch per active game)
CREATE OR REPLACE VIEW LIVE_GAME_STATE AS
SELECT 
    game_pk,
    game_date,
    home_team,
    away_team,
    home_score,
    away_score,
    inning,
    inning_topbot,
    balls,
    strikes,
    COUNT(*) as total_pitches,
    MAX(processed_at) as last_updated
FROM LIVE_PITCHES
WHERE processed_at > DATEADD('hour', -4, CURRENT_TIMESTAMP())
GROUP BY game_pk, game_date, home_team, away_team, home_score, away_score, 
         inning, inning_topbot, balls, strikes
ORDER BY last_updated DESC;

-- Grant permissions (adjust roles as needed)
-- GRANT SELECT ON LIVE_PITCHES TO ROLE PUBLIC;
-- GRANT SELECT ON LIVE_PITCHES_RECENT TO ROLE PUBLIC;
-- GRANT SELECT ON LIVE_GAME_STATE TO ROLE PUBLIC;

-- Cleanup command (run manually to clear old data)
-- DELETE FROM LIVE_PITCHES WHERE processed_at < DATEADD('day', -1, CURRENT_TIMESTAMP());

COMMENT ON TABLE LIVE_PITCHES IS 'Real-time pitch data from Kafka streaming pipeline';
