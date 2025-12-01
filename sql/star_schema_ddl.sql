-- =============================================================================
-- MLB STATCAST STAR SCHEMA DDL
-- =============================================================================
-- This script creates a star schema for MLB pitch-by-pitch data
-- Fact Table: FACT_PITCH (grain = one pitch)
-- Dimension Tables: DIM_GAME, DIM_PLAYER, DIM_PITCH_TYPE, DIM_DATE
-- =============================================================================

-- Use the appropriate database and schema
-- USE DATABASE your_database;
-- USE SCHEMA your_schema;

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIM_DATE: Date dimension for time-based analysis
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_DATE (
    date_key            INTEGER PRIMARY KEY,        -- YYYYMMDD format
    game_date           DATE NOT NULL,
    game_year           INTEGER NOT NULL,
    game_month          INTEGER NOT NULL,
    game_day            INTEGER NOT NULL,
    day_of_week         VARCHAR(10),                -- Monday, Tuesday, etc.
    week_of_year        INTEGER,
    is_weekend          BOOLEAN,
    season_phase        VARCHAR(20)                 -- Spring Training, Regular, Postseason
);

-- -----------------------------------------------------------------------------
-- DIM_GAME: Game-level information (static for entire game)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_GAME (
    game_key            INTEGER PRIMARY KEY AUTOINCREMENT,
    game_pk             INTEGER NOT NULL UNIQUE,    -- MLB Game ID (natural key)
    game_date           DATE NOT NULL,
    game_year           INTEGER NOT NULL,
    game_type           VARCHAR(1),                 -- E=Exhibition, S=Spring, R=Regular, F=WC, D=Div, L=LCS, W=WS
    home_team           VARCHAR(3) NOT NULL,
    away_team           VARCHAR(3) NOT NULL,
    
    -- Fielder positions (MLB Player IDs for defensive lineup)
    fielder_2           INTEGER,                    -- Catcher
    fielder_3           INTEGER,                    -- 1B
    fielder_4           INTEGER,                    -- 2B
    fielder_5           INTEGER,                    -- 3B
    fielder_6           INTEGER,                    -- SS
    fielder_7           INTEGER,                    -- LF
    fielder_8           INTEGER,                    -- CF
    fielder_9           INTEGER,                    -- RF
    
    -- Metadata
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- DIM_PLAYER: Player dimension (batters and pitchers)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PLAYER (
    player_key          INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id           INTEGER NOT NULL UNIQUE,    -- MLB Player ID (natural key)
    player_name         VARCHAR(100),
    
    -- Player attributes (can be updated over time)
    throws              VARCHAR(1),                 -- L/R
    bats                VARCHAR(1),                 -- L/R/S
    
    -- Current age (as of last update)
    age                 INTEGER,
    
    -- Metadata
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- DIM_PITCH_TYPE: Pitch type reference
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PITCH_TYPE (
    pitch_type_key      INTEGER PRIMARY KEY AUTOINCREMENT,
    pitch_type_code     VARCHAR(10) NOT NULL UNIQUE,    -- FF, SL, CU, etc.
    pitch_name          VARCHAR(50),                     -- 4-Seam Fastball, Slider, etc.
    pitch_category      VARCHAR(20)                      -- Fastball, Breaking, Offspeed
);

-- Seed pitch type dimension with common pitch types
INSERT INTO DIM_PITCH_TYPE (pitch_type_code, pitch_name, pitch_category) VALUES
    ('FF', '4-Seam Fastball', 'Fastball'),
    ('SI', 'Sinker', 'Fastball'),
    ('FC', 'Cutter', 'Fastball'),
    ('SL', 'Slider', 'Breaking'),
    ('CU', 'Curveball', 'Breaking'),
    ('KC', 'Knuckle Curve', 'Breaking'),
    ('SV', 'Sweeper', 'Breaking'),
    ('CH', 'Changeup', 'Offspeed'),
    ('FS', 'Splitter', 'Offspeed'),
    ('KN', 'Knuckleball', 'Offspeed'),
    ('EP', 'Eephus', 'Offspeed'),
    ('SC', 'Screwball', 'Breaking'),
    ('ST', 'Sweeping Curve', 'Breaking'),
    ('FA', 'Fastball', 'Fastball'),
    ('PO', 'Pitchout', 'Other'),
    ('IN', 'Intentional Ball', 'Other'),
    ('UN', 'Unknown', 'Other');

-- -----------------------------------------------------------------------------
-- DIM_TEAM: Team reference dimension
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_TEAM (
    team_key            INTEGER PRIMARY KEY AUTOINCREMENT,
    team_abbr           VARCHAR(3) NOT NULL UNIQUE,
    team_name           VARCHAR(50),
    league              VARCHAR(2),                 -- AL or NL
    division            VARCHAR(10)                 -- East, Central, West
);

-- Seed team dimension with MLB teams
INSERT INTO DIM_TEAM (team_abbr, team_name, league, division) VALUES
    ('ARI', 'Arizona Diamondbacks', 'NL', 'West'),
    ('ATL', 'Atlanta Braves', 'NL', 'East'),
    ('BAL', 'Baltimore Orioles', 'AL', 'East'),
    ('BOS', 'Boston Red Sox', 'AL', 'East'),
    ('CHC', 'Chicago Cubs', 'NL', 'Central'),
    ('CWS', 'Chicago White Sox', 'AL', 'Central'),
    ('CIN', 'Cincinnati Reds', 'NL', 'Central'),
    ('CLE', 'Cleveland Guardians', 'AL', 'Central'),
    ('COL', 'Colorado Rockies', 'NL', 'West'),
    ('DET', 'Detroit Tigers', 'AL', 'Central'),
    ('HOU', 'Houston Astros', 'AL', 'West'),
    ('KC', 'Kansas City Royals', 'AL', 'Central'),
    ('LAA', 'Los Angeles Angels', 'AL', 'West'),
    ('LAD', 'Los Angeles Dodgers', 'NL', 'West'),
    ('MIA', 'Miami Marlins', 'NL', 'East'),
    ('MIL', 'Milwaukee Brewers', 'NL', 'Central'),
    ('MIN', 'Minnesota Twins', 'AL', 'Central'),
    ('NYM', 'New York Mets', 'NL', 'East'),
    ('NYY', 'New York Yankees', 'AL', 'East'),
    ('OAK', 'Oakland Athletics', 'AL', 'West'),
    ('PHI', 'Philadelphia Phillies', 'NL', 'East'),
    ('PIT', 'Pittsburgh Pirates', 'NL', 'Central'),
    ('SD', 'San Diego Padres', 'NL', 'West'),
    ('SF', 'San Francisco Giants', 'NL', 'West'),
    ('SEA', 'Seattle Mariners', 'AL', 'West'),
    ('STL', 'St. Louis Cardinals', 'NL', 'Central'),
    ('TB', 'Tampa Bay Rays', 'AL', 'East'),
    ('TEX', 'Texas Rangers', 'AL', 'West'),
    ('TOR', 'Toronto Blue Jays', 'AL', 'East'),
    ('WSH', 'Washington Nationals', 'NL', 'East');

-- =============================================================================
-- FACT TABLE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- FACT_PITCH: One row per pitch (grain = individual pitch)
-- Composite Key: game_pk + at_bat_number + pitch_number
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_PITCH (
    -- Primary Key (composite)
    pitch_uid           VARCHAR(50) PRIMARY KEY,    -- game_pk_at_bat_number_pitch_number
    
    -- Foreign Keys to Dimensions
    game_key            INTEGER REFERENCES DIM_GAME(game_key),
    date_key            INTEGER REFERENCES DIM_DATE(date_key),
    pitcher_key         INTEGER REFERENCES DIM_PLAYER(player_key),
    batter_key          INTEGER REFERENCES DIM_PLAYER(player_key),
    pitch_type_key      INTEGER REFERENCES DIM_PITCH_TYPE(pitch_type_key),
    home_team_key       INTEGER REFERENCES DIM_TEAM(team_key),
    away_team_key       INTEGER REFERENCES DIM_TEAM(team_key),
    
    -- Degenerate Dimensions (IDs from source)
    game_pk             INTEGER NOT NULL,
    at_bat_number       INTEGER NOT NULL,
    pitch_number        INTEGER NOT NULL,
    pitcher_id          INTEGER NOT NULL,
    batter_id           INTEGER NOT NULL,
    
    -- ==========================================================================
    -- GAME SITUATION (Dynamic State before pitch)
    -- ==========================================================================
    inning              INTEGER,
    inning_topbot       VARCHAR(3),                 -- Top or Bot
    balls               INTEGER,
    strikes             INTEGER,
    outs_when_up        INTEGER,
    
    -- Baserunners (MLB Player IDs, NULL if base empty)
    on_1b               INTEGER,
    on_2b               INTEGER,
    on_3b               INTEGER,
    
    -- Score before pitch
    home_score          INTEGER,
    away_score          INTEGER,
    home_score_diff     INTEGER,
    bat_score_diff      INTEGER,
    
    -- Win Probability before pitch
    home_win_exp        FLOAT,
    bat_win_exp         FLOAT,
    
    -- Player state
    stand               VARCHAR(1),                 -- Batter stance (L/R)
    p_throws            VARCHAR(1),                 -- Pitcher hand (L/R)
    n_thruorder_pitcher INTEGER,
    n_priorpa_thisgame_player_at_bat INTEGER,
    age_pit             INTEGER,
    age_bat             INTEGER,
    
    -- Defensive Alignment
    if_fielding_alignment VARCHAR(20),
    of_fielding_alignment VARCHAR(20),
    
    -- ==========================================================================
    -- PITCH EVENT MEASURES
    -- ==========================================================================
    pitch_type          VARCHAR(10),
    pitch_name          VARCHAR(50),
    release_speed       FLOAT,                      -- MPH
    release_spin_rate   INTEGER,                    -- RPM
    release_extension   FLOAT,                      -- Feet
    spin_axis           INTEGER,                    -- Degrees
    
    -- Pitch Movement
    pfx_x               FLOAT,                      -- Horizontal movement (ft)
    pfx_z               FLOAT,                      -- Vertical movement (ft)
    
    -- Strike Zone
    zone                INTEGER,                    -- 1-14 zone number
    sz_top              FLOAT,                      -- Top of zone
    sz_bot              FLOAT,                      -- Bottom of zone
    plate_x             FLOAT,                      -- Horizontal plate position
    plate_z             FLOAT,                      -- Vertical plate position
    
    -- ==========================================================================
    -- BATTED BALL EVENT MEASURES
    -- ==========================================================================
    launch_speed        FLOAT,                      -- Exit velocity (MPH)
    launch_angle        FLOAT,                      -- Degrees
    bb_type             VARCHAR(20),                -- ground_ball, line_drive, fly_ball, popup
    hit_distance_sc     FLOAT,                      -- Projected distance (ft)
    launch_speed_angle  INTEGER,                    -- Category 1-6
    
    -- Bat Tracking (2024+ data)
    bat_speed           FLOAT,
    swing_length        FLOAT,
    attack_angle        FLOAT,
    arm_angle           FLOAT,
    attack_direction    FLOAT,
    swing_path_tilt     FLOAT,
    
    -- ==========================================================================
    -- PLAY OUTCOME
    -- ==========================================================================
    type                VARCHAR(1),                 -- B=ball, S=strike, X=in play
    description         VARCHAR(50),                -- e.g., 'swinging_strike', 'called_strike'
    events              VARCHAR(50),                -- Final result: 'single', 'strikeout', etc.
    des                 VARCHAR(500),               -- Full play description
    hit_location        INTEGER,                    -- Fielder position number
    
    -- Score after pitch
    post_home_score     INTEGER,
    post_away_score     INTEGER,
    
    -- ==========================================================================
    -- METADATA
    -- ==========================================================================
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- CLUSTERING KEYS FOR PERFORMANCE (Snowflake alternative to indexes)
-- =============================================================================
-- Snowflake uses micro-partitions and clustering keys instead of traditional indexes.
-- Clustering is recommended for large tables (100GB+). For smaller tables, 
-- Snowflake's automatic optimization is usually sufficient.

-- Uncomment these if your FACT_PITCH table grows very large:
-- ALTER TABLE FACT_PITCH CLUSTER BY (game_pk, pitcher_id, date_key);
-- ALTER TABLE FACT_PITCH CLUSTER BY (batter_id);

-- =============================================================================
-- VIEWS FOR COMMON QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- View: Pitcher Performance Summary
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_PITCHER_SUMMARY AS
SELECT 
    p.player_name,
    p.player_id,
    g.game_year,
    COUNT(*) AS total_pitches,
    AVG(f.release_speed) AS avg_velocity,
    AVG(f.release_spin_rate) AS avg_spin_rate,
    SUM(CASE WHEN f.type = 'S' THEN 1 ELSE 0 END) AS strikes,
    SUM(CASE WHEN f.type = 'B' THEN 1 ELSE 0 END) AS balls,
    SUM(CASE WHEN f.events = 'strikeout' THEN 1 ELSE 0 END) AS strikeouts,
    SUM(CASE WHEN f.events IN ('single', 'double', 'triple', 'home_run') THEN 1 ELSE 0 END) AS hits_allowed
FROM FACT_PITCH f
JOIN DIM_PLAYER p ON f.pitcher_id = p.player_id
JOIN DIM_GAME g ON f.game_pk = g.game_pk
GROUP BY p.player_name, p.player_id, g.game_year;

-- -----------------------------------------------------------------------------
-- View: Batter Performance Summary
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_BATTER_SUMMARY AS
SELECT 
    p.player_name,
    p.player_id,
    g.game_year,
    COUNT(DISTINCT f.at_bat_number || '_' || f.game_pk) AS plate_appearances,
    AVG(f.launch_speed) AS avg_exit_velocity,
    AVG(f.launch_angle) AS avg_launch_angle,
    AVG(f.bat_speed) AS avg_bat_speed,
    SUM(CASE WHEN f.events = 'single' THEN 1 ELSE 0 END) AS singles,
    SUM(CASE WHEN f.events = 'double' THEN 1 ELSE 0 END) AS doubles,
    SUM(CASE WHEN f.events = 'triple' THEN 1 ELSE 0 END) AS triples,
    SUM(CASE WHEN f.events = 'home_run' THEN 1 ELSE 0 END) AS home_runs,
    SUM(CASE WHEN f.events = 'strikeout' THEN 1 ELSE 0 END) AS strikeouts
FROM FACT_PITCH f
JOIN DIM_PLAYER p ON f.batter_id = p.player_id
JOIN DIM_GAME g ON f.game_pk = g.game_pk
WHERE f.events IS NOT NULL
GROUP BY p.player_name, p.player_id, g.game_year;

-- -----------------------------------------------------------------------------
-- View: Pitch Type Analysis
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_PITCH_TYPE_ANALYSIS AS
SELECT 
    pt.pitch_name,
    pt.pitch_category,
    COUNT(*) AS total_thrown,
    AVG(f.release_speed) AS avg_velocity,
    AVG(f.release_spin_rate) AS avg_spin,
    AVG(f.pfx_x) AS avg_horizontal_movement,
    AVG(f.pfx_z) AS avg_vertical_movement,
    SUM(CASE WHEN f.type = 'S' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS strike_pct,
    SUM(CASE WHEN f.description LIKE '%swinging%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS whiff_pct
FROM FACT_PITCH f
JOIN DIM_PITCH_TYPE pt ON f.pitch_type = pt.pitch_type_code
GROUP BY pt.pitch_name, pt.pitch_category;

-- =============================================================================
-- END OF STAR SCHEMA DDL
-- =============================================================================
