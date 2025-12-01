# Star Schema Implementation Documentation

**Date:** November 30, 2025  
**Time:** ~5:45 PM CST  
**Authors:** Aadarsha Gopala Reddy, Eddy Sul (with GitHub Copilot assistance)

---

## Overview

This document details the implementation of a star schema for MLB Statcast pitch-by-pitch data in Snowflake. The schema transforms raw pitch data into a dimensional model optimized for analytical queries.

---

## What Was Accomplished

### 1. Star Schema Design & DDL Creation

Created `sql/star_schema_ddl.sql` containing:

#### Dimension Tables
| Table | Purpose | Key |
|-------|---------|-----|
| `DIM_DATE` | Time-based analysis (date, year, month, day, season phase) | `date_key` (YYYYMMDD) |
| `DIM_GAME` | Game-level info (teams, fielders, game type) | `game_pk` (MLB Game ID) |
| `DIM_PLAYER` | Player details (name, throws/bats, age) | `player_id` (MLB Player ID) |
| `DIM_PITCH_TYPE` | Pitch type reference (FF, SL, CU, etc.) | `pitch_type_code` |
| `DIM_TEAM` | MLB team reference (30 teams) | `team_abbr` |

#### Fact Table
| Table | Grain | Primary Key |
|-------|-------|-------------|
| `FACT_PITCH` | One row per pitch | `pitch_uid` (game_pk + at_bat_number + pitch_number) |

#### Analytical Views
- `VW_PITCHER_SUMMARY` - Pitcher performance aggregations
- `VW_BATTER_SUMMARY` - Batter performance aggregations
- `VW_PITCH_TYPE_ANALYSIS` - Pitch type breakdown with strike/whiff rates

### 2. Pre-Seeded Reference Data

- **17 pitch types** with categories (Fastball, Breaking, Offspeed, Other)
- **30 MLB teams** with league and division assignments

### 3. Raw Data Upload

- Fixed `pybaseball` version issue (upgraded from 2.0.0 to >=2.2.7)
- Successfully uploaded **5,297 pitches** from July 1-2, 2024 to `STATCAST` table
- Data covers **59 games** with **469 unique players**

### 4. Dimension & Fact Table Population

Populated all tables from raw `STATCAST` data:

| Table | Row Count |
|-------|-----------|
| DIM_DATE | 8,036 |
| DIM_GAME | 59 |
| DIM_PLAYER | 469 |
| DIM_PITCH_TYPE | 17 |
| DIM_TEAM | 30 |
| FACT_PITCH | 5,297 |

---

## Technical Details

### Snowflake Considerations

1. **No Traditional Indexes**: Snowflake uses micro-partitions instead of indexes. Added commented clustering key suggestions for future large-scale data.

2. **Case Sensitivity**: Raw data columns are lowercase. Must use double quotes (`"game_pk"`) when querying `STATCAST` table.

3. **Reserved Keywords**: Avoided using `rows` as an alias (Snowflake reserved word).

### Data Type Mappings

Key columns that required casting:
- `game_pk`: VARCHAR → INTEGER (for DIM_GAME FK)
- `at_bat_number`: VARCHAR → INTEGER
- `pitch_number`: VARCHAR → INTEGER

---

## Files Created/Modified

### New Files
- `sql/star_schema_ddl.sql` - Complete DDL for star schema

### Modified Files
- `requirements.txt` - Updated `pybaseball==2.0.0` to `pybaseball>=2.2.7`

---

## SQL Scripts Used

### Populate DIM_DATE
```sql
INSERT INTO DIM_DATE (date_key, game_date, game_year, game_month, game_day, 
                      day_of_week, week_of_year, is_weekend, season_phase)
SELECT 
    TO_NUMBER(TO_CHAR(d.date_val, 'YYYYMMDD')) AS date_key,
    d.date_val AS game_date,
    YEAR(d.date_val) AS game_year,
    MONTH(d.date_val) AS game_month,
    DAY(d.date_val) AS game_day,
    DAYNAME(d.date_val) AS day_of_week,
    WEEKOFYEAR(d.date_val) AS week_of_year,
    CASE WHEN DAYOFWEEK(d.date_val) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE 
        WHEN MONTH(d.date_val) IN (2, 3) THEN 'Spring Training'
        WHEN MONTH(d.date_val) BETWEEN 4 AND 9 THEN 'Regular Season'
        WHEN MONTH(d.date_val) IN (10, 11) THEN 'Postseason'
        ELSE 'Offseason'
    END AS season_phase
FROM (
    SELECT DATEADD(day, SEQ4(), '2020-01-01'::DATE) AS date_val
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))
) d
WHERE d.date_val <= '2030-12-31';
```

### Populate DIM_GAME
```sql
INSERT INTO DIM_GAME (game_pk, game_date, game_year, game_type, home_team, away_team,
                      fielder_2, fielder_3, fielder_4, fielder_5, fielder_6, 
                      fielder_7, fielder_8, fielder_9)
SELECT DISTINCT
    "game_pk"::INTEGER,
    TO_DATE("game_date") AS game_date,
    "game_year",
    "game_type",
    "home_team",
    "away_team",
    "fielder_2", "fielder_3", "fielder_4", "fielder_5", "fielder_6",
    "fielder_7", "fielder_8", "fielder_9"
FROM STATCAST;
```

### Populate DIM_PLAYER
```sql
-- Insert pitchers
INSERT INTO DIM_PLAYER (player_id, player_name, throws, age)
SELECT DISTINCT
    "pitcher" AS player_id,
    "player_name",
    "p_throws" AS throws,
    "age_pit" AS age
FROM STATCAST;

-- Insert batters (avoid duplicates)
INSERT INTO DIM_PLAYER (player_id, bats, age)
SELECT DISTINCT
    "batter" AS player_id,
    "stand" AS bats,
    "age_bat" AS age
FROM STATCAST
WHERE "batter" NOT IN (SELECT player_id FROM DIM_PLAYER);
```

### Populate FACT_PITCH
```sql
INSERT INTO FACT_PITCH (
    pitch_uid, game_pk, at_bat_number, pitch_number,
    pitcher_id, batter_id, pitch_type, pitch_name,
    inning, inning_topbot, balls, strikes, outs_when_up,
    on_1b, on_2b, on_3b,
    home_score, away_score, home_score_diff, bat_score_diff,
    home_win_exp, bat_win_exp,
    stand, p_throws, age_pit, age_bat,
    n_thruorder_pitcher, n_priorpa_thisgame_player_at_bat,
    if_fielding_alignment, of_fielding_alignment,
    release_speed, release_spin_rate, release_extension, spin_axis,
    pfx_x, pfx_z, zone, sz_top, sz_bot, plate_x, plate_z,
    launch_speed, launch_angle, bb_type, hit_distance_sc, launch_speed_angle,
    bat_speed, swing_length, attack_angle, arm_angle, attack_direction, swing_path_tilt,
    type, description, events, des, hit_location,
    post_home_score, post_away_score,
    date_key
)
SELECT 
    CONCAT("game_pk", '_', "at_bat_number", '_', "pitch_number") AS pitch_uid,
    "game_pk"::INTEGER, "at_bat_number"::INTEGER, "pitch_number"::INTEGER,
    "pitcher", "batter", "pitch_type", "pitch_name",
    "inning", "inning_topbot", "balls", "strikes", "outs_when_up",
    "on_1b", "on_2b", "on_3b",
    "home_score", "away_score", "home_score_diff", "bat_score_diff",
    "home_win_exp", "bat_win_exp",
    "stand", "p_throws", "age_pit", "age_bat",
    "n_thruorder_pitcher", "n_priorpa_thisgame_player_at_bat",
    "if_fielding_alignment", "of_fielding_alignment",
    "release_speed", "release_spin_rate", "release_extension", "spin_axis",
    "pfx_x", "pfx_z", "zone", "sz_top", "sz_bot", "plate_x", "plate_z",
    "launch_speed", "launch_angle", "bb_type", "hit_distance_sc", "launch_speed_angle",
    "bat_speed", "swing_length", "attack_angle", "arm_angle", "attack_direction", "swing_path_tilt",
    "type", "description", "events", "des", "hit_location",
    "post_home_score", "post_away_score",
    TO_NUMBER(TO_CHAR(TO_DATE("game_date"), 'YYYYMMDD')) AS date_key
FROM STATCAST;
```

---

## Issues Encountered & Solutions

| Issue | Solution |
|-------|----------|
| `pybaseball` KeyError on `pitcher.1`, `fielder_2.1` | Upgraded to `pybaseball>=2.2.7` |
| Snowflake index creation error | Removed indexes (Snowflake uses micro-partitions) |
| `STATCAST` table not found | Ran `python code.py` to upload raw data first |
| Column name case sensitivity | Wrapped lowercase columns in double quotes |
| `rows` reserved keyword | Changed alias to `row_count` |

---

## Next Steps

1. **Load Historical Data** - Expand from 2 days to full 2024 MLB season
2. **Build Airflow DAG** - Automate batch processing in `dags/` folder
3. **Create ETL Module** - Reusable transforms in `statcast_etl/`
4. **Live Streaming** - Kafka + Flink for 2025 season simulation

---

## Verification Queries

```sql
-- Check row counts
SELECT 'DIM_DATE' AS tbl, COUNT(*) AS row_count FROM DIM_DATE
UNION ALL SELECT 'DIM_GAME', COUNT(*) FROM DIM_GAME
UNION ALL SELECT 'DIM_PLAYER', COUNT(*) FROM DIM_PLAYER
UNION ALL SELECT 'DIM_PITCH_TYPE', COUNT(*) FROM DIM_PITCH_TYPE
UNION ALL SELECT 'DIM_TEAM', COUNT(*) FROM DIM_TEAM
UNION ALL SELECT 'FACT_PITCH', COUNT(*) FROM FACT_PITCH;

-- Test analytical views
SELECT * FROM VW_PITCHER_SUMMARY ORDER BY total_pitches DESC LIMIT 10;
SELECT * FROM VW_BATTER_SUMMARY ORDER BY home_runs DESC LIMIT 10;
SELECT * FROM VW_PITCH_TYPE_ANALYSIS ORDER BY total_thrown DESC;
```
