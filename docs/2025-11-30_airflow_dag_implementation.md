# Airflow DAG Implementation Documentation

**Date:** November 30, 2025  
**Last Updated:** December 1, 2025  
**Authors:** Aadarsha Gopala Reddy, Eddy Sul (with GitHub Copilot assistance)

---

## Overview

This document details the implementation of an Apache Airflow DAG for automating the Statcast ETL pipeline. The DAG fetches MLB pitch-by-pitch data from the pybaseball API and loads it into the Snowflake star schema.

**Key Feature:** Incremental loading with MERGE (upsert) logic - no duplicates, updates existing records if data changes.

---

## What Was Accomplished

### 1. DAGs Created

Three DAGs were created for different use cases:

| DAG | Purpose | Use Case |
|-----|---------|----------|
| `statcast_historical_backfill` | **Automated** multi-season backfill | Loads 2020-2024 seasons automatically |
| `statcast_backfill` | Manual batch backfill for date ranges | Ad-hoc historical data loading |
| `statcast_etl` | Single-day incremental loads | Daily ETL during MLB season |

All DAGs use **MERGE (upsert)** logic for safe, idempotent data loading.

---

## DAG 1: `statcast_backfill`

**Location:** `dags/statcast_backfill_dag.py`

**Purpose:** Batch backfill Statcast data for a configurable date range in a single DAG run with intelligent deduplication.

### Architecture

```
fetch_date_range  →  upload_to_snowflake  →  update_star_schema
      ↓                      ↓                      ↓
 Loop day-by-day      MERGE into STATCAST     MERGE into DIM_GAME,
 via pybaseball       (upsert - no dupes)     DIM_PLAYER, FACT_PITCH
```

### Parameters (UI Form)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `start_date` | 2024-07-01 | Start of date range (YYYY-MM-DD) |
| `end_date` | 2024-07-07 | End of date range (YYYY-MM-DD) |

---

## DAG 2: `statcast_etl`

**Location:** `dags/statcast_etl_dag.py`

**Purpose:** Daily incremental ETL for loading single-day Statcast data during the MLB season.

### Architecture

```
fetch_statcast  →  upload_to_snowflake  →  populate_dimensions
      ↓                    ↓                      ↓
 Fetch single day   MERGE into STATCAST    MERGE into DIM_GAME,
 via pybaseball     (upsert - no dupes)    DIM_PLAYER, FACT_PITCH
```

### Parameters (UI Form)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `date` | Yesterday | Single date to fetch (YYYY-MM-DD) |

### Scheduling

- Currently set to `schedule_interval=None` (manual trigger)
- For production during MLB season, change to `schedule_interval="@daily"`

---

## DAG 3: `statcast_historical_backfill` (NEW)

**Location:** `dags/statcast_historical_backfill_dag.py`

**Purpose:** Fully automated backfill of all MLB seasons from 2020-2024. Runs every 3 minutes and processes one month per run.

### Architecture

```
check_progress (Branch)
    ├── fetch_data → upload_to_snowflake → update_star_schema → advance_to_next_month
    └── all_complete (when done)
```

### Seasons Covered

| Season | Months | Date Range | Notes |
|--------|--------|------------|-------|
| 2020 | 4 | Jul 23 - Oct 27 | COVID-shortened (60 games) |
| 2021 | 8 | Apr 1 - Nov 2 | Full season |
| 2022 | 8 | Apr 7 - Nov 5 | Full season |
| 2023 | 9 | Mar 30 - Nov 1 | Full season |
| 2024 | 9 | Mar 28 - Nov 2 | Full season |
| **Total** | **38 months** | | ~2 hours to complete |

### Key Features

- **Runs every 3 minutes** (`*/3 * * * *`)
- **One month per run** - processes sequentially through all 38 months
- **Progress tracking** via Airflow Variable: `statcast_historical_current_month`
- **Checks existing data** before fetching (MERGE handles updates)
- **Auto-advances** to next month on success
- **Self-stopping** when all months complete

### Progress Monitoring

```
Admin → Variables → statcast_historical_current_month
```

| Value | Meaning |
|-------|--------|
| 0 | Starting (2020 July) |
| 4 | 2021 April |
| 12 | 2022 April |
| 20 | 2023 March |
| 29 | 2024 March |
| 38 | Complete |

### To Reset/Re-run

```
Admin → Variables → statcast_historical_current_month → Set to 0
```

---

## Common Features (All DAGs)

### Incremental Loading (MERGE/Upsert)

The DAG now uses Snowflake's `MERGE` statement for all data loading:

| Scenario | Behavior |
|----------|----------|
| New records | Inserted |
| Existing records (same data) | No action (skipped) |
| Existing records (data changed) | Updated |
| Re-running same date range | No duplicates created |

**Unique Keys Used:**
- `STATCAST`: `game_pk` + `at_bat_number` + `pitch_number`
- `DIM_GAME`: `game_pk`
- `DIM_PLAYER`: `player_id`
- `FACT_PITCH`: `pitch_uid` (composite key)

### Configuration (Both DAGs)

```python
SNOWFLAKE_CONN_ID = "snowflake"      # Airflow connection ID
DATABASE = "BLUEJAY_DB"               # Snowflake database
SCHEMA = "BLUEJAY_SCHEMA"             # Snowflake schema
```

---

## Successful Runs (`statcast_backfill`)

### Run 1: Initial Load (November 30, 2025)

**Date Range:** July 3-7, 2024 (5 days)

| Table | Records Added |
|-------|---------------|
| DIM_GAME | +267 games |
| DIM_PLAYER (pitchers) | +258 |
| DIM_PLAYER (batters) | +111 |
| FACT_PITCH | +22,367 pitches |

### Run 2: Re-run with MERGE (December 1, 2025)

**Date Range:** Same July 3-7, 2024 - demonstrates MERGE upsert working

| Table | Records Merged |
|-------|----------------|
| DIM_GAME | 326 rows merged |
| DIM_PLAYER (pitchers) | 409 rows merged |
| DIM_PLAYER (batters) | 431 rows merged |
| FACT_PITCH | 27,664 rows merged |

**Note:** "Merged" means insert OR update - no duplicates created despite re-running the same date range.

### Run 3: Extended Backfill with Data Quality Checks (December 1, 2025)

**Date Range:** July 1-14, 2024 (14 days)

**Data Quality Results:** ✅ **14/14 checks passed**

| Table | Final Row Count |
|-------|-----------------|
| STATCAST | 55,167 rows |
| DIM_GAME | 421 rows |
| DIM_PLAYER | 905 rows |
| DIM_DATE | 8,036 rows |
| FACT_PITCH | 55,167 rows |

| Quality Check | Result |
|---------------|--------|
| Row counts (5 tables) | ✅ All have data |
| Null checks (5 columns) | ✅ No nulls |
| Referential integrity (game_pk) | ✅ Pass |
| Referential integrity (pitcher_id) | ✅ Pass |
| Data freshness | ✅ 2024-07-01 to 2024-07-14 |
| Duplicates | ✅ None |

---

## Automated Historical Backfill Runs (`statcast_historical_backfill`)

### Run 1: 2020 July (December 1, 2025)

**Period:** 2020-07-23 to 2020-07-31 (9 days)

| Metric | Value |
|--------|-------|
| **Pitches Fetched** | 19,585 |
| Days Successful | 6 of 9 |
| Days Failed (API errors) | 3 (July 26, 27, 28) |

**API Errors Encountered:**
```
ERROR: Error tokenizing data. C error: Expected 1 fields in line 13, saw 2
```

This is a **known intermittent issue** with the MLB Statcast API - sometimes returns malformed CSV data. The DAG continued processing other days and will advance to the next month.

**Days Loaded:**
- ✅ July 23: 431 pitches
- ✅ July 24: 4,020 pitches  
- ✅ July 25: 4,358 pitches
- ❌ July 26: API error (skipped)
- ❌ July 27: API error (skipped)
- ❌ July 28: API error (skipped)
- ✅ July 29: 4,392 pitches
- ✅ July 30: 2,682 pitches
- ✅ July 31: 3,702 pitches

**Note:** Missing days can be backfilled later using the manual `statcast_backfill` DAG.

---

## How to Use the DAGs

### `statcast_backfill` - For Date Ranges

#### Trigger via Airflow UI (Recommended)

1. Go to Airflow UI → DAGs → `statcast_backfill`
2. Click **Trigger DAG w/ config** (play button with gear icon)
3. You'll see a form with `start_date` and `end_date` fields
4. Edit the dates to your desired range
5. Click **Trigger**

#### Trigger via CLI

```bash
airflow dags trigger statcast_backfill --conf '{"start_date": "2024-03-28", "end_date": "2024-04-07"}'
```

### `statcast_etl` - For Single Days

#### Trigger via Airflow UI

1. Go to Airflow UI → DAGs → `statcast_etl`
2. Click **Trigger DAG w/ config**
3. You'll see a form with `date` field (defaults to yesterday)
4. Edit the date if needed
5. Click **Trigger**

#### Trigger via CLI

```bash
airflow dags trigger statcast_etl --conf '{"date": "2024-07-15"}'
```

#### Enable Daily Schedule (Production)

To run automatically every day during MLB season, edit the DAG and change:
```python
schedule_interval="@daily"  # Instead of None
```

---

## Recommended Backfill Strategy

For the full 2024 MLB season, backfill in chunks to avoid API timeouts:

| Chunk | Start Date | End Date | Approx. Pitches |
|-------|------------|----------|-----------------|
| 1 | 2024-03-28 | 2024-04-30 | ~150,000 |
| 2 | 2024-05-01 | 2024-05-31 | ~150,000 |
| 3 | 2024-06-01 | 2024-06-30 | ~150,000 |
| 4 | 2024-07-01 | 2024-07-31 | ~150,000 |
| 5 | 2024-08-01 | 2024-08-31 | ~150,000 |
| 6 | 2024-09-01 | 2024-09-30 | ~150,000 |
| 7 | 2024-10-01 | 2024-10-30 | ~50,000 |

**Total estimated:** ~900,000 pitches for full season

---

## Infrastructure Details

### Airflow Environment

- **Platform:** WashU Open OnDemand - Academic Apache Airflow
- **Host:** `iht32-1501.engr.wustl.edu`
- **Python Version:** 3.9.20
- **Snowflake Connector:** 3.12.3

### DAG Location on Server

```
~/airflow/dags/statcast/statcast_backfill_dag.py
```

This is a symlink to:
```
~/CSE 5114 Project/dags/statcast_backfill_dag.py
```

### Snowflake Connection

- **Connection ID:** `snowflake`
- **Authentication:** JWT with private key file
- **Database:** `BLUEJAY_DB`
- **Schema:** `BLUEJAY_SCHEMA`
- **Warehouse:** `BLUEJAY_WH`
- **Role:** `TRAINING_ROLE`

---

## Issues Encountered & Solutions

| Issue | Error | Solution |
|-------|-------|----------|
| Missing `pybaseball` | `ModuleNotFoundError: No module named 'pybaseball'` | `pip install pybaseball>=2.2.7` on Airflow server |
| Missing `python-dotenv` | `ModuleNotFoundError: No module named 'dotenv'` | Refactored to use Airflow's SnowflakeHook instead |
| Private key format error | `Could not deserialize key data` | Switched from manual `.env` credentials to Airflow connection |
| Wrong connection ID | `conn_id 'snowflake_conn' isn't defined` | Changed to `snowflake` (from `airflow connections list`) |
| Wrong database/schema | N/A | Updated to `BLUEJAY_DB.BLUEJAY_SCHEMA` |
| Duplicate row in MERGE | `Duplicate row detected during DML action` | Used `QUALIFY ROW_NUMBER()` to dedupe source data before MERGE |

---

## Technical Details: MERGE Deduplication

The raw STATCAST data can have "duplicate" rows per key due to:
- **DIM_GAME:** Fielder substitutions create multiple rows per `game_pk` with different fielder IDs
- **DIM_PLAYER:** Same player appears with different ages across games
- **FACT_PITCH:** Potential duplicates from overlapping API calls

**Solution:** Use Snowflake's `QUALIFY ROW_NUMBER() OVER (PARTITION BY <key>)` to select exactly one row per key before the MERGE.

```sql
-- Example: DIM_GAME deduplication
SELECT ...
FROM STATCAST
QUALIFY ROW_NUMBER() OVER (PARTITION BY "game_pk" ORDER BY "game_date") = 1
```

---

## Files Created/Modified

### DAG Files
| File | Status | Description |
|------|--------|-------------|
| `dags/statcast_historical_backfill_dag.py` | ✅ Working | **Automated** 2020-2024 backfill (3-min intervals) |
| `dags/statcast_backfill_dag.py` | ✅ Working | Manual batch backfill for date ranges |
| `dags/statcast_etl_dag.py` | ✅ Updated | Daily ETL (updated to match backfill patterns) |

### Key Design Decisions

1. **Used Airflow's SnowflakeHook** instead of custom `snowflake_connect.py` for better integration with Airflow's connection management

2. **Temp file storage** instead of XCom for large DataFrames (Parquet format for efficiency)

3. **Day-by-day fetching** to avoid MLB API timeouts, then combine

4. **MERGE (upsert) for all tables** - enables safe re-runs without duplicates, updates changed data

5. **QUALIFY ROW_NUMBER()** to deduplicate source data before MERGE (Snowflake-specific optimization)

6. **Staging table pattern** for STATCAST uploads - data goes to temp table first, then MERGE into main table

---

## Sample Log Output (Successful MERGE Run)

```
[2025-12-01, 00:53:57 UTC] INFO - Merging DIM_GAME...
[2025-12-01, 00:53:58 UTC] INFO - DIM_GAME: 326 rows merged (insert/update)
[2025-12-01, 00:53:58 UTC] INFO - Merging DIM_PLAYER (pitchers)...
[2025-12-01, 00:53:59 UTC] INFO - DIM_PLAYER (pitchers): 409 rows merged
[2025-12-01, 00:53:59 UTC] INFO - Merging DIM_PLAYER (batters)...
[2025-12-01, 00:54:00 UTC] INFO - DIM_PLAYER (batters): 431 rows merged
[2025-12-01, 00:54:00 UTC] INFO - Merging FACT_PITCH...
[2025-12-01, 00:54:02 UTC] INFO - FACT_PITCH: 27664 rows merged (insert/update)
[2025-12-01, 00:54:02 UTC] INFO - Done. Returned value was: {
    'games_merged': 326, 
    'pitchers_merged': 409, 
    'batters_merged': 431, 
    'facts_merged': 27664, 
    'status': 'success'
}
```

---

## Next Steps

| Task | Status |
|------|--------|
| Star Schema DDL | ✅ Complete |
| Snowflake Deployment | ✅ Complete |
| Backfill DAG (`statcast_backfill`) | ✅ Working |
| Daily ETL DAG (`statcast_etl`) | ✅ Updated |
| Incremental Loading (MERGE) | ✅ Working |
| Data Quality Checks | ✅ Working (14/14 passed) |
| **Automated Historical Backfill DAG** | ✅ Running (2020-2024) |
| Set up daily schedule for 2025 | ⬜ Future (when season starts) |
| Implement live streaming (Kafka/Flink) | ⬜ Future |

---

## Data Quality Checks

Both DAGs now include a `data_quality_checks` task that runs after the star schema update.

### Checks Performed

| Check | Description | Pass Criteria |
|-------|-------------|---------------|
| **Row Counts** | Verify all tables have data | Count > 0 |
| **Null Checks** | Critical columns not null | `pitch_uid`, `game_pk`, `pitcher_id`, `batter_id` have 0 nulls |
| **Referential Integrity** | FACT_PITCH.game_pk exists in DIM_GAME | 0 orphan records |
| **Referential Integrity** | FACT_PITCH.pitcher_id exists in DIM_PLAYER | 0 orphan records |
| **Data Freshness** | Date range of loaded data | Reports min/max dates |
| **Duplicates** | No duplicate pitch_uid in FACT_PITCH | 0 duplicates |

### Updated DAG Architecture

```
fetch_date_range  →  upload_to_snowflake  →  update_star_schema  →  data_quality_checks
      ↓                      ↓                      ↓                      ↓
 Loop day-by-day      MERGE into STATCAST     MERGE into DIM_*      Run all checks,
 via pybaseball       (upsert - no dupes)     and FACT_PITCH        log results
```

### Sample Quality Check Output

```
[2025-12-01, 01:00:00 UTC] INFO - Running row count checks...
[2025-12-01, 01:00:01 UTC] INFO - ✅ STATCAST: 27,664 rows
[2025-12-01, 01:00:01 UTC] INFO - ✅ DIM_GAME: 326 rows
[2025-12-01, 01:00:01 UTC] INFO - ✅ DIM_PLAYER: 840 rows
[2025-12-01, 01:00:01 UTC] INFO - ✅ FACT_PITCH: 27,664 rows
[2025-12-01, 01:00:01 UTC] INFO - Running null checks...
[2025-12-01, 01:00:02 UTC] INFO - ✅ FACT_PITCH.pitch_uid: No nulls
[2025-12-01, 01:00:02 UTC] INFO - ✅ FACT_PITCH.game_pk: No nulls
[2025-12-01, 01:00:02 UTC] INFO - Running referential integrity checks...
[2025-12-01, 01:00:03 UTC] INFO - ✅ All game_pk have matching DIM_GAME
[2025-12-01, 01:00:03 UTC] INFO - Checking for duplicates...
[2025-12-01, 01:00:04 UTC] INFO - ✅ No duplicates in FACT_PITCH
[2025-12-01, 01:00:04 UTC] INFO - Data Quality: 10/10 checks passed
```

---

## Verification Queries

```sql
-- Check row counts after backfill
SELECT 'DIM_DATE' AS tbl, COUNT(*) AS row_count FROM DIM_DATE
UNION ALL SELECT 'DIM_GAME', COUNT(*) FROM DIM_GAME
UNION ALL SELECT 'DIM_PLAYER', COUNT(*) FROM DIM_PLAYER
UNION ALL SELECT 'FACT_PITCH', COUNT(*) FROM FACT_PITCH
UNION ALL SELECT 'STATCAST (raw)', COUNT(*) FROM STATCAST;

-- Check data by date
SELECT TO_DATE("game_date") as game_date, COUNT(*) as pitches
FROM STATCAST
GROUP BY TO_DATE("game_date")
ORDER BY game_date;

-- Test analytical views
SELECT * FROM VW_PITCHER_SUMMARY ORDER BY total_pitches DESC LIMIT 10;
SELECT * FROM VW_PITCH_TYPE_ANALYSIS ORDER BY total_thrown DESC;
```
