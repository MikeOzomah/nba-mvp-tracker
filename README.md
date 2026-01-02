# NBA MVP Tracker (Daily Refresh)

A small ETL pipeline that:
1) pulls NBA per-game stats from Basketball-Reference,
2) saves a daily CSV snapshot + archive copy,
3) loads data into SQL Server (dims + fact),
4) powers SQL views for MVP ranking + momentum.

## Why This Project
This project explores how MVP narratives can diverge from traditional
media rankings by applying standardized statistical scoring and daily
momentum tracking.

## Data Engineering Pipeline

This project implements an end-to-end ETL pipeline:

- **Extract**: Pulls NBA per-game player stats from Basketball-Reference
- **Transform**:
  - Standardizes column names
  - Deduplicates multi-team (TOT) rows
  - Casts numeric types and handles NULLs
- **Load**:
  - Loads data into SQL Server using a star schema
  - Uses idempotent MERGE statements for daily upserts
- **Analytics Layer**:
  - SQL views compute MVP z-scores, rankings, and momentum

## Database Design

**Schema Overview**
- `stg_player_daily_stats` — raw daily ingestion (staging)
- `dim_player` — unique players
- `dim_team` — NBA teams
- `fact_player_daily_stats` — daily player performance metrics

This structure supports historical analysis, trend tracking,
and repeatable daily loads.

## SQL Portfolio Examples

### Top MVP Candidates (Today)
```sql
SELECT TOP 10
    stat_date,
    player_name,
    team_abbr,
    mvp_score_z
FROM nba.v_mvp_race_z_latest
ORDER BY mvp_score_z DESC;


## Automation

- Python pipeline runs daily via Windows Task Scheduler
- Each run:
  - Saves a CSV snapshot
  - Archives historical data
  - Updates SQL analytics views

## Tech Stack

- Python (requests, pandas, pyodbc)
- SQL Server (star schema, MERGE upserts, analytics views)
- GitHub (version control + documentation)
- Windows Task Scheduler (automation)

## Data Flow
**Basketball-Reference HTML** → `data/per_game.csv` + `data/archive/per_game_YYYY-MM-DD.csv`  
→ `nba.stg_player_daily_stats` (raw strings)  
→ upsert `nba.dim_player`, `nba.dim_team`  
→ merge into `nba.fact_player_daily_stats` (typed numeric columns)  
→ analytics views (latest ranking, z-score ranking, momentum)

## Database Objects (Schema)
### Tables
- `nba.dim_player (player_id, player_name)`
- `nba.dim_team (team_id, team_abbr)`
- `nba.stg_player_daily_stats` (raw load, varchar columns)
- `nba.fact_player_daily_stats` (typed numeric stats by date/player/team)

### Views
- `nba.v_mvp_race_latest`
- `nba.v_mvp_race_z_latest`
- `nba.v_mvp_race_momentum`
- `nba.v_mvp_race_contenders`

## Setup
1) Create and activate a virtual environment
2) Install dependencies:
   ```bash
   pip install -r requirements.txt
