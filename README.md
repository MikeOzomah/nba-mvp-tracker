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


## Tech
- Python: requests, pandas, pyodbc
- SQL Server Express (star schema)
- Windows Task Scheduler for automation

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
