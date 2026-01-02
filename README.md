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

## Top MVP Candidates (Today)

```sql
SELECT TOP 10
    stat_date,
    player_name,
    team_abbr,
    mvp_score_z
FROM nba.v_mvp_race_z_latest
ORDER BY mvp_score_z DESC;


## Biggest Movers (MVP Momentum)

Identifies players with the largest day-over-day changes in MVP ranking,
highlighting short-term momentum shifts.

```sql
SELECT TOP 10
    stat_date,
    player_name,
    team_abbr,
    mvp_score_z,
    rank_yesterday,
    rank_today,
    rank_change
FROM nba.v_mvp_race_momentum
WHERE rank_change IS NOT NULL
ORDER BY ABS(rank_change) DESC, mvp_score_z DESC;



