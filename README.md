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
```

## Biggest Movers (MVP Momentum)

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
```

## Player Comparison (Head-to-Head)

Compares two players across core performance metrics over the season.
Useful for narrative analysis (e.g., MVP debates, player impact discussions).

```sql
SELECT
    p.player_name,
    t.team_abbr,
    COUNT(*) AS games_played,
    ROUND(AVG(f.pts), 1) AS avg_points,
    ROUND(AVG(f.reb), 1) AS avg_rebounds,
    ROUND(AVG(f.ast), 1) AS avg_assists,
    ROUND(AVG(f.ts_pct), 3) AS true_shooting_pct,
    ROUND(AVG(f.bpm), 2) AS avg_bpm,
    ROUND(AVG(f.vorp), 2) AS total_vorp
FROM fact_player_daily_stats f
JOIN dim_player p ON f.player_id = p.player_id
JOIN dim_team t ON f.team_id = t.team_id
WHERE p.player_name IN ('Nikola Jokic', 'Shai Gilgeous-Alexander')
GROUP BY p.player_name, t.team_abbr
ORDER BY total_vorp DESC;
```

## Player Performance Trend (Momentum Over Time)

Tracks a single player’s rolling performance to identify upward or downward momentum
throughout the season.

```sql
SELECT
    stat_date,
    player_name,
    team_abbr,
    pts,
    reb,
    ast,
    mvp_score_z,
    ROUND(
        AVG(mvp_score_z) OVER (
            PARTITION BY player_name
            ORDER BY stat_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 3
    ) AS rolling_7_day_mvp_score
FROM nba.v_mvp_race_z_latest
WHERE player_name = 'Nikola Jokic'
ORDER BY stat_date;
```



