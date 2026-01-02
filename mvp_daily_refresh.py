import datetime as dt
import os
import shutil
import time
from io import StringIO

import pandas as pd
import pyodbc
import requests


# -----------------------
# CONFIG
# -----------------------
SQL_SERVER = r"LAPTOP-1989DQS8\SQLEXPRESS"
SQL_DATABASE = "nba_analytics"
SOURCE_NAME = "basketball_reference_per_game_auto"

DATA_DIR = "data"
CSV_PATH = os.path.join(DATA_DIR, "per_game.csv")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")

CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    f"Server={SQL_SERVER};"
    f"Database={SQL_DATABASE};"
    "Trusted_Connection=yes;"
)


# -----------------------
# TEXT FIX (mojibake)
# -----------------------
def fix_mojibake(s):
    """
    Fix strings like 'JokiÄ?' -> 'Jokić' when UTF-8 text got decoded as cp1252/latin1.
    Safe to run on all names.
    """
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)

    s = s.strip()

    # common mojibake markers
    if any(x in s for x in ["Ã", "Ä", "Å", "Â", "â€™", "â€œ", "â€"]):
        try:
            return s.encode("latin1").decode("utf-8")
        except Exception:
            return s
    return s


# -----------------------
# URL HELPERS
# -----------------------
def season_end_year(today: dt.date) -> int:
    return today.year + 1 if today.month >= 10 else today.year


def br_per_game_url(today: dt.date) -> str:
    end_year = season_end_year(today)
    return f"https://www.basketball-reference.com/leagues/NBA_{end_year}_per_game.html"


# -----------------------
# DATA HELPERS
# -----------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def df_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    df_obj = df.astype(object)
    return df_obj.where(pd.notnull(df_obj), None)


def download_per_game_table(today: dt.date) -> pd.DataFrame:
    url = br_per_game_url(today)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NBA-MVP-Tracker/1.0"}

    last_err = None
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()

            # IMPORTANT: force correct decoding
            resp.encoding = "utf-8"

            tables = pd.read_html(StringIO(resp.text))

            for t in tables:
                cols = [str(c).strip().lower() for c in t.columns]
                if "player" in cols:
                    return t

            raise ValueError("Could not find a table with a 'Player' column on the page.")

        except Exception as e:
            last_err = e
            time.sleep(2 * attempt)

    raise RuntimeError(f"Failed to download/parse per-game table: {last_err}")


# -----------------------
# MAIN
# -----------------------
def main():
    today = dt.date.today()

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    # 1) EXTRACT
    raw = download_per_game_table(today)

    # 2) Save latest CSV (with BOM for Excel + consistent read-back)
    raw.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    # 3) Archive daily snapshot
    archive_path = os.path.join(ARCHIVE_DIR, f"per_game_{today.isoformat()}.csv")
    shutil.copyfile(CSV_PATH, archive_path)

    # 4) TRANSFORM: read back using SAME encoding you wrote
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    df = normalize_columns(df)

    rename_map = {
        "player": "player_name",
        "tm": "team_abbr",
        "team": "team_abbr",
        "team_id": "team_abbr",
        "t": "team_abbr",
        "g": "gp",
        "mp": "min",
        "mp_per_g": "min",
        "pts": "pts",
        "pts_per_g": "pts",
        "trb": "reb",
        "trb_per_g": "reb",
        "ast": "ast",
        "ast_per_g": "ast",
        "stl": "stl",
        "stl_per_g": "stl",
        "blk": "blk",
        "blk_per_g": "blk",
        "tov": "tov",
        "tov_per_g": "tov",
        "fg%": "fg_pct",
        "3p%": "fg3_pct",
        "ft%": "ft_pct",
        "ts%": "ts_pct",
    }
    df = df.rename(columns=rename_map)

    # Add metadata
    df["stat_date"] = today
    df["source_name"] = SOURCE_NAME

    required_cols = [
        "stat_date", "player_name", "team_abbr",
        "gp", "min", "pts", "reb", "ast", "stl", "blk", "tov",
        "fg_pct", "fg3_pct", "ft_pct",
        "ts_pct", "bpm", "ws", "vorp",
        "source_name"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df = df[required_cols].copy()

    # Clean repeated header rows
    df = df[df["player_name"].notna()]
    df = df[df["player_name"].astype(str).str.lower() != "player"]

    # FIX NAMES + TEAMS (THIS IS THE KEY CHANGE)
    df["player_name"] = df["player_name"].apply(fix_mojibake)
    df["team_abbr"] = df["team_abbr"].apply(fix_mojibake)

    # Dedupe: prefer TOT row per player per day
    df["team_sort_key"] = df["team_abbr"].apply(lambda x: 0 if str(x).upper() == "TOT" else 1)
    df = df.sort_values(["player_name", "team_sort_key"]).drop_duplicates(
        subset=["stat_date", "player_name"], keep="first"
    )
    df = df.drop(columns=["team_sort_key"])

    # Numeric conversion
    numeric_cols = [
        "gp", "min", "pts", "reb", "ast", "stl", "blk", "tov",
        "fg_pct", "fg3_pct", "ft_pct",
        "ts_pct", "bpm", "ws", "vorp"
    ]
    for c in numeric_cols:
        df[c] = to_num(df[c])

    # NaN -> None (SQL NULL)
    df = df_nan_to_none(df)

    # 5) LOAD
    conn = pyodbc.connect(CONN_STR)
    try:
        cur = conn.cursor()
        cur.fast_executemany = True

        insert_sql = """
            INSERT INTO nba.stg_player_daily_stats (
                stat_date, player_name, team_abbr,
                gp, min, pts, reb, ast, stl, blk, tov,
                fg_pct, fg3_pct, ft_pct,
                ts_pct, bpm, ws, vorp,
                source_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        cur.executemany(insert_sql, df.itertuples(index=False, name=None))

        # Upsert dims
        cur.execute("""
            INSERT INTO nba.dim_team (team_abbr)
            SELECT DISTINCT s.team_abbr
            FROM nba.stg_player_daily_stats s
            WHERE s.team_abbr IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM nba.dim_team t WHERE t.team_abbr = s.team_abbr
              );
        """)

        cur.execute("""
            INSERT INTO nba.dim_player (player_name)
            SELECT DISTINCT s.player_name
            FROM nba.stg_player_daily_stats s
            WHERE s.player_name IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM nba.dim_player p WHERE p.player_name = s.player_name
              );
        """)

        # Merge fact
        cur.execute("""
            MERGE nba.fact_player_daily_stats AS tgt
            USING (
                SELECT
                    s.stat_date,
                    p.player_id,
                    t.team_id,

                    TRY_CONVERT(INT, s.gp) AS gp,
                    TRY_CONVERT(DECIMAL(6,2), s.min) AS min,
                    TRY_CONVERT(DECIMAL(6,2), s.pts) AS pts,
                    TRY_CONVERT(DECIMAL(6,2), s.reb) AS reb,
                    TRY_CONVERT(DECIMAL(6,2), s.ast) AS ast,
                    TRY_CONVERT(DECIMAL(6,2), s.stl) AS stl,
                    TRY_CONVERT(DECIMAL(6,2), s.blk) AS blk,
                    TRY_CONVERT(DECIMAL(6,2), s.tov) AS tov,

                    TRY_CONVERT(DECIMAL(6,4), s.fg_pct) AS fg_pct,
                    TRY_CONVERT(DECIMAL(6,4), s.fg3_pct) AS fg3_pct,
                    TRY_CONVERT(DECIMAL(6,4), s.ft_pct) AS ft_pct,

                    TRY_CONVERT(DECIMAL(6,4), s.ts_pct) AS ts_pct,
                    TRY_CONVERT(DECIMAL(6,3), s.bpm) AS bpm,
                    TRY_CONVERT(DECIMAL(6,3), s.ws) AS ws,
                    TRY_CONVERT(DECIMAL(6,3), s.vorp) AS vorp,

                    s.source_name
                FROM nba.stg_player_daily_stats s
                JOIN nba.dim_player p ON p.player_name = s.player_name
                LEFT JOIN nba.dim_team t ON t.team_abbr = s.team_abbr
            ) AS src
            ON (tgt.stat_date = src.stat_date AND tgt.player_id = src.player_id)

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.team_id = src.team_id,
                    tgt.gp = src.gp,
                    tgt.min = src.min,
                    tgt.pts = src.pts,
                    tgt.reb = src.reb,
                    tgt.ast = src.ast,
                    tgt.stl = src.stl,
                    tgt.blk = src.blk,
                    tgt.tov = src.tov,
                    tgt.fg_pct = src.fg_pct,
                    tgt.fg3_pct = src.fg3_pct,
                    tgt.ft_pct = src.ft_pct,
                    tgt.ts_pct = src.ts_pct,
                    tgt.bpm = src.bpm,
                    tgt.ws = src.ws,
                    tgt.vorp = src.vorp,
                    tgt.source_name = src.source_name,
                    tgt.loaded_at = SYSUTCDATETIME()

            WHEN NOT MATCHED THEN
                INSERT (
                    stat_date, player_id, team_id,
                    gp, min, pts, reb, ast, stl, blk, tov,
                    fg_pct, fg3_pct, ft_pct,
                    ts_pct, bpm, ws, vorp,
                    source_name
                )
                VALUES (
                    src.stat_date, src.player_id, src.team_id,
                    src.gp, src.min, src.pts, src.reb, src.ast, src.stl, src.blk, src.tov,
                    src.fg_pct, src.fg3_pct, src.ft_pct,
                    src.ts_pct, src.bpm, src.ws, src.vorp,
                    src.source_name
                );
        """)

        cur.execute("TRUNCATE TABLE nba.stg_player_daily_stats;")
        conn.commit()

        print("Auto-download + load completed successfully.")
        print(f"Saved latest CSV: {CSV_PATH}")
        print(f"Archived CSV: {archive_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

