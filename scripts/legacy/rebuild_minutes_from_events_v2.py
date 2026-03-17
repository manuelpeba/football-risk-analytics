import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_match_minutes_v2 AS
WITH match_len AS (
    SELECT
        competition_id,
        season_id,
        match_id,
        LEAST(MAX(minute), 130) AS match_max_minute
    FROM read_parquet('lakehouse/bronze/events_flat/*/*/*.parquet')
    GROUP BY 1,2,3
),
player_first_last AS (
    SELECT
        competition_id,
        season_id,
        match_id,
        player_id,
        MIN(minute) AS first_minute,
        MAX(minute) AS last_minute
    FROM read_parquet('lakehouse/bronze/events_flat/*/*/*.parquet')
    WHERE player_id IS NOT NULL
    GROUP BY 1,2,3,4
)
SELECT
    p.competition_id,
    p.season_id,
    p.match_id,
    p.player_id,
    p.first_minute,
    p.last_minute,
    -- minutos aproximados acotados
    GREATEST(
        0,
        LEAST(p.last_minute, m.match_max_minute) - GREATEST(p.first_minute, 0)
    ) AS approx_minutes,
    m.match_max_minute AS match_minutes_cap
FROM player_first_last p
JOIN match_len m
  ON p.competition_id=m.competition_id
 AND p.season_id=m.season_id
 AND p.match_id=m.match_id
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_match_minutes_v2").fetchall())
print("Minutes sanity:", con.execute("SELECT MIN(approx_minutes), MAX(approx_minutes) FROM player_match_minutes_v2").fetchall())
print("Match length sanity:", con.execute("SELECT MIN(match_minutes_cap), MAX(match_minutes_cap) FROM player_match_minutes_v2").fetchall())

con.close()
