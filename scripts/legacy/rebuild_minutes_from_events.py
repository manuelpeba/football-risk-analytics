import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_match_minutes_v2 AS
WITH max_minute AS (
    SELECT
        competition_id,
        season_id,
        match_id,
        MAX(minute) AS match_max_minute
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
    (p.last_minute - p.first_minute) AS approx_minutes
FROM player_first_last p
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_match_minutes_v2").fetchall())
print("Minutes sanity:", con.execute("SELECT MIN(approx_minutes), MAX(approx_minutes) FROM player_match_minutes_v2").fetchall())

con.close()
