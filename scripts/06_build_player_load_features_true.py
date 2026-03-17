import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_load_features_true AS
SELECT
    player_id,
    competition_id,
    season_id,
    match_id,
    match_date,
    minutes,

    SUM(minutes) OVER (
        PARTITION BY player_id
        ORDER BY match_date
        RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW
    ) AS minutes_last_7d,

    SUM(minutes) OVER (
        PARTITION BY player_id
        ORDER BY match_date
        RANGE BETWEEN INTERVAL 14 DAY PRECEDING AND CURRENT ROW
    ) AS minutes_last_14d,

    SUM(minutes) OVER (
        PARTITION BY player_id
        ORDER BY match_date
        RANGE BETWEEN INTERVAL 28 DAY PRECEDING AND CURRENT ROW
    ) AS minutes_last_28d,

    SUM(minutes) OVER (
        PARTITION BY player_id
        ORDER BY match_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS minutes_last_5_matches

FROM player_match_features_true_time
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_load_features_true").fetchall())

print("Sanity:", con.execute("""
SELECT 
    MIN(minutes_last_7d),
    MAX(minutes_last_7d),
    MIN(minutes_last_28d),
    MAX(minutes_last_28d)
FROM player_load_features_true
""").fetchall())

con.close()
