import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_acwr_true_seasonal AS
SELECT
    *,
    CASE
      WHEN minutes_last_28d >= 180
      THEN minutes_last_7d / (minutes_last_28d / 4.0)
      ELSE NULL
    END AS acwr
FROM (
    SELECT
        player_id,
        competition_id,
        season_id,
        match_id,
        match_date,
        minutes,

        SUM(minutes) OVER (
            PARTITION BY player_id, season_id
            ORDER BY match_date
            RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW
        ) AS minutes_last_7d,

        SUM(minutes) OVER (
            PARTITION BY player_id, season_id
            ORDER BY match_date
            RANGE BETWEEN INTERVAL 28 DAY PRECEDING AND CURRENT ROW
        ) AS minutes_last_28d

    FROM player_match_features_true_time
)
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_acwr_true_seasonal").fetchall())

print("Valid share:", con.execute("""
SELECT AVG(CASE WHEN acwr IS NOT NULL THEN 1 ELSE 0 END)
FROM player_acwr_true_seasonal
""").fetchall())

print("ACWR sanity:", con.execute("""
SELECT MIN(acwr), MAX(acwr), AVG(acwr)
FROM player_acwr_true_seasonal
WHERE acwr IS NOT NULL
""").fetchall())

print("High risk share (>1.5):", con.execute("""
SELECT AVG(CASE WHEN acwr > 1.5 THEN 1 ELSE 0 END)
FROM player_acwr_true_seasonal
WHERE acwr IS NOT NULL
""").fetchall())

con.close()
