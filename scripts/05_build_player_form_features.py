import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_form_features AS
SELECT
    player_id,
    competition_id,
    season_id,
    match_id,
    match_date,

    xg,
    shots,
    progressive_x,

    -- Rolling últimos 5 partidos
    SUM(xg) OVER (
        PARTITION BY player_id, season_id
        ORDER BY match_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS xg_last_5,

    SUM(shots) OVER (
        PARTITION BY player_id, season_id
        ORDER BY match_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS shots_last_5,

    SUM(progressive_x) OVER (
        PARTITION BY player_id, season_id
        ORDER BY match_date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS progressive_last_5,

    -- Trend últimos 3 vs anteriores 3
    (
      SUM(xg) OVER (
        PARTITION BY player_id, season_id
        ORDER BY match_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      )
      -
      SUM(xg) OVER (
        PARTITION BY player_id, season_id
        ORDER BY match_date
        ROWS BETWEEN 5 PRECEDING AND 3 PRECEDING
      )
    ) AS trend_xg_3v3

FROM player_match_features_true_time
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_form_features").fetchall())

print("Sanity (xg_last_5 max):", con.execute("""
SELECT MAX(xg_last_5) FROM player_form_features
""").fetchall())

con.close()
