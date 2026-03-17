import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_dataset_final AS
SELECT
    f.player_id,
    f.competition_id,
    f.season_id,
    f.match_id,
    f.match_date,
    f.team,

    -- Minutos y rendimiento base
    f.minutes,
    f.shots_per90,
    f.xg_per90,
    f.passes_per90,
    f.carries_per90,
    f.progressive_x_per90,

    -- Rolling form
    pf.xg_last_5,
    pf.shots_last_5,
    pf.progressive_last_5,
    pf.trend_xg_3v3,

    -- Load
    pl.minutes_last_7d,
    pl.minutes_last_14d,
    pl.minutes_last_28d,
    pl.minutes_last_5_matches,

    -- ACWR
    pa.acwr,

    -- Target risk label
    CASE 
        WHEN pa.acwr > 1.5 THEN 1
        ELSE 0
    END AS high_risk

FROM player_match_features_true_time f
LEFT JOIN player_form_features pf
  ON f.player_id = pf.player_id
 AND f.match_id = pf.match_id
LEFT JOIN player_load_features_true pl
  ON f.player_id = pl.player_id
 AND f.match_id = pl.match_id
LEFT JOIN player_acwr_true pa
  ON f.player_id = pa.player_id
 AND f.match_id = pa.match_id
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_dataset_final").fetchall())

print("Risk share:", con.execute("""
SELECT AVG(high_risk) FROM player_dataset_final
""").fetchall())

print("Null ACWR share:", con.execute("""
SELECT AVG(CASE WHEN acwr IS NULL THEN 1 ELSE 0 END)
FROM player_dataset_final
""").fetchall())

con.close()
