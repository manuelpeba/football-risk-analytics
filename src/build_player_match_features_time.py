import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_match_features_time AS
SELECT
    f.*,
    -- convertir a DATE por seguridad
    CAST(m.match_date AS DATE) AS match_date
FROM player_match_features_v2 f
JOIN matches m
  ON f.competition_id = m.competition_id
 AND f.season_id      = m.season_id
 AND f.match_id       = m.match_id
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_match_features_time").fetchall())
print("Date sanity:", con.execute("SELECT MIN(match_date), MAX(match_date) FROM player_match_features_time").fetchall())

con.close()
