import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_match_features_v2 AS
SELECT
    m.competition_id,
    m.season_id,
    m.match_id,
    m.player_id,
    -- minutos aproximados desde eventos
    m.approx_minutes AS minutes,

    -- stats (si faltan eventos, quedarán 0 por el join)
    COALESCE(s.events_count, 0) AS events_count,
    COALESCE(s.shots, 0)        AS shots,
    COALESCE(s.xg, 0.0)         AS xg,
    COALESCE(s.passes, 0)       AS passes,
    COALESCE(s.total_pass_length, 0.0) AS total_pass_length,
    COALESCE(s.carries, 0)      AS carries,
    COALESCE(s.progressive_x, 0.0) AS progressive_x,

    CASE WHEN m.approx_minutes > 0 THEN COALESCE(s.shots, 0) * 90.0 / m.approx_minutes ELSE 0 END AS shots_per90,
    CASE WHEN m.approx_minutes > 0 THEN COALESCE(s.xg, 0.0) * 90.0 / m.approx_minutes ELSE 0 END AS xg_per90,
    CASE WHEN m.approx_minutes > 0 THEN COALESCE(s.passes, 0) * 90.0 / m.approx_minutes ELSE 0 END AS passes_per90,
    CASE WHEN m.approx_minutes > 0 THEN COALESCE(s.carries, 0) * 90.0 / m.approx_minutes ELSE 0 END AS carries_per90,
    CASE WHEN m.approx_minutes > 0 THEN COALESCE(s.progressive_x, 0.0) * 90.0 / m.approx_minutes ELSE 0 END AS progressive_x_per90

FROM player_match_minutes_v2 m
LEFT JOIN player_match_stats s
  ON  m.competition_id = s.competition_id
  AND m.season_id      = s.season_id
  AND m.match_id       = s.match_id
  AND m.player_id      = s.player_id
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_match_features_v2").fetchall())
print("Minutes sanity:", con.execute("SELECT MIN(minutes), MAX(minutes) FROM player_match_features_v2").fetchall())
print("Share 0-minute:", con.execute("SELECT AVG(CASE WHEN minutes=0 THEN 1 ELSE 0 END) FROM player_match_features_v2").fetchall())

con.close()
