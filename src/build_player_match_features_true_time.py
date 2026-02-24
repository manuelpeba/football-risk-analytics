import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE VIEW player_match_minutes_true AS
SELECT *
FROM read_parquet('lakehouse/bronze/player_match_minutes_true/*/*/*.parquet')
""")

con.execute("""
CREATE OR REPLACE TABLE player_match_features_true_time AS
SELECT
    mt.competition_id,
    mt.season_id,
    mt.match_id,
    mt.match_max_minute,
    CAST(m.match_date AS DATE) AS match_date,

    mt.team,
    mt.player_id,
    mt.player,
    mt.minutes_played AS minutes,

    COALESCE(s.events_count, 0) AS events_count,
    COALESCE(s.shots, 0)        AS shots,
    COALESCE(s.xg, 0.0)         AS xg,
    COALESCE(s.passes, 0)       AS passes,
    COALESCE(s.total_pass_length, 0.0) AS total_pass_length,
    COALESCE(s.carries, 0)      AS carries,
    COALESCE(s.progressive_x, 0.0) AS progressive_x,

    CASE WHEN mt.minutes_played > 0 THEN COALESCE(s.shots, 0) * 90.0 / mt.minutes_played ELSE 0 END AS shots_per90,
    CASE WHEN mt.minutes_played > 0 THEN COALESCE(s.xg, 0.0) * 90.0 / mt.minutes_played ELSE 0 END AS xg_per90,
    CASE WHEN mt.minutes_played > 0 THEN COALESCE(s.passes, 0) * 90.0 / mt.minutes_played ELSE 0 END AS passes_per90,
    CASE WHEN mt.minutes_played > 0 THEN COALESCE(s.carries, 0) * 90.0 / mt.minutes_played ELSE 0 END AS carries_per90,
    CASE WHEN mt.minutes_played > 0 THEN COALESCE(s.progressive_x, 0.0) * 90.0 / mt.minutes_played ELSE 0 END AS progressive_x_per90

FROM player_match_minutes_true mt
JOIN matches m
  ON mt.competition_id = m.competition_id
 AND mt.season_id      = m.season_id
 AND mt.match_id       = m.match_id
LEFT JOIN player_match_stats s
  ON mt.competition_id = s.competition_id
 AND mt.season_id      = s.season_id
 AND mt.match_id       = s.match_id
 AND mt.player_id      = s.player_id
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_match_features_true_time").fetchall())
print("Minutes sanity:", con.execute("SELECT MIN(minutes), MAX(minutes), AVG(minutes) FROM player_match_features_true_time").fetchall())
print("Date sanity:", con.execute("SELECT MIN(match_date), MAX(match_date) FROM player_match_features_true_time").fetchall())

con.close()
