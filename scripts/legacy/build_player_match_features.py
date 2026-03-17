import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

# Creamos una vista/tabla sobre minutes parquet
con.execute("""
CREATE OR REPLACE VIEW player_match_minutes AS
SELECT *
FROM read_parquet('lakehouse/bronze/player_match_minutes/*/*/*.parquet')
""")

# Y una vista sobre stats ya creada
# (player_match_stats ya existe como tabla)

con.execute("""
CREATE OR REPLACE TABLE player_match_features AS
SELECT
    m.competition_id,
    m.season_id,
    m.match_id,
    m.player_id,
    m.player,
    m.team,
    m.minutes,
    m.started,
    m.position,

    -- stats (pueden ser NULL si el jugador no tuvo eventos)
    COALESCE(s.events_count, 0) AS events_count,
    COALESCE(s.shots, 0)        AS shots,
    COALESCE(s.xg, 0.0)         AS xg,
    COALESCE(s.passes, 0)       AS passes,
    COALESCE(s.total_pass_length, 0.0) AS total_pass_length,
    COALESCE(s.carries, 0)      AS carries,
    COALESCE(s.progressive_x, 0.0) AS progressive_x,

    -- per90 (evitamos div/0)
    CASE WHEN m.minutes > 0 THEN COALESCE(s.shots, 0) * 90.0 / m.minutes ELSE 0 END AS shots_per90,
    CASE WHEN m.minutes > 0 THEN COALESCE(s.xg, 0.0) * 90.0 / m.minutes ELSE 0 END AS xg_per90,
    CASE WHEN m.minutes > 0 THEN COALESCE(s.passes, 0) * 90.0 / m.minutes ELSE 0 END AS passes_per90,
    CASE WHEN m.minutes > 0 THEN COALESCE(s.carries, 0) * 90.0 / m.minutes ELSE 0 END AS carries_per90,
    CASE WHEN m.minutes > 0 THEN COALESCE(s.progressive_x, 0.0) * 90.0 / m.minutes ELSE 0 END AS progressive_x_per90

FROM player_match_minutes m
LEFT JOIN player_match_stats s
  ON  m.competition_id = s.competition_id
  AND m.season_id      = s.season_id
  AND m.match_id       = s.match_id
  AND m.player_id      = s.player_id
""")

print("Rows player_match_features:", con.execute("SELECT COUNT(*) FROM player_match_features").fetchall())
print("Players with 0 events:", con.execute("SELECT COUNT(*) FROM player_match_features WHERE events_count = 0").fetchall())
print("Minutes sanity (min/max):", con.execute("SELECT MIN(minutes), MAX(minutes) FROM player_match_features").fetchall())

con.close()
