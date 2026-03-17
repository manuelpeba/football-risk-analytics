import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_match_stats AS
SELECT
    competition_id,
    season_id,
    match_id,
    player_id,
    player,
    team,

    COUNT(*) AS events_count,

    SUM(CASE WHEN type = 'Shot' THEN 1 ELSE 0 END) AS shots,
    SUM(CASE WHEN type = 'Shot' THEN shot_statsbomb_xg ELSE 0 END) AS xg,

    SUM(CASE WHEN type = 'Pass' THEN 1 ELSE 0 END) AS passes,
    SUM(pass_length) AS total_pass_length,

    SUM(CASE WHEN type = 'Carry' THEN 1 ELSE 0 END) AS carries,

    SUM(
        CASE 
            WHEN end_x IS NOT NULL AND x IS NOT NULL
            THEN end_x - x
            ELSE 0
        END
    ) AS progressive_x

FROM read_parquet('lakehouse/bronze/events_flat/*/*/*.parquet')
GROUP BY
    competition_id,
    season_id,
    match_id,
    player_id,
    player,
    team
""")

result = con.execute("SELECT COUNT(*) FROM player_match_stats").fetchall()
print("Filas en player_match_stats:", result)

con.close()
