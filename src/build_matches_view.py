import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE VIEW matches AS
SELECT
    competition_id,
    season_id,
    match_id,
    match_date
FROM read_parquet('lakehouse/bronze/matches/*/*/*.parquet')
""")

print("Matches:", con.execute("SELECT COUNT(*) FROM matches").fetchall())
print("Date sanity:", con.execute("SELECT MIN(match_date), MAX(match_date) FROM matches").fetchall())

con.close()
