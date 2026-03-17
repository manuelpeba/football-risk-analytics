import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_acwr AS
SELECT
    *,
    CASE 
        WHEN minutes_last_28d > 0
        THEN minutes_last_7d / (minutes_last_28d / 4.0)
        ELSE NULL
    END AS acwr
FROM player_load_features
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_acwr").fetchall())
print("ACWR sanity:", con.execute("""
SELECT 
    MIN(acwr), 
    MAX(acwr), 
    AVG(acwr)
FROM player_acwr
WHERE acwr IS NOT NULL
""").fetchall())

print("High risk share (>1.5):", con.execute("""
SELECT AVG(CASE WHEN acwr > 1.5 THEN 1 ELSE 0 END)
FROM player_acwr
WHERE acwr IS NOT NULL
""").fetchall())

con.close()
