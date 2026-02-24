import duckdb
con = duckdb.connect("lakehouse/analytics.duckdb")

print(con.execute("""
SELECT DISTINCT competition_id, COUNT(*)
FROM player_match_features_true_time
GROUP BY competition_id
ORDER BY COUNT(*) DESC
""").fetchall())

con.close()
