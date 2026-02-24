import duckdb

con = duckdb.connect()

result = con.execute("""
SELECT COUNT(*) 
FROM read_parquet('lakehouse/bronze/events_flat/*/*/*.parquet')
""").fetchall()

print("Total events:", result)
