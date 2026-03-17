import duckdb
con = duckdb.connect("lakehouse/analytics.duckdb")
print(con.execute("SHOW TABLES").fetchall())
con.close()
