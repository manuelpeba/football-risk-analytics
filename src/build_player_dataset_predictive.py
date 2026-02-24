import duckdb

con = duckdb.connect("lakehouse/analytics.duckdb")

con.execute("""
CREATE OR REPLACE TABLE player_dataset_predictive AS
WITH base AS (
    SELECT
        *,
        LEAD(high_risk) OVER (
            PARTITION BY player_id, season_id
            ORDER BY match_date
        ) AS high_risk_next
    FROM player_dataset_final
)

SELECT *
FROM base
WHERE high_risk_next IS NOT NULL
""")

print("Rows:", con.execute("SELECT COUNT(*) FROM player_dataset_predictive").fetchall())

print("Risk next share:", con.execute("""
SELECT AVG(high_risk_next) FROM player_dataset_predictive
""").fetchall())

con.close()
