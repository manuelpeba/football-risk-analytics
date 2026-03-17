import duckdb


def build_player_dataset_predictive(
    db_path: str = "lakehouse/analytics.duckdb",
    source_table: str = "player_dataset_final",
    target_table: str = "player_dataset_predictive",
) -> None:
    """
    Build predictive dataset by shifting the target forward one match
    within each player-season trajectory.

    Target:
        high_risk_next = LEAD(high_risk) over (player_id, season_id, match_date)
    """
    con = duckdb.connect(db_path)

    con.execute(f"""
    CREATE OR REPLACE TABLE {target_table} AS
    WITH base AS (
        SELECT
            *,
            LEAD(high_risk) OVER (
                PARTITION BY player_id, season_id
                ORDER BY match_date
            ) AS high_risk_next
        FROM {source_table}
    )
    SELECT *
    FROM base
    WHERE high_risk_next IS NOT NULL
    """)

    rows = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchall()

    risk_next_share = con.execute(f"""
    SELECT AVG(high_risk_next)
    FROM {target_table}
    """).fetchall()

    print("Rows:", rows)
    print("Risk next share:", risk_next_share)

    con.close()


if __name__ == "__main__":
    build_player_dataset_predictive()
