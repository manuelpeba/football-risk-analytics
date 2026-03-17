import duckdb


def build_player_load_features_true(
    db_path: str = "lakehouse/analytics.duckdb",
    source_table: str = "player_match_features_true_time",
    target_table: str = "player_load_features_true",
) -> None:
    """
    Build rolling workload features using true match-minute data.

    Features:
    - minutes_last_7d
    - minutes_last_14d
    - minutes_last_28d
    - minutes_last_5_matches
    """
    con = duckdb.connect(db_path)

    con.execute(f"""
    CREATE OR REPLACE TABLE {target_table} AS
    SELECT
        player_id,
        competition_id,
        season_id,
        match_id,
        match_date,
        minutes,

        SUM(minutes) OVER (
            PARTITION BY player_id
            ORDER BY match_date
            RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW
        ) AS minutes_last_7d,

        SUM(minutes) OVER (
            PARTITION BY player_id
            ORDER BY match_date
            RANGE BETWEEN INTERVAL 14 DAY PRECEDING AND CURRENT ROW
        ) AS minutes_last_14d,

        SUM(minutes) OVER (
            PARTITION BY player_id
            ORDER BY match_date
            RANGE BETWEEN INTERVAL 28 DAY PRECEDING AND CURRENT ROW
        ) AS minutes_last_28d,

        SUM(minutes) OVER (
            PARTITION BY player_id
            ORDER BY match_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS minutes_last_5_matches

    FROM {source_table}
    """)

    rows = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchall()

    sanity = con.execute(f"""
    SELECT
        MIN(minutes_last_7d),
        MAX(minutes_last_7d),
        MIN(minutes_last_28d),
        MAX(minutes_last_28d)
    FROM {target_table}
    """).fetchall()

    print("Rows:", rows)
    print("Sanity:", sanity)

    con.close()


if __name__ == "__main__":
    build_player_load_features_true()
