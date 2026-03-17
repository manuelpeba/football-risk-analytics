import duckdb


def build_player_acwr_true(
    db_path: str = "lakehouse/analytics.duckdb",
    source_table: str = "player_load_features_true",
    target_table: str = "player_acwr_true",
    min_minutes_28d: int = 180,
    risk_threshold: float = 1.5,
) -> None:
    """
    Build ACWR table using the 'true' load features table.

    ACWR definition:
        minutes_last_7d / (minutes_last_28d / 4.0)

    Valid only when minutes_last_28d >= min_minutes_28d.
    """
    con = duckdb.connect(db_path)

    con.execute(f"""
    CREATE OR REPLACE TABLE {target_table} AS
    SELECT
        *,
        CASE
          WHEN minutes_last_28d >= {min_minutes_28d}
          THEN minutes_last_7d / (minutes_last_28d / 4.0)
          ELSE NULL
        END AS acwr
    FROM {source_table}
    """)

    rows = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchall()
    valid_share = con.execute(f"""
    SELECT AVG(CASE WHEN acwr IS NOT NULL THEN 1 ELSE 0 END)
    FROM {target_table}
    """).fetchall()

    acwr_sanity = con.execute(f"""
    SELECT MIN(acwr), MAX(acwr), AVG(acwr)
    FROM {target_table}
    WHERE acwr IS NOT NULL
    """).fetchall()

    high_risk_share = con.execute(f"""
    SELECT AVG(CASE WHEN acwr > {risk_threshold} THEN 1 ELSE 0 END)
    FROM {target_table}
    WHERE acwr IS NOT NULL
    """).fetchall()

    print("Rows:", rows)
    print("Valid share:", valid_share)
    print("ACWR sanity:", acwr_sanity)
    print(f"High risk share (>{risk_threshold}):", high_risk_share)

    con.close()


if __name__ == "__main__":
    build_player_acwr_true()
