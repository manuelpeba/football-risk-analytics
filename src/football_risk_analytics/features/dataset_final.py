import duckdb


def build_player_dataset_final(
    db_path: str = "lakehouse/analytics.duckdb",
    source_match_features: str = "player_match_features_true_time",
    source_form_features: str = "player_form_features",
    source_load_features: str = "player_load_features_true",
    source_acwr: str = "player_acwr_true",
    target_table: str = "player_dataset_final",
    risk_threshold: float = 1.5,
) -> None:
    """
    Build final modeling dataset by joining:
    - match-level player features
    - rolling form features
    - rolling load features
    - ACWR features

    The target label 'high_risk' is defined from ACWR threshold.
    """
    con = duckdb.connect(db_path)

    con.execute(f"""
    CREATE OR REPLACE TABLE {target_table} AS
    SELECT
        f.player_id,
        f.competition_id,
        f.season_id,
        f.match_id,
        f.match_date,
        f.team,

        -- Base minutes and performance
        f.minutes,
        f.shots_per90,
        f.xg_per90,
        f.passes_per90,
        f.carries_per90,
        f.progressive_x_per90,

        -- Rolling form
        pf.xg_last_5,
        pf.shots_last_5,
        pf.progressive_last_5,
        pf.trend_xg_3v3,

        -- Load
        pl.minutes_last_7d,
        pl.minutes_last_14d,
        pl.minutes_last_28d,
        pl.minutes_last_5_matches,

        -- ACWR
        pa.acwr,

        -- Target risk label
        CASE
            WHEN pa.acwr > {risk_threshold} THEN 1
            ELSE 0
        END AS high_risk

    FROM {source_match_features} f
    LEFT JOIN {source_form_features} pf
      ON f.player_id = pf.player_id
     AND f.match_id = pf.match_id
    LEFT JOIN {source_load_features} pl
      ON f.player_id = pl.player_id
     AND f.match_id = pl.match_id
    LEFT JOIN {source_acwr} pa
      ON f.player_id = pa.player_id
     AND f.match_id = pa.match_id
    """)

    rows = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchall()

    risk_share = con.execute(f"""
    SELECT AVG(high_risk)
    FROM {target_table}
    """).fetchall()

    null_acwr_share = con.execute(f"""
    SELECT AVG(CASE WHEN acwr IS NULL THEN 1 ELSE 0 END)
    FROM {target_table}
    """).fetchall()

    print("Rows:", rows)
    print("Risk share:", risk_share)
    print("Null ACWR share:", null_acwr_share)

    con.close()


if __name__ == "__main__":
    build_player_dataset_final()
