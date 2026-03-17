from pathlib import Path
from datetime import datetime, timezone
import duckdb
import pandas as pd

from football_risk_analytics.inference.load_model import load_model_artifacts


DEFAULT_FEATURES = [
    "shots_per90",
    "xg_per90",
    "passes_per90",
    "carries_per90",
    "progressive_x_per90",
    "xg_last_5",
    "shots_last_5",
    "progressive_last_5",
    "trend_xg_3v3",
    "minutes_last_7d",
    "minutes_last_14d",
    "minutes_last_28d",
    "minutes_last_5_matches",
    "acwr",
]


def score_batch(
    db_path: str = "lakehouse/analytics.duckdb",
    source_table: str = "player_dataset_final",
    model_dir: str = "models/baseline",
    output_csv: str = "outputs/predictions/player_risk_scores.csv",
    output_parquet: str = "outputs/predictions/player_risk_scores.parquet",
    features: list[str] | None = None,
) -> pd.DataFrame:
    """
    Score batch player workload risk using a trained model.

    Reads features from DuckDB, applies the model, and exports predictions.
    """
    features = features or DEFAULT_FEATURES

    artifacts = load_model_artifacts(model_dir=model_dir)
    model = artifacts["model"]
    metadata = artifacts.get("metadata", {})

    con = duckdb.connect(db_path)

    id_cols = [
        "player_id",
        "competition_id",
        "season_id",
        "match_id",
        "match_date",
        "team",
    ]

    select_cols = id_cols + features

    query = f"""
    SELECT {", ".join(select_cols)}
    FROM {source_table}
    """

    df = con.execute(query).df()
    con.close()

    X = df[features].copy()
    X = X.fillna(0)

    if hasattr(model, "predict_proba"):
        scores = model.predict_proba(X)[:, 1]
    elif hasattr(model, "decision_function"):
        scores = model.decision_function(X)
    else:
        scores = model.predict(X)

    scored = df[id_cols].copy()
    scored["risk_score"] = scores
    scored["model_name"] = metadata.get("model_name", "baseline_model")
    scored["model_version"] = metadata.get("model_version", "v1")
    scored["scored_at_utc"] = datetime.now(timezone.utc).isoformat()

    output_csv_path = Path(output_csv)
    output_parquet_path = Path(output_parquet)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    scored.to_csv(output_csv_path, index=False)
    scored.to_parquet(output_parquet_path, index=False)

    print(f"Scored rows: {len(scored)}")
    print(f"CSV saved to: {output_csv_path}")
    print(f"Parquet saved to: {output_parquet_path}")

    return scored


if __name__ == "__main__":
    score_batch()
