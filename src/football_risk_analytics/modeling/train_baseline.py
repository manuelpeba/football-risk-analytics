import duckdb
import joblib
from pathlib import Path

from sklearn.linear_model import LogisticRegression


FEATURES = [
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


def train_baseline(
    db_path: str = "lakehouse/analytics.duckdb",
    source_table: str = "player_dataset_predictive",
    model_dir: str = "models/baseline",
):
    con = duckdb.connect(db_path)

    df = con.execute(f"""
    SELECT *
    FROM {source_table}
    """).df()

    con.close()

    df = df.dropna()

    X = df[FEATURES]
    y = df["high_risk_next"]

    model = LogisticRegression(max_iter=3000)
    model.fit(X, y)

    model_path = Path(model_dir) / "model.pkl"
    model_path.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, model_path)

    print(f"Model saved to: {model_path}")
    print(f"Rows used: {len(df)}")


if __name__ == "__main__":
    train_baseline()
