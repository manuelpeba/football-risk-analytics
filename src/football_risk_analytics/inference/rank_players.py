from pathlib import Path
import pandas as pd


def rank_players(
    input_csv: str = "outputs/predictions/player_risk_scores.csv",
    output_csv: str = "outputs/alerts/ranked_players.csv",
    output_parquet: str = "outputs/alerts/ranked_players.parquet",
    rank_within_date: bool = True,
) -> pd.DataFrame:
    """
    Rank players by risk score.

    If rank_within_date=True, ranking is computed within each match_date.
    Otherwise, ranking is global across all rows.
    """
    df = pd.read_csv(input_csv)

    if rank_within_date and "match_date" in df.columns:
        df = df.sort_values(["match_date", "risk_score"], ascending=[True, False]).copy()
        df["risk_rank"] = (
            df.groupby("match_date")["risk_score"]
              .rank(method="first", ascending=False)
              .astype(int)
        )
    else:
        df = df.sort_values("risk_score", ascending=False).copy()
        df["risk_rank"] = range(1, len(df) + 1)

    output_csv_path = Path(output_csv)
    output_parquet_path = Path(output_parquet)
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_csv_path, index=False)
    df.to_parquet(output_parquet_path, index=False)

    print(f"Ranked rows: {len(df)}")
    print(f"CSV saved to: {output_csv_path}")
    print(f"Parquet saved to: {output_parquet_path}")

    return df


if __name__ == "__main__":
    rank_players()
