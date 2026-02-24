from pathlib import Path
import json
import polars as pl

DATA_ROOT = Path("data")
OUT_ROOT = Path("lakehouse/bronze/matches")

OUT_ROOT.mkdir(parents=True, exist_ok=True)

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

competitions = read_json(DATA_ROOT / "competitions.json")

total_exported = 0

for comp in competitions:
    competition_id = comp["competition_id"]
    season_id = comp["season_id"]

    matches_path = DATA_ROOT / "matches" / str(competition_id) / f"{season_id}.json"

    if not matches_path.exists():
        continue

    matches = read_json(matches_path)
    df = pl.from_dicts(matches, infer_schema_length=None)

    df = df.with_columns([
        pl.lit(competition_id).alias("competition_id"),
        pl.lit(season_id).alias("season_id"),
    ])

    out_dir = OUT_ROOT / f"competition_id={competition_id}" / f"season_id={season_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    df.write_parquet(out_dir / "matches.parquet")
    total_exported += df.height

print("✅ Matches exportados:", total_exported)
