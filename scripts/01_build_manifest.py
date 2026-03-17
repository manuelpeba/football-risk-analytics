from pathlib import Path
import json
import polars as pl

DATA_ROOT = Path("data")
OUT_ROOT = Path("lakehouse/manifests")
OUT_ROOT.mkdir(parents=True, exist_ok=True)

def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

competitions = read_json(DATA_ROOT / "competitions.json")

rows = []

for comp in competitions:
    competition_id = comp["competition_id"]
    season_id = comp["season_id"]

    matches_path = DATA_ROOT / "matches" / str(competition_id) / f"{season_id}.json"
    if not matches_path.exists():
        continue

    matches = read_json(matches_path)

    for m in matches:
        match_id = m["match_id"]

        events_path = DATA_ROOT / "events" / f"{match_id}.json"
        lineups_path = DATA_ROOT / "lineups" / f"{match_id}.json"
        threesixty_path = DATA_ROOT / "three-sixty" / f"{match_id}.json"

        rows.append({
            "competition_id": competition_id,
            "season_id": season_id,
            "match_id": match_id,
            "match_date": m.get("match_date"),
            "has_events": events_path.exists(),
            "has_lineups": lineups_path.exists(),
            "has_threesixty": threesixty_path.exists(),
        })

df = pl.DataFrame(rows)

out_file = OUT_ROOT / "match_manifest.parquet"
df.write_parquet(out_file)

print("✅ Manifest creado:", out_file)
print("Total partidos:", df.shape[0])
print(df.select([
    (pl.col("has_events").mean() * 100).alias("pct_events"),
    (pl.col("has_lineups").mean() * 100).alias("pct_lineups"),
    (pl.col("has_threesixty").mean() * 100).alias("pct_360"),
]))
