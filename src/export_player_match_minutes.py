from pathlib import Path
import json
import polars as pl

DATA_ROOT = Path("data")
MANIFEST_PATH = Path("lakehouse/manifests/match_manifest.parquet")
OUT_ROOT = Path("lakehouse/bronze/player_match_minutes")

OUT_ROOT.mkdir(parents=True, exist_ok=True)

def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

df_manifest = (
    pl.read_parquet(MANIFEST_PATH)
    .filter(pl.col("has_lineups") == True)
    .select(["competition_id", "season_id", "match_id"])
)

rows = []
processed = 0

def extract_minutes(lineups_json, comp_id, season_id, match_id):
    out = []
    # lineups_json suele ser lista de equipos
    for team_block in lineups_json:
        team = (team_block.get("team") or {}).get("name")
        team_id = (team_block.get("team") or {}).get("id")
        for p in team_block.get("lineup", []):
            player = (p.get("player") or {}).get("name")
            player_id = (p.get("player") or {}).get("id")
            # En StatsBomb, minutes suelen venir en "positions" (from/to) o "cards"/"substitution"
            # pero lo más estable para minutos: sumar intervalos de positions si existen
            positions = p.get("positions") or []
            minutes = 0
            started = None

            if positions:
                # suelen venir con "from" y "to" en minutos
                for pos in positions:
                    frm = pos.get("from")
                    to = pos.get("to")
                    if frm is None:
                        frm = 0
                    if to is None:
                        # si no hay "to", asumimos 90 (o 120 si prórroga, pero empezamos con 90)
                        to = 90
                    try:
                        minutes += max(0, int(to) - int(frm))
                    except Exception:
                        pass
                started = True if (positions and (positions[0].get("from") in (0, "0", None))) else None
                main_position = (positions[0].get("position") or {}).get("name") if isinstance(positions[0].get("position"), dict) else None
            else:
                main_position = None

            out.append({
                "competition_id": int(comp_id),
                "season_id": int(season_id),
                "match_id": int(match_id),
                "team": team,
                "team_id": team_id,
                "player": player,
                "player_id": player_id,
                "minutes": minutes if minutes is not None else 0,
                "started": started,
                "position": main_position,
            })
    return out

for row in df_manifest.iter_rows(named=True):
    comp_id = row["competition_id"]
    season_id = row["season_id"]
    match_id = row["match_id"]

    path = DATA_ROOT / "lineups" / f"{match_id}.json"
    lineups_json = read_json(path)

    rows.extend(extract_minutes(lineups_json, comp_id, season_id, match_id))

    processed += 1
    if processed % 200 == 0:
        print(f"Procesados {processed}/{df_manifest.height} partidos | filas: {len(rows)}")

# Guardar en parquet particionado
df = pl.from_dicts(rows, infer_schema_length=None)

# Particionado manual por comp/season
for (comp_id, season_id), part in df.group_by(["competition_id", "season_id"]):
    out_dir = OUT_ROOT / f"competition_id={comp_id}" / f"season_id={season_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    part.write_parquet(out_dir / "player_match_minutes.parquet")

print("✅ player_match_minutes exportado")
print("Filas totales:", df.height)
print("Salida:", OUT_ROOT)
