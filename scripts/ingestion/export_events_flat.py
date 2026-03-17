from pathlib import Path
import json
import polars as pl

DATA_ROOT = Path("data")
MANIFEST_PATH = Path("lakehouse/manifests/match_manifest.parquet")
OUT_ROOT = Path("lakehouse/bronze/events_flat")

OUT_ROOT.mkdir(parents=True, exist_ok=True)

def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

df_manifest = pl.read_parquet(MANIFEST_PATH).filter(pl.col("has_events") == True)

# Bajar si tu PC va justo
BATCH_SIZE = 100

total_rows = 0
processed = 0
buffer = []

def flatten_event(e: dict, comp_id: int, season_id: int, match_id: int) -> dict:
    # Campos base bastante estables en StatsBomb
    out = {
        "competition_id": comp_id,
        "season_id": season_id,
        "match_id": match_id,
        "id": e.get("id"),
        "index": e.get("index"),
        "period": e.get("period"),
        "timestamp": e.get("timestamp"),
        "minute": e.get("minute"),
        "second": e.get("second"),
        "type": (e.get("type") or {}).get("name"),
        "possession": e.get("possession"),
        "possession_team": (e.get("possession_team") or {}).get("name"),
        "play_pattern": (e.get("play_pattern") or {}).get("name"),
        "team": (e.get("team") or {}).get("name"),
        "player": (e.get("player") or {}).get("name"),
        "player_id": (e.get("player") or {}).get("id"),
    }

    loc = e.get("location")
    if isinstance(loc, list) and len(loc) >= 2:
        out["x"] = loc[0]
        out["y"] = loc[1]
    else:
        out["x"] = None
        out["y"] = None

    # Si quieres luego features de progresión: end_location (pass/carry/shot)
    end_loc = None
    if isinstance(e.get("pass"), dict):
        end_loc = e["pass"].get("end_location")
        out["pass_outcome"] = (e["pass"].get("outcome") or {}).get("name")
        out["pass_length"] = e["pass"].get("length")
    else:
        out["pass_outcome"] = None
        out["pass_length"] = None

    if isinstance(e.get("shot"), dict):
        out["shot_outcome"] = (e["shot"].get("outcome") or {}).get("name")
        out["shot_statsbomb_xg"] = e["shot"].get("statsbomb_xg")
    else:
        out["shot_outcome"] = None
        out["shot_statsbomb_xg"] = None

    if isinstance(e.get("carry"), dict):
        end_loc = e["carry"].get("end_location") if end_loc is None else end_loc

    if isinstance(end_loc, list) and len(end_loc) >= 2:
        out["end_x"] = end_loc[0]
        out["end_y"] = end_loc[1]
    else:
        out["end_x"] = None
        out["end_y"] = None

    return out

def flush(comp_id, season_id, buf):
    global total_rows
    if not buf:
        return
    out_dir = OUT_ROOT / f"competition_id={comp_id}" / f"season_id={season_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pl.from_dicts(buf, infer_schema_length=None)
    file = out_dir / f"events_flat_{flush.batch_idx:05d}.parquet"
    df.write_parquet(file)
    total_rows += df.height
    flush.batch_idx += 1

flush.batch_idx = 0

current_comp = None
current_season = None

for row in df_manifest.iter_rows(named=True):
    comp_id = int(row["competition_id"])
    season_id = int(row["season_id"])
    match_id = int(row["match_id"])

    if current_comp is None:
        current_comp, current_season = comp_id, season_id

    # si cambiamos de partición, volcamos
    if (comp_id != current_comp) or (season_id != current_season):
        flush(current_comp, current_season, buffer)
        buffer = []
        current_comp, current_season = comp_id, season_id

    events_path = DATA_ROOT / "events" / f"{match_id}.json"
    events = read_json(events_path)

    for e in events:
        buffer.append(flatten_event(e, comp_id, season_id, match_id))

    processed += 1
    if processed % 50 == 0:
        print(f"Procesados {processed}/{df_manifest.height} partidos | filas escritas: {total_rows} | buffer_events: {len(buffer)}")

    if len(buffer) >= BATCH_SIZE * 2000:  # umbral aproximado (depende del partido)
        flush(current_comp, current_season, buffer)
        buffer = []

# flush final
flush(current_comp, current_season, buffer)

print(f"✅ Events FLAT exportados. Filas totales: {total_rows}")
print(f"📁 Salida: {OUT_ROOT}")
