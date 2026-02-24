from pathlib import Path
import json
import polars as pl

DATA_ROOT = Path("data")
MANIFEST_PATH = Path("lakehouse/manifests/match_manifest.parquet")
OUT_ROOT = Path("lakehouse/bronze/events")

OUT_ROOT.mkdir(parents=True, exist_ok=True)

def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

df_manifest = pl.read_parquet(MANIFEST_PATH).filter(pl.col("has_events") == True)

# Ajusta este número si tu PC va justo de RAM
BATCH_SIZE = 50

total_events = 0
buffer = []
current_key = None  # (competition_id, season_id)

def flush_buffer(key, buf):
    if not buf:
        return 0
    comp_id, season_id = key
    out_dir = OUT_ROOT / f"competition_id={comp_id}" / f"season_id={season_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pl.concat(buf, how="diagonal_relaxed")
    # Guardamos un parquet por batch para evitar ficheros gigantes
    batch_file = out_dir / f"events_batch_{flush_buffer.batch_idx:05d}.parquet"
    df.write_parquet(batch_file)
    flush_buffer.batch_idx += 1
    return df.height

flush_buffer.batch_idx = 0

for row in df_manifest.iter_rows(named=True):
    comp_id = row["competition_id"]
    season_id = row["season_id"]
    match_id = row["match_id"]

    key = (comp_id, season_id)

    # Si cambiamos de competition/season, volcamos lo acumulado antes
    if current_key is None:
        current_key = key

    if key != current_key:
        total_events += flush_buffer(current_key, buffer)
        buffer = []
        current_key = key

    events_path = DATA_ROOT / "events" / f"{match_id}.json"
    events = read_json(events_path)

    # polars tolerante a esquemas
    df_events = pl.from_dicts(events, infer_schema_length=None).with_columns([
        pl.lit(int(comp_id)).alias("competition_id"),
        pl.lit(int(season_id)).alias("season_id"),
        pl.lit(int(match_id)).alias("match_id"),
    ])

    buffer.append(df_events)

    # Flush por tamaño de batch
    if len(buffer) >= BATCH_SIZE:
        total_events += flush_buffer(current_key, buffer)
        buffer = []

# Flush final
total_events += flush_buffer(current_key, buffer)

print(f"✅ Events exportados (filas totales): {total_events}")
print(f"📁 Salida: {OUT_ROOT}")
