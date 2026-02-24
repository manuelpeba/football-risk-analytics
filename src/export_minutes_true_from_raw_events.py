from pathlib import Path
import json
import polars as pl

DATA_ROOT = Path("data")
MANIFEST_PATH = Path("lakehouse/manifests/match_manifest.parquet")
OUT_ROOT = Path("lakehouse/bronze/player_match_minutes_true")

OUT_ROOT.mkdir(parents=True, exist_ok=True)

def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

manifest = pl.read_parquet(MANIFEST_PATH).filter(pl.col("has_events") == True)

# Ajusta si quieres ver más/menos progreso
PRINT_EVERY = 100

rows = []
processed = 0

def event_type(e):
    t = e.get("type")
    return (t or {}).get("name") if isinstance(t, dict) else None

for r in manifest.iter_rows(named=True):
    comp_id = int(r["competition_id"])
    season_id = int(r["season_id"])
    match_id = int(r["match_id"])

    events_path = DATA_ROOT / "events" / f"{match_id}.json"
    events = read_json(events_path)

    if not events:
        continue

    # Duración del partido (cap 130)
    match_max = max([e.get("minute", 0) or 0 for e in events])
    match_max = min(int(match_max), 130)

    # 1) Starting XI -> starters por equipo
    starters = {}  # team_name -> {player_id: player_name}
    for e in events:
        if event_type(e) == "Starting XI":
            team_name = ((e.get("team") or {}).get("name")) if isinstance(e.get("team"), dict) else None
            tactics = e.get("tactics") or {}
            lineup = tactics.get("lineup") or []
            if team_name is None:
                continue
            if team_name not in starters:
                starters[team_name] = {}
            for p in lineup:
                player = p.get("player") or {}
                pid = player.get("id")
                pname = player.get("name")
                if pid is not None:
                    starters[team_name][int(pid)] = pname

    # Si por lo que sea no hay Starting XI, saltamos (raro, pero posible)
    if not starters:
        continue

    # 2) Inicializamos intervals: starters juegan 0 -> match_max
    # intervals[(team, player_id)] = {"player": name, "start": 0, "end": match_max}
    intervals = {}
    for team_name, plist in starters.items():
        for pid, pname in plist.items():
            intervals[(team_name, pid)] = {"player": pname, "start": 0, "end": match_max}

    # 3) Procesamos substitutions en orden cronológico
    # Substitution event: e["player"] = el que sale; e["substitution"]["replacement"] = el que entra
    # Nota: un jugador puede entrar y luego salir (segunda sustitución) -> lo soportamos
    events_sorted = sorted(events, key=lambda x: (x.get("minute", 0) or 0, x.get("second", 0) or 0))

    for e in events_sorted:
        if event_type(e) != "Substitution":
            continue

        team_name = ((e.get("team") or {}).get("name")) if isinstance(e.get("team"), dict) else None
        if team_name is None:
            continue

        minute = e.get("minute", 0) or 0
        minute = max(0, min(int(minute), match_max))

        off_player = (e.get("player") or {})
        off_id = off_player.get("id")

        sub = e.get("substitution") or {}
        rep = sub.get("replacement") or {}
        on_id = rep.get("id")
        on_name = rep.get("name")

        # El que sale: end = minute
        if off_id is not None:
            key_off = (team_name, int(off_id))
            if key_off in intervals:
                intervals[key_off]["end"] = min(intervals[key_off]["end"], minute)
            else:
                # edge: sale alguien no detectado en starting xi (raro)
                intervals[key_off] = {"player": off_player.get("name"), "start": 0, "end": minute}

        # El que entra: start = minute, end = match_max
        if on_id is not None:
            key_on = (team_name, int(on_id))
            # si ya existía por alguna razón, no pisamos el start si es 0
            if key_on not in intervals:
                intervals[key_on] = {"player": on_name, "start": minute, "end": match_max}
            else:
                # si estaba (caso raro), ajustamos start al minuto de entrada si tiene start=0
                intervals[key_on]["start"] = max(intervals[key_on]["start"], minute)

    # 4) (Opcional simple) Ajuste por roja directa o 2ª amarilla.
    # StatsBomb suele tener eventos "Bad Behaviour" o "Foul Committed" con card.
    # Para no complicarlo demasiado: si detectamos card roja asociada a player_id,
    # cortamos su end en ese minuto.
    for e in events_sorted:
        minute = e.get("minute", 0) or 0
        minute = max(0, min(int(minute), match_max))
        t = event_type(e)
        if t not in ("Foul Committed", "Bad Behaviour"):
            continue

        player = e.get("player") or {}
        pid = player.get("id")
        if pid is None:
            continue

        card_name = None
        if t == "Foul Committed":
            fc = e.get("foul_committed") or {}
            card = fc.get("card") or {}
            card_name = card.get("name")
        elif t == "Bad Behaviour":
            bb = e.get("bad_behaviour") or {}
            card = bb.get("card") or {}
            card_name = card.get("name")

        if card_name in ("Red Card", "Second Yellow"):
            team_name = ((e.get("team") or {}).get("name")) if isinstance(e.get("team"), dict) else None
            if team_name is None:
                continue
            key = (team_name, int(pid))
            if key in intervals:
                intervals[key]["end"] = min(intervals[key]["end"], minute)

    # 5) Emitimos filas
    for (team_name, pid), itv in intervals.items():
        start = int(itv["start"])
        end = int(itv["end"])
        minutes_played = max(0, end - start)
        rows.append({
            "competition_id": comp_id,
            "season_id": season_id,
            "match_id": match_id,
            "team": team_name,
            "player_id": pid,
            "player": itv.get("player"),
            "start_minute": start,
            "end_minute": end,
            "minutes_played": minutes_played,
            "match_max_minute": match_max,
        })

    processed += 1
    if processed % PRINT_EVERY == 0:
        print(f"Procesados {processed}/{manifest.height} partidos | filas minutos: {len(rows)}")

df = pl.from_dicts(rows, infer_schema_length=None)

# Particionado por comp/season
for (comp_id, season_id), part in df.group_by(["competition_id", "season_id"]):
    out_dir = OUT_ROOT / f"competition_id={comp_id}" / f"season_id={season_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    part.write_parquet(out_dir / "player_match_minutes_true.parquet")

print("✅ Export terminado")
print("Filas totales:", df.height)
print(
    "Min/Max minutes_played:",
    df.select([
        pl.col("minutes_played").min().alias("min_minutes"),
        pl.col("minutes_played").max().alias("max_minutes"),
    ]).to_dicts()
)
print("Salida:", OUT_ROOT)
