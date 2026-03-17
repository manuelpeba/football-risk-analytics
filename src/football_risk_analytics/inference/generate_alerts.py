from pathlib import Path
import pandas as pd


def generate_alerts(
    input_csv: str = "outputs/alerts/ranked_players.csv",
    output_alerts_csv: str = "outputs/alerts/top_players_alerts.csv",
    output_queue_csv: str = "outputs/alerts/medical_review_queue.csv",
    top_n: int = 10,
) -> pd.DataFrame:
    """
    Generate alert lists based on top-N risk ranking per match_date.
    """
    df = pd.read_csv(input_csv)

    # Seleccionar top N por fecha
    alerts = df[df["risk_rank"] <= top_n].copy()

    # Orden final
    alerts = alerts.sort_values(["match_date", "risk_rank"])

    # Crear versión simplificada (cola médica)
    queue_cols = [
        "match_date",
        "player_id",
        "team",
        "risk_score",
        "risk_rank",
    ]

    queue = alerts[queue_cols].copy()

    # Crear carpetas
    alerts_path = Path(output_alerts_csv)
    queue_path = Path(output_queue_csv)
    alerts_path.parent.mkdir(parents=True, exist_ok=True)

    # Guardar
    alerts.to_csv(alerts_path, index=False)
    queue.to_csv(queue_path, index=False)

    print(f"Alerts rows: {len(alerts)}")
    print(f"Alerts saved to: {alerts_path}")
    print(f"Medical queue saved to: {queue_path}")

    return alerts


if __name__ == "__main__":
    generate_alerts()
