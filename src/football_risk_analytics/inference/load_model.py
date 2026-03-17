from pathlib import Path
import json
import joblib


def load_model_artifacts(model_dir: str = "models/baseline") -> dict:
    """
    Load model artifacts from disk.

    Expected files:
    - model.pkl
    - metadata.json (optional)
    """
    model_path = Path(model_dir) / "model.pkl"
    metadata_path = Path(model_dir) / "metadata.json"

    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")

    model = joblib.load(model_path)

    metadata = {}
    if metadata_path.exists():
        with metadata_path.open("r", encoding="utf-8") as f:
            metadata = json.load(f)

    return {
        "model": model,
        "metadata": metadata,
        "model_path": str(model_path),
    }
