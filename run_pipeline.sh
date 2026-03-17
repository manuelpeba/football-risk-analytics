#!/bin/bash

export PYTHONPATH=src

echo "Building data..."
python scripts/01_build_manifest.py
python scripts/02_build_matches_view.py
python scripts/03_build_player_match_stats.py
python scripts/04_build_player_match_features_true_time.py
python scripts/05_build_player_form_features.py
python scripts/06_build_player_load_features_true.py
python scripts/07_build_player_acwr_true.py
python scripts/08_build_player_dataset_final.py
python scripts/09_build_player_dataset_predictive.py

echo "Training model..."
python scripts/20_train_baseline.py

echo "Scoring..."
python scripts/10_score_batch.py

echo "Ranking..."
python scripts/11_rank_players.py

echo "Generating alerts..."
python scripts/12_generate_alerts.py

echo "Done."
