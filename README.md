# ⚽ Football Risk Analytics

### Governance-Oriented Workload Risk Monitoring for Professional Football

[![Python](https://img.shields.io/badge/Python-3.10-blue)]()
[![DuckDB](https://img.shields.io/badge/Database-DuckDB-orange)]()
[![Status](https://img.shields.io/badge/Status-Analytical%20Prototype-green)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📌 Overview

This project develops a **governance-oriented workload risk monitoring framework** for professional football.

Instead of predicting injuries directly, the system estimates **short-term elevated workload exposure** and ranks players under **realistic operational constraints**.

The focus is on:

- Interpretability
- Temporal robustness
- Calibration stability
- Operational decision support

---

## 🚀 Key Results

### Chronological Hold-out

| Metric | Value |
|------|------|
| ROC-AUC | ~0.77 |
| PR-AUC | ~0.70 |
| Brier Score | ~0.20 |
| Precision @10% | ~0.75 |
| Recall @10% | ~0.15 |

### Rolling Backtest (4 Seasons)

| Metric | Mean |
|------|------|
| ROC-AUC | ~0.75 |
| PR-AUC | ~0.79 |
| Brier Score | ~0.19 |
| ECE | ~0.18 |

### Key Findings

- Logistic regression outperformed gradient boosting in **temporal stability**
- Calibration remained stable across competitive phases
- Score drift reflects **true workload volatility**, not model failure
- Fixed-capacity policy introduces **realistic operational trade-offs**

---

## 🧠 Key Features

- ✅ Strict chronological validation (no leakage)
- ✅ Multi-season rolling backtesting (walk-forward)
- ✅ Capacity-constrained decision simulation (top 10%)
- ✅ Calibration monitoring (ECE, Brier)
- ✅ Drift detection (PSI, label shift)
- ✅ Model robustness experiments
- ✅ Interpretability-first modeling (Logistic Regression)
- ✅ Production-style data pipeline (DuckDB lakehouse)

---

## 🛠️ Key Technologies

- **Python**
- **DuckDB** (analytical lakehouse)
- **Polars / Pandas**
- **Scikit-learn**
- **SQL-based feature engineering**

---

## 📂 Project Structure

```bash
football-risk-analytics/
│
├── notebooks/
│   ├── 01_eda_risk_scouting.ipynb
│   ├── 02_model_high_risk_baseline.ipynb
│   ├── 03_model_comparison_logit_vs_hgb.ipynb
│   ├── 04_operational_thresholding.ipynb
│   └── 05_rolling_backtest_and_monitoring.ipynb
│
├── lakehouse/
│   └── analytics.duckdb
│
├── scripts/
│   ├── build_player_dataset_final.py
│   ├── build_player_dataset_predictive.py
│   ├── build_player_load_features.py
│   ├── build_player_acwr.py
│   └── ...
│
├── requirements.txt
└── README.md
```
---

# ⚙️ Pipeline Overview

The project follows a lakehouse-style architecture:

Raw Data (Events, Matches)
        ↓
Player Match Stats
        ↓
Feature Engineering (Load, Form, ACWR)
        ↓
Final Dataset (Predictive)
        ↓
Modeling
        ↓
Evaluation & Monitoring


### Core Tables

* `player_match_stats`
* `player_load_features`
* `player_acwr`
* `player_dataset_predictive`

---

## ▶️ Quickstart

```bash
git clone https://github.com/yourusername/football-risk-analytics.git
cd football-risk-analytics

pip install -r requirements.txt
```

### Build datasets

```bash
python build_player_dataset_final.py
python build_player_dataset_predictive.py
```

### Run notebooks

```bash
jupyter notebook
```

---

## 🧪 Modeling Approach

### Baseline Model

* Logistic Regression
* Standard scaling + median imputation
* L2 regularization

### Why Logistic Regression?

* Stable under temporal drift
* Interpretable coefficients
* Lower variance vs complex models
* Better governance properties

### Benchmark

* HistGradientBoosting (comparison only)

---

## 📊 Evaluation Strategy

### 1. Chronological Split

Simulates first deployment

### 2. Rolling Walk-Forward

* 4 seasons
* 22 validation windows
* Non-stationary environment simulation

---

## ⚠️ Operational Policy Simulation

The system enforces **real-world constraints**:

* Only top **10% highest-risk players** are flagged
* Threshold selected on training data
* Evaluated under test-time drift

This mirrors **limited medical staff capacity**

---

## 🧭 Model Governance

Monitoring framework includes:

* Calibration tracking (ECE, Brier)
* Score distribution drift (PSI)
* Label prevalence monitoring
* Alert rate deviation
* Control-style stability tracking

### Trigger-based actions

* Recalibration
* Retraining
* Threshold adjustment

---

## ⚠️ Limitations

* Proxy risk signal (not real injuries)
* No GPS / training load data
* No biomechanical inputs
* High non-stationarity in workload dynamics

---

## 🔮 Future Improvements

* Hybrid model (workload + performance)
* Bayesian temporal smoothing
* Dynamic thresholding policies
* Real-time monitoring dashboard
* Automated retraining pipeline

---

## 💼 Professional Context

This project demonstrates:

* Time-aware validation under non-stationarity
* Operational decision support under constraints
* Calibration and drift monitoring
* Model robustness vs performance trade-offs
* Governance-first ML design

---

## Author

**Manuel Pérez Bañuls**  
Data Scientist | Football Analytics Enthusiast | Probabilistic Modeling

Specializing in:
- Sports analytics and forecasting
- Probabilistic simulation systems
- Machine learning for football prediction
- Production-ready data pipelines

**Connect & Collaborate**:
- 📧 Email: [manuelpeba@gmail.com](mailto:manuelpeba@gmail.com)
- 💼 LinkedIn: [manuel-perez-banuls](https://www.linkedin.com/in/manuel-perez-banuls/)
- 🐙 GitHub: [manuelpeba](https://github.com/manuelpeba)

Interested in discussing sports analytics, forecasting systems, or data-driven decision-making? Feel free to reach out!

---

# 📜 License

MIT License

