# Football Injury Risk Modelling using Public Event Data

## Project Overview

This project presents a reproducible injury-risk modelling framework built using publicly available football event data (StatsBomb Open Data).  

The objective is to reconstruct competitive workload exposure and evaluate its relationship with short-term injury risk using machine learning techniques.

This project is designed to demonstrate:

- End-to-end data pipeline construction
- True match minute reconstruction from raw event data
- Workload feature engineering (7/14/28-day rolling windows)
- ACWR computation
- Temporal validation strategy (leakage-aware)
- Baseline and non-linear modelling approaches
- Performance interpretation from both Data Science and Performance perspectives

This repository is intended as a professional portfolio project bridging Data Science and Football Performance Analytics.


---

## Data Source

- StatsBomb Open Data (not included in this repository)
- Competitive match event data only
- No access to internal club training or GPS data

Because public datasets do not include training load, internal load, or medical records, this project models injury risk using competitive workload as a proxy.

The `data/` and `lakehouse/` folders are excluded from version control due to size.


---

## Methodology

### 1. True Minutes Reconstruction

Starting XI and substitution events are parsed from raw event data to reconstruct actual minutes played per match.

This avoids relying on simplified or incomplete minute assumptions.

### 2. Workload Feature Engineering

Rolling workload features are computed:

- `minutes_last_7d`
- `minutes_last_14d`
- `minutes_last_28d`
- Acute:Chronic Workload Ratio (ACWR)

These represent external competitive load only.

### 3. Target Definition

`high_risk_next` is defined as a proxy indicator based on workload-derived thresholds.

This is not a clinical injury label but a workload-based risk indicator.

### 4. Temporal Split Strategy

To avoid data leakage:

- Data is split chronologically
- The most recent 20% is used as test set
- No random shuffling is performed

This simulates real-world forward prediction.


---

## Modelling

### Baseline Model

Logistic Regression (with feature scaling)

Evaluation metrics:

- ROC-AUC
- Precision-Recall AUC
- Confusion Matrix
- Coefficient interpretation

### Model Comparison (Planned / Extended)

- HistGradientBoosting
- Tree-based models
- Feature importance analysis
- Calibration analysis

The objective is to balance predictive performance with interpretability.


---

## Key Findings

- Short-term competitive workload shows measurable association with next-match risk proxy.
- Logistic baseline achieves ROC-AUC above random classification.
- Precision-Recall performance indicates meaningful predictive signal relative to class prevalence.
- Load effects appear non-linear, suggesting benefit from tree-based models.


---

## Limitations

This project does not include:

- GPS-derived external load metrics (HSR, sprint distance, accelerations)
- Internal load metrics (HRV, sRPE, fatigue scores)
- Clinical injury records

In professional environments, these data sources would be essential for robust injury prediction models.

Therefore, this project demonstrates the modelling framework rather than a medically complete injury prediction system.


---

## Future Work

- Integration of training load metrics
- Player-specific baseline modelling
- Survival analysis (time-to-injury modelling)
- Walk-forward cross-validation
- Deployment as a club-facing monitoring tool


---

## Technical Stack

- Python
- DuckDB
- Pandas / Polars
- Scikit-learn
- Matplotlib

Reproducible environment defined in `requirements.txt`.


---

## Author

Data Science & Football Performance Analytics Portfolio Project

