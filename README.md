# Football Risk Analytics

### A Governance-Oriented Workload Risk Monitoring Framework for Professional Football

This project develops a governance-oriented risk monitoring framework to estimate short-term elevated workload exposure in professional football.

Rather than predicting medical injuries directly, the objective is to construct an interpretable, capacity-aware risk proxy that supports performance and medical staff under realistic operational constraints.

The framework emphasizes temporal robustness, calibration stability, and controlled alert generation rather than maximized headline accuracy.

The repository reflects a production-style sports analytics workflow, prioritizing robustness, interpretability, and governance over inflated performance metrics.

---

## Key Highlights

- Strict chronological validation
- Four-season walk-forward rolling evaluation
- Operational threshold simulation under fixed capacity (10%)
- Feature ablation experiments
- Score drift and policy drift monitoring
- Calibration tracking (ECE, Brier)
- Interpretability-driven model choice

---

## 1. Project Motivation

In elite football environments:

- Player overload contributes to performance decline and injury exposure.
- Medical and performance staff operate under limited review capacity.
- Risk assessment must be interpretable, stable, and operationally deployable.

This project simulates a realistic scenario where a club wants to:

- Rank players by short-term workload risk
- Flag the top X% for review
- Monitor model stability across competitive phases
- Detect calibration and distribution drift over time

The focus is on decision-support robustness rather than injury prediction claims.

---

## 2. Data Architecture

Data is stored in a local DuckDB lakehouse format.

Primary modeling table: `player_dataset_predictive_v2`

Features include:

- Rolling workload metrics (7d / 14d / 28d)
- Acute-Chronic Workload Ratio (ACWR)
- Workload volatility measures
- Short-term deltas and ratios
- Season cumulative exposure
- Multi-scale workload interactions
- Target proxy: `high_risk_next`

All splits are strictly chronological to prevent temporal leakage.

---

## 3. Modeling Framework

### Baseline Model

- Logistic Regression
- Median imputation
- Standard scaling
- L2 regularization

Why logistic regression?

- Interpretability
- Stable deployment characteristics
- Transparent coefficient structure
- Reduced variance under temporal shift

A HistGradientBoosting benchmark model is included for robustness comparison.

---

## 4. Evaluation Strategy

Two complementary validation approaches are used.

### A) Chronological Hold-Out

Single forward split simulating first deployment.

Metrics:

- ROC-AUC ≈ 0.77  
- PR-AUC ≈ 0.70  
- Brier ≈ 0.20  
- Precision @10% capacity ≈ 0.75  
- Recall @10% capacity ≈ 0.15  

Interpretation:

The model ranks elevated workload exposure effectively while remaining conservative under fixed review capacity.

---

### B) Rolling Backtest (Four-Season Walk-Forward)

A four-season rolling evaluation (22 monthly validation windows) simulates realistic deployment under non-stationary competitive dynamics.

Mean performance across folds:

- ROC-AUC ≈ 0.75  
- PR-AUC ≈ 0.79  
- Brier ≈ 0.19  
- ECE ≈ 0.18  

Findings:

- Logistic regression demonstrated stronger temporal robustness than the gradient boosting benchmark.
- Calibration remained stable across competitive phases.
- Score distribution shifts (PSI often > 1) reflect structural workload volatility rather than model degradation.
- Alert rate deviations illustrate operational drift under fixed-capacity constraints.

The rolling framework prioritizes stability and monitoring signals over static performance reporting.

---

## 5. Operational Policy Simulation

Thresholds are selected via training quantiles to simulate limited medical review capacity.

At 10% alert capacity:

- High precision
- Controlled alert volume
- Moderate recall
- Realistic test-period drift in alert rates

This mirrors real-world decision trade-offs where review capacity is constrained.

---

## 6. Model Robustness Experiments

Conducted:

- Regularization strength adjustments
- Feature pruning (ratio removal)
- Benchmark comparison (Logit vs HGB)
- Rolling stability validation

Findings:

- Regularization had limited impact on temporal variance.
- Removing multi-scale workload ratios degraded performance.
- Predictive signal emerges from interaction between acute, chronic, and volatility features.
- Logistic regression showed superior stability under temporal drift.

---

## 7. Model Governance Framework

The monitoring layer includes:

- Calibration tracking (ECE, Brier)
- Score distribution drift (PSI)
- Label prevalence drift
- Alert policy drift under fixed 10% capacity
- Control-chart style stability analysis

Trigger-based actions:

- Recalibration if calibration error increases materially
- Retraining if ranking performance degrades persistently
- Threshold review if alert capacity deviates beyond tolerance

This governance structure mirrors model risk monitoring practices adapted to elite football performance environments.

---

## 8. Repository Structure

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
├── requirements.txt
├── .gitignore
└── README.md
```
---

## 9. Limitations

- Proxy risk indicator, not confirmed medical injuries.

- No GPS or training load data.

- No biomechanical inputs.

- Workload dynamics are structurally non-stationary.

- PSI thresholds from financial domains are not directly transferable to sport contexts.

---

## 10. Future improvements

- Hybrid model: workload + performance metrics

- Season-specific recalibration policies

- Bayesian temporal smoothing

- Real-time monitoring dashboard

- Automated retraining triggers

---

## 11. Professional context

This project demonstrates:

- Time-aware validation under non-stationarity

- Operational decision support under capacity constraints

- Calibration and drift monitoring

- Model comparison with robustness prioritization

- Governance-oriented evaluation rather than static benchmarking

It reflects a deployment-ready analytics mindset aligned with elite football performance departments, where interpretability, stability, and operational feasibility outweigh marginal metric gains.

---

## Author

Manuel Pérez Bañuls  
Data Science & Football Performance Analytics  
Portfolio Project

