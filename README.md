# Football Risk Analytics

### A Workload-Based Injury Risk Proxy Model for Professional Football

This project develops a data-driven framework to estimate short-term elevated workload risk in professional football players using match-derived workload metrics.

The objective is not to predict medical injuries directly, but to construct a robust, operational risk proxy that can support performance and medical decision-making under realistic capacity constraints.

This repository focuses on methodological rigor:

- Chronological validation
- Rolling stability testing
- Feature ablation experiments
- Operational policy simulation

The project is structured to reflect a real-world sports analytics workflow, prioritizing methodological rigor over inflated performance metrics.

Key Highlights:

- Time-aware validation
- Operational threshold simulation
- Feature ablation experiments
- Interpretability-driven model choice

---

## 1. Project motivation

In elite football environments:

- Player overload leads to performance decline and injury risk.
- Medical staff operate under limited review capacity.
- Risk assessment must be interpretable and operationally deployable.

This project simulates a realistic scenario where a club wants to:

- Rank players by short-term risk
- Flag the top X% for review
- Monitor temporal stability of the model

---

## 2. Data architecture

Data stored in DuckDB lakehouse format.

Primary table used for modeling: `player_dataset_predictive_v2`


Contains:

- Rolling workload features (7d / 14d / 28d)
- Acute-Chronic Workload Ratio (ACWR)
- Volatility metrics
- Season cumulative load
- Short-term workload deltas
- Target: `high_risk_next`

All splits are chronological to prevent leakage.

---

## 3. Modeling framework

### Baseline model

- Logistic Regression
- Median imputation
- Standard scaling
- L2 regularization

Why logistic regression?

- Interpretability
- Stable deployment
- Transparent coefficient interpretation

---

## 4. Evaluation strategy

Two complementary validation approaches:

### A) Chronological hold-out

Single forward split to simulate deployment.

Metrics:
- ROC-AUC
- PR-AUC
- Brier Score
- Precision/Recall under 10% capacity

### B) Rolling backtest (time-aware)

Weekly block rolling validation to evaluate:

- Temporal robustness
- Stability across competitive periods
- Variance of predictive performance

This prevents inflated performance from static splits.

---

## 5. Key results

### Chronological test split

- ROC-AUC ≈ 0.77
- PR-AUC ≈ 0.70
- Brier ≈ 0.20
- Precision @10% capacity ≈ 0.75
- Recall @10% capacity ≈ 0.15

Interpretation:
The model effectively ranks elevated risk cases while remaining conservative under operational constraints. Results are reported under strictly chronological splits to avoid temporal leakage.

---

### Rolling backtest

- ROC-AUC mean ≈ 0.76
- Moderate temporal variance observed
- Calibration remained within acceptable range across folds

Feature ablation experiments confirmed:

- Interaction ratios contribute predictive signal
- Instability is not driven by overfitting
- Temporal variability likely reflects non-stationary workload dynamics

---

## 6. Operational policy simulation

Thresholds are selected via training quantiles to simulate limited medical review capacity.

At 10% alert capacity:

- High precision
- Moderate recall
- Controlled alert volume

This mirrors real-world decision trade-offs.

---

## 7. Model robustness experiments

Conducted:

- Regularization strength adjustment
- Feature pruning (ratio removal)
- Rolling validation stability testing

Findings:

- Regularization had limited impact on variance
- Removing workload ratios degraded performance
- Predictive signal arises from multi-scale workload interactions

---

## 8. Repository structure

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

### Notebook overview

**01 — EDA & Risk scouting**  
Exploratory analysis of workload distribution, temporal dynamics and initial proxy definition.

**02 — Baseline model**  
Logistic regression with expanded workload features and chronological split evaluation.

**03 — Model comparison**  
Logit vs HistGradientBoosting comparison under consistent evaluation framework.

**04 — Operational thresholding**  
Simulation of medical review capacity constraints using quantile-based thresholds.

**05 — Rolling backtest & Monitoring**  
Time-aware validation to assess temporal stability and deployment robustness.


---

## 9. Limitations

- Risk proxy, not confirmed medical injuries.
- No GPS or training load data.
- No biomechanical inputs.
- Temporal non-stationarity affects stability.

---

## 10. Future improvements

- Hybrid model: workload + performance metrics
- Season-specific calibration
- Bayesian temporal smoothing
- Real-time monitoring dashboard

---

## 11. Professional context

This project demonstrates:

- Time-aware model validation
- Feature ablation testing
- Operational constraint simulation
- Interpretability-focused modeling
- Governance-oriented experimentation

It reflects a production-oriented analytics workflow aligned with professional football performance environments and modern model governance standards, emphasizing robustness over inflated headline metrics.

---

## Author

Manuel Pérez Bañuls  
Data Science & Football Performance Analytics  
Portfolio Project

