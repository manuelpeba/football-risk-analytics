import json
from pathlib import Path
from textwrap import dedent

OUT = Path("notebooks/05_rolling_backtest_and_monitoring_EN.ipynb")
OUT.parent.mkdir(parents=True, exist_ok=True)

nb = {
    "cells": [],
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.11"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

def add(cell_type: str, content: str):
    cell = {"cell_type": cell_type, "metadata": {}, "source": [l + "\n" for l in content.strip().splitlines()]}
    if cell_type == "code":
        cell.update({"execution_count": None, "outputs": []})
    nb["cells"].append(cell)

add("markdown", """
# Notebook 05 — Rolling Backtest + Monitoring (Elite-Club Validation)

Rolling-origin backtest + monitoring signals (capacity-constrained alerts).

**What you get**
- Rolling (walk-forward) validation across time (preferred: weekly blocks if `match_date` exists)
- Out-of-sample metrics per fold: ROC AUC, PR AUC, Brier
- Operational KPIs per fold using TRAIN-only capacity threshold (quantile):
  - alert_rate, precision@thr, recall@thr, alerts per 25
- Optional Top-K per `match_id` if available
""")

add("code", dedent("""
import os, warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import duckdb

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier

from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    brier_score_loss,
    precision_score,
    recall_score,
    f1_score,
)

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 140)

# -----------------------
# CONFIG
# -----------------------
DB_PATH = os.getenv("FRA_DUCKDB_PATH", "lakehouse/analytics.duckdb")
TABLE  = os.getenv("FRA_TABLE", "player_dataset_predictive")

FEATURES = ["minutes_last_7d","minutes_last_14d","minutes_last_28d","acwr"]
TARGET = "high_risk_next"

TIME_COL_CANDIDATES = ["match_date","date","fixture_date","timestamp"]
MATCH_ID_COL = "match_id"

# Capacity constraint (policy)
TARGET_ALERT_RATE = 0.10

# Time folds (preferred if time column exists)
TIME_BLOCK = "W"         # weekly blocks
MIN_TRAIN_BLOCKS = 10
TEST_BLOCKS = 2
STEP_BLOCKS = 1

# Row folds (fallback if no time column)
MIN_TRAIN_ROWS = 8000
TEST_HORIZON_ROWS = 2000
STEP_ROWS = 2000

# -----------------------
# LOAD DATA
# -----------------------
con = duckdb.connect(DB_PATH, read_only=True)
cols = con.execute(f"DESCRIBE {TABLE}").fetchdf()["column_name"].tolist()
time_col = next((c for c in TIME_COL_CANDIDATES if c in cols), None)

select_cols = [c for c in FEATURES if c in cols] + [TARGET]
if MATCH_ID_COL in cols:
    select_cols.append(MATCH_ID_COL)
if time_col:
    select_cols.append(time_col)

df = con.execute(f"SELECT {', '.join(select_cols)} FROM {TABLE}").fetchdf()
con.close()

df = df.dropna(subset=[TARGET] + [c for c in FEATURES if c in df.columns]).copy()
df[TARGET] = df[TARGET].astype(int)

if time_col:
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col]).sort_values(time_col).reset_index(drop=True)
else:
    df = df.reset_index(drop=True)

display(df.head())
print("Shape:", df.shape, "| time_col:", time_col)
"""))

add("code", dedent("""
# -----------------------
# MODELS + POLICY
# -----------------------
def build_models():
    logit = Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(max_iter=2000)),
    ])
    hgb = HistGradientBoostingClassifier(
        max_depth=3,
        learning_rate=0.1,
        max_iter=300,
        random_state=42,
    )
    return logit, hgb

def threshold_for_target_alert_rate(p_train, target_rate=0.10):
    # TRAIN-only quantile threshold to hit target alert volume
    return float(np.quantile(np.asarray(p_train), 1 - float(target_rate)))

def policy_metrics(y_true, p, thr):
    pred = (np.asarray(p) >= thr).astype(int)
    return dict(
        threshold=float(thr),
        alerts=int(pred.sum()),
        alert_rate=float(pred.mean()),
        precision=float(precision_score(y_true, pred, zero_division=0)),
        recall=float(recall_score(y_true, pred, zero_division=0)),
        f1=float(f1_score(y_true, pred, zero_division=0)),
    )

def expected_alerts_per_25(alert_rate):
    return float(alert_rate) * 25.0

def topk_metrics(df_block, p, y_col, k, group_col):
    out = df_block.copy()
    out["_p"] = p
    out["_r"] = out.groupby(group_col)["_p"].rank(method="first", ascending=False)
    out["flag_topk"] = (out["_r"] <= k).astype(int)

    y_true = out[y_col].astype(int).values
    y_pred = out["flag_topk"].values

    groups = out[group_col].nunique()
    alerts = int(y_pred.sum())

    return dict(
        topk_k=int(k),
        topk_groups=int(groups),
        topk_alerts_total=alerts,
        topk_alerts_per_group=float(alerts / max(groups, 1)),
        topk_precision=float(precision_score(y_true, y_pred, zero_division=0)),
        topk_recall=float(recall_score(y_true, y_pred, zero_division=0)),
    )
"""))

add("code", dedent("""
# -----------------------
# ROLLING FOLDS
# -----------------------
def make_time_blocks(df, time_col, block="W"):
    return pd.to_datetime(df[time_col]).dt.to_period(block).astype(str)

def rolling_folds_time(df, time_col, block, min_train_blocks, test_blocks, step_blocks):
    blocks = make_time_blocks(df, time_col, block)
    uniq = pd.Index(blocks.unique())

    folds = []
    start = min_train_blocks
    while start + test_blocks <= len(uniq):
        tr = uniq[:start]
        te = uniq[start:start + test_blocks]

        tr_idx = df.index[blocks.isin(tr)].values
        te_idx = df.index[blocks.isin(te)].values

        folds.append((tr_idx, te_idx, tr[-1], te[-1]))
        start += step_blocks

    return folds

def rolling_folds_rows(n_rows, min_train_rows, test_horizon_rows, step_rows):
    folds = []
    start = min_train_rows
    while start + test_horizon_rows <= n_rows:
        folds.append((
            np.arange(0, start),
            np.arange(start, start + test_horizon_rows),
            start,
            start + test_horizon_rows,
        ))
        start += step_rows
    return folds

folds = (
    rolling_folds_time(df, time_col, TIME_BLOCK, MIN_TRAIN_BLOCKS, TEST_BLOCKS, STEP_BLOCKS)
    if time_col else
    rolling_folds_rows(len(df), MIN_TRAIN_ROWS, TEST_HORIZON_ROWS, STEP_ROWS)
)

print("Folds:", len(folds))
"""))

add("code", dedent("""
# -----------------------
# BACKTEST LOOP
# -----------------------
def model_metrics(y_true, p):
    return dict(
        roc_auc=float(roc_auc_score(y_true, p)),
        pr_auc=float(average_precision_score(y_true, p)),
        brier=float(brier_score_loss(y_true, p)),
        prevalence=float(np.mean(y_true)),
        avg_p=float(np.mean(p)),
    )

rows = []

for fold, (tr_idx, te_idx, tr_end, te_end) in enumerate(folds, start=1):
    tr = df.iloc[tr_idx].copy()
    te = df.iloc[te_idx].copy()

    X_tr = tr[FEATURES]
    y_tr = tr[TARGET].values
    X_te = te[FEATURES]
    y_te = te[TARGET].values

    logit, hgb = build_models()
    logit.fit(X_tr, y_tr)
    hgb.fit(X_tr, y_tr)

    pL_tr = logit.predict_proba(X_tr)[:, 1]
    pL_te = logit.predict_proba(X_te)[:, 1]
    pH_tr = hgb.predict_proba(X_tr)[:, 1]
    pH_te = hgb.predict_proba(X_te)[:, 1]

    tL = threshold_for_target_alert_rate(pL_tr, TARGET_ALERT_RATE)
    tH = threshold_for_target_alert_rate(pH_tr, TARGET_ALERT_RATE)

    label = f"{tr_end} → {te_end}" if time_col else f"rows {tr_end} → {te_end}"

    for name, p_tr, p_te, thr in [("logit", pL_tr, pL_te, tL), ("hgb", pH_tr, pH_te, tH)]:
        mm = model_metrics(y_te, p_te)
        pol_tr = policy_metrics(y_tr, p_tr, thr)
        pol_te = policy_metrics(y_te, p_te, thr)

        r = dict(
            fold=fold,
            fold_label=label,
            model=name,
            roc_auc_test=mm["roc_auc"],
            pr_auc_test=mm["pr_auc"],
            brier_test=mm["brier"],
            prevalence_test=mm["prevalence"],
            avg_p_test=mm["avg_p"],
            threshold_train_q=float(thr),
            train_alert_rate=pol_tr["alert_rate"],
            test_alert_rate=pol_te["alert_rate"],
            test_precision_at_thr=pol_te["precision"],
            test_recall_at_thr=pol_te["recall"],
            test_f1_at_thr=pol_te["f1"],
            expected_alerts_per_25=expected_alerts_per_25(pol_te["alert_rate"]),
        )

        if MATCH_ID_COL in te.columns:
            r.update(topk_metrics(te, p_te, TARGET, k=3, group_col=MATCH_ID_COL))

        rows.append(r)

res = pd.DataFrame(rows)
display(res.head())

exec_tbl = res.groupby("model")[
    ["roc_auc_test","pr_auc_test","brier_test","test_alert_rate","test_precision_at_thr","test_recall_at_thr","expected_alerts_per_25"]
].agg(["mean","std"])
display(exec_tbl)
"""))

add("code", dedent("""
# -----------------------
# STABILITY PLOTS
# -----------------------
def plot_over_folds(df, y, title):
    for m in ["logit", "hgb"]:
        d = df[df.model == m].sort_values("fold")
        plt.plot(d["fold"], d[y], marker="o", label=m)
    plt.title(title)
    plt.xlabel("Fold")
    plt.ylabel(y)
    plt.legend()
    plt.tight_layout()
    plt.show()

plot_over_folds(res, "pr_auc_test", "PR AUC over rolling folds (TEST)")
plot_over_folds(res, "brier_test", "Brier score over rolling folds (TEST)")
plot_over_folds(res, "test_alert_rate", "Alert rate over rolling folds (TEST, thr from TRAIN)")
plot_over_folds(res, "test_precision_at_thr", "Precision@thr over rolling folds (TEST)")
plot_over_folds(res, "test_recall_at_thr", "Recall@thr over rolling folds (TEST)")
"""))

OUT.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"Written: {OUT}")
