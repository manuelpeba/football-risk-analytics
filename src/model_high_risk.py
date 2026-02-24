import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score

con = duckdb.connect("lakehouse/analytics.duckdb")

df = con.execute("""
SELECT *
FROM player_dataset_final
WHERE acwr IS NOT NULL
""").df()

con.close()

# Features
features = [
    "minutes_last_7d",
    "minutes_last_14d",
    "minutes_last_28d",
    "minutes_last_5_matches",
    "xg_last_5",
    "shots_last_5",
    "progressive_last_5",
    "trend_xg_3v3",
    "shots_per90",
    "xg_per90",
    "progressive_x_per90"
]

X = df[features]
y = df["high_risk"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = RandomForestClassifier(
    n_estimators=200,
    max_depth=8,
    random_state=42
)

model.fit(X_train, y_train)

pred = model.predict(X_test)
proba = model.predict_proba(X_test)[:,1]

print("ROC-AUC:", roc_auc_score(y_test, proba))
print(classification_report(y_test, pred))

# Feature importance
importances = pd.Series(
    model.feature_importances_,
    index=features
).sort_values(ascending=False)

print("\nTop Features:")
print(importances)
