from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Football Risk Analytics",
    page_icon="⚽",
    layout="wide",
)

PREDICTIONS_PATH = Path("outputs/predictions/player_risk_scores.csv")
RANKED_PATH = Path("outputs/alerts/ranked_players.csv")
QUEUE_PATH = Path("outputs/alerts/medical_review_queue.csv")
ALERTS_PATH = Path("outputs/alerts/top_players_alerts.csv")


@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    return df


pred_df = load_csv(PREDICTIONS_PATH)
ranked_df = load_csv(RANKED_PATH)
queue_df = load_csv(QUEUE_PATH)
alerts_df = load_csv(ALERTS_PATH)

st.title("Football Risk Analytics Dashboard")
st.caption("Workload risk scoring, ranking, and medical review queue")

missing_files = [
    str(p) for p in [PREDICTIONS_PATH, RANKED_PATH, QUEUE_PATH, ALERTS_PATH] if not p.exists()
]
if missing_files:
    st.warning(
        "Faltan algunos outputs. Ejecuta antes el pipeline de scoring/ranking/alerts.\n\n"
        + "\n".join(missing_files)
    )

if ranked_df.empty:
    st.stop()

# Normalización de tipos
if "match_date" in ranked_df.columns:
    ranked_df["match_date"] = pd.to_datetime(ranked_df["match_date"], errors="coerce")

if not queue_df.empty and "match_date" in queue_df.columns:
    queue_df["match_date"] = pd.to_datetime(queue_df["match_date"], errors="coerce")

if not alerts_df.empty and "match_date" in alerts_df.columns:
    alerts_df["match_date"] = pd.to_datetime(alerts_df["match_date"], errors="coerce")

# Sidebar filtros
st.sidebar.header("Filtros")

available_dates = (
    ranked_df["match_date"].dropna().dt.strftime("%Y-%m-%d").sort_values().unique().tolist()
    if "match_date" in ranked_df.columns
    else []
)
selected_date = st.sidebar.selectbox(
    "Match date",
    options=["All"] + available_dates,
    index=0,
)

available_teams = sorted(ranked_df["team"].dropna().astype(str).unique().tolist()) if "team" in ranked_df.columns else []
selected_team = st.sidebar.selectbox(
    "Team",
    options=["All"] + available_teams,
    index=0,
)

top_n = st.sidebar.slider("Top N jugadores a mostrar", min_value=5, max_value=50, value=10, step=5)

filtered_ranked = ranked_df.copy()

if selected_date != "All":
    filtered_ranked = filtered_ranked[
        filtered_ranked["match_date"].dt.strftime("%Y-%m-%d") == selected_date
    ]

if selected_team != "All":
    filtered_ranked = filtered_ranked[filtered_ranked["team"].astype(str) == selected_team]

# KPIs
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Rows scored", f"{len(pred_df):,}")

with col2:
    st.metric("Rows ranked", f"{len(ranked_df):,}")

with col3:
    st.metric("Alerts generated", f"{len(alerts_df):,}" if not alerts_df.empty else "0")

with col4:
    mean_score = filtered_ranked["risk_score"].mean() if "risk_score" in filtered_ranked.columns and not filtered_ranked.empty else 0
    st.metric("Mean filtered risk", f"{mean_score:.3f}")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "Top Risk Players",
    "Medical Review Queue",
    "Alerts",
    "Summary",
])

with tab1:
    st.subheader("Top Risk Players")
    top_players = filtered_ranked.sort_values("risk_score", ascending=False).head(top_n)
    show_cols = [
        c for c in [
            "match_date", "player_id", "team", "risk_score", "risk_rank",
            "model_name", "model_version", "scored_at_utc"
        ] if c in top_players.columns
    ]
    st.dataframe(top_players[show_cols], use_container_width=True)

    if not top_players.empty and "risk_score" in top_players.columns:
        st.subheader("Risk Score Distribution")
        st.bar_chart(top_players.set_index("player_id")["risk_score"])

with tab2:
    st.subheader("Medical Review Queue")
    queue_view = queue_df.copy()

    if not queue_view.empty:
        if selected_date != "All":
            queue_view = queue_view[
                queue_view["match_date"].dt.strftime("%Y-%m-%d") == selected_date
            ]
        if selected_team != "All" and "team" in queue_view.columns:
            queue_view = queue_view[queue_view["team"].astype(str) == selected_team]

        st.dataframe(queue_view.head(100), use_container_width=True)
    else:
        st.info("No medical review queue available.")

with tab3:
    st.subheader("Alerts")
    alerts_view = alerts_df.copy()

    if not alerts_view.empty:
        if selected_date != "All":
            alerts_view = alerts_view[
                alerts_view["match_date"].dt.strftime("%Y-%m-%d") == selected_date
            ]
        if selected_team != "All" and "team" in alerts_view.columns:
            alerts_view = alerts_view[alerts_view["team"].astype(str) == selected_team]

        st.dataframe(alerts_view.head(100), use_container_width=True)
    else:
        st.info("No alerts file available.")

with tab4:
    st.subheader("Summary")
    st.write("**Selected filters**")
    st.write({
        "match_date": selected_date,
        "team": selected_team,
        "top_n": top_n,
    })

    if "team" in filtered_ranked.columns and not filtered_ranked.empty:
        st.subheader("Risk by Team")
        team_summary = (
            filtered_ranked.groupby("team", dropna=False)["risk_score"]
            .agg(["count", "mean", "max"])
            .sort_values("mean", ascending=False)
            .reset_index()
        )
        st.dataframe(team_summary, use_container_width=True)

st.divider()
st.caption("Built on exported batch scoring, ranking, and alert outputs.")
