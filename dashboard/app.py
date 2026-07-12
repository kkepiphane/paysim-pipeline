"""Dashboard Streamlit — lecture seule sur le warehouse Postgres.

Ne touche jamais au Gold Parquet directement : tout passe par les tables déjà
chargées par `make load-warehouse` (src/warehouse/load.py), pour ne jamais
resservir un scan Parquet complet à chaque interaction utilisateur.

Palette et règles de la skill dataviz du projet :
  - un seul hue (bleu) pour les graphiques à une seule série (magnitude par
    catégorie) : pas de légende nécessaire, le titre + l'axe suffisent.
  - le rouge "critical" est réservé au KPI de fraude, jamais réutilisé ailleurs.
  - ordre catégoriel fixe pour transaction_type et amount_band, jamais trié
    par valeur (l'identité ne doit pas dépendre du filtre courant).
"""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from src import config
from src.warehouse.load import read_dataframe

ACCENT = "#2a78d6"    # bleu — magnitude, séries uniques
CRITICAL = "#d03b3b"  # rouge "status" — réservé à la fraude
AMOUNT_BAND_ORDER = ["0-1K", "1K-10K", "10K-100K", "100K-1M", "1M+"]

st.set_page_config(page_title="PaySim — Fraude Mobile Money", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(config.warehouse_url())


@st.cache_data(ttl=300)
def load_table(name: str) -> pd.DataFrame:
    return read_dataframe(get_engine(), name)


st.title("PaySim — Détection de fraude mobile money")
st.caption(
    "Lecture depuis le warehouse Postgres (agrégats Gold), rafraîchi via "
    "`make load-warehouse`."
)

kpi = load_table("kpi_summary").iloc[0]

col1, col2, col3 = st.columns(3)
col1.metric("Transactions", f"{kpi['n_transactions']:,.0f}")
col2.metric("Fraudes détectées", f"{kpi['n_fraud']:,.0f}")
col3.metric("Taux de fraude", f"{kpi['fraud_pct']:.3f}%", delta_color="inverse")

st.divider()

# --- Fraude par type de transaction -----------------------------------------
fraud_by_type_hour = load_table("agg_fraud_by_type_hour")
by_type = (
    fraud_by_type_hour.groupby("transaction_type", as_index=False)
    .agg(n_transactions=("n_transactions", "sum"), n_fraud=("n_fraud", "sum"))
)
by_type["fraud_pct"] = (by_type["n_fraud"] / by_type["n_transactions"] * 100).round(3)
by_type["transaction_type"] = pd.Categorical(
    by_type["transaction_type"], categories=config.TRANSACTION_TYPES, ordered=True,
)
by_type = by_type.sort_values("transaction_type")

fig_type = go.Figure(go.Bar(
    x=by_type["transaction_type"], y=by_type["fraud_pct"],
    marker_color=ACCENT,
    text=by_type["fraud_pct"].map(lambda v: f"{v:.2f}%"),
    textposition="outside",
    hovertemplate="%{x}<br>Taux de fraude : %{y:.3f}%<extra></extra>",
))
fig_type.update_layout(
    title="Taux de fraude par type de transaction",
    yaxis_title="Taux de fraude (%)", xaxis_title=None,
    template="plotly_white", showlegend=False, margin=dict(t=60),
)
st.plotly_chart(fig_type, use_container_width=True)

# --- Fraude par heure de la journée ------------------------------------------
by_hour = (
    fraud_by_type_hour.groupby("hour_of_day", as_index=False)["n_fraud"]
    .sum()
    .sort_values("hour_of_day")
)

fig_hour = go.Figure(go.Scatter(
    x=by_hour["hour_of_day"], y=by_hour["n_fraud"],
    mode="lines+markers", line=dict(color=ACCENT, width=2), marker=dict(size=8),
    hovertemplate="Heure %{x}h<br>Fraudes : %{y}<extra></extra>",
))
fig_hour.update_layout(
    title="Fraudes par heure de la journée (1 step PaySim = 1h)",
    xaxis_title="Heure", yaxis_title="Nombre de fraudes",
    template="plotly_white", margin=dict(t=60),
)
st.plotly_chart(fig_hour, use_container_width=True)

# --- Fraude par tranche de montant ------------------------------------------
by_band = load_table("agg_fraud_by_amount_band")
by_band["amount_band"] = pd.Categorical(
    by_band["amount_band"], categories=AMOUNT_BAND_ORDER, ordered=True,
)
by_band = by_band.sort_values("amount_band")

fig_band = go.Figure(go.Bar(
    x=by_band["amount_band"], y=by_band["fraud_rate"] * 100,
    marker_color=ACCENT,
    text=(by_band["fraud_rate"] * 100).map(lambda v: f"{v:.2f}%"),
    textposition="outside",
    hovertemplate="%{x}<br>Taux de fraude : %{y:.3f}%<extra></extra>",
))
fig_band.update_layout(
    title="Taux de fraude par tranche de montant",
    yaxis_title="Taux de fraude (%)", xaxis_title=None,
    template="plotly_white", showlegend=False, margin=dict(t=60),
)
st.plotly_chart(fig_band, use_container_width=True)

# --- Top comptes -------------------------------------------------------------
st.subheader("Top 10 comptes par volume sortant")
top = load_table("agg_top_accounts").sort_values("total_out", ascending=False).head(10)
st.dataframe(
    top.rename(columns={
        "name_orig": "Compte", "total_out": "Volume sortant",
        "n_transactions": "Transactions", "n_fraud": "Fraudes",
    }),
    use_container_width=True, hide_index=True,
)
