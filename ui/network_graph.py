"""
ui/network_graph.py — v11 ULTRA (FINAL)

County ↔ Lender Network Graph
Visual-stable for large states (CA / TX / NY)

Guarantees:
- State-first default (performance)
- Top-lender auto-focus (visual clarity)
- Rule-based edges (not time-series lines)
- Fallback coloring when signal is flat
- Never renders visually empty unless data is truly empty
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st
import altair as alt


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _table_exists(con, name: str) -> bool:
    try:
        return bool(
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [name.lower()],
            ).fetchone()[0]
        )
    except Exception:
        return False


def _get_db_path(con) -> str:
    try:
        df = con.execute("PRAGMA database_list").fetchdf()
        for _, r in df.iterrows():
            f = str(r.get("file") or "")
            if f and f.lower() not in ("", ":memory:"):
                return f
    except Exception:
        pass
    return "data/db/v_finder.duckdb"


# ---------------------------------------------------------------------
# Cached data load
# ---------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def _load_edges(db_path: str) -> pd.DataFrame:
    con = duckdb.connect(db_path, read_only=True)

    if not _table_exists(con, "county_lender_signals"):
        return pd.DataFrame()

    raw = con.execute("SELECT * FROM county_lender_signals").fetchdf()
    if raw.empty:
        return pd.DataFrame()

    # Column resolution (defensive)
    cols = {c.lower(): c for c in raw.columns}

    def pick(*names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    geoid  = pick("geoid")
    state  = pick("stusps", "state")
    county = pick("county_name", "county", "name")
    lender = pick("lender_name", "lender", "servicinglendername")
    loans  = pick("loan_count", "loans")
    total  = pick("total_approved", "ppp_current_total")
    signal = pick("signal_score", "hidden_signal_score")

    if not geoid or not state or not lender:
        return pd.DataFrame()

    df = pd.DataFrame({
        "GEOID": raw[geoid].astype(str),
        "State": raw[state].astype(str),
        "County": raw[county].astype(str) if county else raw[geoid].astype(str),
        "Lender": raw[lender].astype(str),
        "loan_count": pd.to_numeric(raw[loans], errors="coerce").fillna(0) if loans else 0,
        "total_approved": pd.to_numeric(raw[total], errors="coerce").fillna(0) if total else 0,
        "signal_score": pd.to_numeric(raw[signal], errors="coerce").fillna(0) if signal else 0,
    })

    # -----------------------------------------------------------------
    # Edge strength (rank-based primary)
    # -----------------------------------------------------------------
    EPS = 1e-6
    df["edge_strength"] = (
        0.45 * df["total_approved"].rank(pct=True)
        + 0.35 * df["signal_score"].rank(pct=True)
        + 0.20 * df["loan_count"].rank(pct=True)
    ).clip(lower=EPS)

    # -----------------------------------------------------------------
    # Visibility fallback (critical for large states)
    # -----------------------------------------------------------------
    if df["signal_score"].std() < 1e-6:
        max_amt = df["total_approved"].max() or 1.0
        max_loans = df["loan_count"].max() or 1.0
        df["edge_strength"] = (
            0.6 * (df["total_approved"] / max_amt)
            + 0.4 * (df["loan_count"] / max_loans)
        ).fillna(0.01)

    df["edge_strength"] = df["edge_strength"].clip(lower=0.03)

    return df


# ---------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------

def render_network_graph(con: duckdb.DuckDBPyConnection) -> None:
    st.subheader("County ↔ Lender Network")

    db_path = _get_db_path(con)
    edges = _load_edges(db_path)

    if edges.empty:
        st.warning("No lender network data available.")
        return

    # -----------------------------------------------------------------
    # Scope (STATE FIRST)
    # -----------------------------------------------------------------
    states = sorted(edges["State"].unique().tolist())
    default_state = (
        edges.groupby("State")["total_approved"]
        .sum()
        .sort_values(ascending=False)
        .index[0]
    )

    scope = st.radio(
        "Scope",
        ["State (fast)", "Nationwide (slow)"],
        horizontal=True,
        index=0,
    )

    if scope.startswith("State"):
        state = st.selectbox("State", states, index=states.index(default_state))
        edges = edges[edges["State"] == state]

    # -----------------------------------------------------------------
    # Density
    # -----------------------------------------------------------------
    expand_pct = st.slider(
        "Network Density (%)",
        10, 100,
        40 if scope.startswith("State") else 15,
        step=5,
    )

    keep_n = max(75, int(len(edges) * expand_pct / 100))
    edges = edges.nlargest(keep_n, "edge_strength")

    # -----------------------------------------------------------------
    # TOP LENDER AUTO-FOCUS (VISUAL FIX)
    # -----------------------------------------------------------------
    TOP_LENDER_DEFAULT = 25 if scope.startswith("State") else 15
    top_lenders = (
        edges.groupby("Lender")["total_approved"]
        .sum()
        .sort_values(ascending=False)
        .head(TOP_LENDER_DEFAULT)
        .index
    )
    edges = edges[edges["Lender"].isin(top_lenders)]

    # Optional lender focus
    lenders = sorted(edges["Lender"].unique().tolist())
    lender_pick = st.selectbox("Focus on Lender", ["All"] + lenders)
    if lender_pick != "All":
        edges = edges[edges["Lender"] == lender_pick]

    # -----------------------------------------------------------------
    # Min edge (default = 0)
    # -----------------------------------------------------------------
    min_edge = st.slider(
        "Minimum Edge Strength",
        0.0,
        float(edges["edge_strength"].max()),
        0.0,
        step=0.01,
    )
    edges = edges[edges["edge_strength"] >= min_edge]

    if edges.empty:
        st.warning("No edges remain after filtering.")
        return

    # -----------------------------------------------------------------
    # Analyst feedback
    # -----------------------------------------------------------------
    flat_signal = edges["signal_score"].std() < 1e-6
    if flat_signal:
        st.info(
            "Signal scores are uniform in this view. "
            "Edge color reflects lender dominance (PPP volume) instead of hidden signal."
        )

    color_field = "signal_score" if not flat_signal else "edge_strength"

    # -----------------------------------------------------------------
    # Chart (RULES, NOT LINES)
    # -----------------------------------------------------------------
    chart = (
        alt.Chart(edges)
        .mark_rule(opacity=0.6)
        .encode(
            x=alt.X("Lender:N", title="Lender"),
            y=alt.Y(
                "County:N",
                title="County",
                sort="-x",
                axis=alt.Axis(labelLimit=200),
            ),
            size=alt.Size(
                "edge_strength:Q",
                scale=alt.Scale(range=[0.8, 7]),
                legend=alt.Legend(title="Edge Strength"),
            ),
            color=alt.Color(
                f"{color_field}:Q",
                scale=alt.Scale(scheme="redyellowgreen"),
                legend=alt.Legend(
                    title="Signal Heat" if color_field == "signal_score" else "Edge Strength"
                ),
            ),
            detail="GEOID:N",
            tooltip=[
                "State:N",
                "County:N",
                "Lender:N",
                alt.Tooltip("total_approved:Q", title="Total Approved", format=",.0f"),
                alt.Tooltip("loan_count:Q", title="Loans"),
                alt.Tooltip("signal_score:Q", title="Signal"),
                alt.Tooltip("edge_strength:Q", title="Edge Strength", format=".2f"),
            ],
        )
    )

    st.altair_chart(chart, use_container_width=True)

    # -----------------------------------------------------------------
    # Export
    # -----------------------------------------------------------------
    st.download_button(
        "Download Network Edges (CSV)",
        data=edges.to_csv(index=False),
        file_name="lender_network_edges.csv",
        mime="text/csv",
    )

    with st.expander("Edge Debug Table"):
        st.dataframe(
            edges.sort_values("edge_strength", ascending=False),
            use_container_width=True,
        )
