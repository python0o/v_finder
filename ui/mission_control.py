"""
ui/mission_control.py — FINAL (Launch Locked)

Mission Control:
- Fast operational dashboard
- Uses interpreted outliers from ui/outliers.py
- Peer normalization toggle (demographic cohorts)
- Filters and routing hub
- No heavy PPP joins on first render
- Streamlit-compatible rerun (st.rerun)

Depends on:
- county_scores (risk + PPP + demographics + hidden signal)
- ui/outliers.py (load_outliers)
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from ui.outliers import load_outliers


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        return bool(
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [name.lower()],
            ).fetchone()[0]
        )
    except Exception:
        return False


def _fmt_money(x) -> str:
    try:
        return f"${float(x):,.0f}"
    except Exception:
        return "—"


def _fmt_num(x) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return "—"


def _fmt_pct(x) -> str:
    try:
        v = float(x)
        if v <= 1.5:
            return f"{v * 100:.1f}%"
        return f"{v:.1f}%"
    except Exception:
        return "—"


# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------
def _load_ops_frame(con: duckdb.DuckDBPyConnection, use_peer_norm: bool) -> pd.DataFrame:
    """
    Mission Control should be fast: pull interpreted outliers which already includes
    county_scores fields (risk + PPP + demographics + hidden signal).
    """
    if not _table_exists(con, "county_scores"):
        return pd.DataFrame()

    df = load_outliers(con, use_peer_norm=use_peer_norm)
    if df is None or df.empty:
        return pd.DataFrame()

    # ensure canonical types (best-effort)
    for c in ["risk_score", "ppp_current_total", "ppp_per_capita", "ppp_loan_count", "hidden_signal_score"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # canonical identifiers
    for c in ["GEOID", "STUSPS", "NAME"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    # UI label
    if "STUSPS" in df.columns and "NAME" in df.columns:
        df["label"] = df["NAME"] + ", " + df["STUSPS"]
    else:
        df["label"] = df.get("GEOID", "").astype(str)

    # robust defaults
    if "outlier_tier" not in df.columns:
        df["outlier_tier"] = df.get("outlier_flag", False).map(lambda x: "SEVERE" if x else "NORMAL")
    if "outlier_score" not in df.columns:
        # if peer-normalized engine doesn’t output a score, treat flag as 1
        df["outlier_score"] = df.get("outlier_flag", False).astype(int)

    if "risk_tier" not in df.columns:
        df["risk_tier"] = "UNKNOWN"

    if "hidden_signal_score" not in df.columns:
        df["hidden_signal_score"] = 0.0

    return df


# ---------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------
def render_mission_control_page(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Mission Control")
    st.caption("Operational triage for county risk, peer-normalized outliers, and investigative routing.")

    # -----------------------------------------------------------------
    # Guard rails
    # -----------------------------------------------------------------
    if not _table_exists(con, "county_scores"):
        st.error("Missing table: county_scores")
        return

    # -----------------------------------------------------------------
    # Peer normalization toggle
    # -----------------------------------------------------------------
    use_peer_norm = st.toggle(
        "Use Demographic Peer Normalization",
        value=True,
        help="Compare counties only against demographically similar peers (population, poverty, unemployment).",
    )

    df = _load_ops_frame(con, use_peer_norm=use_peer_norm)
    if df.empty:
        st.warning("No county scoring data available to render Mission Control.")
        return

    st.caption("Peer normalization is ON." if use_peer_norm else "Peer normalization is OFF (global comparisons).")

    # -----------------------------------------------------------------
    # Filters
    # -----------------------------------------------------------------
    st.markdown("### Filters")

    f1, f2, f3, f4 = st.columns([1, 1, 1, 1])

    states = ["All"] + sorted(df["STUSPS"].dropna().unique().tolist()) if "STUSPS" in df.columns else ["All"]
    tiers = ["All"] + sorted(df["risk_tier"].dropna().astype(str).unique().tolist()) if "risk_tier" in df.columns else ["All"]
    outlier_tiers = ["All", "SEVERE", "HIGH", "MILD", "NORMAL"]

    with f1:
        state_pick = st.selectbox("State", states, index=0)
    with f2:
        tier_pick = st.selectbox("Risk Tier", tiers, index=0)
    with f3:
        outlier_pick = st.selectbox("Outlier Tier", outlier_tiers, index=0)
    with f4:
        only_anomalous = st.toggle("Only flagged", value=False, help="Show only counties flagged by the outlier engine.")

    q = st.text_input("Search (county name contains)", value="")

    view = df.copy()
    if state_pick != "All" and "STUSPS" in view.columns:
        view = view[view["STUSPS"] == state_pick]
    if tier_pick != "All" and "risk_tier" in view.columns:
        view = view[view["risk_tier"].astype(str) == tier_pick]
    if outlier_pick != "All" and "outlier_tier" in view.columns:
        view = view[view["outlier_tier"].astype(str) == outlier_pick]
    if only_anomalous and "outlier_flag" in view.columns:
        view = view[view["outlier_flag"] == True]
    if q.strip() and "NAME" in view.columns:
        view = view[view["NAME"].str.contains(q.strip(), case=False, na=False)]

    # Default sorting: most actionable first
    sort_mode = st.selectbox(
        "Sort",
        [
            "Risk Score (desc)",
            "Outlier (desc)",
            "Hidden Signal (desc)",
            "PPP Total (desc)",
            "PPP Per Capita (desc)",
        ],
        index=0,
    )

    if sort_mode.startswith("Risk Score") and "risk_score" in view.columns:
        view = view.sort_values("risk_score", ascending=False)
    elif sort_mode.startswith("Outlier"):
        if "outlier_score" in view.columns and "risk_score" in view.columns:
            view = view.sort_values(["outlier_score", "risk_score"], ascending=[False, False])
        elif "outlier_flag" in view.columns:
            view = view.sort_values("outlier_flag", ascending=False)
    elif sort_mode.startswith("Hidden") and "hidden_signal_score" in view.columns:
        view = view.sort_values(["hidden_signal_score", "risk_score"], ascending=[False, False]) if "risk_score" in view.columns else view.sort_values("hidden_signal_score", ascending=False)
    elif sort_mode.startswith("PPP Total") and "ppp_current_total" in view.columns:
        view = view.sort_values("ppp_current_total", ascending=False)
    elif sort_mode.startswith("PPP Per Capita") and "ppp_per_capita" in view.columns:
        view = view.sort_values("ppp_per_capita", ascending=False)

    st.markdown("---")

    # -----------------------------------------------------------------
    # Selection + KPI strip
    # -----------------------------------------------------------------
    st.markdown("### County Focus")

    labels = view["label"].tolist() if "label" in view.columns else []
    if not labels:
        st.warning("No counties match current filters.")
        return

    label_to_geoid = dict(zip(view["label"], view["GEOID"])) if "GEOID" in view.columns else {}
    default_geoid = st.session_state.get("vf_county_focus")

    default_label = None
    if default_geoid and "GEOID" in view.columns and "label" in view.columns:
        m = view[view["GEOID"].astype(str) == str(default_geoid)]
        if not m.empty:
            default_label = m.iloc[0]["label"]

    selected_label = st.selectbox(
        "Select a county",
        options=labels,
        index=labels.index(default_label) if default_label in labels else 0,
    )

    selected_geoid = str(label_to_geoid[selected_label]) if label_to_geoid else None
    if selected_geoid:
        st.session_state["vf_county_focus"] = selected_geoid

    row = view[view["label"] == selected_label].iloc[0]

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Risk Score", f"{float(row.get('risk_score', 0.0)):.1f}" if "risk_score" in view.columns else "—")
    k2.metric("Risk Tier", str(row.get("risk_tier", "—")) if "risk_tier" in view.columns else "—")
    k3.metric("Outlier Tier", str(row.get("outlier_tier", "—")) if "outlier_tier" in view.columns else "—")
    k4.metric("Hidden Signal", f"{float(row.get('hidden_signal_score', 0.0)):.2f}" if "hidden_signal_score" in view.columns else "—")
    k5.metric("PPP Total", _fmt_money(row.get("ppp_current_total", 0.0)) if "ppp_current_total" in view.columns else "—")

    demo_cols_present = any(c in view.columns for c in ["Total_Pop", "Poverty_Rate", "Unemployment_Rate"])
    if demo_cols_present:
        d1, d2, d3 = st.columns(3)
        d1.metric("Population", _fmt_num(row.get("Total_Pop", None)))
        d2.metric("Poverty", _fmt_pct(row.get("Poverty_Rate", None)))
        d3.metric("Unemployment", _fmt_pct(row.get("Unemployment_Rate", None)))

    if use_peer_norm:
        # Peer explanation if present
        peer_group = row.get("peer_group", None)
        peer_basis = row.get("outlier_basis", "PEER")
        peer_z = row.get("ppp_peer_z", None)
        with st.expander("Peer normalization details", expanded=False):
            st.write(f"Outlier basis: **{peer_basis}**")
            if peer_group is not None:
                st.write(f"Peer group: `{peer_group}`")
            if peer_z is not None:
                try:
                    st.write(f"Peer z-score (PPP per capita): **{float(peer_z):.2f}**")
                except Exception:
                    st.write(f"Peer z-score (PPP per capita): `{peer_z}`")

    st.markdown("---")

    # -----------------------------------------------------------------
    # Navigation actions (routing hub)
    # -----------------------------------------------------------------
    st.markdown("### Actions")
    a1, a2, a3, a4 = st.columns(4)

    with a1:
        if st.button("Open County Profile"):
            st.session_state["vf_nav_target"] = "County Profile"
            st.rerun()

    with a2:
        if st.button("Compare Counties"):
            st.session_state["vf_compare_seed"] = selected_geoid
            st.session_state["vf_nav_target"] = "Compare Counties"
            st.rerun()

    with a3:
        if st.button("Open Lender Network"):
            st.session_state["vf_nav_target"] = "Lender Network"
            st.rerun()

    with a4:
        if st.button("Fraud Simulator"):
            st.session_state["vf_nav_target"] = "Fraud Simulator"
            st.rerun()

    st.markdown("---")

    # -----------------------------------------------------------------
    # Triage Table
    # -----------------------------------------------------------------
    st.markdown("### Triage Table")

    cols = [
        "NAME",
        "STUSPS",
        "GEOID",
        "risk_score",
        "risk_tier",
        "outlier_tier",
        "outlier_score",
        "outlier_basis",
        "hidden_signal_score",
        "ppp_current_total",
        "ppp_per_capita",
        "ppp_loan_count",
    ]

    # peer-specific
    for c in ["peer_group", "ppp_peer_z", "ppp_global_z", "outlier_flag"]:
        if c in view.columns and c not in cols:
            cols.append(c)

    # demographics if present
    for c in ["Total_Pop", "Poverty_Rate", "Unemployment_Rate"]:
        if c in view.columns and c not in cols:
            cols.append(c)

    cols = [c for c in cols if c in view.columns]

    triage = view[cols].copy()
    triage.rename(
        columns={
            "NAME": "County",
            "STUSPS": "State",
            "ppp_current_total": "PPP Total",
            "ppp_per_capita": "PPP / Capita",
            "ppp_loan_count": "PPP Loans",
            "Total_Pop": "Population",
            "Poverty_Rate": "Poverty",
            "Unemployment_Rate": "Unemployment",
            "hidden_signal_score": "Hidden Signal",
            "ppp_peer_z": "Peer Z (PPP/Cap)",
            "ppp_global_z": "Global Z (PPP/Cap)",
            "outlier_flag": "Outlier Flag",
        },
        inplace=True,
    )

    st.dataframe(triage, use_container_width=True)

    st.download_button(
        "Download triage table (CSV)",
        data=triage.to_csv(index=False),
        file_name="mission_control_triage.csv",
        mime="text/csv",
    )

    with st.expander("How to interpret Outlier Tier"):
        st.markdown(
            """
**Outlier Tier** is intended for triage and is explainable:

- **GLOBAL** basis: county compared against all counties nationwide.
- **PEER** basis: county compared only against demographic peers
  (population, poverty rate, unemployment rate buckets).

Peer normalization reduces false positives where large metro counties
are compared to rural counties.
            """.strip()
        )
