from __future__ import annotations

from typing import Optional, Tuple, List

import duckdb
import pandas as pd
import streamlit as st


# =============================================================================
# Hybrid-safe helpers
# =============================================================================

def _table_exists(con: duckdb.DuckDBPyConnection, name: str, schema: str | None = None) -> bool:
    try:
        if schema:
            return bool(con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)
                """,
                [schema, name],
            ).fetchone()[0])
        return bool(con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE lower(table_name) = lower(?)
            """,
            [name],
        ).fetchone()[0])
    except Exception:
        return False


def _resolve_table(con: duckdb.DuckDBPyConnection, name: str) -> Optional[str]:
    if _table_exists(con, name):
        return name
    if _table_exists(con, name, schema="analytics"):
        return f"analytics.{name}"
    return None


def _resolve_county_ref(con: duckdb.DuckDBPyConnection) -> Optional[str]:
    if _table_exists(con, "county_ref"):
        return "county_ref"
    if _table_exists(con, "county_ref", schema="core"):
        return "core.county_ref"
    return None


def _safe_import_pyvis():
    try:
        from pyvis.network import Network  # type: ignore
        return Network
    except Exception:
        return None


# =============================================================================
# Core page
# =============================================================================

def render_network_graph(con: duckdb.DuckDBPyConnection) -> None:
    st.title("Lender Network")
    st.caption("Hybrid mode: network edges from DuckDB `county_lender_signals` (PPP stays in Capella).")

    cls = _resolve_table(con, "county_lender_signals")
    if not cls:
        st.error(
            "Missing `county_lender_signals` in DuckDB (main or analytics schema). "
            "This page requires lender-edge analytics."
        )
        return

    cr = _resolve_county_ref(con)

    with st.sidebar:
        st.subheader("Graph Filters")
        min_loans = st.number_input("Min loans per edge", min_value=1, max_value=1_000_000, value=250, step=50)
        min_dom = st.slider("Min dominance score", min_value=0.0, max_value=1.0, value=0.30, step=0.05)
        limit_edges = st.number_input("Max edges", min_value=50, max_value=50_000, value=2000, step=50)
        show_table = st.checkbox("Show edge table", value=False)

    df = _load_edges(con, cls, cr, int(min_loans), float(min_dom), int(limit_edges))

    if df.empty:
        st.info("No edges match current filters.")
        return

    # Quick metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Edges", f"{len(df):,}")
    c2.metric("Lenders", f"{df['LenderName'].nunique():,}" if "LenderName" in df.columns else "—")
    c3.metric("Counties", f"{df['GEOID'].nunique():,}" if "GEOID" in df.columns else "—")

    st.markdown("---")

    if show_table:
        st.subheader("Edge Table")
        st.dataframe(df, use_container_width=True)

    st.subheader("Network View")

    Network = _safe_import_pyvis()
    if Network is None:
        st.warning("pyvis is not installed in this environment. Showing a ranked view instead.")
        _render_ranked_views(df)
        return

    html = _render_pyvis(df, Network)
    st.components.v1.html(html, height=700, scrolling=True)


# =============================================================================
# Data loading
# =============================================================================

def _load_edges(
    con: duckdb.DuckDBPyConnection,
    cls: str,
    cr: Optional[str],
    min_loans: int,
    min_dom: float,
    limit_edges: int,
) -> pd.DataFrame:
    # Join county_ref if present for readable county labels
    county_cols = "s.GEOID"
    join = ""
    extra = ""
    group_extra = ""

    if cr:
        join = f"LEFT JOIN {cr} cr ON s.GEOID = cr.GEOID"
        extra = ", cr.NAME AS CountyName, cr.STUSPS AS STUSPS"
        group_extra = ", cr.NAME, cr.STUSPS"

    q = f"""
    SELECT
        s.LenderName,
        {county_cols}
        {extra},
        SUM(COALESCE(s.loan_count, 0))::BIGINT AS loan_count,
        AVG(COALESCE(s.dominance_score, 0)) AS dominance_score,
        AVG(COALESCE(s.concentration_z, 0)) AS concentration_z,
        MAX(CASE WHEN COALESCE(s.anomaly_flag, FALSE) THEN 1 ELSE 0 END)::INT AS anomaly_flag
    FROM {cls} s
    {join}
    WHERE COALESCE(s.loan_count, 0) >= ?
      AND COALESCE(s.dominance_score, 0) >= ?
      AND s.LenderName IS NOT NULL AND s.LenderName <> ''
      AND s.GEOID IS NOT NULL AND s.GEOID <> ''
    GROUP BY s.LenderName, s.GEOID {group_extra}
    ORDER BY loan_count DESC
    LIMIT {int(limit_edges)}
    """
    try:
        df = con.execute(q, [min_loans, min_dom]).fetchdf()
        return df
    except Exception as e:
        st.error(f"Failed to load network edges: {e}")
        return pd.DataFrame()


# =============================================================================
# Rendering
# =============================================================================

def _render_ranked_views(df: pd.DataFrame) -> None:
    st.markdown("### Top Lenders by Loan Count (in edges)")
    top_l = (
        df.groupby("LenderName")["loan_count"]
        .sum()
        .sort_values(ascending=False)
        .head(25)
        .reset_index()
    )
    st.dataframe(top_l, use_container_width=True)

    st.markdown("### Top Counties by Loan Count (in edges)")
    if "CountyName" in df.columns:
        label = df["CountyName"].fillna(df["GEOID"])
    else:
        label = df["GEOID"]
    top_c = (
        df.assign(_county=label)
        .groupby("_county")["loan_count"]
        .sum()
        .sort_values(ascending=False)
        .head(25)
        .reset_index()
        .rename(columns={"_county": "County"})
    )
    st.dataframe(top_c, use_container_width=True)


def _render_pyvis(df: pd.DataFrame, Network) -> str:
    net = Network(height="680px", width="100%", bgcolor="#0E1117", font_color="#FAFAFA", directed=False)

    # Build nodes: lenders and counties
    # Keep graph from exploding by limiting unique nodes automatically
    max_nodes = 600
    lenders = df["LenderName"].astype(str).unique().tolist()
    counties = df["GEOID"].astype(str).unique().tolist()

    # Adaptive pruning if huge
    if len(lenders) + len(counties) > max_nodes:
        # keep top lenders/counties by total loan_count
        top_l = df.groupby("LenderName")["loan_count"].sum().sort_values(ascending=False).head(200).index.tolist()
        top_c = df.groupby("GEOID")["loan_count"].sum().sort_values(ascending=False).head(350).index.tolist()
        df = df[df["LenderName"].isin(top_l) & df["GEOID"].isin(top_c)]
        lenders = df["LenderName"].astype(str).unique().tolist()
        counties = df["GEOID"].astype(str).unique().tolist()

    for lender in lenders:
        net.add_node(f"L::{lender}", label=lender, title=lender, color="#1f77b4")

    for _, r in df.iterrows():
        geoid = str(r.get("GEOID", ""))
        cname = str(r.get("CountyName", "")) if "CountyName" in df.columns else ""
        stusps = str(r.get("STUSPS", "")) if "STUSPS" in df.columns else ""
        label = cname if cname else geoid
        if stusps:
            label = f"{label}, {stusps}"
        net.add_node(f"C::{geoid}", label=label, title=f"GEOID {geoid}", color="#ff7f0e")

    # Edges
    for _, r in df.iterrows():
        lender = str(r.get("LenderName", ""))
        geoid = str(r.get("GEOID", ""))
        loans = float(r.get("loan_count", 0) or 0)
        dom = float(r.get("dominance_score", 0) or 0)
        anom = int(r.get("anomaly_flag", 0) or 0)

        width = max(1.0, min(8.0, loans / 5000.0))
        title = f"Loans: {int(loans):,}<br/>Dominance: {dom:.3f}<br/>Anomaly: {bool(anom)}"

        net.add_edge(f"L::{lender}", f"C::{geoid}", value=loans, width=width, title=title)

    net.repulsion(node_distance=180, central_gravity=0.2, spring_length=110, spring_strength=0.04, damping=0.09)

    # Return HTML
    return net.generate_html()
