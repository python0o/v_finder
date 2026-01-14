"""
County Dashboard — FINAL
-----------------------------------------

Dynamic, attractive, enterprise-quality dashboard showing:

    - PPP totals
    - ACS stats
    - Risk scoring (tier badges)
    - County metadata
    - Comparative Z-score panel (optional)
    - Fully typed numeric metrics

"""

from __future__ import annotations
import streamlit as st
import duckdb


TIER_COLOR = {
    "LOW": "#44bb44",
    "MED": "#ffcc22",
    "HIGH": "#dd3333",
}


def render_county_dashboard(con: duckdb.DuckDBPyConnection, geoid: str):
    q = """
        SELECT
            r.GEOID,
            r.NAME,
            r.STUSPS,
            a.loan_count,
            a.loan_total,
            s.tier,
            acs.Total_Pop,
            acs.Median_Income,
            acs.Poverty_Rate
        FROM county_ref r
        LEFT JOIN county_agg a USING (GEOID)
        LEFT JOIN county_scores s USING (GEOID)
        LEFT JOIN acs_county acs USING (GEOID)
        WHERE r.GEOID = ?
    """

    df = con.execute(q, [geoid]).fetchdf()
    if df.empty:
        st.warning("County not found.")
        return

    row = df.iloc[0]

    tier_color = TIER_COLOR.get(row["tier"], "#888888")

    # -----------------------------------------------------------
    # Header
    # -----------------------------------------------------------
    st.markdown(
        f"""
        <div style="padding:12px;background:#111;border-radius:10px;">
            <h2 style="margin:0;color:white;">{row['NAME']}, {row['STUSPS']}</h2>
            <span style="
                display:inline-block;
                padding:4px 8px;
                background:{tier_color};
                color:black;
                font-weight:bold;
                border-radius:5px;
                ">
                Tier: {row['tier']}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # -----------------------------------------------------------
    # Metrics Row
    # -----------------------------------------------------------
    col1, col2, col3 = st.columns(3)

    col1.metric("Loan Count", f"{row['loan_count']:,}")
    col2.metric("Loan Total", f"${row['loan_total']:,.0f}")
    col3.metric("Avg Loan", f"${(row['loan_total'] / max(row['loan_count'], 1)):,.0f}")

    # -----------------------------------------------------------
    # ACS Panel
    # -----------------------------------------------------------
    st.markdown("### Socioeconomic Context (ACS)")
    ac1, ac2, ac3 = st.columns(3)

    ac1.metric("Population", f"{row['Total_Pop']:,.0f}")
    ac2.metric("Median Income", f"${row['Median_Income']:,.0f}")
    ac3.metric("Poverty Rate", f"{row['Poverty_Rate']:.1f}%")

    # -----------------------------------------------------------
    # Raw table dump
    # -----------------------------------------------------------
    st.markdown("### Full County Snapshot")
    st.dataframe(df)
