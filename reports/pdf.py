import io
from datetime import datetime, UTC

import streamlit as st
import duckdb
from reportlab.lib.pagesizes import LETTER
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet


def _make_pdf(title: str, summary: list[list[str]], top: list[list[str]]) -> bytes:
    styles = getSampleStyleSheet()
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=LETTER, rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36)

    story = []
    story.append(Paragraph(title, styles["Title"]))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Summary", styles["Heading2"]))
    t = Table(summary, colWidths=[200, 340])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
    ]))
    story.append(t)
    story.append(Spacer(1, 12))

    story.append(Paragraph("Top counties (fraud_score)", styles["Heading2"]))
    t2 = Table(top, repeatRows=1)
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
    ]))
    story.append(t2)

    doc.build(story)
    return buf.getvalue()


def render_pdf_exports(con: duckdb.DuckDBPyConnection) -> None:
    st.caption("Creates a regulator-friendly PDF summarizing the current database state and top fraud-ranked counties.")
    if not st.button("Generate regulator PDF"):
        return

    now = datetime.now(UTC).replace(tzinfo=None)
    counts = con.execute("""
      SELECT
        (SELECT COUNT(*) FROM ppp_clean) AS ppp_rows,
        (SELECT COUNT(*) FROM ppp_agg_by_geoid) AS counties_with_ppp,
        (SELECT COUNT(*) FROM acs_county) AS acs_counties,
        (SELECT COUNT(*) FROM fraud_county) AS fraud_rows
    """).fetchone()

    summary = [
        ["Field", "Value"],
        ["Generated", now.strftime("%Y-%m-%d %H:%M:%S UTC")],
        ["PPP rows", f"{int(counts[0]):,}"],
        ["Counties w/ PPP", f"{int(counts[1]):,}"],
        ["ACS counties", f"{int(counts[2]):,}"],
        ["Fraud rows", f"{int(counts[3]):,}"],
    ]

    top = con.execute("""
        SELECT GEOID, NAME, risk_band, fraud_score, loan_total, loan_count
        FROM fraud_county
        ORDER BY fraud_score DESC
        LIMIT 25
    """).df()

    top_tbl = [["GEOID", "NAME", "RISK", "SCORE", "PPP_TOTAL", "PPP_COUNT"]]
    for _, r in top.iterrows():
        top_tbl.append([
            str(r["GEOID"]),
            str(r["NAME"])[:30],
            str(r["risk_band"]),
            f"{float(r['fraud_score']):.1f}",
            f"${float(r['loan_total']):,.0f}",
            f"{int(r['loan_count']):,}",
        ])

    pdf_bytes = _make_pdf("V_FINDER — Regulator Summary (County Baseline)", summary, top_tbl)
    st.download_button(
        "Download PDF",
        data=pdf_bytes,
        file_name="v_finder_regulator_summary.pdf",
        mime="application/pdf"
    )
