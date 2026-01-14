from __future__ import annotations

from typing import Tuple, List

import duckdb
import pandas as pd

try:
    from fpdf import FPDF
except ImportError:
    FPDF = None  # type: ignore[assignment]


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
    """
    return con.execute(q, [name]).fetchone()[0] == 1


def build_diagnostics_snapshot(
    con: duckdb.DuckDBPyConnection,
) -> Tuple[List[List[str]], pd.DataFrame]:
    if not _table_exists(con, "county_scores"):
        return [["Status", "Run fraud scoring first."]], pd.DataFrame()

    total_scored = con.execute("SELECT COUNT(*) FROM county_scores").fetchone()[0]
    max_score = con.execute("SELECT MAX(risk_score) FROM county_scores").fetchone()[0] or 0.0
    min_score = con.execute("SELECT MIN(risk_score) FROM county_scores").fetchone()[0] or 0.0

    df_top = con.execute(
        """
        SELECT
            GEOID,
            STUSPS,
            NAME,
            loan_count,
            loan_total,
            forgiven_total,
            population,
            median_income,
            loan_per_capita,
            count_per_1k,
            forgiveness_rate,
            unemp_rate,
            poverty_rate,
            missing_demo_rate,
            risk_score,
            risk_rank
        FROM county_scores
        ORDER BY risk_rank
        LIMIT 100
        """
    ).fetchdf()

    summary: List[List[str]] = [
        ["Total scored counties", f"{int(total_scored):,}"],
        ["Max risk score", f"{max_score:,.2f}"],
        ["Min risk score", f"{min_score:,.2f}"],
        ["Top slice in table", "Top 100 by risk_rank"],
    ]

    return summary, df_top


def export_diagnostics_pdf(
    con: duckdb.DuckDBPyConnection,
    title: str = "PPP County Diagnostics — Risk & Context",
) -> bytes:
    if FPDF is None:
        raise RuntimeError("fpdf2 is required for PDF export. Install with: pip install fpdf2")

    summary, df_top = build_diagnostics_snapshot(con)

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, title, ln=True)

    pdf.ln(4)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, "Summary", ln=True)

    for k, v in summary:
        pdf.cell(0, 6, f"- {k}: {v}", ln=True)

    pdf.ln(4)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, "Top Counties by Risk Rank", ln=True)
    pdf.ln(2)

    if not df_top.empty:
        pdf.set_font("Helvetica", "B", 9)
        headers = ["Rank", "GEOID", "Name", "State", "Score", "Loans", "Volume"]
        col_widths = [12, 26, 60, 12, 18, 20, 30]

        for h, w in zip(headers, col_widths):
            pdf.cell(w, 6, h, border=1)
        pdf.ln()

        pdf.set_font("Helvetica", "", 8)
        for _, row in df_top.iterrows():
            pdf.cell(col_widths[0], 5, str(int(row.get("risk_rank") or 0)), border=1)
            pdf.cell(col_widths[1], 5, str(row.get("GEOID") or ""), border=1)
            pdf.cell(col_widths[2], 5, str(row.get("NAME") or "")[:26], border=1)
            pdf.cell(col_widths[3], 5, str(row.get("STUSPS") or ""), border=1)
            pdf.cell(col_widths[4], 5, f"{float(row.get('risk_score') or 0):.1f}", border=1)
            pdf.cell(col_widths[5], 5, f"{int(row.get('loan_count') or 0):,}", border=1)
            pdf.cell(col_widths[6], 5, f"${float(row.get('loan_total') or 0):,.0f}", border=1)
            pdf.ln()

    return pdf.output(dest="S").encode("latin-1")
