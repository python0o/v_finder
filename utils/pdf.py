from __future__ import annotations
import io
import pandas as pd
from reportlab.lib.pagesizes import LETTER
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet

def make_regulator_pdf(title: str, summary: list[list[str]], top_rows: pd.DataFrame) -> bytes:
    styles = getSampleStyleSheet()
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=LETTER, rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36)
    story = [Paragraph(title, styles["Title"]), Spacer(1, 12)]

    if summary:
        story.append(Paragraph("Summary", styles["Heading2"]))
        t = Table(summary, colWidths=[200, 320])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTSIZE", (0,0), (-1,-1), 9),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
        ]))
        story.append(t)
        story.append(Spacer(1, 14))

    if top_rows is not None and not top_rows.empty:
        story.append(Paragraph("Top PPP Records (sample)", styles["Heading2"]))
        cols = list(top_rows.columns)[:6]
        data = [cols] + top_rows[cols].astype(str).head(25).values.tolist()
        t2 = Table(data, repeatRows=1)
        t2.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("GRID", (0,0), (-1,-1), 0.3, colors.grey),
            ("FONTSIZE", (0,0), (-1,-1), 8),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
        ]))
        story.append(t2)

    doc.build(story)
    return buf.getvalue()