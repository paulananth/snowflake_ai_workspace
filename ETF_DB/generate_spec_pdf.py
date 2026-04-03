# /// script
# requires-python = ">=3.11"
# dependencies = ["reportlab"]
# ///
import re
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, Preformatted, PageBreak
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

OUTPUT = "ETF_Intelligence_Portal_Specification.pdf"

doc = SimpleDocTemplate(
    OUTPUT,
    pagesize=letter,
    rightMargin=0.75 * inch,
    leftMargin=0.75 * inch,
    topMargin=0.85 * inch,
    bottomMargin=0.75 * inch,
    title="ETF Intelligence Portal — Implementation Specification",
    author="ETF DB Project",
)

styles = getSampleStyleSheet()

H1 = ParagraphStyle(
    "H1", parent=styles["Heading1"], fontSize=18, spaceAfter=10, spaceBefore=18,
    textColor=colors.HexColor("#1a1a2e"), fontName="Helvetica-Bold",
)
H2 = ParagraphStyle(
    "H2", parent=styles["Heading2"], fontSize=13, spaceAfter=8, spaceBefore=14,
    textColor=colors.HexColor("#16213e"), fontName="Helvetica-Bold",
)
H3 = ParagraphStyle(
    "H3", parent=styles["Heading3"], fontSize=11, spaceAfter=5, spaceBefore=10,
    textColor=colors.HexColor("#0f3460"), fontName="Helvetica-Bold",
)
H4 = ParagraphStyle(
    "H4", parent=styles["Heading4"], fontSize=10, spaceAfter=4, spaceBefore=8,
    textColor=colors.HexColor("#533483"), fontName="Helvetica-Bold",
)
BODY = ParagraphStyle(
    "Body", parent=styles["Normal"], fontSize=9, spaceAfter=4, leading=14,
    fontName="Helvetica",
)
BULLET = ParagraphStyle(
    "Bullet", parent=BODY, leftIndent=18, bulletIndent=6, spaceAfter=2,
)
BULLET2 = ParagraphStyle(
    "Bullet2", parent=BODY, leftIndent=36, bulletIndent=24, spaceAfter=2, fontSize=8.5,
)
CODE = ParagraphStyle(
    "Code", parent=styles["Code"], fontSize=7.5, fontName="Courier",
    backColor=colors.HexColor("#f4f4f4"), leading=11,
    leftIndent=10, rightIndent=10, spaceAfter=6, spaceBefore=4,
    borderColor=colors.HexColor("#cccccc"), borderWidth=0.5, borderPad=6,
)
NOTE = ParagraphStyle(
    "Note", parent=BODY, fontSize=8.5, textColor=colors.HexColor("#555555"),
    fontName="Helvetica-Oblique",
)

AGENT_COLORS = {
    "Foundation": colors.HexColor("#e8f4f8"),
    "Agent A": colors.HexColor("#e8f8e8"),
    "Agent B": colors.HexColor("#fff8e8"),
    "Agent C": colors.HexColor("#f8e8f8"),
    "Agent D": colors.HexColor("#ffe8e8"),
    "Agent E": colors.HexColor("#e8ffe8"),
    "Agent F": colors.HexColor("#e8e8ff"),
    "Agent G": colors.HexColor("#fff0e8"),
}


def esc(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def inline_fmt(text):
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", esc(text))
    text = re.sub(
        r"`([^`]+)`",
        r'<font name="Courier" size="8" color="#c0392b">\1</font>',
        text,
    )
    return text


def make_md_table(header_cells, data_rows):
    available = 6.5 * inch
    col_w = [available / len(header_cells)] * len(header_cells)
    th_style = ParagraphStyle(
        "th", parent=BODY, fontSize=8, textColor=colors.white, fontName="Helvetica-Bold"
    )
    td_style = ParagraphStyle("td", parent=BODY, fontSize=8)
    header = [Paragraph(f"<b>{esc(c)}</b>", th_style) for c in header_cells]
    rows = [[Paragraph(inline_fmt(c), td_style) for c in row] for row in data_rows]
    data = [header] + rows
    t = Table(data, colWidths=col_w, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f7f7")]),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#cccccc")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return t


def cover_page():
    items = []
    items.append(Spacer(1, 0.4 * inch))
    items.append(
        Paragraph(
            "ETF Intelligence Portal",
            ParagraphStyle(
                "cov1", parent=H1, fontSize=26,
                textColor=colors.HexColor("#1a1a2e"), alignment=TA_CENTER, spaceAfter=6,
            ),
        )
    )
    items.append(
        Paragraph(
            "Implementation Specification",
            ParagraphStyle(
                "cov2", parent=H2, fontSize=16,
                textColor=colors.HexColor("#4a90d9"), alignment=TA_CENTER, spaceAfter=4,
            ),
        )
    )
    items.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor("#4a90d9")))
    items.append(Spacer(1, 0.1 * inch))
    items.append(
        Paragraph(
            "Multi-Agent Parallel Implementation Guide",
            ParagraphStyle("cov3", parent=BODY, fontSize=11, textColor=colors.HexColor("#555"), alignment=TA_CENTER),
        )
    )
    items.append(
        Paragraph(
            "Next.js 14  |  FastAPI  |  Snowflake  |  7 Agents",
            ParagraphStyle("cov4", parent=NOTE, alignment=TA_CENTER, spaceAfter=2),
        )
    )
    items.append(Spacer(1, 0.35 * inch))

    th_s = ParagraphStyle("covth", parent=BODY, textColor=colors.white, fontName="Helvetica-Bold", fontSize=8.5)
    td_s = ParagraphStyle("covtd", parent=BODY, fontSize=8.5)
    agent_rows = [
        [Paragraph("<b>Agent</b>", th_s), Paragraph("<b>Phase</b>", th_s), Paragraph("<b>Scope</b>", th_s)],
        [Paragraph("Foundation", td_s), Paragraph("Pre-req", td_s), Paragraph("Project scaffold, shared types, DB client, home page", td_s)],
        [Paragraph("Agent A", td_s), Paragraph("Phase 1", td_s), Paragraph("ETF Screener — filter, sort, paginate all ETFs", td_s)],
        [Paragraph("Agent B", td_s), Paragraph("Phase 1", td_s), Paragraph("ETF Profile Page — holdings, exposure, history tabs", td_s)],
        [Paragraph("Agent C", td_s), Paragraph("Phase 1", td_s), Paragraph("Security Profile Page — EDGAR metadata, ETF memberships", td_s)],
        [Paragraph("Agent D", td_s), Paragraph("Phase 2", td_s), Paragraph("Holdings Overlap Tool — compare up to 4 ETFs", td_s)],
        [Paragraph("Agent E", td_s), Paragraph("Phase 2", td_s), Paragraph("Issuer Page + Historical Holdings trends", td_s)],
        [Paragraph("Agent F", td_s), Paragraph("Phase 3", td_s), Paragraph("AI Research Assistant — Cortex Analyst chat UI", td_s)],
        [Paragraph("Agent G", td_s), Paragraph("Phase 4", td_s), Paragraph("Inactive Securities Monitor + Alert system", td_s)],
    ]
    tbl_style_list = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#cccccc")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
    ]
    bg_list = list(AGENT_COLORS.values())
    for idx, bg in enumerate(bg_list):
        tbl_style_list.append(("BACKGROUND", (0, idx + 1), (-1, idx + 1), bg))

    tbl = Table(agent_rows, colWidths=[1.1 * inch, 0.8 * inch, 4.6 * inch])
    tbl.setStyle(TableStyle(tbl_style_list))
    items.append(tbl)
    items.append(Spacer(1, 0.5 * inch))
    items.append(PageBreak())
    return items


def parse_md(md_text):
    story = cover_page()
    lines = md_text.split("\n")
    i = 0
    in_code = False
    code_lines = []
    in_table = False
    table_header = []
    table_rows = []

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # --- Code block ---
        if stripped.startswith("```"):
            if not in_code:
                in_code = True
                code_lines = []
            else:
                in_code = False
                if code_lines:
                    story.append(Preformatted("\n".join(code_lines), CODE))
            i += 1
            continue

        if in_code:
            code_lines.append(line)
            i += 1
            continue

        # --- Markdown table ---
        if stripped.startswith("|") and "|" in stripped:
            cells = [c.strip() for c in stripped.strip("|").split("|")]
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            if re.match(r"^[\|\s\-:]+$", next_line.strip()):
                # header
                table_header = cells
                table_rows = []
                in_table = True
                i += 2
                continue
            elif in_table:
                table_rows.append(cells)
                i += 1
                continue
        else:
            if in_table and table_header:
                story.append(make_md_table(table_header, table_rows))
                story.append(Spacer(1, 4))
                in_table = False
                table_header = []
                table_rows = []

        # --- HR ---
        if stripped == "---":
            story.append(Spacer(1, 4))
            story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc")))
            story.append(Spacer(1, 4))
            i += 1
            continue

        # --- Headings ---
        if stripped.startswith("#### "):
            story.append(Paragraph(inline_fmt(stripped[5:]), H4))
        elif stripped.startswith("### "):
            story.append(Paragraph(inline_fmt(stripped[4:]), H3))
        elif stripped.startswith("## "):
            story.append(Paragraph(inline_fmt(stripped[3:]), H2))
        elif stripped.startswith("# "):
            story.append(Paragraph(inline_fmt(stripped[2:]), H1))
        # --- Bullets ---
        elif stripped.startswith("- [ ]"):
            story.append(Paragraph("\u25a1 " + inline_fmt(stripped[6:]), BULLET))
        elif re.match(r"^[-*] ", stripped):
            indent = len(line) - len(line.lstrip())
            style = BULLET2 if indent >= 4 else BULLET
            story.append(Paragraph("\u2022 " + inline_fmt(stripped[2:]), style))
        elif re.match(r"^\d+\. ", stripped):
            text = re.sub(r"^\d+\. ", "", stripped)
            story.append(Paragraph("\u2022 " + inline_fmt(text), BULLET))
        elif stripped == "":
            story.append(Spacer(1, 4))
        else:
            story.append(Paragraph(inline_fmt(stripped), BODY))

        i += 1

    return story


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(colors.HexColor("#888888"))
    canvas.drawString(0.75 * inch, 0.45 * inch, "ETF Intelligence Portal — Implementation Specification")
    canvas.drawRightString(letter[0] - 0.75 * inch, 0.45 * inch, f"Page {doc.page}")
    canvas.restoreState()


with open("SPECIFICATION.md", "r", encoding="utf-8") as f:
    md = f.read()

story = parse_md(md)
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print(f"PDF saved: {OUTPUT}")
