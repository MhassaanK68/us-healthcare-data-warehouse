"""Build the CareWatch Phase 3 report as a restrained A4 portrait PDF.

Source screenshots live in docs/evidence/phase3/dashboard. Run this script again
after the Power BI operations page is captured and added to the evidence folder.
"""

from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    HRFlowable, Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate,
    Spacer, Table, TableStyle,
)
from PIL import Image as PILImage


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs" / "evidence" / "phase3" / "dashboard"
OUT = ROOT / "Architectural-Report.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

BLACK = colors.HexColor("#181818")
DARK = colors.HexColor("#373737")
MID = colors.HexColor("#666666")
RULE = colors.HexColor("#C9C9C9")
PALE = colors.HexColor("#F5F5F5")
WHITE = colors.white
PAGE_W, PAGE_H = A4
MARGIN = 48
CONTENT_W = PAGE_W - 2 * MARGIN

font_regular = Path(r"C:\Windows\Fonts\segoeui.ttf")
font_bold = Path(r"C:\Windows\Fonts\segoeuib.ttf")
if font_regular.exists() and font_bold.exists():
    pdfmetrics.registerFont(TTFont("Segoe", str(font_regular)))
    pdfmetrics.registerFont(TTFont("Segoe-Bold", str(font_bold)))
    pdfmetrics.registerFontFamily("Segoe", normal="Segoe", bold="Segoe-Bold")
    FONT, BOLD = "Segoe", "Segoe-Bold"
else:
    FONT, BOLD = "Helvetica", "Helvetica-Bold"

styles = {
    "eyebrow": ParagraphStyle(
        "eyebrow", fontName=BOLD, fontSize=8, leading=11,
        textColor=MID, spaceAfter=14,
    ),
    "title": ParagraphStyle(
        "title", fontName=BOLD, fontSize=25, leading=31,
        textColor=BLACK, spaceAfter=13,
    ),
    "lead": ParagraphStyle(
        "lead", fontName=FONT, fontSize=10.5, leading=16.5,
        textColor=DARK, spaceAfter=20,
    ),
    "h2": ParagraphStyle(
        "h2", fontName=BOLD, fontSize=13, leading=17,
        textColor=BLACK, spaceBefore=15, spaceAfter=8,
    ),
    "body": ParagraphStyle(
        "body", fontName=FONT, fontSize=9.4, leading=14.3,
        textColor=DARK, spaceAfter=10,
    ),
    "small": ParagraphStyle(
        "small", fontName=FONT, fontSize=8.4, leading=12.3,
        textColor=DARK, spaceAfter=8,
    ),
    "caption": ParagraphStyle(
        "caption", fontName=FONT, fontSize=7.5, leading=10.8,
        textColor=MID, spaceBefore=6, spaceAfter=11,
    ),
    "cell": ParagraphStyle(
        "cell", fontName=FONT, fontSize=8.2, leading=11.5,
        textColor=DARK,
    ),
    "cellhead": ParagraphStyle(
        "cellhead", fontName=BOLD, fontSize=8.0, leading=11,
        textColor=BLACK,
    ),
    "metric": ParagraphStyle(
        "metric", fontName=BOLD, fontSize=19, leading=23,
        textColor=BLACK,
    ),
    "metric_label": ParagraphStyle(
        "metric_label", fontName=FONT, fontSize=8, leading=11,
        textColor=MID,
    ),
}

story = []


def p(text: str, style: str = "body") -> Paragraph:
    return Paragraph(text, styles[style])


def add_page(section: str, title: str, lead: str | None = None) -> None:
    if story:
        story.append(PageBreak())
    story.append(p(section.upper(), "eyebrow"))
    story.append(p(title, "title"))
    if lead:
        story.append(p(lead, "lead"))
    else:
        story.append(Spacer(1, 8))
    story.append(HRFlowable(width="100%", thickness=0.9, color=BLACK,
                            spaceAfter=17))


def heading(title: str) -> None:
    story.append(p(title, "h2"))


def body(text: str, style: str = "body") -> None:
    story.append(p(text, style))


def figure(filename: str, max_h: float, caption: str) -> None:
    path = EVIDENCE / filename
    with PILImage.open(path) as im:
        iw, ih = im.size
    ratio = min(CONTENT_W / iw, max_h / ih)
    chart = Image(str(path), width=iw * ratio, height=ih * ratio)
    chart.hAlign = "CENTER"
    story.append(chart)
    story.append(p(caption, "caption"))


def table(headers: list[str], rows: list[list[str]], widths: list[float],
          pad: float = 8) -> None:
    cells = [[p(escape(v), "cellhead") for v in headers]]
    for row in rows:
        cells.append([p(v, "cell") for v in row])
    t = Table(cells, colWidths=widths, repeatRows=1, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), PALE),
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, BLACK),
        ("LINEBELOW", (0, 1), (-1, -1), 0.45, RULE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), pad),
        ("RIGHTPADDING", (0, 0), (-1, -1), pad),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(t)
    story.append(Spacer(1, 10))


def metrics(items: list[tuple[str, str]]) -> None:
    cell_width = CONTENT_W / len(items)
    cells = []
    for value, label in items:
        inner = Table([[p(value, "metric")], [p(label, "metric_label")]],
                      colWidths=[cell_width - 14])
        inner.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        cells.append(inner)
    t = Table([cells], colWidths=[cell_width] * len(items))
    t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LINEABOVE", (0, 0), (-1, 0), 0.6, RULE),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, RULE),
    ]))
    story.append(t)
    story.append(Spacer(1, 13))


def footer(c, doc) -> None:
    c.saveState()
    c.setStrokeColor(RULE)
    c.setLineWidth(0.6)
    c.line(MARGIN, 43, PAGE_W - MARGIN, 43)
    c.setFont(BOLD, 7.3)
    c.setFillColor(MID)
    c.drawString(MARGIN, 29, "CAREWATCH  /  PHASE 3")
    c.setFont(FONT, 7.3)
    c.drawRightString(PAGE_W - MARGIN, 29, f"{doc.page:02d}")
    c.restoreState()


# 1. Cover
story.append(Spacer(1, 46))
story.append(p("CAREWATCH  /  ARCHITECTURE REPORT", "eyebrow"))
story.append(HRFlowable(width="100%", thickness=1.2, color=BLACK,
                        spaceAfter=61))
story.append(p("Nursing home quality<br/>from lakehouse to insight", "title"))
body("Architecture, dimensional modeling, pipeline execution, Power BI, and FinOps.", "lead")
story.append(Spacer(1, 22))
metrics([
    ("09", "managed Gold tables"),
    ("04", "dependent Job tasks"),
    ("1.36", "observed Job DBU"),
])
heading("Executive summary")
body("CareWatch transforms public CMS nursing-home data through Landing, Bronze, and Silver into a Gold dimensional model. A scheduled Databricks Job completed on 25 October 2026. The Gold task reconciled 698,750 Silver rows and made no Gold mutations on unchanged input.")
body("This report separates measured Job usage from annual planning estimates. Its Power BI and Spark screenshots are direct evidence, while citation, fine, staffing, and rating visuals remain descriptive screening tools.")
story.append(Spacer(1, 35))
body("25 OCTOBER 2026  /  AWS US EAST (OHIO)  /  DATABRICKS + POWER BI", "caption")

# 2. Architecture
add_page("01  Architecture", "A governed path from CMS to Power BI",
         "The business data moves through four distinct layers. Operational logging and quarantine run alongside the analytical path.")
for title, desc in [
    ("01  CMS source", "Public provider, health deficiency, penalty, and MDS publications."),
    ("02  Landing", "Source manifest, checksum, publication overlap, and acquisition watermark."),
    ("03  Bronze", "Raw string records with batch and file lineage; source shape retained."),
    ("04  Silver", "Typed, deduplicated, validated records with idempotent updates."),
    ("05  Gold", "Conformed dimensions, event facts, and a state-month aggregate."),
    ("06  Power BI", "Databricks SQL Warehouse and a one-to-many semantic model."),
]:
    row = Table([[p(title, "cellhead"), p(desc, "cell")]],
                colWidths=[132, CONTENT_W - 132])
    row.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.55, RULE),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 11),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 11),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 11),
    ]))
    story.append(row)
    story.append(Spacer(1, 9))
heading("Control plane")
body("The 03:00 UTC daily Job enforces acquire_cms → raw_to_bronze → bronze_to_silver → silver_to_gold. Validation, quarantine, execution audit, and row-level reconciliation determine whether BI should refresh.", "small")
body("Source: deployment/carewatch_daily_incremental.job.json; Job 1053304552502201.", "caption")

# 3. Model screenshot and relationship contract
add_page("02  Dimensional model", "The implemented semantic model",
         "The Power BI Model view confirms dimensions filter facts in one direction. Monitoring views are separate from care-quality facts.")
figure("power_bi_model_view.png", 300,
       "<b>Figure 1.</b> User-supplied Power BI Model view. The image is embedded at its original resolution for zooming.")
table(["One-side dimension", "Many-side target and key"], [
    ["dim_facility", "fact_deficiency, fact_penalty, fact_mds_quality via facility_key"],
    ["dim_date", "Three event facts via their date keys; state-month aggregate via month_date_key"],
    ["dim_geography", "Three event facts and state-month aggregate via state_key"],
    ["dim_deficiency / dim_measure", "Deficiency and MDS facts via their respective type/measure keys"],
], [157, CONTENT_W - 157])
body("vw_gold_pipeline_runs, vw_gold_errors, and _measures have no physical relationship to business facts. Their independence is intentional.", "small")

# 4. Model grain and business definitions
add_page("02  Dimensional model", "Gold grains and metric contracts",
         "Five dimensions, three detailed facts, and one aggregate serve reporting without copying Silver tables.")
table(["Object", "Analytical grain", "Rows"], [
    ["fact_deficiency", "One citation (deficiency_key)", "418,911"],
    ["fact_penalty", "One penalty event (penalty_key)", "15,419"],
    ["fact_mds_quality", "One facility × measure × period (mds_key)", "249,730"],
    ["agg_state_monthly_compliance", "One state × month (state_key, month_date_key)", "3,112"],
], [190, 246, CONTENT_W - 436])
heading("Dimensions")
table(["Dimension", "Rows", "Purpose"], [
    ["dim_facility", "14,690", "Current provider profile and capacity"],
    ["dim_date", "3,466", "Calendar and event-period attributes"],
    ["dim_geography", "54", "Observed state and unknown member"],
    ["dim_deficiency", "263", "Citation classification"],
    ["dim_measure", "17", "MDS measure metadata"],
], [147, 55, CONTENT_W - 202], pad=7)
heading("Reporting rules")
body("Serious citations use severity G–I or J–L and exclude soft-deleted facts. The current 12-complete-month window is 1 October 2025 through 30 September 2026. Facility rating and ownership describe the current profile, not historical event state.", "small")
body("Source: docs/gold_data_dictionary.md; live Gold counts, 25 October 2026.", "caption")

# 5. Scheduled pipeline
add_page("03  Pipeline automation", "A successful scheduled end-to-end run",
         "The Job chain succeeded on 25 October 2026. The screenshot is the scheduled run timeline, not a manual notebook test.")
figure("scheduled_job_timeline.png", 245,
       "<b>Figure 2.</b> Job run 886646173959938; four dependent tasks finished in 489.320 seconds (8 minutes 9 seconds).")
metrics([
    ("698,750", "Silver rows read by Gold"),
    ("434,330", "active event rows aggregated"),
    ("0", "Gold row mutations"),
])
heading("What the run proves")
body("The final Gold audit record has status SUCCESS. All table merges and validations completed, and an unchanged input produced zero inserts, updates, or deletes. This run supports scheduled chaining and retry-safe current state; it does not by itself demonstrate a newly changed fact reaching Gold.")
body("The Gold audit reports duration_seconds = 217. The Job task timer is about 227 seconds; these measure slightly different boundaries.", "small")
body("Source: Databricks Job timeline and carewatch.pipeline.vw_gold_pipeline_runs.", "caption")

# 6. Decisions
add_page("04  Engineering decisions", "Design choices and boundaries",
         "Each choice follows the observed data volume, source behavior, or recovery contract.")
table(["Decision", "Reason and consequence"], [
    ["Unpartitioned managed Delta", "Nine Gold tables use 51.78 MiB of active data files. Partitioning would create small files without a measured query benefit."],
    ["No routine OPTIMIZE", "A trial skipped the single 30.3 MB deficiency file and rewrote nothing. Reassess when file counts and query latency justify it."],
    ["Schema drift and quarantine", "Bronze preserves source shape and logs changes. Silver safe-casts typed fields; invalid rows enter restricted quarantine."],
    ["Deterministic MERGE", "Stable keys and business hashes make retries idempotent. The facility dimension is current-state SCD1."],
    ["Privacy boundary", "Clear-text contact fields are removed; addresses are salted-hashed in Silver. Gold error views expose counts, not raw payloads."],
    ["Repair after failure", "Gold targets commit separately. Retry the task after a partial failure and refresh Power BI only after all targets validate."],
], [158, CONTENT_W - 158])
heading("Source-date interpretation")
body("State on an event comes from the event fact. Current state and ownership on dim_facility describe today's provider profile. Analyses that mix them must name the difference.", "small")
body("Source: Phase 2 evidence, Gold data dictionary, deployment test, and OPTIMIZE result.", "caption")

# 7. DAG evidence
add_page("05  Spark execution", "A Gold query-profile DAG",
         "This image is an actual Databricks operator graph from a Gold MERGE during the scheduled run.")
figure("gold_query_profile_dag.png", 300,
       "<b>Figure 3.</b> User-supplied query profile for merge.execute(), execution 2 of 5 in Gold task run 450438229000020.")
heading("Scope of this screenshot")
body("The visible path reads dim_date, joins on date_key, crosses a Shuffle boundary, and uses Object Hash Aggregate operators. The screenshot is a partial graph of one date-key MERGE. It is not a physical plan for the entire Gold Job or for the largest deficiency fact operation.")
body("The profile does not show enough of the scan predicates or task distribution to prove predicate pushdown or stage skew. Those claims are intentionally excluded.", "small")
body("Statement: be30292f-1e16-4500-901f-7c06a79217ff.", "caption")

# 8. Query measurements
add_page("05  Spark execution", "What the observed plan shows",
         "Operator timing and statement metrics give a narrower, defensible reading of Spark execution.")
table(["Visible operator", "Observed", "Interpretation"], [
    ["Scan Table: dim_date", "206 ms", "The calendar dimension participates in key matching."],
    ["Inner Join on date_key", "1 ms", "A join is visible; its distribution strategy is not established."],
    ["Object Hash Aggregate", "392 ms; 97 ms", "Aggregation occurs in two visible nodes."],
    ["Shuffle", "4 ms; 0 ms", "An exchange boundary is visible in the displayed path."],
], [160, 96, CONTENT_W - 256])
heading("Statement measurements")
metrics([
    ("4.767 s", "wall-clock time"),
    ("13,864", "rows read"),
    ("0", "rows written"),
])
body("The statement read 753,338 bytes from four files. Query history reports 0 spilled bytes and 0 shuffle-read bytes for this statement; these summaries do not erase the visible Shuffle operator.")
heading("Scale comparison")
body("A separate fact_deficiency MERGE in the same run processed 418,911 source rows and read 176.1 MB in 7.142 seconds. Its operator screenshot was not supplied, so Figure 3 is not used to describe that larger plan.")
body("Source: Figure 3; system.query.history; Delta history for the scheduled Job.", "caption")

# 9. FinOps
add_page("06  FinOps", "Observed cost and an hourly scenario",
         "Only the Job charge is tied to this scheduled run. The BI warehouse estimate below is explicitly a planning assumption.")
table(["Observed scheduled run", "Measured value"], [
    ["Whole Job wall time", "489.320 seconds"],
    ["Serverless Jobs usage", "1.359740075 DBU"],
    ["Effective Jobs list price", "USD 0.35 per DBU"],
    ["Calculated Job list cost", "<b>USD 0.475909</b>"],
], [243, CONTENT_W - 243])
heading("Hourly enterprise scenario")
body("Repeating the observed Job 8,760 times gives <b>USD 4,168.96/year</b> for Jobs compute. An illustrative SQL Import refresh uses 10 active minutes per hour, 0.8 DBU/hour, and USD 0.70/DBU: <b>USD 817.60/year</b>. Together these are <b>USD 4,986.56/year</b>, before storage, transfer, discounts, taxes, and data growth.")
heading("Cost controls")
body("At one daily run, the observed Jobs usage implies USD 173.71/year at list price. Skip unchanged publications, trigger BI only after Gold succeeds, and retain SQL auto-stop. Hourly ingestion may be unnecessary because provider and MDS sources do not necessarily change hourly.")
body("No SQL warehouse usage row was attributable to this Job's 03:00–05:00 UTC window. The USD 0.70 SQL rate is a planning assumption informed by a separate historical hour, not a measured Power BI refresh.", "small")
body("Source: system.billing.usage joined to system.billing.list_prices, 25 October 2026.", "caption")

# 10. Dashboard overview
add_page("07  Business intelligence", "Page 1: quality overview",
         "The overview highlights regulatory events and current provider ratings.")
figure("quality_overview.png", 190,
       "<b>Figure 4.</b> User-supplied Power BI quality overview, 25 October 2026.")
metrics([
    ("4,684", "serious citations, 12M"),
    ("2,933", "facilities cited, 12M"),
    ("$95.09M", "fines, 12M"),
])
heading("Interpretation")
body("The ownership chart compares serious-citation rates per 100 currently listed facilities. The current average rating is 2.99. These measures help prioritize investigation; they are not a clinical verdict.")
heading("Visual checks before submission")
body("The line chart currently spans roughly 18 months and ends with a near-zero August 2026 point. Confirm event completeness and align its filter or title with the 12-month cards. A state map is not visible in this screenshot.", "small")

# 11. Facility and staffing
add_page("07  Business intelligence", "Page 2: facility and staffing",
         "Facility-level penalties and compliance sit beside current staffing and rating.")
figure("facility_staffing.png", 310,
       "<b>Figure 5.</b> User-supplied Power BI facility and staffing page.")
heading("What this page answers")
body("The ranked bar chart identifies facilities with the largest 12-month fine totals. The matrix allows a reader to inspect citations, fines, and current ratings for a named provider. The staffing visual shows that average reported nurse hours rise across current rating groups.")
heading("Interpretation boundary")
body("Current rating incorporates staffing inputs, so this association is not a causal estimate. Use the bar and matrix as a review shortlist, then inspect facility history and source records.", "small")

# 12. MDS context
add_page("07  Business intelligence", "MDS measure context",
         "The selected quality measure currently contains one period-end observation, so the chart is a cross-section.")
figure("mds_single_period.png", 400,
       "<b>Figure 6.</b> The selected major-injury falls measure has one March 2026 point in the supplied view.")
heading("Read the visual precisely")
body("About 14,320 active scored facilities contribute to this selected measure. Its mean is approximately 3.23. With one period, this cannot support a trend claim. The measure selector remains useful for comparing different quality signals when their direction and denominator are explained.")
body("Source: Power BI screenshot and Gold fact_mds_quality / dim_measure model.", "caption")

# 13. Monitoring dashboard and handoff
add_page("08  Monitoring and handoff", "Page 3: pipeline health and data quality",
         "The monitoring page reads sanitized Gold audit and error views, separate from the business facts.")
figure("pipeline_health_data_quality.png", 420,
       "<b>Figure 7.</b> User-supplied Power BI monitoring page. The capture shows 53 logged execution records and 42 recorded error events.")
heading("What the capture shows")
body("A stacked bar compares execution status by pipeline layer. The activity table exposes task duration, rows read, rows aggregated, and rows quarantined. The error card sums aggregated error_count from vw_gold_errors; it does not expose raw error payloads.", "small")
heading("Final dashboard check")
body("The independently queried audit view contained 61 records on 25 October, while this Power BI capture displays 53. Reconcile Power BI refresh and filter context before submission. The proposed error-by-rule chart and detail matrix are not visible in this capture.", "small")


doc = SimpleDocTemplate(
    str(OUT), pagesize=A4, leftMargin=MARGIN, rightMargin=MARGIN,
    topMargin=54, bottomMargin=62, title="CareWatch | Architecture, Analytics and FinOps",
    author="CareWatch Nursing Homes project",
    subject="Phase 3 architecture report, A4 portrait working edition",
)
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print(OUT)
