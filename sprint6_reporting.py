import sqlite3
import json
import os
from datetime import datetime, timezone
from collections import Counter

# PDF generation
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

# --- CONFIGURATION ---
from config import (
    DB_NAME,
    REPORT_FILE,
    SITE_NAME,
    SITE_URL
)

# DB_NAME     = "seo_master.db"
# REPORT_FILE = "seo_report.pdf"
# SITE_NAME   = "Bodycraft Academy"
# SITE_URL    = "https://bodycraftacademy.com"

CLR_PRIMARY = colors.HexColor("#1a1a2e")
CLR_ACCENT  = colors.HexColor("#e94560")
CLR_GOOD    = colors.HexColor("#2ecc71")
CLR_WARN    = colors.HexColor("#f39c12")
CLR_BAD     = colors.HexColor("#e74c3c")
CLR_INFO    = colors.HexColor("#3498db")
CLR_LIGHT   = colors.HexColor("#f5f5f5")
CLR_MID     = colors.HexColor("#ecf0f1")
CLR_TEXT    = colors.HexColor("#2c3e50")
CLR_SUBTEXT = colors.HexColor("#7f8c8d")


def make_styles():
    styles = {}
    styles['h1'] = ParagraphStyle(
        'h1', fontSize=18, fontName='Helvetica-Bold',
        textColor=CLR_PRIMARY, spaceBefore=16, spaceAfter=8)
    styles['h2'] = ParagraphStyle(
        'h2', fontSize=13, fontName='Helvetica-Bold',
        textColor=CLR_PRIMARY, spaceBefore=12, spaceAfter=6)
    styles['h3'] = ParagraphStyle(
        'h3', fontSize=11, fontName='Helvetica-Bold',
        textColor=CLR_ACCENT, spaceBefore=8, spaceAfter=4)
    styles['body'] = ParagraphStyle(
        'body', fontSize=9, fontName='Helvetica',
        textColor=CLR_TEXT, spaceAfter=4, leading=14)
    styles['small'] = ParagraphStyle(
        'small', fontSize=8, fontName='Helvetica',
        textColor=CLR_SUBTEXT, spaceAfter=2)
    styles['bullet'] = ParagraphStyle(
        'bullet', fontSize=9, fontName='Helvetica',
        textColor=CLR_TEXT, leftIndent=12, spaceAfter=3, leading=13)
    styles['cover_title'] = ParagraphStyle(
        'cover_title', fontSize=32, fontName='Helvetica-Bold',
        textColor=colors.white, alignment=TA_CENTER, spaceAfter=6)
    return styles


def short_url(url):
    return url.replace(SITE_URL, '').replace('//', '/') or '/'

def safe_json(val, default=None):
    try:
        return json.loads(val or '[]')
    except Exception:
        return default or []

def safe_int(val, default=0):
    try:
        return int(val or default)
    except Exception:
        return default

def score_color(score, max_score=100):
    pct = score / max_score
    if pct >= 0.7: return CLR_GOOD
    if pct >= 0.5: return CLR_WARN
    return CLR_BAD

def severity_color(sev):
    return {'critical': CLR_BAD, 'warning': CLR_WARN,
            'info': CLR_INFO, 'ok': CLR_GOOD}.get(sev, CLR_SUBTEXT)

def eeat_label(score):
    if score >= 8: return "Excellent"
    if score >= 6: return "Good"
    if score >= 4: return "Needs Work"
    return "Poor"

def divider():
    return HRFlowable(width="100%", thickness=1,
                      color=CLR_MID, spaceAfter=8, spaceBefore=4)

def tbl_style(has_header=True):
    s = [
        ('FONTSIZE',      (0,0), (-1,-1), 8),
        ('ROWBACKGROUNDS',(0,1), (-1,-1), [colors.white, CLR_LIGHT]),
        ('GRID',          (0,0), (-1,-1), 0.3, CLR_MID),
        ('TOPPADDING',    (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LEFTPADDING',   (0,0), (-1,-1), 6),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]
    if has_header:
        s += [
            ('BACKGROUND', (0,0), (-1,0), CLR_PRIMARY),
            ('TEXTCOLOR',  (0,0), (-1,0), colors.white),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
        ]
    return TableStyle(s)


def on_page(canvas, doc):
    canvas.saveState()
    canvas.setFont('Helvetica', 8)
    canvas.setFillColor(CLR_SUBTEXT)
    canvas.drawString(2*cm, 1.2*cm,
        f"{SITE_NAME} — SEO Audit — {datetime.now().strftime('%B %Y')}")
    canvas.drawRightString(A4[0]-2*cm, 1.2*cm, f"Page {doc.page}")
    canvas.restoreState()


def load_all():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT url, ga4_sessions, status_code,
               meta_title, meta_description, scraped_h1,
               word_count, has_schema, schema_types,
               page_load_ms, images_missing_alt,
               tech_score, tech_severity, tech_issues,
               priority_score, priority_tier,
               llm_eeat_score, llm_intent, llm_intent_match,
               llm_recommendation, llm_quick_wins,
               llm_content_score, llm_title_rewrite, llm_meta_rewrite,
               llm_content_gaps, llm_schema_suggestions,
               llm_geo_score, llm_geo_improvements,
               is_analyzed, is_deep_analyzed,
               is_thin_content, is_duplicate_title, is_duplicate_meta
        FROM Pages WHERE is_scraped = 1
        ORDER BY priority_score DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── COVER ────────────────────────────────────────────────────

def build_cover(styles):
    story = []
    # Fix 1: explicit rowHeight + VALIGN MIDDLE = perfectly centred in the box
    cover_data = [[Paragraph("SEO AUDIT REPORT", styles['cover_title'])]]
    ct = Table(cover_data, colWidths=[16*cm], rowHeights=[5.5*cm])
    ct.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), CLR_PRIMARY),
        ('ALIGN',         (0,0), (-1,-1), 'CENTER'),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING',    (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
    ]))
    story.append(ct)
    story.append(Spacer(1, 0.5*cm))

    info = [
        [Paragraph(SITE_NAME, ParagraphStyle('cs', fontSize=22,
            fontName='Helvetica-Bold', textColor=CLR_PRIMARY, alignment=TA_CENTER))],
        [Paragraph(SITE_URL, ParagraphStyle('cu', fontSize=11,
            fontName='Helvetica', textColor=CLR_SUBTEXT, alignment=TA_CENTER))],
        [Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y')}",
            ParagraphStyle('cd', fontSize=10, fontName='Helvetica',
            textColor=CLR_SUBTEXT, alignment=TA_CENTER))],
    ]
    it = Table(info, colWidths=[16*cm])
    it.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(it)
    story.append(PageBreak())
    return story


# ── EXECUTIVE SUMMARY ────────────────────────────────────────

def build_executive_summary(pages, styles):
    story = [Paragraph("Executive Summary", styles['h1']), divider()]

    total          = len(pages)
    analyzed       = sum(1 for p in pages if p.get('is_analyzed'))
    deep           = sum(1 for p in pages if p.get('is_deep_analyzed'))
    avg_tech       = round(sum(safe_int(p.get('tech_score'),100) for p in pages)/max(total,1),1)
    eeat_pages     = [p for p in pages if p.get('llm_eeat_score')]
    avg_eeat       = round(sum(safe_int(p.get('llm_eeat_score')) for p in eeat_pages)/max(len(eeat_pages),1),1)
    total_sessions = sum(safe_int(p.get('ga4_sessions')) for p in pages)
    critical_count = sum(1 for p in pages if p.get('tech_severity') == 'critical')
    thin_count     = sum(1 for p in pages if p.get('is_thin_content'))
    no_meta        = sum(1 for p in pages if not (p.get('meta_description') or '').strip())
    no_schema      = sum(1 for p in pages if not p.get('has_schema'))

    # Fix 2: 3 KPI cards on row 1, 2 on row 2 — wider cols, no squeezing
    COL_W = 16*cm / 3   # ~5.33 cm each

    def kpi_block(value, label, color=CLR_PRIMARY):
        """Returns a mini-table for one KPI card — fixed row heights prevent overlap."""
        t = Table([
            [Paragraph(str(value), ParagraphStyle(f'kv_{label}', fontSize=22,
                fontName='Helvetica-Bold', textColor=color, alignment=TA_CENTER))],
            [Paragraph(label, ParagraphStyle(f'kl_{label}', fontSize=8,
                fontName='Helvetica', textColor=CLR_SUBTEXT, alignment=TA_CENTER))],
        ], colWidths=[COL_W - 0.4*cm], rowHeights=[1.5*cm, 0.5*cm])
        t.setStyle(TableStyle([
            ('ALIGN',         (0,0), (-1,-1), 'CENTER'),
            ('VALIGN',        (0,0), (0,0),   'MIDDLE'),   # number row — middle
            ('VALIGN',        (0,1), (0,1),   'TOP'),      # label row — top
            ('TOPPADDING',    (0,0), (0,0),   0),
            ('BOTTOMPADDING', (0,0), (0,0),   4),
            ('TOPPADDING',    (0,1), (0,1),   2),
            ('BOTTOMPADDING', (0,1), (0,1),   8),
        ]))
        return t

    row1_cards = [
        kpi_block(total,              "Pages Audited"),
        kpi_block(f"{avg_tech}/100",  "Avg Tech Score",   score_color(avg_tech)),
        kpi_block(f"{avg_eeat}/10",   "Avg E-E-A-T",      score_color(avg_eeat, 10)),
    ]
    row2_cards = [
        kpi_block(critical_count,     "Critical Issues",  CLR_BAD if critical_count > 0 else CLR_GOOD),
        kpi_block(f"{total_sessions:,}", "GA4 Sessions"),
    ]

    def kpi_row(cards, col_w, total_cols=3):
        # Pad with empty cells so grid lines stay consistent
        cells = cards + [''] * (total_cols - len(cards))
        widths = [col_w] * total_cols
        t = Table([cells], colWidths=widths)
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), CLR_LIGHT),
            ('ALIGN',         (0,0), (-1,-1), 'CENTER'),
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
            ('BOX',           (0,0), (-1,-1), 0.5, CLR_MID),
            ('LINEAFTER',     (0,0), (total_cols-2,-1), 0.5, CLR_MID),
            ('TOPPADDING',    (0,0), (-1,-1), 10),
            ('BOTTOMPADDING', (0,0), (-1,-1), 10),
        ]))
        return t

    story.append(kpi_row(row1_cards, COL_W, total_cols=3))
    story.append(Spacer(1, 0.2*cm))
    story.append(kpi_row(row2_cards, COL_W, total_cols=3))
    story.append(Spacer(1, 0.4*cm))

    # Health rating
    if avg_tech >= 80:
        health_text, health_color = "GOOD — Minor improvements needed", CLR_GOOD
    elif avg_tech >= 60:
        health_text, health_color = "FAIR — Several issues require attention", CLR_WARN
    elif avg_tech >= 40:
        health_text, health_color = "POOR — Significant SEO problems", colors.HexColor("#e67e22")
    else:
        health_text, health_color = "CRITICAL — Immediate action required", CLR_BAD

    ht = Table([[
        Paragraph("OVERALL SITE HEALTH", ParagraphStyle('hl', fontSize=9,
            fontName='Helvetica', textColor=CLR_SUBTEXT)),
        Paragraph(health_text, ParagraphStyle('hv', fontSize=11,
            fontName='Helvetica-Bold', textColor=health_color)),
    ]], colWidths=[5*cm, 11*cm])
    ht.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), CLR_LIGHT),
        ('TOPPADDING',    (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
        ('LEFTPADDING',   (0,0), (-1,-1), 12),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(ht)
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("Key Findings", styles['h2']))

    findings = [
        ["Finding", "Count", "Priority"],
        ["Critical pages",                str(critical_count), "URGENT" if critical_count > 0 else "OK"],
        ["Thin content pages (<300 words)",str(thin_count),    "HIGH"   if thin_count > 5    else "MEDIUM"],
        ["Missing meta descriptions",      str(no_meta),       "HIGH"   if no_meta > 10       else "MEDIUM"],
        ["Pages without schema",           str(no_schema),     "MEDIUM"],
        ["AI analysed",                    str(analyzed),      "—"],
        ["Deep analysed (Tier 3)",         str(deep),          "—"],
    ]
    ft = Table(findings, colWidths=[9*cm, 3*cm, 4*cm])
    ft.setStyle(tbl_style())
    ft.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0),  CLR_PRIMARY),
        ('TEXTCOLOR',  (0,0), (-1,0),  colors.white),
        ('FONTNAME',   (0,0), (-1,0),  'Helvetica-Bold'),
        ('FONTSIZE',   (0,0), (-1,-1), 9),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white, CLR_LIGHT]),
        ('GRID',       (0,0), (-1,-1), 0.5, CLR_MID),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING',(0,0),(-1,-1),6),
        ('LEFTPADDING',(0,0), (-1,-1), 8),
        ('ALIGN',      (1,0), (-1,-1), 'CENTER'),
    ]))
    story.append(ft)
    story.append(PageBreak())
    return story


# ── QUICK WINS ───────────────────────────────────────────────

def build_quick_wins(pages, styles):
    story = [Paragraph("Quick Wins", styles['h1']), divider()]
    story.append(Paragraph(
        "Highest impact fixes sorted by traffic x severity. Best ROI per hour of work.",
        styles['body']))
    story.append(Spacer(1, 0.3*cm))

    wins = []
    for p in pages:
        url    = short_url(p['url'])
        sess   = safe_int(p.get('ga4_sessions'))
        issues = safe_json(p.get('tech_issues'))
        ai_w   = safe_json(p.get('llm_quick_wins'))

        if sess > 10 and not (p.get('meta_description') or '').strip():
            wins.append((sess*2, "Write meta description", url, str(sess), "HIGH"))
        for i in issues:
            if i.get('code') == 'MISSING_H1' and sess > 5:
                wins.append((sess*3, "Add H1 tag", url, str(sess), "URGENT"))
        for i in issues:
            if i.get('code') == 'TITLE_TOO_LONG' and sess > 10:
                wins.append((sess, "Shorten title <60 chars", url, str(sess), "MEDIUM"))
        if sess > 20 and p.get('is_thin_content'):
            wins.append((sess*2, "Expand thin content", url, str(sess), "HIGH"))
        if sess > 15 and ai_w:
            wins.append((sess, ai_w[0][:60], url, str(sess), "MEDIUM"))

    wins.sort(reverse=True)

    # Fix 3: every cell is a Paragraph so text wraps instead of overlapping
    def cell(text, bold=False, color=CLR_TEXT, size=8):
        fn = 'Helvetica-Bold' if bold else 'Helvetica'
        return Paragraph(str(text), ParagraphStyle(
            f'wc_{text[:10]}', fontSize=size, fontName=fn,
            textColor=color, leading=12, wordWrap='LTR'))

    data = [[
        cell("#",        bold=True, color=colors.white),
        cell("Page",     bold=True, color=colors.white),
        cell("Action",   bold=True, color=colors.white),
        cell("Sessions", bold=True, color=colors.white),
        cell("Priority", bold=True, color=colors.white),
    ]]
    for i, (_, action, url, sess, sev) in enumerate(wins[:15], 1):
        sev_c = CLR_BAD if sev == 'URGENT' else CLR_WARN if sev == 'HIGH' else CLR_INFO
        data.append([
            cell(str(i)),
            cell(url),          # full URL — wraps automatically
            cell(action),       # full action — wraps automatically
            cell(sess),
            cell(sev, bold=True, color=sev_c),
        ])

    # col widths sum to 16cm; Page and Action get the most space
    t = Table(data, colWidths=[0.7*cm, 5.8*cm, 6.0*cm, 1.5*cm, 2.0*cm],
              repeatRows=1)   # header repeats if table spans pages
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,0),  CLR_PRIMARY),
        ('FONTSIZE',      (0,0), (-1,-1), 8),
        ('ROWBACKGROUNDS',(0,1), (-1,-1), [colors.white, CLR_LIGHT]),
        ('GRID',          (0,0), (-1,-1), 0.3, CLR_MID),
        ('TOPPADDING',    (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LEFTPADDING',   (0,0), (-1,-1), 5),
        ('RIGHTPADDING',  (0,0), (-1,-1), 5),
        ('VALIGN',        (0,0), (-1,-1), 'TOP'),
    ]))
    story.append(t)
    story.append(PageBreak())
    return story


# ── CRITICAL PAGES ───────────────────────────────────────────

def build_critical_pages(pages, styles):
    critical = sorted([p for p in pages if p.get('tech_severity') == 'critical'],
                      key=lambda p: safe_int(p.get('ga4_sessions')), reverse=True)
    if not critical:
        return []

    story = [Paragraph("Critical Pages", styles['h1']), divider()]
    story.append(Paragraph(f"{len(critical)} pages with critical issues:", styles['body']))
    story.append(Spacer(1, 0.2*cm))

    for p in critical:
        url    = short_url(p['url'])
        sess   = safe_int(p.get('ga4_sessions'))
        score  = safe_int(p.get('tech_score'), 100)
        issues = safe_json(p.get('tech_issues'))
        crits  = [i for i in issues if i['severity'] == 'critical']
        rec    = p.get('llm_recommendation') or ''

        ht = Table([[
            Paragraph(url, ParagraphStyle('ph', fontSize=9,
                fontName='Helvetica-Bold', textColor=colors.white)),
            Paragraph(f"Score: {score}/100  Sessions: {sess}",
                ParagraphStyle('ps', fontSize=8, fontName='Helvetica',
                textColor=colors.HexColor("#cccccc"), alignment=TA_RIGHT)),
        ]], colWidths=[11*cm, 5*cm])
        ht.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), CLR_BAD),
            ('TOPPADDING', (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('RIGHTPADDING', (0,0), (-1,-1), 8),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ]))
        story.append(ht)

        body_rows = []
        for issue in crits:
            body_rows.append(['',
                Paragraph(f"<b>{issue['code']}</b> — {issue['message']}", styles['body'])])
        if rec:
            body_rows.append(['',
                Paragraph(f"Recommendation: {rec}", styles['small'])])

        if body_rows:
            bt = Table(body_rows, colWidths=[0.5*cm, 15.5*cm])
            bt.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#fff5f5")),
                ('TOPPADDING', (0,0), (-1,-1), 4),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                ('LEFTPADDING', (0,0), (-1,-1), 8),
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ]))
            story.append(bt)
        story.append(Spacer(1, 0.3*cm))

    story.append(PageBreak())
    return story


# ── AI INSIGHTS ──────────────────────────────────────────────

def build_ai_insights(pages, styles):
    analysed = [p for p in pages if p.get('llm_eeat_score')]
    if not analysed:
        return []

    story = [Paragraph("AI Analysis Insights", styles['h1']), divider()]

    # E-E-A-T distribution
    story.append(Paragraph("E-E-A-T Score Distribution", styles['h2']))
    buckets = [("8-10 Excellent",0,CLR_GOOD),("6-7 Good",0,colors.HexColor("#27ae60")),
               ("4-5 Needs Work",0,CLR_WARN),("1-3 Poor",0,CLR_BAD)]
    for p in analysed:
        s = safe_int(p.get('llm_eeat_score'))
        if s>=8: buckets[0]=(buckets[0][0],buckets[0][1]+1,buckets[0][2])
        elif s>=6: buckets[1]=(buckets[1][0],buckets[1][1]+1,buckets[1][2])
        elif s>=4: buckets[2]=(buckets[2][0],buckets[2][1]+1,buckets[2][2])
        else: buckets[3]=(buckets[3][0],buckets[3][1]+1,buckets[3][2])

    bar_data = []
    for label, count, clr in buckets:
        bar_w = max(0.2, count/max(len(analysed),1))*10*cm
        bar_cell = Table([['']], colWidths=[bar_w], rowHeights=[0.35*cm])
        bar_cell.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),clr)]))
        bar_data.append([
            Paragraph(label, styles['small']),
            Paragraph(str(count), ParagraphStyle('bc', fontSize=9,
                fontName='Helvetica-Bold', textColor=clr)),
            bar_cell,
        ])
    bt = Table(bar_data, colWidths=[4*cm, 1.2*cm, 10.8*cm])
    bt.setStyle(TableStyle([('TOPPADDING',(0,0),(-1,-1),3),
                             ('BOTTOMPADDING',(0,0),(-1,-1),3),
                             ('VALIGN',(0,0),(-1,-1),'MIDDLE')]))
    story.append(bt)
    story.append(Spacer(1, 0.4*cm))

    # Intent
    story.append(Paragraph("Search Intent Distribution", styles['h2']))
    intent_counts = Counter(p.get('llm_intent') or 'unknown' for p in analysed)
    idata = [["Intent", "Pages", "% of Analysed"]]
    for intent, count in intent_counts.most_common():
        pct = round(count/len(analysed)*100, 1)
        idata.append([intent.capitalize(), str(count), f"{pct}%"])
    it = Table(idata, colWidths=[6*cm, 3*cm, 7*cm])
    it.setStyle(tbl_style())
    story.append(it)
    story.append(Spacer(1, 0.4*cm))

    # Deep analysis
    deep = [p for p in pages if p.get('is_deep_analyzed')]
    if deep:
        story.append(Paragraph("Deep Analysis — High Priority Pages", styles['h2']))
        for p in deep:
            url   = short_url(p['url'])
            eeat  = safe_int(p.get('llm_eeat_score'))
            cs    = safe_int(p.get('llm_content_score'))
            geo   = safe_int(p.get('llm_geo_score'))
            trw   = p.get('llm_title_rewrite') or '—'
            mrw   = p.get('llm_meta_rewrite') or '—'
            gaps  = safe_json(p.get('llm_content_gaps'))
            schema= safe_json(p.get('llm_schema_suggestions'))

            story.append(Paragraph(url, styles['h3']))
            srow = Table([[
                Paragraph(f"E-E-A-T: <b>{eeat}/10</b> — {eeat_label(eeat)}", styles['body']),
                Paragraph(f"Content: <b>{cs}/10</b>", styles['body']),
                Paragraph(f"GEO/AEO: <b>{geo}/10</b>", styles['body']),
            ]], colWidths=[5.3*cm, 5.3*cm, 5.4*cm])
            srow.setStyle(TableStyle([
                ('BACKGROUND',(0,0),(-1,-1),CLR_LIGHT),
                ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),
                ('LEFTPADDING',(0,0),(-1,-1),8),('GRID',(0,0),(-1,-1),0.3,CLR_MID),
            ]))
            story.append(srow)
            story.append(Spacer(1, 0.2*cm))
            if trw != '—':
                story.append(Paragraph(f"<b>Suggested Title:</b> {trw}", styles['body']))
            if mrw != '—':
                story.append(Paragraph(f"<b>Suggested Meta:</b> {mrw}", styles['body']))
            if gaps:
                story.append(Paragraph("<b>Content Gaps:</b>", styles['body']))
                for g in gaps:
                    story.append(Paragraph(f"• {g}", styles['bullet']))
            if schema:
                story.append(Paragraph(f"<b>Schema to Add:</b> {', '.join(schema)}", styles['body']))
            story.append(Spacer(1, 0.3*cm))

    story.append(PageBreak())
    return story


# ── TECHNICAL BREAKDOWN ──────────────────────────────────────

def build_technical_breakdown(pages, styles):
    story = [Paragraph("Technical SEO Breakdown", styles['h1']), divider()]

    all_issues = []
    for p in pages:
        all_issues.extend(safe_json(p.get('tech_issues')))
    counter = Counter(i['code'] for i in all_issues)

    actions = {
        'MISSING_META_DESC': 'Write unique 120-155 char descriptions',
        'TITLE_TOO_LONG': 'Trim title under 60 characters',
        'TITLE_TOO_SHORT': 'Expand title to 30-60 characters',
        'MISSING_ALT_TAGS': 'Add alt text to all images',
        'THIN_CONTENT': 'Expand content to 500+ words',
        'MISSING_H1': 'Add exactly one H1 per page',
        'MULTIPLE_H1': 'Remove extra H1 tags',
        'NO_SCHEMA': 'Add JSON-LD schema markup',
        'ORPHAN_PAGE': 'Add internal links from related pages',
        'DUPLICATE_TITLE': 'Write unique titles per page',
        'DUPLICATE_META_DESC': 'Write unique meta descriptions',
        'NOINDEX': 'Remove noindex if page should be indexed',
        'NOT_FOUND': 'Fix or remove broken URLs',
        'MISSING_CANONICAL': 'Add self-referencing canonical tag',
        'SLOW_PAGE': 'Optimise images, minify CSS/JS',
        'NO_VIEWPORT': 'Add viewport meta tag',
        'REDIRECT': 'Update internal links to final URL',
    }

    data = [["Issue", "Sev", "Pages", "Action"]]
    for code, count in counter.most_common(18):
        sev   = next((i['severity'] for i in all_issues if i['code'] == code), 'info')
        sc    = severity_color(sev)
        action= actions.get(code, 'Review and fix')
        data.append([
            Paragraph(f"<b>{code}</b>", styles['body']),
            Paragraph(sev[:4].upper(), ParagraphStyle(f'sc{code}', fontSize=7,
                fontName='Helvetica-Bold', textColor=sc)),
            str(count),
            Paragraph(action, styles['small']),
        ])

    t = Table(data, colWidths=[5*cm, 1.5*cm, 1.5*cm, 8*cm])
    t.setStyle(tbl_style())
    story.append(t)
    story.append(PageBreak())
    return story


# ── 30-DAY ACTION PLAN ───────────────────────────────────────

def build_action_plan(pages, styles):
    story = [Paragraph("30-Day Action Plan", styles['h1']), divider()]
    story.append(Paragraph(
        "Prioritised by impact/effort. Complete in order for maximum ROI.",
        styles['body']))
    story.append(Spacer(1, 0.3*cm))

    weeks = [
        ("Week 1 — Critical Fixes", CLR_BAD,
         [p for p in pages if p.get('tech_severity')=='critical'
          and safe_int(p.get('ga4_sessions'))>0][:6]),
        ("Week 2 — Meta & Title Rewrites", CLR_WARN,
         [p for p in pages if not (p.get('meta_description') or '').strip()
          and safe_int(p.get('ga4_sessions'))>5][:8]),
        ("Week 3 — Content Expansion", colors.HexColor("#e67e22"),
         [p for p in pages if p.get('is_thin_content')
          and safe_int(p.get('ga4_sessions'))>5][:6]),
        ("Week 4 — Schema Additions", CLR_INFO,
         [p for p in pages if not p.get('has_schema')
          and safe_int(p.get('ga4_sessions'))>5][:8]),
    ]

    for week_title, color, week_pages in weeks:
        if not week_pages:
            continue
        story.append(Paragraph(week_title, ParagraphStyle('wt', fontSize=11,
            fontName='Helvetica-Bold', textColor=color, spaceBefore=10, spaceAfter=4)))

        for p in week_pages:
            url   = short_url(p['url'])
            sess  = safe_int(p.get('ga4_sessions'))
            gaps  = safe_json(p.get('llm_content_gaps'))
            sugg_schema = safe_json(p.get('llm_schema_suggestions'))
            mrw   = p.get('llm_meta_rewrite') or ''
            issues= safe_json(p.get('tech_issues'))
            codes = [i['code'] for i in issues if i['severity']=='critical']

            if not (p.get('meta_description') or '').strip():
                action = f"Meta: {mrw[:80]}" if mrw else "Write 120-155 char description"
            elif p.get('is_thin_content'):
                action = f"Add: {', '.join(gaps[:2])}" if gaps else "Expand to 500+ words"
            elif not p.get('has_schema'):
                action = f"Add: {', '.join(sugg_schema)}" if sugg_schema else "Add schema markup"
            elif codes:
                action = f"Fix: {', '.join(codes)}"
            else:
                action = p.get('llm_recommendation') or 'Review and fix'

            rt = Table([['',
                Paragraph(f"<b>{url}</b>", styles['body']),
                Paragraph(f"{sess} sessions", styles['small']),
                Paragraph(action[:85], styles['small']),
            ]], colWidths=[0.5*cm, 4.5*cm, 2*cm, 9*cm])
            rt.setStyle(TableStyle([
                ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
                ('LEFTPADDING',(0,0),(-1,-1),4),('VALIGN',(0,0),(-1,-1),'TOP'),
                ('LINEBELOW',(0,0),(-1,-1),0.3,CLR_MID),
            ]))
            story.append(rt)
        story.append(Spacer(1, 0.3*cm))

    story.append(PageBreak())
    return story


# ── PAGE SCORECARD ───────────────────────────────────────────

def build_page_scorecard(pages, styles):
    story = [Paragraph("Full Page Scorecard", styles['h1']), divider()]
    story.append(Paragraph("All pages sorted by priority score.", styles['body']))
    story.append(Spacer(1, 0.3*cm))

    data = [["Page", "Sessions", "Tech", "E-E-A-T", "Intent", "Top Issue"]]
    for p in pages:
        if p.get('priority_tier') == 'excluded':
            continue
        url    = short_url(p['url'])[:42]
        sess   = safe_int(p.get('ga4_sessions'))
        tech   = safe_int(p.get('tech_score'), 100)
        eeat   = p.get('llm_eeat_score') or '—'
        intent = (p.get('llm_intent') or '—')[:11]
        issues = safe_json(p.get('tech_issues'))
        top    = next((i['code'] for i in issues if i['severity']=='critical'), None) or \
                 next((i['code'] for i in issues if i['severity']=='warning'), '—')

        data.append([
            Paragraph(url, styles['small']),
            str(sess),
            Paragraph(str(tech), ParagraphStyle(f'ts{sess}{tech}', fontSize=8,
                fontName='Helvetica-Bold', textColor=score_color(tech))),
            f"{eeat}/10" if isinstance(eeat, int) else eeat,
            intent,
            Paragraph(top, styles['small']),
        ])

    t = Table(data, colWidths=[5.5*cm, 1.8*cm, 1.5*cm, 1.8*cm, 2.4*cm, 3*cm])
    t.setStyle(tbl_style())
    story.append(t)
    return story


# ── MAIN ─────────────────────────────────────────────────────

def run_report():
    print(f"\n{'='*60}")
    print("  SPRINT 6 — PDF REPORTING ENGINE")
    print(f"{'='*60}\n")

    if not os.path.exists(DB_NAME):
        print(f"[!] FATAL: {DB_NAME} not found.")
        return

    pages = load_all()
    if not pages:
        print("[!] No data. Run previous sprints first.")
        return

    print(f"[*] Loaded {len(pages)} pages")
    print(f"[*] Building PDF...")

    styles = make_styles()
    story  = []
    story += build_cover(styles)
    story += build_executive_summary(pages, styles)
    story += build_quick_wins(pages, styles)
    story += build_critical_pages(pages, styles)
    story += build_ai_insights(pages, styles)
    story += build_technical_breakdown(pages, styles)
    story += build_action_plan(pages, styles)
    story += build_page_scorecard(pages, styles)

    doc = SimpleDocTemplate(
        REPORT_FILE, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2.5*cm,
        title=f"SEO Audit — {SITE_NAME}",
        author="SEO Master Pipeline",
    )
    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)

    size_kb = os.path.getsize(REPORT_FILE) // 1024
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT INTO Audit_Log (run_at, phase, pages_processed, notes) VALUES (?,?,?,?)",
                 (datetime.now(timezone.utc).isoformat(), 'reporting_pdf', len(pages), REPORT_FILE))
    conn.commit()
    conn.close()

    print(f"[+] PDF saved: {REPORT_FILE} ({size_kb}KB)")
    print(f"\n{'='*60}")
    print(f"  Open: {REPORT_FILE}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_report()
