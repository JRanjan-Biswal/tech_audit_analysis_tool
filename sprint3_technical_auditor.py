import sqlite3
import json
import os
import re
from datetime import datetime, timezone
from collections import Counter

# --- CONFIGURATION ---
from config import (
    DB_NAME
)
# DB_NAME = "seo_master.db"

# ── Thresholds ──────────────────────────────────────────────
TITLE_MIN          = 30
TITLE_MAX          = 60
META_DESC_MIN      = 70
META_DESC_MAX      = 160
THIN_CONTENT_WORDS = 300
SLOW_PAGE_MS       = 3000   # 3 seconds
LARGE_PAGE_BYTES   = 1_000_000  # 1MB

# ── Scoring weights (deducted from 100) ─────────────────────
SEVERITY_WEIGHTS = {
    "critical": 25,
    "warning":  10,
    "info":      3,
}


# ─────────────────────────────────────────────────────────────
# LOAD ALL PAGES INTO RAM
# ─────────────────────────────────────────────────────────────

def load_all_pages():
    """Load entire Pages table into RAM as a list of dicts."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT url, status_code, meta_title, meta_description,
               canonical_url, robots_meta, scraped_h1,
               word_count, page_load_ms, page_size_bytes,
               internal_links_count, external_links_count,
               images_missing_alt, image_count,
               has_schema, schema_types, h_tag_structure,
               has_viewport_meta, is_https,
               internal_links_list, ga4_sessions
        FROM Pages
        WHERE is_scraped = 1
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    print(f"[*] Loaded {len(rows)} scraped pages into RAM.")
    return rows


# ─────────────────────────────────────────────────────────────
# DUPLICATE DETECTION (needs all pages in RAM)
# ─────────────────────────────────────────────────────────────

def build_duplicate_maps(pages):
    """
    Returns two sets:
      duplicate_titles — URLs with a title seen more than once
      duplicate_metas  — URLs with a meta desc seen more than once
    """
    title_counter = Counter()
    meta_counter  = Counter()

    for p in pages:
        t = (p.get('meta_title') or '').strip().lower()
        m = (p.get('meta_description') or '').strip().lower()
        if t:
            title_counter[t] += 1
        if m:
            meta_counter[m] += 1

    duplicate_titles = set()
    duplicate_metas  = set()

    for p in pages:
        t = (p.get('meta_title') or '').strip().lower()
        m = (p.get('meta_description') or '').strip().lower()
        if t and title_counter[t] > 1:
            duplicate_titles.add(p['url'])
        if m and meta_counter[m] > 1:
            duplicate_metas.add(p['url'])

    return duplicate_titles, duplicate_metas


def build_orphan_map(pages):
    """
    A page is an orphan if no other page links to it internally.
    Build a set of all URLs that appear in any page's internal_links_list.
    Pages NOT in that set are orphans.
    """
    linked_urls = set()
    for p in pages:
        raw = p.get('internal_links_list') or '[]'
        try:
            links = json.loads(raw)
            for link in links:
                linked_urls.add(link.rstrip('/'))
        except Exception:
            pass

    orphans = set()
    for p in pages:
        url_clean = p['url'].rstrip('/')
        if url_clean not in linked_urls:
            orphans.add(p['url'])

    return orphans


# ─────────────────────────────────────────────────────────────
# SINGLE PAGE AUDIT
# ─────────────────────────────────────────────────────────────

def audit_page(page, duplicate_titles, duplicate_metas, orphan_urls):
    issues   = []   # list of {"severity": ..., "code": ..., "message": ...}
    url      = page['url']

    def add(severity, code, message):
        issues.append({"severity": severity, "code": code, "message": message})

    # ── Status Code ─────────────────────────────────────────
    status = page.get('status_code') or 0
    if status == 0:
        add("critical", "FETCH_FAILED", "Page could not be fetched.")
    elif status in (301, 302):
        add("warning", "REDIRECT", f"Page returns HTTP {status} redirect.")
    elif status == 404:
        add("critical", "NOT_FOUND", "Page returns 404 — remove from sitemap/internal links.")
    elif status >= 500:
        add("critical", "SERVER_ERROR", f"Server error HTTP {status}.")

    # ── HTTPS ────────────────────────────────────────────────
    if not page.get('is_https'):
        add("critical", "NO_HTTPS", "Page is served over HTTP, not HTTPS.")

    # ── Meta Title ───────────────────────────────────────────
    title = (page.get('meta_title') or '').strip()
    title_len = len(title)

    if not title:
        add("critical", "MISSING_TITLE", "Meta title tag is missing.")
    elif title_len < TITLE_MIN:
        add("warning", "TITLE_TOO_SHORT",
            f"Title is {title_len} chars (min {TITLE_MIN}): '{title}'")
    elif title_len > TITLE_MAX:
        add("warning", "TITLE_TOO_LONG",
            f"Title is {title_len} chars (max {TITLE_MAX}): '{title[:50]}...'")

    if url in duplicate_titles:
        add("critical", "DUPLICATE_TITLE",
            f"Title '{title[:40]}' is shared with another page.")

    # ── Meta Description ─────────────────────────────────────
    meta_desc = (page.get('meta_description') or '').strip()
    meta_len  = len(meta_desc)

    if not meta_desc:
        add("warning", "MISSING_META_DESC", "Meta description is missing.")
    elif meta_len < META_DESC_MIN:
        add("info", "META_DESC_TOO_SHORT",
            f"Meta description is {meta_len} chars (min {META_DESC_MIN}).")
    elif meta_len > META_DESC_MAX:
        add("warning", "META_DESC_TOO_LONG",
            f"Meta description is {meta_len} chars (max {META_DESC_MAX}).")

    if url in duplicate_metas:
        add("warning", "DUPLICATE_META_DESC",
            "Meta description is shared with another page.")

    # ── H1 ───────────────────────────────────────────────────
    h_structure = {}
    try:
        h_structure = json.loads(page.get('h_tag_structure') or '{}')
    except Exception:
        pass

    h1_count = h_structure.get('h1', 0)
    scraped_h1 = (page.get('scraped_h1') or '').strip()

    if h1_count == 0 or scraped_h1 == 'MISSING_H1':
        add("critical", "MISSING_H1", "Page has no H1 tag.")
    elif h1_count > 1:
        add("warning", "MULTIPLE_H1", f"Page has {h1_count} H1 tags — should have exactly 1.")

    # ── H tag hierarchy ──────────────────────────────────────
    if h_structure.get('h3', 0) > 0 and h_structure.get('h2', 0) == 0:
        add("info", "SKIPPED_H2",
            "Page uses H3 tags but has no H2 — broken heading hierarchy.")

    # ── Thin Content ─────────────────────────────────────────
    word_count = page.get('word_count') or 0
    if word_count < THIN_CONTENT_WORDS and status == 200:
        add("warning", "THIN_CONTENT",
            f"Only {word_count} words — below {THIN_CONTENT_WORDS} word minimum.")

    # ── Canonical ────────────────────────────────────────────
    canonical = (page.get('canonical_url') or '').strip()
    if not canonical:
        add("info", "MISSING_CANONICAL", "No canonical tag found.")
    else:
        # Self-referencing canonical is good; pointing elsewhere may be intentional
        page_url_clean = url.rstrip('/')
        canonical_clean = canonical.rstrip('/')
        if canonical_clean != page_url_clean and canonical_clean not in page_url_clean:
            add("info", "CANONICAL_POINTS_ELSEWHERE",
                f"Canonical points to: {canonical[:80]}")

    # ── Robots Meta ──────────────────────────────────────────
    robots = (page.get('robots_meta') or '').lower()
    if 'noindex' in robots:
        add("critical", "NOINDEX",
            f"Page has robots meta 'noindex' — Google will NOT index this page.")
    if 'nofollow' in robots:
        add("warning", "NOFOLLOW_META",
            "Page has robots meta 'nofollow' — links won't pass authority.")

    # ── Images ───────────────────────────────────────────────
    missing_alt = page.get('images_missing_alt') or 0
    image_count = page.get('image_count') or 0
    if missing_alt > 0:
        add("warning", "MISSING_ALT_TAGS",
            f"{missing_alt}/{image_count} images missing alt text.")

    # ── Schema ───────────────────────────────────────────────
    has_schema = page.get('has_schema') or False
    schema_types = []
    try:
        schema_types = json.loads(page.get('schema_types') or '[]')
    except Exception:
        pass

    if not has_schema:
        add("info", "NO_SCHEMA",
            "No JSON-LD schema markup found. Add relevant schema (Article, LocalBusiness, etc.)")

    # ── Viewport / Mobile ────────────────────────────────────
    if not page.get('has_viewport_meta'):
        add("warning", "NO_VIEWPORT",
            "Missing viewport meta tag — page may not be mobile-friendly.")

    # ── Page Speed ───────────────────────────────────────────
    load_ms = page.get('page_load_ms') or 0
    if load_ms > SLOW_PAGE_MS:
        add("warning", "SLOW_PAGE",
            f"Page took {load_ms}ms to load (threshold: {SLOW_PAGE_MS}ms).")

    # ── Page Size ────────────────────────────────────────────
    page_size = page.get('page_size_bytes') or 0
    if page_size > LARGE_PAGE_BYTES:
        add("info", "LARGE_PAGE_SIZE",
            f"Page is {round(page_size/1024)}KB — consider optimizing assets.")

    # ── Internal Links ───────────────────────────────────────
    internal_count = page.get('internal_links_count') or 0
    if internal_count == 0 and status == 200:
        add("warning", "NO_INTERNAL_LINKS",
            "Page has no outgoing internal links — hurts crawlability and authority flow.")
    elif internal_count > 100:
        add("info", "TOO_MANY_INTERNAL_LINKS",
            f"{internal_count} internal links — Google may dilute link equity.")

    # ── Orphan Page ──────────────────────────────────────────
    if url in orphan_urls:
        add("warning", "ORPHAN_PAGE",
            "No other page links to this URL — it's invisible to crawlers.")

    # ── Score calculation ────────────────────────────────────
    score = 100
    for issue in issues:
        score -= SEVERITY_WEIGHTS.get(issue['severity'], 0)
    score = max(0, score)

    # ── Overall severity ─────────────────────────────────────
    severities = [i['severity'] for i in issues]
    if 'critical' in severities:
        overall = 'critical'
    elif 'warning' in severities:
        overall = 'warning'
    elif 'info' in severities:
        overall = 'info'
    else:
        overall = 'ok'

    return {
        'url':              url,
        'tech_issues':      json.dumps(issues),
        'tech_severity':    overall,
        'tech_score':       score,
        'title_length':     title_len,
        'meta_desc_length': meta_len,
        'h1_count':         h1_count,
        'has_canonical':    bool(canonical),
        'is_thin_content':  word_count < THIN_CONTENT_WORDS,
        'is_duplicate_title': url in duplicate_titles,
        'is_duplicate_meta':  url in duplicate_metas,
        'is_orphan_page':     url in orphan_urls,
    }


# ─────────────────────────────────────────────────────────────
# BATCH WRITE
# ─────────────────────────────────────────────────────────────

def batch_write_audit(results):
    conn = sqlite3.connect(DB_NAME)
    now  = datetime.now(timezone.utc).isoformat()
    for r in results:
        conn.execute("""
            UPDATE Pages SET
                tech_issues        = ?,
                tech_severity      = ?,
                tech_score         = ?,
                title_length       = ?,
                meta_desc_length   = ?,
                h1_count           = ?,
                has_canonical      = ?,
                is_thin_content    = ?,
                is_duplicate_title = ?,
                is_duplicate_meta  = ?,
                is_orphan_page     = ?,
                is_audited         = 1,
                audited_at         = ?
            WHERE url = ?
        """, (
            r['tech_issues'],
            r['tech_severity'],
            r['tech_score'],
            r['title_length'],
            r['meta_desc_length'],
            r['h1_count'],
            r['has_canonical'],
            r['is_thin_content'],
            r['is_duplicate_title'],
            r['is_duplicate_meta'],
            r['is_orphan_page'],
            now,
            r['url']
        ))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────
# REPORT PRINTER
# ─────────────────────────────────────────────────────────────

def print_report(results, pages):
    """Print a clean, actionable summary to terminal."""

    total     = len(results)
    criticals = [r for r in results if r['tech_severity'] == 'critical']
    warnings  = [r for r in results if r['tech_severity'] == 'warning']
    infos     = [r for r in results if r['tech_severity'] == 'info']
    oks       = [r for r in results if r['tech_severity'] == 'ok']

    avg_score = round(sum(r['tech_score'] for r in results) / total, 1) if total else 0

    print(f"\n{'='*60}")
    print(f"  TECHNICAL AUDIT REPORT — {total} pages")
    print(f"{'='*60}")
    print(f"  Average Tech Score : {avg_score}/100")
    print(f"  🔴 Critical        : {len(criticals)} pages")
    print(f"  🟡 Warning         : {len(warnings)} pages")
    print(f"  🔵 Info            : {len(infos)} pages")
    print(f"  ✅ Clean           : {len(oks)} pages")

    # ── Top issue codes across all pages ──
    all_issues = []
    for r in results:
        try:
            all_issues.extend(json.loads(r['tech_issues']))
        except Exception:
            pass

    code_counter = Counter(i['code'] for i in all_issues)
    print(f"\n  TOP ISSUES ACROSS SITE:")
    for code, count in code_counter.most_common(10):
        sev = next((i['severity'] for i in all_issues if i['code'] == code), 'info')
        icon = '🔴' if sev == 'critical' else '🟡' if sev == 'warning' else '🔵'
        print(f"    {icon} {code:<30} affects {count} pages")

    # ── Critical pages detail ──
    if criticals:
        print(f"\n  🔴 CRITICAL PAGES (need immediate action):")
        # Sort by ga4_sessions descending — fix high traffic pages first
        page_sessions = {p['url']: p.get('ga4_sessions', 0) for p in pages}
        criticals_sorted = sorted(
            criticals,
            key=lambda r: page_sessions.get(r['url'], 0),
            reverse=True
        )
        for r in criticals_sorted[:15]:  # show top 15
            sessions = page_sessions.get(r['url'], 0)
            issues   = json.loads(r['tech_issues'])
            critical_codes = [i['code'] for i in issues if i['severity'] == 'critical']
            short_url = r['url'].replace('https://bodycraftacademy.com', '')
            print(f"    [{r['tech_score']:3}/100] {short_url}")
            print(f"           Sessions: {sessions} | Issues: {', '.join(critical_codes)}")

    # ── Duplicate issues summary ──
    dup_titles = sum(1 for r in results if r['is_duplicate_title'])
    dup_metas  = sum(1 for r in results if r['is_duplicate_meta'])
    orphans    = sum(1 for r in results if r['is_orphan_page'])
    thin       = sum(1 for r in results if r['is_thin_content'])

    print(f"\n  SITE-WIDE FLAGS:")
    print(f"    Duplicate titles     : {dup_titles} pages")
    print(f"    Duplicate meta descs : {dup_metas} pages")
    print(f"    Orphan pages         : {orphans} pages")
    print(f"    Thin content (<{THIN_CONTENT_WORDS}w) : {thin} pages")

    print(f"\n{'='*60}")
    print(f"  Next step: Run sprint4_priority_engine.py")
    print(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def run_audit():
    print(f"\n{'='*60}")
    print("  SPRINT 3 — TECHNICAL AUDITOR (Pure Python, 0 GPU)")
    print(f"{'='*60}\n")

    if not os.path.exists(DB_NAME):
        print(f"[!] FATAL: {DB_NAME} not found.")
        return

    # Load everything into RAM
    pages = load_all_pages()
    if not pages:
        print("[!] No scraped pages found. Run sprint2_enhanced_crawler.py first.")
        return

    # Pre-compute site-wide maps (needs all pages)
    print("[*] Building duplicate and orphan maps...")
    duplicate_titles, duplicate_metas = build_duplicate_maps(pages)
    orphan_urls = build_orphan_map(pages)

    print(f"    Duplicate titles found : {len(duplicate_titles)}")
    print(f"    Duplicate metas found  : {len(duplicate_metas)}")
    print(f"    Orphan pages found     : {len(orphan_urls)}")

    # Audit every page
    print(f"\n[*] Auditing {len(pages)} pages...")
    results = []
    for page in pages:
        result = audit_page(page, duplicate_titles, duplicate_metas, orphan_urls)
        results.append(result)

    # Write to DB
    print(f"[*] Writing audit results to DB...")
    batch_write_audit(results)

    # Log run
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        INSERT INTO Audit_Log (run_at, phase, pages_processed)
        VALUES (?, 'technical_audit', ?)
    """, (datetime.now(timezone.utc).isoformat(), len(results)))
    conn.commit()
    conn.close()

    # Print report
    print_report(results, pages)


if __name__ == "__main__":
    run_audit()
