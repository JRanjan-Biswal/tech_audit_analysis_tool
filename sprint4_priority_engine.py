import sqlite3
import json
import os
from datetime import datetime, timezone

# --- CONFIGURATION ---
from config import (
    DB_NAME,
    JUNK_URL_PATTERNS as EXCLUDE_PATTERNS
)
# DB_NAME = "seo_master.db"

# ── URL patterns to exclude from analysis (junk/system pages) ──
# EXCLUDE_PATTERNS = [
#     'wp-login', 'wp-admin', 'wp-content', 'wp-json',
#     'zombie.php', '/404', '///checkout', 'xmlrpc',
#     'feed/', '?', 'checkout', 'cart', 'my-account',
#     'landing-page',  # noindex pages not worth AI time
# ]

# ── Priority scoring weights ──────────────────────────────────
# Each factor contributes to a 0-100 priority score
# Higher score = process with deeper AI tier

WEIGHT_TRAFFIC    = 40   # GA4 sessions (normalized)
WEIGHT_TECH       = 30   # Tech issues severity
WEIGHT_CONTENT    = 20   # Content quality signals
WEIGHT_QUICK_WIN  = 10   # Is this an easy fix with high impact?

# ── Tier thresholds ───────────────────────────────────────────
TIER_HIGH_MIN   = 60    # → Tier 3 (27b deep analysis)
TIER_MEDIUM_MIN = 30    # → Tier 2 (9b fast analysis)
# Below 30       → Tier 1 (no AI, already audited by Python)


# ─────────────────────────────────────────────────────────────
# LOAD DATA INTO RAM
# ─────────────────────────────────────────────────────────────

def load_pages():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT url, ga4_sessions, status_code,
               tech_score, tech_severity, tech_issues,
               word_count, has_schema, is_thin_content,
               is_orphan_page, is_duplicate_title,
               images_missing_alt, h1_count,
               meta_title, meta_description,
               gsc_avg_position, gsc_impressions,
               is_analyzed, is_deep_analyzed
        FROM Pages
        WHERE is_scraped = 1
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    print(f"[*] Loaded {len(rows)} pages into RAM.")
    return rows


# ─────────────────────────────────────────────────────────────
# URL FILTER
# ─────────────────────────────────────────────────────────────

def is_junk_url(url):
    """Filter out system/junk URLs that should never be analyzed."""
    url_lower = url.lower()
    for pattern in EXCLUDE_PATTERNS:
        if pattern in url_lower:
            return True
    # Filter malformed URLs (triple slash etc.)
    if '///' in url:
        return True
    # Filter URLs with uppercase path segments (likely malformed)
    path = url.replace('https://', '').replace('http://', '')
    if any(c.isupper() for c in path.split('/', 1)[-1]):
        return True
    return False


# ─────────────────────────────────────────────────────────────
# NORMALIZE TRAFFIC SCORE (0-40)
# ─────────────────────────────────────────────────────────────

def normalize_traffic(sessions, max_sessions):
    """Convert raw session count to 0-40 score."""
    if max_sessions == 0:
        return 0
    # Log scale so pages with 1000 sessions don't completely dwarf pages with 100
    import math
    if sessions <= 0:
        return 0
    log_score = math.log1p(sessions) / math.log1p(max_sessions)
    return round(log_score * WEIGHT_TRAFFIC, 2)


# ─────────────────────────────────────────────────────────────
# TECH ISSUE SCORE (0-30)
# ─────────────────────────────────────────────────────────────

def tech_issue_score(page):
    """
    Higher score = more severe technical issues = higher priority to fix.
    Inverts tech_score (100 = perfect) into urgency points.
    """
    tech_score = page.get('tech_score') or 100
    severity   = page.get('tech_severity') or 'ok'

    # Base: inverse of tech score (broken pages get higher priority)
    base = (100 - tech_score) / 100 * WEIGHT_TECH

    # Bonus for critical pages
    if severity == 'critical':
        base = min(WEIGHT_TECH, base + 10)

    return round(base, 2)


# ─────────────────────────────────────────────────────────────
# CONTENT QUALITY SCORE (0-20)
# ─────────────────────────────────────────────────────────────

def content_quality_score(page):
    """
    Higher score = more content improvement needed = higher priority for AI.
    """
    score = 0
    issues = []

    try:
        tech_issues = json.loads(page.get('tech_issues') or '[]')
        issue_codes = {i['code'] for i in tech_issues}
    except Exception:
        issue_codes = set()

    # Missing or thin content
    if page.get('is_thin_content'):
        score += 6
        issues.append("thin_content")

    # Missing meta description (AI can write one)
    if 'MISSING_META_DESC' in issue_codes:
        score += 4
        issues.append("missing_meta_desc")

    # Title too long (AI can rewrite)
    if 'TITLE_TOO_LONG' in issue_codes or 'TITLE_TOO_SHORT' in issue_codes:
        score += 3
        issues.append("bad_title")

    # Missing H1
    if 'MISSING_H1' in issue_codes:
        score += 4
        issues.append("missing_h1")

    # No schema (AI can suggest what to add)
    if not page.get('has_schema'):
        score += 3
        issues.append("no_schema")

    return round(min(score, WEIGHT_CONTENT), 2), issues


# ─────────────────────────────────────────────────────────────
# QUICK WIN SCORE (0-10)
# ─────────────────────────────────────────────────────────────

def quick_win_score(page):
    """
    Quick wins = high traffic pages with simple fixable issues.
    These give the best ROI per hour of work.
    """
    score  = 0
    reasons = []

    sessions = page.get('ga4_sessions') or 0
    position = page.get('gsc_avg_position')

    # High traffic + missing meta = easy win
    if sessions > 50 and not (page.get('meta_description') or '').strip():
        score += 5
        reasons.append("high_traffic_missing_meta")

    # High traffic + missing alt tags
    if sessions > 50 and (page.get('images_missing_alt') or 0) > 0:
        score += 3
        reasons.append("high_traffic_missing_alts")

    # GSC position 11-30 = page 2, one push away from page 1
    if position and 11 <= position <= 30:
        score += 7
        reasons.append(f"ranking_pos_{round(position)}_quick_win")

    # GSC position 4-10 = bottom of page 1, push to top 3
    if position and 4 <= position <= 10:
        score += 5
        reasons.append(f"ranking_pos_{round(position)}_push_to_top3")

    return round(min(score, WEIGHT_QUICK_WIN), 2), reasons


# ─────────────────────────────────────────────────────────────
# SCORE + ROUTE EACH PAGE
# ─────────────────────────────────────────────────────────────

def score_page(page, max_sessions):
    url = page['url']

    # Filter junk
    if is_junk_url(url):
        return {
            'url':             url,
            'priority_score':  0.0,
            'priority_tier':   'excluded',
            'priority_reasons': json.dumps(["junk_or_system_url"])
        }

    # Filter non-200 pages (404s etc. — already flagged in technical audit)
    status = page.get('status_code') or 0
    if status not in (200, None, 0):
        if status in (301, 302):
            pass  # redirects still worth reviewing
        else:
            return {
                'url':             url,
                'priority_score':  0.0,
                'priority_tier':   'excluded',
                'priority_reasons': json.dumps([f"http_{status}_error"])
            }

    reasons = []

    # Calculate each component
    traffic = normalize_traffic(page.get('ga4_sessions') or 0, max_sessions)
    tech    = tech_issue_score(page)
    content, content_reasons = content_quality_score(page)
    qw, qw_reasons = quick_win_score(page)

    reasons.extend(content_reasons)
    reasons.extend(qw_reasons)

    if page.get('tech_severity') == 'critical':
        reasons.append("has_critical_tech_issues")
    if (page.get('ga4_sessions') or 0) > 100:
        reasons.append("high_traffic_page")

    total = round(traffic + tech + content + qw, 2)

    # Route to tier
    if total >= TIER_HIGH_MIN:
        tier = 'high'
    elif total >= TIER_MEDIUM_MIN:
        tier = 'medium'
    else:
        tier = 'low'

    return {
        'url':              url,
        'priority_score':   total,
        'priority_tier':    tier,
        'priority_reasons': json.dumps(reasons)
    }


# ─────────────────────────────────────────────────────────────
# BATCH WRITE
# ─────────────────────────────────────────────────────────────

def batch_write_priorities(results):
    conn = sqlite3.connect(DB_NAME)
    for r in results:
        conn.execute("""
            UPDATE Pages SET
                priority_score   = ?,
                priority_tier    = ?,
                priority_reasons = ?
            WHERE url = ?
        """, (r['priority_score'], r['priority_tier'],
              r['priority_reasons'], r['url']))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────

def print_report(results, pages):
    page_map = {p['url']: p for p in pages}

    high     = [r for r in results if r['priority_tier'] == 'high']
    medium   = [r for r in results if r['priority_tier'] == 'medium']
    low      = [r for r in results if r['priority_tier'] == 'low']
    excluded = [r for r in results if r['priority_tier'] == 'excluded']

    print(f"\n{'='*60}")
    print(f"  PRIORITY ENGINE REPORT — {len(results)} pages")
    print(f"{'='*60}")
    print(f"  🔴 HIGH   (Tier 3 — 27b deep AI) : {len(high)} pages")
    print(f"  🟡 MEDIUM (Tier 2 — 9b fast AI)  : {len(medium)} pages")
    print(f"  🟢 LOW    (no AI needed)          : {len(low)} pages")
    print(f"  ⚫ EXCLUDED (junk/errors)          : {len(excluded)} pages")

    print(f"\n  🔴 HIGH PRIORITY PAGES (fix these first):")
    high_sorted = sorted(high, key=lambda r: r['priority_score'], reverse=True)
    for r in high_sorted[:20]:
        p = page_map.get(r['url'], {})
        sessions = p.get('ga4_sessions', 0)
        reasons  = json.loads(r['priority_reasons'])
        short_url = r['url'].replace('https://bodycraftacademy.com', '')
        print(f"\n    [{r['priority_score']:5.1f}/100] {short_url}")
        print(f"           Sessions: {sessions} | Tier: {r['priority_tier'].upper()}")
        print(f"           Why: {', '.join(reasons[:3])}")

    print(f"\n  🟡 MEDIUM PRIORITY — TOP 10:")
    medium_sorted = sorted(medium, key=lambda r: r['priority_score'], reverse=True)
    for r in medium_sorted[:10]:
        p = page_map.get(r['url'], {})
        sessions = p.get('ga4_sessions', 0)
        short_url = r['url'].replace('https://bodycraftacademy.com', '')
        print(f"    [{r['priority_score']:5.1f}/100] {short_url} | {sessions} sessions")

    print(f"\n{'='*60}")
    print(f"  GPU BUDGET ESTIMATE:")
    print(f"  → {len(high)} pages × Tier 3 (27b) = heavy analysis")
    print(f"  → {len(medium)} pages × Tier 2 (9b) = fast scoring")
    print(f"  → {len(low)} pages = no GPU needed")
    print(f"{'='*60}")
    print(f"\n  Next step: Run sprint5_ai_analyser.py")
    print(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def run_priority_engine():
    print(f"\n{'='*60}")
    print("  SPRINT 4 — PRIORITY ENGINE")
    print(f"{'='*60}\n")

    if not os.path.exists(DB_NAME):
        print(f"[!] FATAL: {DB_NAME} not found.")
        return

    pages = load_pages()
    if not pages:
        print("[!] No pages found. Run previous sprints first.")
        return

    # Find max sessions for normalization
    max_sessions = max((p.get('ga4_sessions') or 0) for p in pages)
    print(f"[*] Max GA4 sessions on any page: {max_sessions}")
    print(f"[*] Scoring and routing {len(pages)} pages...\n")

    results = [score_page(p, max_sessions) for p in pages]

    batch_write_priorities(results)

    # Log
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        INSERT INTO Audit_Log (run_at, phase, pages_processed)
        VALUES (?, 'priority_engine', ?)
    """, (datetime.now(timezone.utc).isoformat(), len(results)))
    conn.commit()
    conn.close()

    print_report(results, pages)


if __name__ == "__main__":
    run_priority_engine()

