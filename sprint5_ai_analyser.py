import sqlite3
import requests
import json
import re
import os
import time
from datetime import datetime, timezone

# --- CONFIGURATION ---
from config import (
    DB_NAME,
    OLLAMA_URL,
    MODEL_TIER2,
    MODEL_TIER3,
    TIER2_OPTIONS,
    TIER3_OPTIONS,
    TIER2_WORDS,
    TIER3_WORDS,
    AI_SLEEP_BETWEEN as SLEEP_BETWEEN
)
# DB_NAME       = "seo_master.db"
# OLLAMA_URL    = "http://localhost:11434/api/chat"
# MODEL_TIER2   = "qwen3.5:9b-q8_0"    # Fast — medium priority pages
# MODEL_TIER3   = "qwen3.5:27b-q4_K_M" # Deep — high priority pages

# ── Token budgets (protect VRAM) ─────────────────────────────
# TIER2_OPTIONS = {
#     "temperature":    0.1,
#     "repeat_penalty": 1.1,
#     "num_ctx":        2048,
#     "num_predict":    400,
# }
# TIER3_OPTIONS = {
#     "temperature":    0.1,
#     "repeat_penalty": 1.1,
#     "num_ctx":        4096,
#     "num_predict":    600,
# }

# ── Content word limits fed to AI ────────────────────────────
# TIER2_WORDS = 60    # very short — model only needs enough to judge intent
# TIER3_WORDS = 120   # still short — tech summary carries most context

# SLEEP_BETWEEN = 1   # seconds between calls — let GPU breathe


# ─────────────────────────────────────────────────────────────
# JSON EXTRACTION (robust)
# ─────────────────────────────────────────────────────────────

def extract_json(text):
    if not text:
        return None
    # Strip Qwen3 thinking blocks
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    # Find outermost JSON object
    start = text.find('{')
    if start == -1:
        return None
    depth = 0
    for i, ch in enumerate(text[start:], start=start):
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i+1])
                except json.JSONDecodeError:
                    return None
    return None


# ─────────────────────────────────────────────────────────────
# OLLAMA CALL
# ─────────────────────────────────────────────────────────────

def call_ollama(model, messages, options, label=""):
    payload = {
        "model":    model,
        "messages": messages,
        "stream":   False,   # non-streaming — simpler, more reliable
        "think":    False,   # Ollama native flag to disable Qwen3 thinking
        "options":  options,
    }
    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=180)
        resp.raise_for_status()
        data = resp.json()

        done_reason = data.get("done_reason", "unknown")
        # Content is in message.content — thinking is suppressed via think:false
        content = data.get("message", {}).get("content", "")

        if done_reason == "length":
            print(f"  [!] {label} hit token limit")

        return content, done_reason

    except requests.exceptions.Timeout:
        print(f"  [!] {label} TIMEOUT")
        return None, "timeout"
    except Exception as e:
        print(f"  [!] {label} ERROR: {e}")
        return None, "error"


# ─────────────────────────────────────────────────────────────
# BUILD TECH SUMMARY (Python pre-processes, AI gets summary)
# ─────────────────────────────────────────────────────────────

def build_tech_summary(page):
    """
    Convert raw DB tech data into a compact human-readable summary.
    This is what gets sent to the AI — NOT raw HTML.
    Keeps token usage minimal.
    """
    lines = []

    try:
        issues = json.loads(page.get('tech_issues') or '[]')
        critical = [i for i in issues if i['severity'] == 'critical']
        warnings = [i for i in issues if i['severity'] == 'warning']
        if critical:
            lines.append("CRITICAL: " + "; ".join(i['code'] for i in critical))
        if warnings:
            lines.append("WARNINGS: " + "; ".join(i['code'] for i in warnings[:5]))
    except Exception:
        pass

    lines.append(f"Tech:{page.get('tech_score','?')}/100 Words:{page.get('word_count',0)} Schema:{bool(page.get('has_schema'))} H1:{page.get('h1_count',0)} Sessions:{page.get('ga4_sessions',0)}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# TIER 2 ANALYSIS (9b model — fast scoring)
# ─────────────────────────────────────────────────────────────

def analyse_tier2(page):
    """
    E-E-A-T score, intent classification, quick wins.
    Uses 9b model with minimal tokens.
    """
    url        = page['url']
    h1         = page.get('scraped_h1') or 'MISSING'
    meta_title = page.get('meta_title') or 'MISSING'
    meta_desc  = page.get('meta_description') or 'MISSING'
    content    = page.get('scraped_text') or ''
    safe_text  = ' '.join(content.split()[:TIER2_WORDS])
    tech_sum   = build_tech_summary(page)

    messages = [
        {
            "role": "system",
            "content": (
                "You are a JSON-only SEO API. "
                "Output ONLY a single valid JSON object. "
                "No markdown. No explanation. No extra text."
            )
        },
        {
            "role": "user",
            "content": (
                f"URL: {url}\n"
                f"Meta Title: {meta_title}\n"
                f"H1: {h1}\n"
                f"Meta Description: {meta_desc}\n"
                f"Technical Summary:\n{tech_sum}\n"
                f"Content snippet: {safe_text}\n\n"
                "Reply with ONLY this JSON (replace values):\n"
                '{\n'
                '  "eeat_score": <1-10>,\n'
                '  "intent": <"informational"|"navigational"|"commercial"|"transactional">,\n'
                '  "intent_match": <true|false>,\n'
                '  "recommendation": "<one sentence max>",\n'
                '  "quick_wins": ["<fix 1>", "<fix 2>", "<fix 3>"]\n'
                '}'
            )
        }
    ]

    raw, reason = call_ollama(MODEL_TIER2, messages, TIER2_OPTIONS,
                               label=f"Tier2:{url[-40:]}")
    # Debug — shows how many chars are being sent
    total_chars = sum(len(m['content']) for m in messages)
    print(f"  [debug] prompt chars={total_chars} (~{total_chars//4} tokens)")
    data = extract_json(raw)

    if data and 'eeat_score' in data:
        return {
            'llm_eeat_score':     int(data.get('eeat_score', 5)),
            'llm_intent':         str(data.get('intent', '')),
            'llm_intent_match':   bool(data.get('intent_match', True)),
            'llm_recommendation': str(data.get('recommendation', ''))[:500],
            'llm_quick_wins':     json.dumps(data.get('quick_wins', [])),
            'llm_model_used':     MODEL_TIER2,
        }

    print(f"  [!] Tier2 JSON failed. Raw: {str(raw)[:200]}")
    return None


# ─────────────────────────────────────────────────────────────
# TIER 3 ANALYSIS (27b model — deep analysis)
# ─────────────────────────────────────────────────────────────

def analyse_tier3(page):
    """
    Deep analysis: content score, title/meta rewrites, content gaps,
    schema suggestions, GEO score.
    Uses 27b model with larger context.
    """
    url        = page['url']
    h1         = page.get('scraped_h1') or 'MISSING'
    meta_title = page.get('meta_title') or 'MISSING'
    meta_desc  = page.get('meta_description') or 'MISSING'
    content    = page.get('scraped_text') or ''
    safe_text  = ' '.join(content.split()[:TIER3_WORDS])
    tech_sum   = build_tech_summary(page)

    schema_types = []
    try:
        schema_types = json.loads(page.get('schema_types') or '[]')
    except Exception:
        pass

    messages = [
        {
            "role": "system",
            "content": (
                "You are a JSON-only senior SEO strategist API. "
                "Output ONLY a single valid JSON object. "
                "No markdown. No explanation. No preamble."
            )
        },
        {
            "role": "user",
            "content": (
                f"URL: {url}\n"
                f"Current Meta Title: {meta_title}\n"
                f"Current H1: {h1}\n"
                f"Current Meta Description: {meta_desc}\n"
                f"Current Schema: {schema_types}\n"
                f"Technical Summary:\n{tech_sum}\n"
                f"Content (first {TIER3_WORDS} words): {safe_text}\n\n"
                "Perform a deep SEO analysis. Reply with ONLY this JSON:\n"
                '{\n'
                '  "content_score": <1-10>,\n'
                '  "title_rewrite": "<optimised title tag under 60 chars>",\n'
                '  "meta_rewrite": "<optimised meta description 120-155 chars>",\n'
                '  "content_gaps": ["<missing topic 1>", "<missing topic 2>", "<missing topic 3>"],\n'
                '  "schema_suggestions": ["<schema type to add 1>", "<schema type to add 2>"],\n'
                '  "geo_score": <1-10>,\n'
                '  "geo_improvements": ["<AI search improvement 1>", "<AI search improvement 2>"]\n'
                '}'
            )
        }
    ]

    raw, reason = call_ollama(MODEL_TIER3, messages, TIER3_OPTIONS,
                               label=f"Tier3:{url[-40:]}")
    data = extract_json(raw)

    if data and 'content_score' in data:
        return {
            'llm_content_score':      int(data.get('content_score', 5)),
            'llm_title_rewrite':      str(data.get('title_rewrite', ''))[:120],
            'llm_meta_rewrite':       str(data.get('meta_rewrite', ''))[:300],
            'llm_content_gaps':       json.dumps(data.get('content_gaps', [])),
            'llm_schema_suggestions': json.dumps(data.get('schema_suggestions', [])),
            'llm_geo_score':          int(data.get('geo_score', 5)),
            'llm_geo_improvements':   json.dumps(data.get('geo_improvements', [])),
        }

    print(f"  [!] Tier3 JSON failed. Raw: {str(raw)[:200]}")
    return None


# ─────────────────────────────────────────────────────────────
# DATABASE WRITES
# ─────────────────────────────────────────────────────────────

def write_tier2(url, data):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        UPDATE Pages SET
            llm_eeat_score     = ?,
            llm_intent         = ?,
            llm_intent_match   = ?,
            llm_recommendation = ?,
            llm_quick_wins     = ?,
            llm_model_used     = ?,
            is_analyzed        = 1,
            analyzed_at        = ?
        WHERE url = ?
    """, (
        data['llm_eeat_score'],
        data['llm_intent'],
        data['llm_intent_match'],
        data['llm_recommendation'],
        data['llm_quick_wins'],
        data['llm_model_used'],
        datetime.now(timezone.utc).isoformat(),
        url
    ))
    conn.commit()
    conn.close()


def write_tier3(url, data):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        UPDATE Pages SET
            llm_content_score      = ?,
            llm_title_rewrite      = ?,
            llm_meta_rewrite       = ?,
            llm_content_gaps       = ?,
            llm_schema_suggestions = ?,
            llm_geo_score          = ?,
            llm_geo_improvements   = ?,
            is_deep_analyzed       = 1,
            deep_analyzed_at       = ?
        WHERE url = ?
    """, (
        data['llm_content_score'],
        data['llm_title_rewrite'],
        data['llm_meta_rewrite'],
        data['llm_content_gaps'],
        data['llm_schema_suggestions'],
        data['llm_geo_score'],
        data['llm_geo_improvements'],
        datetime.now(timezone.utc).isoformat(),
        url
    ))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────
# LOAD PAGES BY TIER
# ─────────────────────────────────────────────────────────────

def load_pages_by_tier(tier):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row

    if tier == 'high':
        cursor = conn.execute("""
            SELECT url, scraped_h1, scraped_text, meta_title,
                   meta_description, tech_issues, tech_score,
                   word_count, has_schema, schema_types,
                   h1_count, images_missing_alt, page_load_ms,
                   ga4_sessions, is_analyzed, is_deep_analyzed
            FROM Pages
            WHERE priority_tier = 'high'
              AND is_scraped = 1
              AND (is_deep_analyzed = 0 OR is_deep_analyzed IS NULL)
        """)
    else:
        cursor = conn.execute("""
            SELECT url, scraped_h1, scraped_text, meta_title,
                   meta_description, tech_issues, tech_score,
                   word_count, has_schema, schema_types,
                   h1_count, images_missing_alt, page_load_ms,
                   ga4_sessions, is_analyzed, is_deep_analyzed
            FROM Pages
            WHERE priority_tier = 'medium'
              AND is_scraped = 1
              AND (is_analyzed = 0 OR is_analyzed IS NULL)
        """)

    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def run_analysis():
    print(f"\n{'='*60}")
    print("  SPRINT 5 — AI ANALYSER")
    print(f"{'='*60}\n")

    if not os.path.exists(DB_NAME):
        print(f"[!] FATAL: {DB_NAME} not found.")
        return

    # ── TIER 3 first (27b — HIGH priority pages) ─────────────
    high_pages = load_pages_by_tier('high')
    print(f"[*] TIER 3 (27b deep): {len(high_pages)} HIGH priority pages")

    tier3_ok = tier3_fail = 0
    for i, page in enumerate(high_pages, 1):
        url = page['url']
        print(f"\n[{i}/{len(high_pages)}] TIER 3 → {url}")

        # Tier 3 pages also get Tier 2 scoring first
        print(f"  → Running Tier 2 (9b) scoring...")
        t2 = analyse_tier2(page)
        if t2:
            write_tier2(url, t2)
            print(f"  ✓ T2: E-E-A-T={t2['llm_eeat_score']} "
                  f"intent={t2['llm_intent']} "
                  f"match={t2['llm_intent_match']}")
        time.sleep(SLEEP_BETWEEN)

        print(f"  → Running Tier 3 (27b) deep analysis...")
        t3 = analyse_tier3(page)
        if t3:
            write_tier3(url, t3)
            print(f"  ✓ T3: content={t3['llm_content_score']} "
                  f"geo={t3['llm_geo_score']} "
                  f"title_rewrite='{t3['llm_title_rewrite'][:50]}'")
            tier3_ok += 1
        else:
            tier3_fail += 1
        time.sleep(SLEEP_BETWEEN)

    # ── TIER 2 (9b — MEDIUM priority pages) ──────────────────
    medium_pages = load_pages_by_tier('medium')
    print(f"\n[*] TIER 2 (9b fast): {len(medium_pages)} MEDIUM priority pages")

    tier2_ok = tier2_fail = 0
    for i, page in enumerate(medium_pages, 1):
        url = page['url']
        print(f"\n[{i}/{len(medium_pages)}] TIER 2 → {url}")

        t2 = analyse_tier2(page)
        if t2:
            write_tier2(url, t2)
            print(f"  ✓ E-E-A-T={t2['llm_eeat_score']} "
                  f"intent={t2['llm_intent']} | "
                  f"{t2['llm_recommendation'][:80]}")
            tier2_ok += 1
        else:
            tier2_fail += 1

        time.sleep(SLEEP_BETWEEN)

    # ── Log run ───────────────────────────────────────────────
    total_ok   = tier2_ok + tier3_ok
    total_fail = tier2_fail + tier3_fail
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        INSERT INTO Audit_Log (run_at, phase, pages_processed, pages_failed)
        VALUES (?, 'ai_analysis', ?, ?)
    """, (datetime.now(timezone.utc).isoformat(), total_ok, total_fail))
    conn.commit()
    conn.close()

    # ── Summary ───────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  AI ANALYSIS COMPLETE")
    print(f"{'='*60}")
    print(f"  Tier 3 (27b): {tier3_ok} ok, {tier3_fail} failed")
    print(f"  Tier 2  (9b): {tier2_ok} ok, {tier2_fail} failed")
    print(f"  LOW pages   : skipped (no AI needed)")
    print(f"\n  Next step: Run sprint6_reporting.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_analysis()

