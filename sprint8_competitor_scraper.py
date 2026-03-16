import sqlite3
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
import json
import re
import os
import time
from datetime import datetime, timezone
from collections import Counter

# --- CONFIGURATION ---
from config import (
    DB_NAME,
    OLLAMA_URL,
    MODEL_TIER2 as MODEL,
    DOMAIN_URL as SITE_DOMAIN,
    COMPETITOR_CONCURRENCY as CONCURRENCY,
    COMPETITOR_TIMEOUT as TIMEOUT,
    COMPETITOR_SLEEP as SLEEP_BETWEEN,
    COMPETITOR_MAX_WORDS as MAX_WORDS,
    KNOWN_COMPETITORS,
    TIER2_OPTIONS as OLLAMA_OPTIONS
)
# DB_NAME        = "seo_master.db"
# OLLAMA_URL     = "http://localhost:11434/api/chat"
# MODEL          = "qwen3.5:9b-q8_0"
# SITE_DOMAIN    = "bodycraftacademy.com"
# CONCURRENCY    = 3        # conservative — we're scraping external sites
# TIMEOUT        = 20
# SLEEP_BETWEEN  = 1.5      # be polite to external servers
# MAX_WORDS      = 150      # words sent to AI per competitor page

# Manually define known competitors (optional — auto-detect also runs)
# KNOWN_COMPETITORS = [
#     # "https://competitor1.com",
#     # "https://competitor2.com",
# ]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# OLLAMA_OPTIONS = {
#     "temperature":    0.1,
#     "repeat_penalty": 1.1,
#     "num_ctx":        3072,
#     "num_predict":    500,
# }


# ─────────────────────────────────────────────────────────────
# DB SETUP — Competitor Tables
# ─────────────────────────────────────────────────────────────

def setup_competitor_tables():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Competitors (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            domain        TEXT NOT NULL UNIQUE,
            discovered_at TEXT,
            source        TEXT   -- 'manual' or 'auto'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Competitor_Pages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            our_url         TEXT NOT NULL,
            competitor_url  TEXT NOT NULL,
            competitor_domain TEXT,
            meta_title      TEXT,
            scraped_h1      TEXT,
            word_count      INTEGER,
            schema_types    TEXT,   -- JSON array
            h_structure     TEXT,   -- JSON object
            scraped_text    TEXT,
            scraped_at      TEXT,
            UNIQUE(our_url, competitor_url)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Competitor_Analysis (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            our_url             TEXT NOT NULL UNIQUE,
            our_word_count      INTEGER,
            avg_competitor_wc   INTEGER,
            word_count_gap      INTEGER,
            our_schema_types    TEXT,   -- JSON
            missing_schema      TEXT,   -- JSON — schema competitors use, we don't
            content_gaps        TEXT,   -- JSON — AI-identified topics we're missing
            structural_gaps     TEXT,   -- JSON — H tag patterns we're missing
            competitor_urls     TEXT,   -- JSON array of competitor URLs used
            analysed_at         TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("[✓] Competitor tables ready.")


# ─────────────────────────────────────────────────────────────
# LOAD OUR PAGES
# ─────────────────────────────────────────────────────────────

def load_our_pages():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT url, scraped_h1, scraped_text, meta_title,
               meta_description, word_count, schema_types,
               h_tag_structure, ga4_sessions, priority_tier,
               llm_intent, llm_eeat_score
        FROM Pages
        WHERE is_scraped = 1
          AND priority_tier != 'excluded'
          AND status_code = 200
        ORDER BY ga4_sessions DESC
    """)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    print(f"[*] Loaded {len(rows)} pages for competitor analysis.")
    return rows


# ─────────────────────────────────────────────────────────────
# GOOGLE SERP SCRAPER — find competitor URLs
# ─────────────────────────────────────────────────────────────

def google_search_competitors(keyword, our_domain, num_results=5):
    """
    Scrapes Google search results for a keyword.
    Returns list of competitor URLs (excluding our domain).
    """
    query   = keyword.replace(' ', '+')
    url     = f"https://www.google.com/search?q={query}&num=10&hl=en"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return []

        soup  = BeautifulSoup(resp.text, 'html.parser')
        links = []

        # Extract result URLs from Google's search results
        for a in soup.select('a[href]'):
            href = a.get('href', '')
            # Google wraps URLs in /url?q=...
            if href.startswith('/url?q='):
                actual = href.split('/url?q=')[1].split('&')[0]
                if actual.startswith('http') and our_domain not in actual:
                    # Skip Google's own pages and common non-competitor domains
                    skip_domains = ['google.', 'youtube.', 'facebook.', 'twitter.',
                                    'instagram.', 'linkedin.', 'wikipedia.', 'reddit.']
                    if not any(skip in actual for skip in skip_domains):
                        links.append(actual)

        return links[:num_results]

    except Exception as e:
        print(f"  [!] Google search failed for '{keyword}': {e}")
        return []


def extract_keyword_from_page(page):
    """Extract the best keyword to search for this page."""
    h1    = (page.get('scraped_h1') or '').strip()
    title = (page.get('meta_title') or '').strip()

    # Use H1 if it's meaningful
    if h1 and h1 != 'MISSING_H1' and len(h1) > 5:
        # Clean it — remove brand name
        kw = re.sub(r'\s*[-|]\s*.*$', '', h1).strip()
        return kw[:80]

    # Fall back to title
    if title:
        kw = re.sub(r'\s*[-|]\s*.*$', '', title).strip()
        return kw[:80]

    # Fall back to URL slug
    slug = page['url'].rstrip('/').split('/')[-1]
    return slug.replace('-', ' ').replace('_', ' ')[:60]


# ─────────────────────────────────────────────────────────────
# SCRAPE COMPETITOR PAGE
# ─────────────────────────────────────────────────────────────

async def scrape_competitor_page(session, semaphore, url):
    async with semaphore:
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT)
            ) as resp:
                if resp.status != 200:
                    return None

                raw  = await resp.read()
                html = raw.decode('utf-8', errors='replace')

                try:
                    soup = BeautifulSoup(html, 'lxml')
                except Exception:
                    soup = BeautifulSoup(html, 'html.parser')

                # Meta title
                title_tag = soup.find('title')
                meta_title = title_tag.get_text(strip=True) if title_tag else ''

                # H1
                h1_tag = soup.find('h1')
                h1     = h1_tag.get_text(strip=True) if h1_tag else ''

                # H structure
                h_structure = {}
                for level in range(1, 5):
                    count = len(soup.find_all(f'h{level}'))
                    if count:
                        h_structure[f'h{level}'] = count

                # Schema types
                schema_types = []
                for script in soup.find_all('script', type='application/ld+json'):
                    try:
                        data  = json.loads(script.string or '')
                        items = data.get('@graph', [data]) if isinstance(data, dict) else data
                        for item in (items if isinstance(items, list) else [items]):
                            t = item.get('@type')
                            if t:
                                schema_types.append(str(t))
                    except Exception:
                        pass

                # Clean text
                for el in soup(["script", "style", "nav", "footer",
                                 "header", "aside", "noscript"]):
                    el.decompose()
                main = soup.find('main') or soup.find('article') or soup.find('body')
                text = ' '.join((main.get_text(separator=' ', strip=True)
                                  if main else '').split())
                word_count = len(text.split())

                return {
                    'url':         url,
                    'meta_title':  meta_title,
                    'scraped_h1':  h1,
                    'word_count':  word_count,
                    'schema_types': list(set(schema_types)),
                    'h_structure': h_structure,
                    'text':        text,
                }

        except asyncio.TimeoutError:
            return None
        except Exception:
            return None


# ─────────────────────────────────────────────────────────────
# AI GAP ANALYSIS
# ─────────────────────────────────────────────────────────────

def call_ollama(messages):
    payload = {
        "model":    MODEL,
        "messages": messages,
        "stream":   False,
        "think":    False,
        "options":  OLLAMA_OPTIONS,
    }
    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=120)
        resp.raise_for_status()
        return resp.json().get("message", {}).get("content", "")
    except Exception as e:
        print(f"  [!] Ollama error: {e}")
        return None


def extract_json(text):
    if not text:
        return None
    text  = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
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
                except Exception:
                    return None
    return None


def ai_gap_analysis(our_page, competitor_pages):
    """
    Send compact summaries of our page vs competitors to AI.
    Returns content gaps and structural gaps.
    """
    our_text   = ' '.join((our_page.get('scraped_text') or '').split()[:MAX_WORDS])
    our_h1     = our_page.get('scraped_h1') or 'MISSING'
    our_words  = our_page.get('word_count') or 0

    comp_summaries = []
    for i, cp in enumerate(competitor_pages[:3], 1):
        comp_text = ' '.join((cp.get('text') or '').split()[:MAX_WORDS])
        comp_summaries.append(
            f"Competitor {i} ({cp['url'][:50]}):\n"
            f"  H1: {cp.get('scraped_h1','?')[:60]}\n"
            f"  Words: {cp.get('word_count',0)}\n"
            f"  Schema: {cp.get('schema_types',[])}\n"
            f"  Content: {comp_text}"
        )

    comp_block = "\n\n".join(comp_summaries)

    messages = [
        {
            "role": "system",
            "content": (
                "You are a JSON-only SEO gap analysis API. "
                "Output ONLY a single valid JSON object. No markdown."
            )
        },
        {
            "role": "user",
            "content": (
                f"Our page H1: {our_h1}\n"
                f"Our word count: {our_words}\n"
                f"Our content: {our_text}\n\n"
                f"--- COMPETITORS ---\n{comp_block}\n\n"
                "Identify what competitors cover that we don't.\n"
                "Reply with ONLY this JSON:\n"
                '{\n'
                '  "content_gaps": ["<topic we are missing 1>", "<topic 2>", "<topic 3>"],\n'
                '  "structural_gaps": ["<structural improvement 1>", "<improvement 2>"]\n'
                '}'
            )
        }
    ]

    raw  = call_ollama(messages)
    data = extract_json(raw)
    if data:
        return (
            data.get('content_gaps', []),
            data.get('structural_gaps', [])
        )
    return [], []


# ─────────────────────────────────────────────────────────────
# SCHEMA GAP DETECTION (pure Python)
# ─────────────────────────────────────────────────────────────

def find_schema_gaps(our_schema_types, competitor_pages):
    """Find schema types competitors use that we don't."""
    our_types  = set(our_schema_types or [])
    comp_types = set()
    for cp in competitor_pages:
        comp_types.update(cp.get('schema_types') or [])

    # Schema types competitors have that we don't
    gaps = comp_types - our_types
    # Filter out very generic types
    ignore = {'WebPage', 'WebSite', 'SiteNavigationElement', 'ListItem'}
    gaps   = gaps - ignore
    return list(gaps)


# ─────────────────────────────────────────────────────────────
# SAVE RESULTS
# ─────────────────────────────────────────────────────────────

def save_competitor_pages(our_url, competitor_pages):
    conn = sqlite3.connect(DB_NAME)
    now  = datetime.now(timezone.utc).isoformat()
    for cp in competitor_pages:
        domain = re.sub(r'https?://(www\.)?', '', cp['url']).split('/')[0]
        try:
            conn.execute("""
                INSERT OR REPLACE INTO Competitor_Pages
                (our_url, competitor_url, competitor_domain, meta_title,
                 scraped_h1, word_count, schema_types, h_structure,
                 scraped_text, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                our_url, cp['url'], domain,
                cp.get('meta_title', ''),
                cp.get('scraped_h1', ''),
                cp.get('word_count', 0),
                json.dumps(cp.get('schema_types', [])),
                json.dumps(cp.get('h_structure', {})),
                (cp.get('text') or '')[:3000],
                now
            ))
        except Exception as e:
            print(f"  [!] DB save error: {e}")
    conn.commit()
    conn.close()


def save_analysis(our_url, our_wc, competitor_pages,
                  content_gaps, structural_gaps, missing_schema):
    avg_comp_wc = (
        sum(cp.get('word_count', 0) for cp in competitor_pages) //
        max(len(competitor_pages), 1)
    )
    wc_gap = avg_comp_wc - our_wc

    conn = sqlite3.connect(DB_NAME)
    now  = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO Competitor_Analysis
        (our_url, our_word_count, avg_competitor_wc, word_count_gap,
         missing_schema, content_gaps, structural_gaps,
         competitor_urls, analysed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        our_url, our_wc, avg_comp_wc, wc_gap,
        json.dumps(missing_schema),
        json.dumps(content_gaps),
        json.dumps(structural_gaps),
        json.dumps([cp['url'] for cp in competitor_pages]),
        now
    ))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────────────────────

async def run_competitor_scraper():
    print(f"\n{'='*60}")
    print("  SPRINT 8 — COMPETITOR SCRAPER")
    print(f"{'='*60}\n")

    if not os.path.exists(DB_NAME):
        print(f"[!] FATAL: {DB_NAME} not found.")
        return

    setup_competitor_tables()
    pages = load_our_pages()

    if not pages:
        print("[!] No pages found. Run previous sprints first.")
        return

    semaphore = asyncio.Semaphore(CONCURRENCY)
    ok = fail = skipped = 0

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for i, page in enumerate(pages, 1):
            url     = page['url']
            keyword = extract_keyword_from_page(page)
            our_wc  = page.get('word_count') or 0

            print(f"\n[{i}/{len(pages)}] {url}")
            print(f"  Keyword: '{keyword}'")

            # ── Find competitor URLs ──────────────────────────
            comp_urls = list(KNOWN_COMPETITORS)  # start with manual list

            # Auto-detect via Google
            found = google_search_competitors(keyword, SITE_DOMAIN, num_results=4)
            for u in found:
                if u not in comp_urls:
                    comp_urls.append(u)

            if not comp_urls:
                print(f"  [~] No competitors found for this keyword — skipping")
                skipped += 1
                continue

            print(f"  Found {len(comp_urls)} competitor URLs")

            # ── Scrape competitor pages ───────────────────────
            tasks    = [scrape_competitor_page(session, semaphore, u)
                        for u in comp_urls[:4]]
            results  = await asyncio.gather(*tasks, return_exceptions=True)
            comp_data = [r for r in results
                         if r and not isinstance(r, Exception)]

            if not comp_data:
                print(f"  [!] All competitor scrapes failed — skipping")
                skipped += 1
                continue

            print(f"  Scraped {len(comp_data)}/{len(comp_urls)} competitor pages")

            # ── Save raw competitor pages ─────────────────────
            save_competitor_pages(url, comp_data)

            # ── Python schema gap analysis ────────────────────
            our_schema = json.loads(page.get('schema_types') or '[]')
            missing_schema = find_schema_gaps(our_schema, comp_data)

            # ── Word count comparison ─────────────────────────
            avg_comp_wc = sum(cp['word_count'] for cp in comp_data) // len(comp_data)
            wc_gap      = avg_comp_wc - our_wc
            print(f"  Word count: ours={our_wc} | competitors avg={avg_comp_wc} | gap={wc_gap:+d}")

            # ── AI content gap analysis ───────────────────────
            print(f"  Running AI gap analysis...")
            content_gaps, structural_gaps = ai_gap_analysis(page, comp_data)

            # ── Save analysis ─────────────────────────────────
            save_analysis(url, our_wc, comp_data,
                          content_gaps, structural_gaps, missing_schema)

            print(f"  ✓ Gaps: {content_gaps[:2]} | Schema missing: {missing_schema}")

            if content_gaps or missing_schema:
                ok += 1
            else:
                ok += 1  # still counts as processed

            await asyncio.sleep(SLEEP_BETWEEN)

    # ── Log ───────────────────────────────────────────────────
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        INSERT INTO Audit_Log (run_at, phase, pages_processed,
                               pages_failed, notes)
        VALUES (?, 'competitor_analysis', ?, ?, ?)
    """, (datetime.now(timezone.utc).isoformat(), ok, fail,
          f"skipped={skipped}"))
    conn.commit()
    conn.close()

    # ── Summary ───────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  COMPETITOR ANALYSIS COMPLETE")
    print(f"{'='*60}")
    print(f"  Analysed : {ok} pages")
    print(f"  Skipped  : {skipped} pages")
    print(f"  Failed   : {fail} pages")
    print(f"\n  Run sprint6_reporting.py again to include competitor")
    print(f"  insights in your updated report.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    if os.sys.platform == 'win32':
        asyncio.set_event_loop(asyncio.ProactorEventLoop())
    asyncio.run(run_competitor_scraper())

