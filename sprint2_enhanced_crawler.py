import sqlite3
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time
import sys
import os
import json
import re
from datetime import datetime, timezone


# --- CONFIGURATION ---
from config import (
    DB_NAME,
    CRAWL_CONCURRENCY as CONCURRENCY_LIMIT, 
    CRAWL_TIMEOUT as TIMEOUT_SECONDS, 
    SITE_URL as BASE_DOMAIN
)


# RAM CACHE — scrape once, never re-fetch
PAGE_CACHE = {}


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}




# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────


def normalize_url(url):
    """Strip trailing slash, normalize double slashes in path."""
    url = url.strip()
    # Fix double slashes in path (not in https://)
    parts = url.split("://", 1)
    if len(parts) == 2:
        url = parts[0] + "://" + re.sub(r'/{2,}', '/', parts[1])
    if url.endswith('/'):
        url = url[:-1]
    return url




def is_internal(href, base=BASE_DOMAIN):
    if not href:
        return False
    href = href.strip()
    if href.startswith('/'):
        return True
    if href.startswith(base):
        return True
    return False




def resolve_url(href, base=BASE_DOMAIN):
    href = href.strip()
    if href.startswith('http'):
        return normalize_url(href)
    if href.startswith('/'):
        return normalize_url(base + href)
    return None




def extract_schema_types(soup):
    """Find all Schema.org @type values from JSON-LD blocks."""
    types = []
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
            # Handle single object or @graph array
            if isinstance(data, dict):
                items = data.get('@graph', [data])
            elif isinstance(data, list):
                items = data
            else:
                items = []
            for item in items:
                t = item.get('@type')
                if t:
                    types.append(t if isinstance(t, str) else str(t))
        except Exception:
            pass
    return list(set(types))




def extract_h_structure(soup):
    """Count H1-H6 tags."""
    structure = {}
    for level in range(1, 7):
        count = len(soup.find_all(f'h{level}'))
        if count > 0:
            structure[f'h{level}'] = count
    return structure




def extract_images_audit(soup):
    """Return total image count and count of images missing alt."""
    images = soup.find_all('img')
    missing_alt = sum(
        1 for img in images
        if not img.get('alt') or img.get('alt', '').strip() == ''
    )
    return len(images), missing_alt




def extract_links(soup, page_url):
    """Split links into internal and external, return lists."""
    internal = []
    external = []
    seen = set()


    for a in soup.find_all('a', href=True):
        href = a['href'].strip()


        # Skip anchors, mailto, tel, javascript
        if href.startswith(('#', 'mailto:', 'tel:', 'javascript:')):
            continue


        resolved = resolve_url(href)
        if not resolved or resolved in seen:
            continue
        seen.add(resolved)


        if is_internal(href):
            internal.append(resolved)
        else:
            external.append(resolved)


    return internal, external




# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────


def get_pending_urls():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT url FROM Pages
        WHERE is_scraped = 0
           OR is_scraped IS NULL
           OR is_scraped = 'FALSE'
           OR is_scraped = '0'
    """)
    urls = [row[0] for row in cursor.fetchall()]
    conn.close()
    print(f"[*] Found {len(urls)} URLs pending scrape.")
    return urls




def batch_write_to_db(results):
    """Write all results in one transaction — fast."""
    if not results:
        return
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()


    for r in results:
        try:
            cursor.execute("""
                UPDATE Pages SET
                    status_code          = ?,
                    scraped_h1           = ?,
                    scraped_text         = ?,
                    meta_title           = ?,
                    meta_description     = ?,
                    canonical_url        = ?,
                    robots_meta          = ?,
                    page_load_ms         = ?,
                    page_size_bytes      = ?,
                    word_count           = ?,
                    internal_links_count = ?,
                    external_links_count = ?,
                    internal_links_list  = ?,
                    image_count          = ?,
                    images_missing_alt   = ?,
                    has_schema           = ?,
                    schema_types         = ?,
                    has_viewport_meta    = ?,
                    is_https             = ?,
                    h_tag_structure      = ?,
                    is_scraped           = 1,
                    scraped_at           = ?
                WHERE url = ?
            """, (
                r['status_code'],
                r['scraped_h1'],
                r['scraped_text'],
                r['meta_title'],
                r['meta_description'],
                r['canonical_url'],
                r['robots_meta'],
                r['page_load_ms'],
                r['page_size_bytes'],
                r['word_count'],
                r['internal_links_count'],
                r['external_links_count'],
                json.dumps(r['internal_links_list']),
                r['image_count'],
                r['images_missing_alt'],
                r['has_schema'],
                json.dumps(r['schema_types']),
                r['has_viewport_meta'],
                r['is_https'],
                json.dumps(r['h_tag_structure']),
                now,
                r['url']
            ))
        except Exception as e:
            print(f"  [!] DB write failed for {r['url']}: {e}")


    conn.commit()
    conn.close()
    print(f"[✓] Batch wrote {len(results)} pages to DB.")




# ─────────────────────────────────────────────
# CORE FETCH + PARSE
# ─────────────────────────────────────────────


async def fetch_and_parse(session, semaphore, url):
    async with semaphore:
        print(f"[>] Fetching: {url}")
        start_ms = time.time()


        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as response:
                status = response.status
                load_ms = int((time.time() - start_ms) * 1000)
                raw_bytes = await response.read()
                page_size = len(raw_bytes)


                if status != 200:
                    print(f"  [-] HTTP {status}: {url}")
                    return {
                        'url': url, 'status_code': status,
                        'scraped_h1': None, 'scraped_text': None,
                        'meta_title': None, 'meta_description': None,
                        'canonical_url': None, 'robots_meta': None,
                        'page_load_ms': load_ms, 'page_size_bytes': page_size,
                        'word_count': 0, 'internal_links_count': 0,
                        'external_links_count': 0, 'internal_links_list': [],
                        'image_count': 0, 'images_missing_alt': 0,
                        'has_schema': False, 'schema_types': [],
                        'has_viewport_meta': False, 'is_https': url.startswith('https'),
                        'h_tag_structure': {}
                    }


                # Parse HTML
                try:
                    html = raw_bytes.decode('utf-8', errors='replace')
                except Exception:
                    html = raw_bytes.decode('latin-1', errors='replace')


                # Store raw HTML in RAM cache
                PAGE_CACHE[url] = html


                try:
                    soup = BeautifulSoup(html, 'lxml')
                except Exception:
                    soup = BeautifulSoup(html, 'html.parser')


                # ── Meta ──
                meta_title_tag = soup.find('title')
                meta_title = meta_title_tag.get_text(strip=True) if meta_title_tag else None


                meta_desc_tag = soup.find('meta', attrs={'name': re.compile(r'^description$', re.I)})
                meta_description = meta_desc_tag.get('content', '').strip() if meta_desc_tag else None


                # ── Canonical ──
                canonical_tag = soup.find('link', rel=lambda r: r and 'canonical' in r)
                canonical_url = canonical_tag.get('href', '').strip() if canonical_tag else None


                # ── Robots meta ──
                robots_tag = soup.find('meta', attrs={'name': re.compile(r'^robots$', re.I)})
                robots_meta = robots_tag.get('content', '').strip() if robots_tag else None


                # ── Viewport ──
                viewport_tag = soup.find('meta', attrs={'name': re.compile(r'^viewport$', re.I)})
                has_viewport = viewport_tag is not None


                # ── H tags ──
                h_structure = extract_h_structure(soup)


                # ── H1 ──
                h1_tag = soup.find('h1')
                h1_text = h1_tag.get_text(strip=True) if h1_tag else "MISSING_H1"


                # ── Schema ──
                schema_types = extract_schema_types(soup)
                has_schema = len(schema_types) > 0


                # ── Images ──
                image_count, images_missing_alt = extract_images_audit(soup)


                # ── Links ──
                internal_links, external_links = extract_links(soup, url)


                # ── Clean text (remove noise) ──
                for el in soup(["script", "style", "nav", "footer",
                                 "header", "aside", "noscript", "form"]):
                    el.decompose()


                main = soup.find('main') or soup.find('article') or soup.find('body')
                if main:
                    raw_text = main.get_text(separator=' ', strip=True)
                    # Normalize whitespace
                    clean_text = ' '.join(raw_text.split())
                else:
                    clean_text = ''


                word_count = len(clean_text.split())


                # Store in RAM — full text preserved, AI will get truncated slice
                PAGE_CACHE[url + '_text'] = clean_text


                print(f"  [+] OK {status} | {load_ms}ms | {word_count}w | "
                      f"schema:{schema_types} | links:{len(internal_links)}i/{len(external_links)}e | "
                      f"imgs:{image_count}({images_missing_alt} no-alt)")


                return {
                    'url':                  url,
                    'status_code':          status,
                    'scraped_h1':           h1_text,
                    'scraped_text':         clean_text,
                    'meta_title':           meta_title,
                    'meta_description':     meta_description,
                    'canonical_url':        canonical_url,
                    'robots_meta':          robots_meta,
                    'page_load_ms':         load_ms,
                    'page_size_bytes':      page_size,
                    'word_count':           word_count,
                    'internal_links_count': len(internal_links),
                    'external_links_count': len(external_links),
                    'internal_links_list':  internal_links[:50],  # cap at 50 for DB size
                    'image_count':          image_count,
                    'images_missing_alt':   images_missing_alt,
                    'has_schema':           has_schema,
                    'schema_types':         schema_types,
                    'has_viewport_meta':    has_viewport,
                    'is_https':             url.startswith('https'),
                    'h_tag_structure':      h_structure,
                }


        except asyncio.TimeoutError:
            print(f"  [!] TIMEOUT: {url}")
            return {'url': url, 'status_code': 408, 'scraped_h1': None,
                    'scraped_text': 'ERROR:Timeout', 'meta_title': None,
                    'meta_description': None, 'canonical_url': None,
                    'robots_meta': None, 'page_load_ms': TIMEOUT_SECONDS * 1000,
                    'page_size_bytes': 0, 'word_count': 0,
                    'internal_links_count': 0, 'external_links_count': 0,
                    'internal_links_list': [], 'image_count': 0,
                    'images_missing_alt': 0, 'has_schema': False,
                    'schema_types': [], 'has_viewport_meta': False,
                    'is_https': url.startswith('https'), 'h_tag_structure': {}}


        except Exception as e:
            print(f"  [!] ERROR {url}: {e}")
            return {'url': url, 'status_code': 0, 'scraped_h1': None,
                    'scraped_text': f'ERROR:{str(e)}', 'meta_title': None,
                    'meta_description': None, 'canonical_url': None,
                    'robots_meta': None, 'page_load_ms': 0,
                    'page_size_bytes': 0, 'word_count': 0,
                    'internal_links_count': 0, 'external_links_count': 0,
                    'internal_links_list': [], 'image_count': 0,
                    'images_missing_alt': 0, 'has_schema': False,
                    'schema_types': [], 'has_viewport_meta': False,
                    'is_https': url.startswith('https'), 'h_tag_structure': {}}




# ─────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────


async def run_crawler():
    if not os.path.exists(DB_NAME):
        print(f"[!] FATAL: {DB_NAME} not found. Run sprint1_db_migration.py first.")
        sys.exit(1)


    urls = get_pending_urls()
    if not urls:
        print("[-] No pending URLs. All pages already scraped.")
        print("    To re-scrape: UPDATE Pages SET is_scraped = 0")
        return


    print(f"\n[*] Starting enhanced crawl — {len(urls)} pages")
    print(f"[*] Concurrency: {CONCURRENCY_LIMIT} | Timeout: {TIMEOUT_SECONDS}s")
    print(f"[*] RAM cache active — HTML stored in memory\n")


    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    results = []
    failed = 0


    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = [fetch_and_parse(session, semaphore, url) for url in urls]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)


    for r in raw_results:
        if isinstance(r, Exception):
            failed += 1
            print(f"[!] Task exception: {r}")
        elif r:
            results.append(r)


    # Batch write all results at once
    print(f"\n[*] Writing {len(results)} results to DB...")
    batch_write_to_db(results)


    # Log this run
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        INSERT INTO Audit_Log (run_at, phase, pages_processed, pages_failed)
        VALUES (?, 'crawler', ?, ?)
    """, (datetime.now(timezone.utc).isoformat(), len(results), failed))
    conn.commit()
    conn.close()


    # Summary
    success = sum(1 for r in results if r.get('status_code') == 200)
    errors  = sum(1 for r in results if r.get('status_code') != 200)


    print(f"\n{'='*60}")
    print(f"✅ CRAWL COMPLETE")
    print(f"{'='*60}")
    print(f"  Success (200):  {success}")
    print(f"  Errors:         {errors}")
    print(f"  Task failures:  {failed}")
    print(f"  RAM cache size: {len(PAGE_CACHE)} entries")
    print(f"\nNext step: Run sprint3_technical_auditor.py")



if __name__ == "__main__":
    start = time.time()
    if sys.platform == 'win32':
        asyncio.set_event_loop(asyncio.ProactorEventLoop())
    asyncio.run(run_crawler())
    print(f"\n[*] Total time: {round(time.time() - start, 2)}s")



