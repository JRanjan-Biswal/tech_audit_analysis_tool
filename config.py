"""
SEO Master Pipeline — Shared Configuration
==========================================
Edit this file once. All sprints read from here.
"""

import sqlite3
import os

# ── Site Identity ─────────────────────────────────────────────
SITE_URL    = "https://bodycraftacademy.com"
DOMAIN_URL  = "bodycraftacademy.com"
SITE_NAME   = "Bodycraft Academy"

# ── Database ──────────────────────────────────────────────────
DB_NAME     = "seo_master.db"

# ── Report Output ─────────────────────────────────────────────
REPORT_FILE = "seo_report.pdf"

# ── Ollama / AI ───────────────────────────────────────────────
OLLAMA_URL    = "http://localhost:11434/api/chat"
MODEL_TIER2   = "qwen3.5:9b-q8_0"     # fast — medium priority pages
MODEL_TIER3   = "qwen3.5:27b-q4_K_M"  # deep — high priority pages

# ── Crawler ───────────────────────────────────────────────────
CRAWL_CONCURRENCY   = 5
CRAWL_TIMEOUT       = 20    # seconds per page
CRAWL_SLEEP_BETWEEN = 0.1   # seconds between requests

# ── AI Analyser ───────────────────────────────────────────────
AI_SLEEP_BETWEEN = 1        # seconds between Ollama calls
TIER2_WORDS      = 60       # content words fed to 9b model
TIER3_WORDS      = 120      # content words fed to 27b model

TIER2_OPTIONS = {
    "temperature":    0.1,
    "repeat_penalty": 1.1,
    "num_ctx":        3072,
    "num_predict":    500,
}
TIER3_OPTIONS = {
    "temperature":    0.1,
    "repeat_penalty": 1.1,
    "num_ctx":        4096,
    "num_predict":    600,
}

# ── Competitor Scraper ────────────────────────────────────────
COMPETITOR_CONCURRENCY  = 3
COMPETITOR_TIMEOUT      = 20
COMPETITOR_SLEEP        = 1.5
COMPETITOR_MAX_WORDS    = 150

# Known competitors (optional — leave empty for auto-detect only)
KNOWN_COMPETITORS = [
    # "https://competitor1.com",
]

# ── Priority Engine ───────────────────────────────────────────
# Junk URL patterns to exclude from AI analysis
JUNK_URL_PATTERNS = [
    'wp-login', 'wp-admin', 'xmlrpc', 'feed', 'zombie',
    '///checkout', '///cart', '///my-account',
]

# ── GA4 CSV ───────────────────────────────────────────────────
GA4_CSV_PATH = "data_uploads/ga4_data.csv"


# ─────────────────────────────────────────────────────────────
# DYNAMIC SITE DETECTION
# If SITE_URL is not set, try to detect from DB
# ─────────────────────────────────────────────────────────────

def get_site_config():
    """
    Returns (site_url, site_name).
    Detects the domain from the DB, but keeps the human-readable
    SITE_NAME from this config file if it's been set explicitly.
    """
    if not os.path.exists(DB_NAME):
        return SITE_URL, SITE_NAME

    try:
        conn = sqlite3.connect(DB_NAME)
        row  = conn.execute(
            "SELECT url FROM Pages WHERE is_scraped=1 LIMIT 1"
        ).fetchone()
        conn.close()

        if row and row[0]:
            from urllib.parse import urlparse
            parsed   = urlparse(row[0])
            detected_url = f"{parsed.scheme}://{parsed.netloc}"
            # Use human-readable SITE_NAME from config; only auto-generate
            # a name if the user is pointing at a completely different domain
            if parsed.netloc and parsed.netloc not in SITE_URL:
                domain    = parsed.netloc.replace('www.', '')
                auto_name = domain.split('.')[0].replace('-', ' ').title()
                return detected_url, auto_name
            return detected_url, SITE_NAME

    except Exception:
        pass

    return SITE_URL, SITE_NAME


def get_site_domain():
    url, _ = get_site_config()
    from urllib.parse import urlparse
    return urlparse(url).netloc.replace('www.', '')


# if __name__ == "__main__":
#     print(get_site_config())