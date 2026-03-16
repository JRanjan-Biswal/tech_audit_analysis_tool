import sqlite3
import os
import shutil
from datetime import datetime


# --- CONFIGURATION ---
from config import (
    DB_NAME
)
# DB_NAME = "seo_master.db"


def backup_database():
    """Always backup before migration."""
    if os.path.exists(DB_NAME):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"seo_master_backup_{timestamp}.db"
        shutil.copy2(DB_NAME, backup_name)
        print(f"[✓] Backup created: {backup_name}")
        return backup_name
    return None




def get_existing_columns(cursor, table="Pages"):
    """Returns set of existing column names."""
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}




def safe_add_column(cursor, table, column, definition):
    """Adds a column only if it doesn't already exist."""
    existing = get_existing_columns(cursor, table)
    if column not in existing:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        print(f"  [+] Added column: {column}")
    else:
        print(f"  [~] Already exists: {column}")




def run_migration():
    print(f"\n{'='*60}")
    print("SEO MASTER DB — SPRINT 1 MIGRATION")
    print(f"{'='*60}\n")


    # Step 1: Backup
    backup_name = backup_database()


    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()


    # Step 2: Create table if it doesn't exist (fresh start support)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Pages (
        url TEXT PRIMARY KEY,
        ga4_sessions INTEGER DEFAULT 0
    )
    """)
    conn.commit()


    print("\n[*] Migrating Pages table...\n")


    # ----------------------------------------------------------------
    # LAYER 1 — CRAWLER DATA (what we scrape from the page)
    # ----------------------------------------------------------------
    print("--- LAYER 1: Crawler Data ---")
    safe_add_column(cursor, "Pages", "status_code",          "INTEGER")
    safe_add_column(cursor, "Pages", "scraped_h1",           "TEXT")
    safe_add_column(cursor, "Pages", "scraped_text",         "TEXT")
    safe_add_column(cursor, "Pages", "meta_title",           "TEXT")
    safe_add_column(cursor, "Pages", "meta_description",     "TEXT")
    safe_add_column(cursor, "Pages", "canonical_url",        "TEXT")
    safe_add_column(cursor, "Pages", "robots_meta",          "TEXT")
    safe_add_column(cursor, "Pages", "page_load_ms",         "INTEGER")
    safe_add_column(cursor, "Pages", "page_size_bytes",      "INTEGER")
    safe_add_column(cursor, "Pages", "word_count",           "INTEGER")
    safe_add_column(cursor, "Pages", "internal_links_count", "INTEGER")
    safe_add_column(cursor, "Pages", "external_links_count", "INTEGER")
    safe_add_column(cursor, "Pages", "internal_links_list",  "TEXT")   # JSON array
    safe_add_column(cursor, "Pages", "image_count",          "INTEGER")
    safe_add_column(cursor, "Pages", "images_missing_alt",   "INTEGER")
    safe_add_column(cursor, "Pages", "has_schema",           "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "schema_types",         "TEXT")   # JSON array e.g. ["Article","BreadcrumbList"]
    safe_add_column(cursor, "Pages", "has_viewport_meta",    "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "is_https",             "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "h_tag_structure",      "TEXT")   # JSON: {"h1":1,"h2":4,"h3":7}
    safe_add_column(cursor, "Pages", "is_scraped",           "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "scraped_at",           "TEXT")   # ISO timestamp


    # ----------------------------------------------------------------
    # LAYER 2 — TECHNICAL AUDIT (Tier 1 Python checks)
    # ----------------------------------------------------------------
    print("\n--- LAYER 2: Technical Audit ---")
    safe_add_column(cursor, "Pages", "title_length",              "INTEGER")
    safe_add_column(cursor, "Pages", "meta_desc_length",          "INTEGER")
    safe_add_column(cursor, "Pages", "title_has_keyword",         "BOOLEAN")
    safe_add_column(cursor, "Pages", "h1_count",                  "INTEGER")
    safe_add_column(cursor, "Pages", "has_canonical",             "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "is_thin_content",           "BOOLEAN DEFAULT FALSE")  # < 300 words
    safe_add_column(cursor, "Pages", "is_duplicate_title",        "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "is_duplicate_meta",         "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "is_orphan_page",            "BOOLEAN DEFAULT FALSE")  # 0 internal links in
    safe_add_column(cursor, "Pages", "crawl_depth",               "INTEGER")                # clicks from homepage
    safe_add_column(cursor, "Pages", "tech_issues",               "TEXT")    # JSON array of issue strings
    safe_add_column(cursor, "Pages", "tech_severity",             "TEXT")    # critical / warning / info / ok
    safe_add_column(cursor, "Pages", "tech_score",                "INTEGER") # 0-100
    safe_add_column(cursor, "Pages", "is_audited",                "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "audited_at",                "TEXT")


    # ----------------------------------------------------------------
    # LAYER 3 — PRIORITY ENGINE
    # ----------------------------------------------------------------
    print("\n--- LAYER 3: Priority Engine ---")
    safe_add_column(cursor, "Pages", "priority_score",   "REAL")    # 0.0 - 100.0
    safe_add_column(cursor, "Pages", "priority_tier",    "TEXT")    # low / medium / high
    safe_add_column(cursor, "Pages", "priority_reasons", "TEXT")    # JSON array explaining why


    # ----------------------------------------------------------------
    # LAYER 4 — AI ANALYSIS TIER 2 (9b model — E-E-A-T + intent)
    # ----------------------------------------------------------------
    print("\n--- LAYER 4: AI Analysis (Tier 2) ---")
    safe_add_column(cursor, "Pages", "llm_eeat_score",        "INTEGER")  # 1-10
    safe_add_column(cursor, "Pages", "llm_intent",            "TEXT")     # informational/navigational/commercial/transactional
    safe_add_column(cursor, "Pages", "llm_intent_match",      "BOOLEAN")  # does content match intent?
    safe_add_column(cursor, "Pages", "llm_recommendation",    "TEXT")     # short advice
    safe_add_column(cursor, "Pages", "llm_quick_wins",        "TEXT")     # JSON array of 3 quick fixes
    safe_add_column(cursor, "Pages", "llm_model_used",        "TEXT")     # which model analyzed this
    safe_add_column(cursor, "Pages", "is_analyzed",           "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "analyzed_at",           "TEXT")


    # ----------------------------------------------------------------
    # LAYER 5 — AI ANALYSIS TIER 3 (27b model — deep analysis)
    # ----------------------------------------------------------------
    print("\n--- LAYER 5: AI Analysis (Tier 3 Deep) ---")
    safe_add_column(cursor, "Pages", "llm_content_score",         "INTEGER")  # 1-10 overall content quality
    safe_add_column(cursor, "Pages", "llm_title_rewrite",         "TEXT")     # suggested new title tag
    safe_add_column(cursor, "Pages", "llm_meta_rewrite",          "TEXT")     # suggested new meta description
    safe_add_column(cursor, "Pages", "llm_content_gaps",          "TEXT")     # JSON array of missing topics
    safe_add_column(cursor, "Pages", "llm_schema_suggestions",    "TEXT")     # JSON: what schema to add
    safe_add_column(cursor, "Pages", "llm_geo_score",             "INTEGER")  # 1-10 AI search readiness
    safe_add_column(cursor, "Pages", "llm_geo_improvements",      "TEXT")     # JSON array
    safe_add_column(cursor, "Pages", "llm_content_brief",         "TEXT")     # full rewrite brief
    safe_add_column(cursor, "Pages", "is_deep_analyzed",          "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "deep_analyzed_at",          "TEXT")


    # ----------------------------------------------------------------
    # LAYER 6 — GSC DATA (keyword performance)
    # ----------------------------------------------------------------
    print("\n--- LAYER 6: GSC Keyword Data ---")
    safe_add_column(cursor, "Pages", "gsc_top_keyword",       "TEXT")
    safe_add_column(cursor, "Pages", "gsc_impressions",       "INTEGER DEFAULT 0")
    safe_add_column(cursor, "Pages", "gsc_clicks",            "INTEGER DEFAULT 0")
    safe_add_column(cursor, "Pages", "gsc_ctr",               "REAL DEFAULT 0.0")
    safe_add_column(cursor, "Pages", "gsc_avg_position",      "REAL")
    safe_add_column(cursor, "Pages", "gsc_opportunity_tier",  "TEXT")   # quick_win / battle / protect / ignore
    safe_add_column(cursor, "Pages", "gsc_data_loaded",       "BOOLEAN DEFAULT FALSE")


    # ----------------------------------------------------------------
    # LAYER 7 — LOCAL SEO
    # ----------------------------------------------------------------
    print("\n--- LAYER 7: Local SEO ---")
    safe_add_column(cursor, "Pages", "has_local_schema",      "BOOLEAN DEFAULT FALSE")
    safe_add_column(cursor, "Pages", "has_nap",               "BOOLEAN DEFAULT FALSE")  # Name/Address/Phone present
    safe_add_column(cursor, "Pages", "local_keywords_found",  "TEXT")   # JSON array


    # ----------------------------------------------------------------
    # INDEXES for fast querying
    # ----------------------------------------------------------------
    print("\n--- Creating Indexes ---")
    indexes = [
        ("idx_priority_tier",    "Pages(priority_tier)"),
        ("idx_is_analyzed",      "Pages(is_analyzed)"),
        ("idx_is_audited",       "Pages(is_audited)"),
        ("idx_is_scraped",       "Pages(is_scraped)"),
        ("idx_ga4_sessions",     "Pages(ga4_sessions DESC)"),
        ("idx_tech_severity",    "Pages(tech_severity)"),
        ("idx_gsc_position",     "Pages(gsc_avg_position)"),
        ("idx_priority_score",   "Pages(priority_score DESC)"),
    ]
    for idx_name, idx_def in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}")
            print(f"  [+] Index: {idx_name}")
        except Exception as e:
            print(f"  [!] Index error {idx_name}: {e}")


    # ----------------------------------------------------------------
    # GSC Keywords table (separate — one page has many keywords)
    # ----------------------------------------------------------------
    print("\n--- Creating GSC_Keywords table ---")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS GSC_Keywords (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        url         TEXT NOT NULL,
        keyword     TEXT NOT NULL,
        impressions INTEGER DEFAULT 0,
        clicks      INTEGER DEFAULT 0,
        ctr         REAL DEFAULT 0.0,
        avg_position REAL,
        opportunity TEXT,   -- quick_win / battle / protect / monitor
        FOREIGN KEY (url) REFERENCES Pages(url),
        UNIQUE(url, keyword)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gsc_keyword ON GSC_Keywords(keyword)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gsc_url ON GSC_Keywords(url)")
    print("  [+] GSC_Keywords table ready")


    # ----------------------------------------------------------------
    # Audit_Log table (track every run)
    # ----------------------------------------------------------------
    print("\n--- Creating Audit_Log table ---")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Audit_Log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        run_at      TEXT NOT NULL,
        phase       TEXT NOT NULL,   -- crawler / technical / ai_tier2 / ai_tier3 / gsc
        pages_processed INTEGER DEFAULT 0,
        pages_failed    INTEGER DEFAULT 0,
        duration_seconds REAL,
        notes       TEXT
    )
    """)
    print("  [+] Audit_Log table ready")


    conn.commit()
    conn.close()


    print(f"\n{'='*60}")
    print("✅ MIGRATION COMPLETE")
    print(f"{'='*60}")
    print(f"\nYour existing data is safe.")
    if backup_name:
        print(f"Backup saved as: {backup_name}")
    print("\nNext step: Run sprint2_enhanced_crawler.py")




if __name__ == "__main__":
    run_migration()

