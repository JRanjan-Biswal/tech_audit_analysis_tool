import argparse
import subprocess
import sqlite3
import os
import sys
import json
from datetime import datetime

# --- CONFIGURATION ---
from config import (
    DB_NAME,
    SITE_NAME
)

PIPELINE_STEPS = [
    ("db",         "db_builder.py",                  "Build database from GA4 CSV"),
    ("migrate",    "sprint1_db_migration.py",         "Run DB schema migration"),
    ("crawl",      "sprint2_enhanced_crawler.py",     "Crawl and scrape all pages"),
    ("audit",      "sprint3_technical_auditor.py",    "Run technical SEO audit"),
    ("priority",   "sprint4_priority_engine.py",      "Score and route pages by priority"),
    ("analyse",    "sprint5_ai_analyser.py",          "Run AI analysis (Tier 2 + Tier 3)"),
    ("report",     "sprint6_reporting.py",            "Generate SEO report"),
    ("compete",    "sprint8_competitor_scraper.py",   "Run competitor gap analysis"),
]

COLORS = {
    "green":  "\033[92m",
    "yellow": "\033[93m",
    "red":    "\033[91m",
    "blue":   "\033[94m",
    "bold":   "\033[1m",
    "reset":  "\033[0m",
}

def c(text, color):
    return f"{COLORS.get(color,'')}{text}{COLORS['reset']}"


# ─────────────────────────────────────────────────────────────
# DB STATUS
# ─────────────────────────────────────────────────────────────

def get_db_status():
    if not os.path.exists(DB_NAME):
        return None
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cur  = conn.cursor()

        # Check tables exist
        tables = {r[0] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

        if 'Pages' not in tables:
            conn.close()
            return {"total": 0}

        stats = {}
        stats['total']        = cur.execute("SELECT COUNT(*) FROM Pages").fetchone()[0]
        stats['scraped']      = cur.execute("SELECT COUNT(*) FROM Pages WHERE is_scraped=1").fetchone()[0]
        stats['audited']      = cur.execute("SELECT COUNT(*) FROM Pages WHERE is_audited=1").fetchone()[0]
        stats['analysed']     = cur.execute("SELECT COUNT(*) FROM Pages WHERE is_analyzed=1").fetchone()[0]
        stats['deep']         = cur.execute("SELECT COUNT(*) FROM Pages WHERE is_deep_analyzed=1").fetchone()[0]
        stats['critical']     = cur.execute("SELECT COUNT(*) FROM Pages WHERE tech_severity='critical'").fetchone()[0]
        stats['avg_tech']     = cur.execute("SELECT ROUND(AVG(tech_score),1) FROM Pages WHERE tech_score IS NOT NULL").fetchone()[0] or 0
        stats['avg_eeat']     = cur.execute("SELECT ROUND(AVG(llm_eeat_score),1) FROM Pages WHERE llm_eeat_score IS NOT NULL").fetchone()[0] or 0
        stats['sessions']     = cur.execute("SELECT SUM(ga4_sessions) FROM Pages").fetchone()[0] or 0
        stats['high']         = cur.execute("SELECT COUNT(*) FROM Pages WHERE priority_tier='high'").fetchone()[0]
        stats['medium']       = cur.execute("SELECT COUNT(*) FROM Pages WHERE priority_tier='medium'").fetchone()[0]
        stats['low']          = cur.execute("SELECT COUNT(*) FROM Pages WHERE priority_tier='low'").fetchone()[0]
        stats['has_compete']  = 'Competitor_Analysis' in tables
        stats['compete_done'] = 0
        if stats['has_compete']:
            stats['compete_done'] = cur.execute(
                "SELECT COUNT(*) FROM Competitor_Analysis").fetchone()[0]

        # Last run times from Audit_Log
        if 'Audit_Log' in tables:
            log = cur.execute("""
                SELECT phase, MAX(run_at) as last_run
                FROM Audit_Log GROUP BY phase
            """).fetchall()
            stats['last_runs'] = {r[0]: r[1][:16].replace('T', ' ') for r in log}
        else:
            stats['last_runs'] = {}

        conn.close()
        return stats
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────────────────

def show_dashboard():
    print(f"\n{c('='*62, 'bold')}")
    print(c(f"  SEO MASTER PIPELINE — {SITE_NAME}", 'bold'))
    print(c(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}", 'blue'))
    print(f"{c('='*62, 'bold')}\n")

    stats = get_db_status()

    if not stats or stats.get('total', 0) == 0:
        print(c("  ⚠️  No data yet. Run: python seo.py run --all", 'yellow'))
        print(f"\n  Quick start:")
        print(f"    python seo.py run --all          # full pipeline")
        print(f"    python seo.py run --step crawl   # single step")
        print(f"    python seo.py status             # this dashboard")
        print(f"    python seo.py reset --step crawl # reset a step\n")
        return

    if 'error' in stats:
        print(c(f"  DB Error: {stats['error']}", 'red'))
        return

    # Pipeline progress
    print(c("  PIPELINE STATUS", 'bold'))
    steps = [
        ("DB Build",      stats['total'] > 0),
        ("Migration",     stats['total'] > 0),
        ("Crawl",         stats['scraped'] > 0),
        ("Tech Audit",    stats['audited'] > 0),
        ("Priority",      stats['high'] + stats['medium'] + stats['low'] > 0),
        ("AI Analysis",   stats['analysed'] > 0),
        ("Report",        os.path.exists("seo_report.md")),
        ("Competitors",   stats['compete_done'] > 0),
    ]
    for name, done in steps:
        icon   = c("✅", 'green') if done else c("🔲", 'yellow')
        status = c("done", 'green') if done else c("pending", 'yellow')
        print(f"    {icon}  {name:<18} {status}")

    # Site health
    print(f"\n{c('  SITE HEALTH', 'bold')}")
    tech_color = 'green' if stats['avg_tech'] >= 70 else 'yellow' if stats['avg_tech'] >= 50 else 'red'
    eeat_color = 'green' if stats['avg_eeat'] >= 7  else 'yellow' if stats['avg_eeat'] >= 5  else 'red'

    tech_score_str = f"{stats['avg_tech']}/100"
    eeat_score_str = f"{stats['avg_eeat']}/10"

    print(f"    Avg Technical Score : {c(tech_score_str, tech_color)}")
    print(f"    Avg E-E-A-T Score   : {c(eeat_score_str, eeat_color)}")
    print(f"    Critical Pages      : {c(str(stats['critical']), 'red' if stats['critical'] > 0 else 'green')}")
    print(f"    Total GA4 Sessions  : {stats['sessions']:,}")

    # Page breakdown
    print(f"\n{c('  PAGES', 'bold')}")
    print(f"    Total         : {stats['total']}")
    print(f"    Scraped       : {stats['scraped']}/{stats['total']}")
    print(f"    Audited       : {stats['audited']}/{stats['total']}")
    print(f"    AI Analysed   : {stats['analysed']}/{stats['total']}")
    print(f"    Deep Analysed : {stats['deep']}/{stats['total']}")
    print(f"    Competitors   : {stats['compete_done']} pages analysed")

    # Priority breakdown
    print(f"\n{c('  AI TIER ROUTING', 'bold')}")
    print(f"    🔴 High   (27b deep)  : {stats['high']} pages")
    print(f"    🟡 Medium (9b fast)   : {stats['medium']} pages")
    print(f"    🟢 Low    (no AI)     : {stats['low']} pages")

    # Last runs
    if stats.get('last_runs'):
        print(f"\n{c('  LAST RUN TIMES', 'bold')}")
        for phase, ts in stats['last_runs'].items():
            print(f"    {phase:<25} {ts}")

    # Report
    if os.path.exists("seo_report.md"):
        size = os.path.getsize("seo_report.md")
        print(f"\n{c('  REPORT', 'bold')}")
        print(f"    seo_report.md — {size//1024}KB")
        print(f"    Open with: code seo_report.md")

    print(f"\n{c('  COMMANDS', 'bold')}")
    print(f"    python seo.py run --all            # full pipeline")
    print(f"    python seo.py run --step analyse   # re-run AI analysis")
    print(f"    python seo.py run --from crawl     # run from a step onward")
    print(f"    python seo.py reset --step crawl   # reset a step's data")
    print(f"    python seo.py inspect --url <url>  # inspect a single page")
    print(f"    python seo.py quick-wins           # show top 10 quick wins")
    print(f"    python seo.py critical             # show critical pages only")
    print(f"\n{c('='*62, 'bold')}\n")


# ─────────────────────────────────────────────────────────────
# RUN STEP
# ─────────────────────────────────────────────────────────────

def run_step(step_name):
    step = next((s for s in PIPELINE_STEPS if s[0] == step_name), None)
    if not step:
        print(c(f"[!] Unknown step: {step_name}", 'red'))
        print(f"    Available: {', '.join(s[0] for s in PIPELINE_STEPS)}")
        return False

    script = step[1]
    desc   = step[2]

    if not os.path.exists(script):
        print(c(f"[!] Script not found: {script}", 'red'))
        return False

    print(f"\n{c('▶', 'green')} Running: {c(desc, 'bold')}")
    print(f"  Script: {script}\n")

    start  = datetime.now()
    result = subprocess.run([sys.executable, script], check=False)
    elapsed = (datetime.now() - start).seconds

    if result.returncode == 0:
        print(f"\n{c('✅', 'green')} {desc} — completed in {elapsed}s")
        return True
    else:
        print(f"\n{c('❌', 'red')} {desc} — failed (exit code {result.returncode})")
        return False


def run_all():
    print(c("\n  RUNNING FULL PIPELINE\n", 'bold'))
    for step in PIPELINE_STEPS:
        success = run_step(step[0])
        if not success:
            print(c(f"\n[!] Pipeline stopped at: {step[0]}", 'red'))
            print(f"    Fix the error and resume with:")
            print(f"    python seo.py run --from {step[0]}")
            return


def run_from(step_name):
    """Run all steps starting from a given step."""
    names = [s[0] for s in PIPELINE_STEPS]
    if step_name not in names:
        print(c(f"[!] Unknown step: {step_name}", 'red'))
        return
    start_idx = names.index(step_name)
    steps_to_run = PIPELINE_STEPS[start_idx:]
    print(c(f"\n  RUNNING PIPELINE FROM: {step_name}\n", 'bold'))
    for step in steps_to_run:
        success = run_step(step[0])
        if not success:
            print(c(f"\n[!] Pipeline stopped at: {step[0]}", 'red'))
            return


# ─────────────────────────────────────────────────────────────
# RESET
# ─────────────────────────────────────────────────────────────

def reset_step(step_name):
    conn = sqlite3.connect(DB_NAME)
    resets = {
        "crawl": [
            "UPDATE Pages SET is_scraped=0, scraped_at=NULL, "
            "meta_title=NULL, meta_description=NULL, canonical_url=NULL, "
            "scraped_h1=NULL, scraped_text=NULL, word_count=NULL, "
            "has_schema=NULL, schema_types=NULL, h_tag_structure=NULL, "
            "internal_links_count=NULL, external_links_count=NULL, "
            "images_missing_alt=NULL, page_load_ms=NULL, page_size_bytes=NULL"
        ],
        "audit": [
            "UPDATE Pages SET is_audited=0, audited_at=NULL, "
            "tech_issues=NULL, tech_severity=NULL, tech_score=NULL, "
            "title_length=NULL, meta_desc_length=NULL, h1_count=NULL, "
            "has_canonical=NULL, is_thin_content=NULL, "
            "is_duplicate_title=NULL, is_duplicate_meta=NULL, is_orphan_page=NULL"
        ],
        "priority": [
            "UPDATE Pages SET priority_score=NULL, priority_tier=NULL, priority_reasons=NULL"
        ],
        "analyse": [
            "UPDATE Pages SET is_analyzed=0, analyzed_at=NULL, "
            "llm_eeat_score=NULL, llm_intent=NULL, llm_intent_match=NULL, "
            "llm_recommendation=NULL, llm_quick_wins=NULL, llm_model_used=NULL, "
            "is_deep_analyzed=0, deep_analyzed_at=NULL, "
            "llm_content_score=NULL, llm_title_rewrite=NULL, llm_meta_rewrite=NULL, "
            "llm_content_gaps=NULL, llm_schema_suggestions=NULL, "
            "llm_geo_score=NULL, llm_geo_improvements=NULL"
        ],
        "compete": [
            "DELETE FROM Competitor_Pages",
            "DELETE FROM Competitor_Analysis",
        ],
    }

    if step_name not in resets:
        print(c(f"[!] Cannot reset step: {step_name}", 'red'))
        print(f"    Resettable steps: {', '.join(resets.keys())}")
        conn.close()
        return

    print(c(f"\n  Resetting: {step_name}", 'yellow'))
    for sql in resets[step_name]:
        try:
            conn.execute(sql)
            print(f"  ✓ {sql[:60]}...")
        except Exception as e:
            print(c(f"  [!] {e}", 'red'))

    conn.commit()
    conn.close()
    print(c(f"  Reset complete. Run: python seo.py run --step {step_name}\n", 'green'))


# ─────────────────────────────────────────────────────────────
# INSPECT
# ─────────────────────────────────────────────────────────────

def inspect_url(url):
    if not url.startswith('http'):
        url = f"https://bodycraftacademy.com/{url.lstrip('/')}"

    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM Pages WHERE url = ?", (url,)).fetchone()
    conn.close()

    if not row:
        # Try partial match
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM Pages WHERE url LIKE ?", (f'%{url}%',)
        ).fetchone()
        conn.close()

    if not row:
        print(c(f"[!] URL not found: {url}", 'red'))
        return

    p = dict(row)
    print(f"\n{c('='*60, 'bold')}")
    print(c(f"  PAGE INSPECTOR", 'bold'))
    print(f"{c('='*60, 'bold')}\n")
    print(f"  URL       : {p['url']}")
    print(f"  Sessions  : {p.get('ga4_sessions', 0)}")
    print(f"  Status    : {p.get('status_code', '?')}")
    print(f"\n{c('  TECHNICAL', 'bold')}")
    print(f"  Tech Score    : {p.get('tech_score', '?')}/100")
    print(f"  Severity      : {p.get('tech_severity', '?')}")
    print(f"  Word Count    : {p.get('word_count', 0)}")
    print(f"  Load Time     : {p.get('page_load_ms', 0)}ms")
    print(f"  Has Schema    : {bool(p.get('has_schema'))}")
    print(f"  Schema Types  : {p.get('schema_types', '[]')}")
    print(f"\n{c('  META', 'bold')}")
    print(f"  Title     : {p.get('meta_title', 'MISSING')}")
    print(f"  Meta Desc : {p.get('meta_description', 'MISSING')}")
    print(f"  H1        : {p.get('scraped_h1', 'MISSING')}")
    print(f"\n{c('  AI ANALYSIS', 'bold')}")
    print(f"  E-E-A-T       : {p.get('llm_eeat_score', '—')}/10")
    print(f"  Intent        : {p.get('llm_intent', '—')}")
    print(f"  Intent Match  : {p.get('llm_intent_match', '—')}")
    print(f"  Recommendation: {p.get('llm_recommendation', '—')}")

    quick_wins = p.get('llm_quick_wins')
    if quick_wins:
        try:
            wins = json.loads(quick_wins)
            print(f"  Quick Wins:")
            for w in wins:
                print(f"    → {w}")
        except Exception:
            pass

    if p.get('is_deep_analyzed'):
        print(f"\n{c('  DEEP ANALYSIS (Tier 3)', 'bold')}")
        print(f"  Content Score : {p.get('llm_content_score', '—')}/10")
        print(f"  GEO Score     : {p.get('llm_geo_score', '—')}/10")
        print(f"  Title Rewrite : {p.get('llm_title_rewrite', '—')}")
        print(f"  Meta Rewrite  : {p.get('llm_meta_rewrite', '—')}")

        gaps = p.get('llm_content_gaps')
        if gaps:
            try:
                print(f"  Content Gaps  :")
                for g in json.loads(gaps):
                    print(f"    → {g}")
            except Exception:
                pass

    print(f"\n{c('  PRIORITY', 'bold')}")
    print(f"  Score  : {p.get('priority_score', '—')}")
    print(f"  Tier   : {p.get('priority_tier', '—')}")

    issues = p.get('tech_issues')
    if issues:
        try:
            issue_list = json.loads(issues)
            print(f"\n{c('  TECH ISSUES', 'bold')}")
            for issue in issue_list:
                icon = {'critical': '🔴', 'warning': '🟡', 'info': '🔵'}.get(
                    issue['severity'], '⚪')
                print(f"  {icon} [{issue['severity'].upper()}] {issue['code']}")
                print(f"     {issue['message']}")
        except Exception:
            pass

    print(f"\n{c('='*60, 'bold')}\n")


# ─────────────────────────────────────────────────────────────
# QUICK WINS
# ─────────────────────────────────────────────────────────────

def show_quick_wins():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT url, ga4_sessions, tech_score, llm_quick_wins,
               llm_recommendation, meta_description, tech_issues
        FROM Pages
        WHERE is_scraped = 1
          AND priority_tier != 'excluded'
          AND status_code = 200
        ORDER BY ga4_sessions DESC
        LIMIT 30
    """).fetchall()
    conn.close()

    print(f"\n{c('='*60, 'bold')}")
    print(c("  ⚡ TOP QUICK WINS", 'bold'))
    print(f"{c('='*60, 'bold')}\n")

    wins = []
    for r in rows:
        url   = r['url'].replace('https://bodycraftacademy.com', '')
        sess  = r['ga4_sessions'] or 0
        ai_wins = json.loads(r['llm_quick_wins'] or '[]')
        issues  = json.loads(r['tech_issues'] or '[]')

        if not (r['meta_description'] or '').strip():
            wins.append((sess * 2, f"`{url}` — Write meta description ({sess} sessions)"))
        for i in issues:
            if i['severity'] == 'critical' and sess > 0:
                wins.append((sess * 3, f"`{url}` — Fix {i['code']} ({sess} sessions)"))
        for w in ai_wins[:1]:
            if sess > 10:
                wins.append((sess, f"`{url}` — {w}"))

    wins.sort(reverse=True)
    for rank, (score, win) in enumerate(wins[:15], 1):
        print(f"  {rank:2}. {win}")
    print()


# ─────────────────────────────────────────────────────────────
# CRITICAL PAGES
# ─────────────────────────────────────────────────────────────

def show_critical():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT url, ga4_sessions, tech_score, tech_issues, llm_recommendation
        FROM Pages
        WHERE tech_severity = 'critical'
        ORDER BY ga4_sessions DESC
    """).fetchall()
    conn.close()

    print(f"\n{c('='*60, 'bold')}")
    print(c("  🔴 CRITICAL PAGES", 'bold'))
    print(f"{c('='*60, 'bold')}\n")

    for r in rows:
        url    = r['url'].replace('https://bodycraftacademy.com', '')
        sess   = r['ga4_sessions'] or 0
        score  = r['tech_score'] or 0
        issues = json.loads(r['tech_issues'] or '[]')
        crits  = [i for i in issues if i['severity'] == 'critical']

        print(f"  {c(url, 'bold')}")
        print(f"    Score: {score}/100 | Sessions: {sess}")
        for i in crits:
            print(f"    🔴 {i['code']} — {i['message']}")
        if r['llm_recommendation']:
            print(f"    💡 {r['llm_recommendation']}")
        print()


# ─────────────────────────────────────────────────────────────
# MAIN CLI
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog='seo',
        description='SEO Master Pipeline CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python seo.py                          # show dashboard
  python seo.py status                   # show dashboard
  python seo.py run --all                # run full pipeline
  python seo.py run --step crawl         # run single step
  python seo.py run --from audit         # run from a step onward
  python seo.py reset --step analyse     # reset AI analysis
  python seo.py inspect --url /kormangala
  python seo.py quick-wins
  python seo.py critical

Available steps:
  db, migrate, crawl, audit, priority, analyse, report, compete
        """
    )

    subparsers = parser.add_subparsers(dest='command')

    # status
    subparsers.add_parser('status', help='Show pipeline dashboard')

    # run
    run_parser = subparsers.add_parser('run', help='Run pipeline steps')
    run_group  = run_parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument('--all',  action='store_true', help='Run full pipeline')
    run_group.add_argument('--step', type=str, help='Run single step')
    run_group.add_argument('--from', dest='from_step', type=str,
                           help='Run from this step onward')

    # reset
    reset_parser = subparsers.add_parser('reset', help='Reset pipeline step data')
    reset_parser.add_argument('--step', required=True, type=str)

    # inspect
    inspect_parser = subparsers.add_parser('inspect', help='Inspect a single URL')
    inspect_parser.add_argument('--url', required=True, type=str)

    # quick-wins
    subparsers.add_parser('quick-wins', help='Show top quick wins')

    # critical
    subparsers.add_parser('critical', help='Show critical pages')

    args = parser.parse_args()

    if args.command is None or args.command == 'status':
        show_dashboard()
    elif args.command == 'run':
        if args.all:
            run_all()
        elif args.step:
            run_step(args.step)
        elif args.from_step:
            run_from(args.from_step)
    elif args.command == 'reset':
        reset_step(args.step)
    elif args.command == 'inspect':
        inspect_url(args.url)
    elif args.command == 'quick-wins':
        show_quick_wins()
    elif args.command == 'critical':
        show_critical()
    else:
        show_dashboard()


if __name__ == "__main__":
    main()

