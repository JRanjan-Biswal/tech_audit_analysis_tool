# Dashboard — see pipeline status at a glance
python seo.py

# Run everything from scratch
python seo.py run --all

# Run a single step
python seo.py run --step crawl
python seo.py run --step analyse
python seo.py run --step report

# Run from a specific step onward (e.g. after fixing something)
python seo.py run --from audit

# Reset a step's data so it re-runs cleanly
python seo.py reset --step crawl
python seo.py reset --step analyse

# Deep dive on any single page
python seo.py inspect --url /kormangala
python seo.py inspect --url /makeup-artist-course-bangalore

# Actionable views
python seo.py quick-wins
python seo.py critical
```

**The dashboard shows** pipeline completion status for all 8 steps, site health scores, page counts by tier, last run timestamps per phase, and the report file size.

**Your complete pipeline is now:**
```
✅ Sprint 1  — DB schema
✅ Sprint 2  — Enhanced crawler
✅ Sprint 3  — Technical auditor
✅ Sprint 4  — Priority engine
✅ Sprint 5  — AI analyser (9b + 27b)
✅ Sprint 6  — Reporting engine
✅ Sprint 8  — Competitor scraper
✅ Sprint 9  — CLI unifier
🔲 Sprint 7  — GSC loader (when you have the export)

