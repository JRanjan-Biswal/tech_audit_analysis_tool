# Topic
This is a tech audit tool, that can be run using, I used Qwen3.5-27b-Q4_KM for this, which worked phenomenally, The model was run using ollam serve. 
Provides indepth analysis

Several tools have been integrated to get the desired result. 

for e.g. BeautifulSoap - for scraping

Note: this tool is still in development, but it is highly capable. 

## Dashboard — see pipeline status at a glance
python seo.py

## Run everything from scratch
python seo.py run --all

## Run a single step
python seo.py run --step crawl
python seo.py run --step analyse
python seo.py run --step report

## Run from a specific step onward (e.g. after fixing something)
python seo.py run --from audit

## Reset a step's data so it re-runs cleanly
python seo.py reset --step crawl
python seo.py reset --step analyse

## Deep dive on any single page
python seo.py inspect --url /kormangala
python seo.py inspect --url /makeup-artist-course-bangalore

## Actionable views
python seo.py quick-wins
python seo.py critical


