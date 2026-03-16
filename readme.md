# Topic
This is a tech audit tool, that can be run using, I used Qwen3.5-27b-Q4_KM for this, which worked phenomenally, The model was run using ollam serve. 
Provides indepth analysis

Several tools have been integrated to get the desired result. 

for e.g. BeautifulSoap - for scraping

Note: this tool is still in development, but it is highly capable. 

## Requirements
1. Run a local llm (serve a local llm)
2. Add the name of the model, served link etc on config.py
3. Add the siteurl of which audit is required, rest nothing else to touch
4. Add ga4.csv file (this can be downloaded from "analytics.google.com" in data_uploads folder (optional)

### Future prospects
1. making it simple to use by creating an user interface
2. easy selection of model, using a toggle to select local or cloud based model
3. adding more tools, eg. serp
4. auto fetching of ga4 and gsc data

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


