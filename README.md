# Data-Science-Project

## Setting up the Scraper

1. cd scraper/reddhog
2. pip install -r requirements.txt && pip install -e .
3. patchright install --force chrome
4. reddhog warmup # Manual step: solve CAPTCHA in browser window, then press ENTER
5. reddhog subreddit CryptoCurrency 50 --export csv
