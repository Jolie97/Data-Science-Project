# Data-Science-Project

## Setting up the Scraper

1. cd scraper/reddhog
2. pip install -r requirements.txt && pip install -e .
3. patchright install --force chrome
4. reddhog warmup # Manual step: solve CAPTCHA in browser window, then press ENTER
5. reddhog subreddit CryptoCurrency 50 --export csv

Current workflow:

- json scraper (runs everyday, pulling neww posts + refreshing old posts) uses reddhog an open-source reddit json scraper
- processes the data into an SQLite database to hold both posts + comments database (claim verification + sentiment analysis models respectively)

## 🗄️ Data Pipeline & Scraping

This project uses a custom ETL (Extract, Transform, Load) pipeline to collect Reddit data without official API keys, handle rate limits automatically, and store the output in a relational SQLite database for efficient NLP processing.

### Architecture

1. **Extract:** [ReddHog](https://github.com/c4pi/reddhog) fetches live JSON data from cryptocurrency subreddits, falling back to a headless browser (Patchright) when rate-limited.
2. **Transform & Load:** A custom Python script (`load_to_sqlite.py`) flattens the nested JSON output and performs an UPSERT (update-or-insert) into our SQLite database, deduplicating posts and preserving hierarchical comment threads.

### Setup Instructions for Team Members

1. **Install Dependencies:**
   Ensure you are in your virtual environment, then run:

   ```bash
   pip install -r requirements.txt
   ```

   _(Note: This automatically installs the local scraper module and all required data science libraries)._

   ```bash
   nbstripout --install
   ```

2. **Install the Browser Fallback:**

   ```bash
   patchright install --force chrome
   ```

3. **Get the Database:**
   Do **not** commit the `.db` file to GitHub. The data engineer will upload the latest `reddit_data.db` to the team's shared cloud drive weekly. Download it and place it in:
   `CryptoTruth/data/processed/reddit_data.db`

### How to Run the Scraper (Data Engineers Only)

If you are generating the data, run these commands from the root of the project to collect new data and update existing scores:

```bash
# 1. Warm up the browser profile (only needed once after install)
reddhog warmup

# 2. Collect 50 new posts from the subreddit
reddhog subreddit CryptoCurrency 50

# 3. Refresh upvote/comment counts on all previously scraped posts
reddhog refresh cryptocurrency

# 4. Upsert the raw JSON into the SQLite database
python src/scraper/load_to_sqlite.py
```
