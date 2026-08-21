import os
import threading
import time
import logging
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify, render_template
from transformers import pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

NEWSAPI_KEY = "b2a0d07341544104b699c5576a1bf7fd"
NEWS_QUERY = "finance AND (stock market OR investment)"
NEWS_LANGUAGE = "en"
PAGE_SIZE = 100
DAYS_BACK = 30
CHUNK_DAYS = 3

REFRESH_INTERVAL_SECONDS = 5 * 60  # re-analyze every 5 minutes
NEUTRAL_THRESHOLD = 0.6

# --------------------------------------------------------------------------
# Shared state
# --------------------------------------------------------------------------

_lock = threading.Lock()
_state = {
    "counts": {"Bullish": 0, "Bearish": 0, "Neutral": 0},
    "total_articles": 0,
    "sample_headlines": [],
    "updated_at": None,
    "status": "starting",   # starting | ok | error
    "error_message": None,
}

# --------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------

log.info("Loading sentiment model...")
model = pipeline("sentiment-analysis")
log.info("Model loaded.")

SCORE_MAP = {"POSITIVE": "Bullish", "NEGATIVE": "Bearish"}


def fetch_news():
    """Fetch recent finance news from NewsAPI, handling developer tier limits gracefully."""
    if not NEWSAPI_KEY:
        raise RuntimeError("NEWSAPI_KEY environment variable is not set.")

    url = "https://newsapi.org/v2/everything"
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=DAYS_BACK)
    delta = timedelta(days=CHUNK_DAYS)

    all_articles = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + delta, end_date)
        params = {
            "q": NEWS_QUERY,
            "language": NEWS_LANGUAGE,
            "pageSize": PAGE_SIZE,
            "page": 1,
            "from": current_start.strftime("%Y-%m-%d"),
            "to": current_end.strftime("%Y-%m-%d"),
            "apiKey": NEWSAPI_KEY,
        }
        response = requests.get(url, params=params, timeout=15)
        data = response.json()

        if data.get("status") != "ok":
            # Gracefully handle the NewsAPI free tier 100-article limit
            if data.get("code") == "maximumResultsReached":
                break
            raise RuntimeError(f"NewsAPI error: {data.get('code')} - {data.get('message')}")

        articles = data.get("articles", [])
        if articles:
            all_articles.extend(articles)
            if len(all_articles) >= 100:
                break

        current_start += delta

    return all_articles[:100]


def analyze():
    """Fetch news, run sentiment analysis, and update the shared state."""
    try:
        articles = fetch_news()

        news_list = []
        headlines = []
        for article in articles:
            title = article.get("title") or ""
            description = article.get("description") or ""
            text = f"{title} {description}".strip()
            if text:
                news_list.append(text)
                headlines.append(title)

        if not news_list:
            raise RuntimeError("No articles with usable text were fetched.")

        results = model(news_list, truncation=True, max_length=512)

        counts = {"Bullish": 0, "Bearish": 0, "Neutral": 0}
        for r in results:
            if r["score"] < NEUTRAL_THRESHOLD:
                counts["Neutral"] += 1
            else:
                counts[SCORE_MAP[r["label"]]] += 1

        with _lock:
            _state["counts"] = counts
            _state["total_articles"] = len(news_list)
            _state["sample_headlines"] = headlines[:8]
            # Use timezone-aware UTC datetime formatted cleanly to ISO 8601
            _state["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            _state["status"] = "ok"
            _state["error_message"] = None

        log.info("Analysis complete: %s (%d articles)", counts, len(news_list))

    except Exception as exc:
        log.exception("Analysis failed")
        with _lock:
            _state["status"] = "error"
            _state["error_message"] = str(exc)


def background_loop():
    while True:
        analyze()
        time.sleep(REFRESH_INTERVAL_SECONDS)


# --------------------------------------------------------------------------
# Flask app
# --------------------------------------------------------------------------

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/sentiment")
def get_sentiment():
    with _lock:
        return jsonify(dict(_state))


@app.route("/api/refresh", methods=["POST"])
def refresh_now():
    analyze()
    with _lock:
        return jsonify(dict(_state))


if __name__ == "__main__":
    threading.Thread(target=background_loop, daemon=True).start()
    app.run(debug=True, port=5000, use_reloader=False)