import os
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify
import requests
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask App
app = Flask(__name__)

# Download lightweight VADER lexicon for sentiment analysis
logger.info("Initializing VADER Sentiment Analyzer...")
nltk.download('vader_lexicon', quiet=True)
sia = SentimentIntensityAnalyzer()

# Retrieve NewsAPI key from environment variable (falls back to provided key)
API_KEY = os.environ.get("NEWS_API_KEY", "b2a0d07341544104b699c5576a1bf7fd")

def fetch_and_analyze_news():
    """Fetches articles using 3-day date chunks and analyzes sentiment dynamically."""
    url = "https://newsapi.org/v2/everything"
    query = "finance AND (stock market OR investment)"
    language = "en"
    page_size = 100  # Max per request

    # Date range setup: last 30 days
    end_date = datetime.today()
    start_date = end_date - timedelta(days=30)

    all_articles = []
    delta = timedelta(days=3)
    current_start = start_date
    stop_all = False

    while current_start < end_date and not stop_all:
        current_end = min(current_start + delta, end_date)
        page = 1

        while True:
            params = {
                "q": query,
                "language": language,
                "pageSize": page_size,
                "page": page,
                "from": current_start.strftime("%Y-%m-%d"),
                "to": current_end.strftime("%Y-%m-%d"),
                "apiKey": API_KEY,
            }

            try:
                response = requests.get(url, params=params, timeout=10)
                data = response.json()
            except Exception as e:
                logger.error(f"Network error during NewsAPI request: {e}")
                stop_all = True
                break

            # Check for API errors (e.g., rate limits, developer plan date restrictions)
            if data.get("status") != "ok":
                logger.warning(
                    f"API notice on {current_start.date()} to {current_end.date()}, "
                    f"page {page}: {data.get('code')} - {data.get('message')}"
                )
                stop_all = True
                break

            articles = data.get("articles", [])
            if articles:
                all_articles.extend(articles)
                if len(articles) < page_size:
                    break  # No more pages for this date range
                page += 1
            else:
                break  # No articles for this page/date range

        current_start += delta

    # Sentiment processing using VADER (Bullish / Bearish / Neutral mapping)
    sentiment_counts = {"Bullish": 0, "Bearish": 0, "Neutral": 0}

    for article in all_articles:
        title = article.get("title") or ""
        description = article.get("description") or ""
        text = f"{title} {description}".strip()

        if not text:
            continue

        # Evaluate text compound score using VADER
        scores = sia.polarity_scores(text[:512])
        compound = scores['compound']

        # Map polarity scores to sentiment categories
        if compound >= 0.05:
            sentiment_counts["Bullish"] += 1
        elif compound <= -0.05:
            sentiment_counts["Bearish"] += 1
        else:
            sentiment_counts["Neutral"] += 1

    return {
        "counts": sentiment_counts,
        "total_fetched": len(all_articles)
    }

@app.route('/')
def home():
    """Renders the main web dashboard UI."""
    return render_template('index.html')

@app.route('/api/news-sentiment', methods=['GET'])
def get_news_sentiment():
    """API Endpoint returning live calculated sentiment metrics to the frontend."""
    results = fetch_and_analyze_news()
    return jsonify(results)

if __name__ == '__main__':
    # Bind to Render's dynamic PORT environment variable (default to 10000 locally)
    port = int(os.environ.get('PORT', 10000))
    logger.info(f"Starting server on host 0.0.0.0 and port {port}...")
    app.run(host='0.0.0.0', port=port)