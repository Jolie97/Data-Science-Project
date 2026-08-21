import os
import logging
from flask import Flask, render_template, request, jsonify
import pandas as pd
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

def analyze_text_sentiment(text: str) -> dict:
    """
    Computes sentiment scores using VADER.
    Returns compound score along with discrete label.
    """
    if not text:
        return {"compound": 0.0, "label": "NEUTRAL"}
    
    scores = sia.polarity_scores(text)
    compound = scores['compound']
    
    if compound >= 0.05:
        label = "POSITIVE"
    elif compound <= -0.05:
        label = "NEGATIVE"
    else:
        label = "NEUTRAL"
        
    return {
        "compound": compound,
        "pos": scores['pos'],
        "neu": scores['neu'],
        "neg": scores['neg'],
        "label": label
    }

@app.route('/')
def home():
    """Renders the main dashboard UI."""
    return render_template('index.html')  # Ensures compatibility with existing UI template

@app.route('/api/sentiment', methods=['POST'])
def sentiment_endpoint():
    """API endpoint for single text sentiment evaluation."""
    data = request.get_json(silent=True) or {}
    text = data.get('text', '')
    result = analyze_text_sentiment(text)
    return jsonify(result)

@app.route('/api/batch-sentiment', methods=['POST'])
def batch_sentiment_endpoint():
    """API endpoint for processing lists or uploaded CSV data for charts."""
    data = request.get_json(silent=True) or {}
    texts = data.get('texts', [])
    
    results = [analyze_text_sentiment(t) for t in texts]
    return jsonify({"results": results})

if __name__ == '__main__':
    # Bind to Render's dynamic PORT environment variable (default to 10000 locally)
    port = int(os.environ.get('PORT', 10000))
    logger.info(f"Starting server on host 0.0.0.0 and port {port}...")
    app.run(host='0.0.0.0', port=port)