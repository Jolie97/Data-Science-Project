import os
import json
import logging
from flask import Flask, render_template, jsonify

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def load_results_data():
    """Reads pre-computed sentiment data directly from results.json."""
    # Check current directory and parent directory for results.json
    possible_paths = [
        os.path.join(os.path.dirname(__file__), 'results.json'),
        os.path.join(os.path.dirname(__file__), '..', 'results.json'),
        'results.json'
    ]
    
    file_path = None
    for path in possible_paths:
        if os.path.exists(path):
            file_path = path
            break

    if file_path:
        try:
            with open(file_path, 'r') as f:
                counts = json.load(f)
                logger.info(f"Successfully loaded results from {file_path}")
                return counts
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")

    # Fallback default values if results.json is missing or unreadable
    logger.warning("results.json not found. Returning default fallback structure.")
    return {"Bullish": 0, "Bearish": 0, "Neutral": 0}

@app.route('/')
def home():
    """Renders the dashboard web interface."""
    return render_template('index.html')

@app.route('/api/news-sentiment', methods=['GET'])
def get_news_sentiment():
    """API endpoint serving pre-calculated JSON output to the UI."""
    counts = load_results_data()
    total = sum(counts.values())
    
    return jsonify({
        "counts": counts,
        "total_fetched": total
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    logger.info(f"Starting server on port {port}...")
    app.run(host='0.0.0.0', port=port)