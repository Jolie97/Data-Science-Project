import json
import os
from flask import Flask, render_template

app = Flask(__name__)

# Locate project directory structure dynamically
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # path to dashboard/
PROTOTYPE_DIR = os.path.join(
    BASE_DIR, "..", "notebooks", "Prototype"
)  # path to notebooks/Prototype/


def load_json(filename):
    """Safely load JSON data from the notebooks/Prototype directory."""
    file_path = os.path.join(PROTOTYPE_DIR, filename)
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return None
    return None


@app.route("/")
def index():
    # Load data from the specified path
    file1_data = load_json("results_file1.json")
    file2_data = load_json("results_file2.json")

    # Fallback values if files are missing or unreadable
    if file1_data is None:
        file1_data = {"Bullish": 0, "Bearish": 0, "Neutral": 0}

    if file2_data is None:
        file2_data = {"POSITIVE": 0, "NEGATIVE": 0}

    return render_template(
        "index.html", file1_data=file1_data, file2_data=file2_data
    )


if __name__ == "__main__":
    app.run(debug=True)