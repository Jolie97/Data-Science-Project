"""
Reads posts from SQLite, runs sentiment & verification scoring,
and exports JSON aggregates for the Flask dashboard.
"""

import json
import logging
import os
import sqlite3
import sys
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# Setup Paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "processed" / "reddit-data-300926.db"
PROTOTYPE_DIR = PROJECT_ROOT / "notebooks" / "Prototype"

# Add src to system path to import your sentiment and verifier modules
sys.path.append(str(PROJECT_ROOT / "src"))
from sentiment.analyser import SentimentEngine
from verifier.claim_verifier import ClaimVerifier
from verifier.decomposer import decompose_post


def main():
    if not DB_PATH.exists():
        logging.error(
            "SQLite database not found at %s. Run scraper & loader first.", DB_PATH
        )
        return

    PROTOTYPE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # 1. Fetch posts and comments from SQLite
    posts_df = pd.read_sql(
        """
        SELECT ID, TITLE, DESCRIPTION, (TITLE || ' ' || COALESCE(DESCRIPTION, '')) AS full_text
        FROM posts
        WHERE (FLAIR IS NULL OR FLAIR NOT IN ('Comedy', 'Meme'))
    """,
        conn,
    )

    if posts_df.empty:
        logging.warning("No posts found in database.")
        return

    logging.info("Scoring %d posts from SQLite...", len(posts_df))

    # --- PART 1: SENTIMENT ANALYSIS (results_file1.json) ---
    sentiment_engine = SentimentEngine(load_finbert=False)  # Fast VADER lexicon run

    bullish_count = 0
    bearish_count = 0
    neutral_count = 0

    for text in posts_df["full_text"]:
        score = sentiment_engine.score_vader(text)["compound"]
        if score >= 0.05:
            bullish_count += 1
        elif score <= -0.05:
            bearish_count += 1
        else:
            neutral_count += 1

    file1_payload = {
        "Bullish": bullish_count,
        "Bearish": bearish_count,
        "Neutral": neutral_count,
    }

    file1_out = PROTOTYPE_DIR / "results_file1.json"
    with open(file1_out, "w", encoding="utf-8") as f:
        json.dump(file1_payload, f, indent=2)
    logging.info("Saved Prototype 1 metrics to %s: %s", file1_out, file1_payload)

    # --- PART 2: CLAIM VERIFICATION (results_file2.json) ---
    logging.info("Running Claim Verification engine...")
    verifier = ClaimVerifier()

    supported_count = 0
    refuted_count = 0

    # Extract claims and verify against ChromaDB
    for _, row in posts_df.iterrows():
        claims = decompose_post(row["ID"], row["TITLE"], row["DESCRIPTION"])
        for c in claims:
            verdict = verifier.verify_claim(c["claim_text"])
            if verdict["veracity"] == "SUPPORTED":
                supported_count += 1
            elif verdict["veracity"] == "REFUTED":
                refuted_count += 1

    # Map SUPPORTED -> SUPPORTED, REFUTED -> REFUTED to match index.html
    file2_payload = {"SUPPORTED": supported_count, "REFUTED": refuted_count}

    file2_out = PROTOTYPE_DIR / "results_file2.json"
    with open(file2_out, "w", encoding="utf-8") as f:
        json.dump(file2_payload, f, indent=2)
    logging.info("Saved Prototype 2 metrics to %s: %s", file2_out, file2_payload)

    conn.close()
    logging.info("Batch scoring complete!")


if __name__ == "__main__":
    main()
