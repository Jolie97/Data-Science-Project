import sqlite3
import json
import logging
from pathlib import Path

# Set up simple logging so we can see what's happening
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# --- PATH SETUP ---
# Find the exact folder where this Python script lives (src/scraper)
SCRIPT_DIR = Path(__file__).resolve().parent

# Define paths relative to the script
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DB_DIR = PROJECT_ROOT / "data" / "processed"
DB_PATH = DB_DIR / "reddit_data.db"
SCHEMA_PATH = SCRIPT_DIR / "schema.sql"
REDDHOG_DATA_DIR = SCRIPT_DIR / "reddhog" / "data"


def setup_database(conn):
    """Reads schema.sql and creates the tables if they don't exist."""
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    conn.executescript(schema_sql)
    conn.commit()
    logging.info("Database schema initialized successfully.")


def process_subreddit_file(conn, json_file_path, subreddit_name):
    """Reads a single data.json file and upserts its posts and comments."""
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    posts_inserted = 0
    comments_inserted = 0
    cursor = conn.cursor()

    for post in data:
        # 1. UPSERT THE POST
        cursor.execute(
            """
            INSERT INTO posts (id, subreddit, title, flair, description, url, upvotes, comments_count, author, timestamp, crawled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                upvotes = excluded.upvotes,
                comments_count = excluded.comments_count,
                crawled_at = excluded.crawled_at;
        """,
            (
                post.get("id"),
                subreddit_name,
                post.get("title"),
                post.get("flair"),
                post.get("description"),
                post.get("url"),
                post.get("upvotes", 0),
                post.get("comments_count", 0),
                post.get("author"),
                post.get("timestamp"),
                post.get("crawled_at"),
            ),
        )
        posts_inserted += 1

        # 2. UPSERT THE COMMENTS
        comments = post.get("comments", [])
        for comment in comments:
            # Safely cast score to int (Reddit API sometimes returns it as a string like "3")
            score_str = comment.get("score", 0)
            score = int(score_str) if str(score_str).lstrip("-").isdigit() else 0

            cursor.execute(
                """
                INSERT INTO comments (id, post_id, parent_id, author, score, text, depth, crawled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    score = excluded.score,
                    crawled_at = excluded.crawled_at;
            """,
                (
                    comment.get("id"),
                    post.get("id"),  # Link comment to its parent post
                    comment.get(
                        "parent_id"
                    ),  # Link to parent comment (if it's a reply)
                    comment.get("author"),
                    score,
                    comment.get("text"),
                    comment.get("depth", 0),
                    post.get("crawled_at"),  # Inherit crawl time from the post
                ),
            )
            comments_inserted += 1

    # Save changes for this file
    conn.commit()
    logging.info(
        f"Processed r/{subreddit_name}: Upserted {posts_inserted} posts and {comments_inserted} comments."
    )


def main():
    # 1. Ensure the output directory exists (data/processed/)
    DB_DIR.mkdir(parents=True, exist_ok=True)

    # 2. Connect to SQLite (this automatically creates the .db file if missing)
    conn = sqlite3.connect(DB_PATH)

    try:
        # 3. Create tables
        setup_database(conn)

        # 4. Find all data.json files in the reddhog data folders
        if not REDDHOG_DATA_DIR.exists():
            logging.warning(
                f"ReddHog data directory not found at {REDDHOG_DATA_DIR}. Have you run the scraper yet?"
            )
            return

        json_files = list(REDDHOG_DATA_DIR.glob("*/data.json"))
        if not json_files:
            logging.info("No data.json files found to process.")
            return

        # 5. Process each file
        for json_file in json_files:
            # The folder name is the subreddit (e.g., 'cryptocurrency')
            subreddit_name = json_file.parent.name
            process_subreddit_file(conn, json_file, subreddit_name)

        logging.info(f"Success! Database updated at: {DB_PATH}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
