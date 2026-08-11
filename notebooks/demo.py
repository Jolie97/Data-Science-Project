import sqlite3
import pandas as pd
from IPython.display import display

# --- 1. PANDAS DISPLAY SETTINGS ---
# Tell Pandas never to truncate the width of a column
pd.set_option("display.max_colwidth", None)
# (Optional) Tell Pandas to show all rows without truncating the middle
pd.set_option("display.max_rows", 50)

# --- 2. CONNECT AND QUERY ---
# Connect to the database we just built
conn = sqlite3.connect("../data/processed/cryptocurrency-110826.db")

query1 = """
SELECT 
    p.title AS claim_or_post,
    p.upvotes AS post_upvotes,
    c.text AS top_comment,
    c.score AS comment_score
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id
WHERE c.text IS NOT NULL AND c.text != '[deleted]'
ORDER BY p.timestamp DESC, c.score DESC
LIMIT 15;
"""
df = pd.read_sql(query1, conn)

# --- 3. MAKE IT LOOK PROFESSIONAL ---
# Apply CSS styling to the dataframe
# so long comments wrap nicely to the next line and align left.
styled_df = df.style.set_properties(
    **{
        "text-align": "left",
        "white-space": "pre-wrap",  # This forces the text to wrap like normal paragraphs
    }
).set_table_styles(
    [
        dict(
            selector="th", props=[("text-align", "left")]
        )  # Left-align the column headers too
    ]
)

# Display the editted version
display(styled_df)
