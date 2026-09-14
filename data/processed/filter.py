import sqlite3
import os

# --------------------------------------------------
# DATABASE FILES
# --------------------------------------------------

input_db = r"C:\Users\felij\Downloads\Capstone\extracted_comment_claims.db"
output_db = r"C:\Users\felij\Downloads\Capstone\final.db"

# --------------------------------------------------
# CONNECT TO ORIGINAL DATABASE
# --------------------------------------------------

conn = sqlite3.connect(input_db)

print("Tables in original database:")

tables = conn.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type='table'
""").fetchall()

for table in tables:
    print(" -", table[0])

# --------------------------------------------------
# CHECK TABLE
# --------------------------------------------------

if ("extracted_claims",) not in tables:
    print("\nERROR: extracted_claims table was not found.")
    conn.close()
    exit()

# --------------------------------------------------
# SHOW COLUMNS
# --------------------------------------------------

columns = conn.execute("""
    PRAGMA table_info(extracted_claims)
""").fetchall()

print("\nColumns:")

for column in columns:
    print(" -", column[1])

# --------------------------------------------------
# CREATE NEW DATABASE
# --------------------------------------------------

if os.path.exists(output_db):
    os.remove(output_db)

new_conn = sqlite3.connect(output_db)

# Attach original database
new_conn.execute(
    "ATTACH DATABASE ? AS original",
    (input_db,)
)

# --------------------------------------------------
# CREATE CLEAN TABLE
# --------------------------------------------------

new_conn.execute("""
    CREATE TABLE extracted_claims AS
    SELECT *
    FROM original.extracted_claims
    WHERE extracted_main_part IS NOT NULL
      AND TRIM(extracted_main_part) != ''
      AND TRIM(extracted_main_part) != 'Text is not about cryptocurrency trends.'
      AND TRIM(extracted_main_part) != 'Text is not about cryptocurrency trends'
""")

new_conn.commit()

# --------------------------------------------------
# COUNT ROWS
# --------------------------------------------------

original_count = conn.execute("""
    SELECT COUNT(*)
    FROM extracted_claims
""").fetchone()[0]

new_count = new_conn.execute("""
    SELECT COUNT(*)
    FROM extracted_claims
""").fetchone()[0]

removed = original_count - new_count

print("\n========================================")
print("FILTERING COMPLETE")
print("========================================")
print("Original rows:", original_count)
print("Rows removed:", removed)
print("Rows remaining:", new_count)
print("New database:", output_db)

# --------------------------------------------------
# CLOSE
# --------------------------------------------------

new_conn.close()
conn.close()