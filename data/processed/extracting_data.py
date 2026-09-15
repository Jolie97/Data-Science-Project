import sqlite3
import pandas as pd
import torch
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

INPUT_DATABASE = "reddit-data-180826.db"
OUTPUT_DATABASE = "extracted_comment_claims.db"
MODEL_REPO = "bpavlsh/bart-crypto-summary"

# Database batch size
BATCH_SIZE = 100

# Number of comments sent to the model at once
MODEL_BATCH_SIZE = 4

device = "cuda" if torch.cuda.is_available() else "cpu"

print("=" * 60)
print("REDDIT COMMENT MAIN CLAIM EXTRACTION")
print("=" * 60)
print(f"Device: {device}")
print(f"Database batch size: {BATCH_SIZE}")
print(f"Model batch size: {MODEL_BATCH_SIZE}")
print("=" * 60)


# --------------------------------------------------
# LOAD MODEL
# --------------------------------------------------

print("\nLoading tokenizer...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_REPO)

print("Loading model...")
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_REPO)
model = model.to(device)
model.eval()

print("Model successfully loaded!")


# --------------------------------------------------
# CONNECT TO DATABASES
# --------------------------------------------------

conn = sqlite3.connect(INPUT_DATABASE)
output_conn = sqlite3.connect(OUTPUT_DATABASE)


# --------------------------------------------------
# CREATE OUTPUT TABLE
# --------------------------------------------------

output_cursor = output_conn.cursor()

output_cursor.execute("""
    CREATE TABLE IF NOT EXISTS extracted_claims (
        ID TEXT PRIMARY KEY,
        POST_ID TEXT,
        AUTHOR TEXT,
        ORIGINAL_TEXT TEXT,
        EXTRACTED_MAIN_PART TEXT
    )
""")

output_conn.commit()


# --------------------------------------------------
# CHECK WHAT HAS ALREADY BEEN PROCESSED
# --------------------------------------------------

processed_df = pd.read_sql(
    "SELECT ID FROM extracted_claims",
    output_conn
)

processed_ids = set(
    processed_df["ID"].astype(str)
)

print(f"\nAlready handled: {len(processed_ids)} comments")


# --------------------------------------------------
# LOAD ALL COMMENTS
# --------------------------------------------------

all_comments = pd.read_sql(
    """
    SELECT
        ID,
        POST_ID,
        AUTHOR,
        TEXT
    FROM COMMENTS
    """,
    conn
)

all_comments["ID"] = all_comments["ID"].astype(str)

total_comments = len(all_comments)

print(f"Total comments available: {total_comments}")


# --------------------------------------------------
# REMOVE COMMENTS ALREADY PROCESSED
# --------------------------------------------------

remaining_comments = all_comments[
    ~all_comments["ID"].isin(processed_ids)
].copy()

print(
    f"Comments remaining to process: "
    f"{len(remaining_comments)}"
)


# --------------------------------------------------
# PROCESS IN BATCHES OF 100
# --------------------------------------------------

batch_number = 1

while len(remaining_comments) > 0:

    print("\n" + "=" * 60)
    print(f"BATCH {batch_number}")
    print("=" * 60)

    # Take the next 100 comments
    df = remaining_comments.iloc[:BATCH_SIZE].copy()

    # Remove these from the remaining list
    remaining_comments = remaining_comments.iloc[len(df):].copy()

    print(f"Loaded {len(df)} comments")


    # --------------------------------------------------
    # CLEAN TEXT
    # --------------------------------------------------

    df["TEXT"] = (
        df["TEXT"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    df["AUTHOR"] = (
        df["AUTHOR"]
        .fillna("")
        .astype(str)
    )

    df["POST_ID"] = (
        df["POST_ID"]
        .fillna("")
        .astype(str)
    )


    batch_results = []


    # --------------------------------------------------
    # PROCESS MODEL IN SMALLER GROUPS
    # --------------------------------------------------

    for start in range(0, len(df), MODEL_BATCH_SIZE):

        end = min(
            start + MODEL_BATCH_SIZE,
            len(df)
        )

        model_batch = df.iloc[start:end]

        valid_rows = []
        short_rows = []


        # Separate usable comments from empty/very short ones
        for idx, row in model_batch.iterrows():

            raw_text = row["TEXT"]

            if (
                len(raw_text.strip()) < 10
                or raw_text.lower() == "null"
            ):
                short_rows.append((idx, row))
            else:
                valid_rows.append((idx, row))


        # --------------------------------------------------
        # SAVE SHORT COMMENTS WITHOUT LOSING THEM
        # --------------------------------------------------

        for idx, row in short_rows:

            batch_results.append({
                "ID": str(row["ID"]),
                "POST_ID": row["POST_ID"],
                "AUTHOR": row["AUTHOR"],
                "ORIGINAL_TEXT": row["TEXT"],
                "EXTRACTED_MAIN_PART": ""
            })


        if not valid_rows:
            continue


        # --------------------------------------------------
        # SEND COMMENTS TO MODEL
        # --------------------------------------------------

        texts = [
            row["TEXT"]
            for idx, row in valid_rows
        ]

        try:

            inputs = tokenizer(
                texts,
                max_length=1024,
                truncation=True,
                padding=True,
                return_tensors="pt"
            )

            inputs = {
                key: value.to(device)
                for key, value in inputs.items()
            }


            with torch.inference_mode():

                summary_ids = model.generate(
                    input_ids=inputs["input_ids"],
                    attention_mask=inputs["attention_mask"],
                    max_length=64,
                    early_stopping=True
                )


            summaries = tokenizer.batch_decode(
                summary_ids,
                skip_special_tokens=True
            )


            # --------------------------------------------------
            # SAVE MODEL RESULTS
            # --------------------------------------------------

            for (idx, row), summary in zip(
                valid_rows,
                summaries
            ):

                batch_results.append({
                    "ID": str(row["ID"]),
                    "POST_ID": row["POST_ID"],
                    "AUTHOR": row["AUTHOR"],
                    "ORIGINAL_TEXT": row["TEXT"],
                    "EXTRACTED_MAIN_PART": summary
                })


            print(
                f"  Model processed "
                f"{end}/{len(df)} comments"
            )


        except Exception as e:

            print(
                f"  ERROR in model batch "
                f"{start}-{end}: {e}"
            )

            # Still save the comments so they are not lost
            for idx, row in valid_rows:

                batch_results.append({
                    "ID": str(row["ID"]),
                    "POST_ID": row["POST_ID"],
                    "AUTHOR": row["AUTHOR"],
                    "ORIGINAL_TEXT": row["TEXT"],
                    "EXTRACTED_MAIN_PART": ""
                })


    # --------------------------------------------------
    # SAVE THIS 100-COMMENT BATCH
    # --------------------------------------------------

    if batch_results:

        results_df = pd.DataFrame(batch_results)

        results_df.to_sql(
            "extracted_claims",
            output_conn,
            if_exists="append",
            index=False
        )

        output_conn.commit()

        processed_ids.update(
            results_df["ID"].astype(str)
        )

        print(
            f"\nSaved {len(results_df)} comments"
        )


    # --------------------------------------------------
    # SHOW OVERALL PROGRESS
    # --------------------------------------------------

    processed_count = len(processed_ids)

    percentage = (
        processed_count / total_comments * 100
    )

    print(
        f"Overall progress: "
        f"{processed_count}/{total_comments} "
        f"({percentage:.2f}%)"
    )

    batch_number += 1


# --------------------------------------------------
# FINISH
# --------------------------------------------------

conn.close()
output_conn.close()

print("\n" + "=" * 60)
print("EXTRACTION COMPLETE!")
print("=" * 60)

print(
    f"Handled: "
    f"{len(processed_ids)}/{total_comments} comments"
)

print(
    f"Output database: "
    f"{OUTPUT_DATABASE}"
)
