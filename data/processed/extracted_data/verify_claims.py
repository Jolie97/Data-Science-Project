import pandas as pd
import yfinance as yf
import sqlite3
from datetime import datetime, timedelta


# ============================================================
# DATABASE PATHS
# ============================================================

claims_db = r"C:\Users\felij\Downloads\Capstone\final.db"

reddit_db = r"C:\Users\felij\Downloads\Capstone\reddit-data-180826.db"

output_csv = r"C:\Users\felij\Downloads\Capstone\final_verified_claims_report.csv"

output_db = r"C:\Users\felij\Downloads\Capstone\final_verified_claims.db"


# ============================================================
# 1. LOAD FILTERED CLAIMS
# ============================================================

print("Loading filtered claims...")

conn = sqlite3.connect(claims_db)

df_claims = pd.read_sql("""
    SELECT *
    FROM extracted_claims
""", conn)

conn.close()

print("Filtered claims loaded:", len(df_claims))


# ============================================================
# 2. GET TIMESTAMP FROM ORIGINAL REDDIT DATABASE
# ============================================================

print("\nLoading post timestamps...")

conn = sqlite3.connect(reddit_db)

df_timestamps = pd.read_sql("""
    SELECT ID AS POST_ID, TIMESTAMP
    FROM POSTS
""", conn)

conn.close()

print("Post timestamps loaded:", len(df_timestamps))


# ============================================================
# 3. MATCH TIMESTAMP TO CLAIMS USING POST_ID
# ============================================================

print("\nMatching timestamps to claims...")

df_claims = df_claims.merge(
    df_timestamps,
    on="POST_ID",
    how="left"
)

missing_timestamps = df_claims["TIMESTAMP"].isna().sum()

print("Claims after matching:", len(df_claims))
print("Missing timestamps:", missing_timestamps)


# ============================================================
# 4. CRYPTOCURRENCY TICKER MAP
# ============================================================

TICKER_MAP = {
    "bitcoin": "BTC-USD",
    "ethereum": "ETH-USD",
    "solana": "SOL-USD",
    "binance coin": "BNB-USD",
    "tether": "USDT-USD"
}


# ============================================================
# 5. VERIFY EACH CLAIM
# ============================================================

def verify_claim(row):

    claim_text = str(
        row["EXTRACTED_MAIN_PART"]
    ).lower()

    timestamp_raw = str(
        row["TIMESTAMP"]
    )


    # --------------------------------------------------------
    # Parse timestamp
    # --------------------------------------------------------

    try:

        date_str = timestamp_raw.split("T")[0]

        post_date = datetime.strptime(
            date_str,
            "%Y-%m-%d"
        )

    except Exception:

        return "SKIPPED: Invalid Timestamp Date format"


    # --------------------------------------------------------
    # Find cryptocurrency
    # --------------------------------------------------------

    target_ticker = None
    target_coin_name = None

    for coin_name, ticker in TICKER_MAP.items():

        if coin_name in claim_text:

            target_ticker = ticker
            target_coin_name = coin_name

            break


    if not target_ticker:

        return (
            "SKIPPED: No monitored crypto coin "
            "detected in claim text"
        )


    # --------------------------------------------------------
    # Determine claimed direction
    # --------------------------------------------------------

    claimed_direction = None

    if "uptrend" in claim_text:

        claimed_direction = "UP"

    elif "downtrend" in claim_text:

        claimed_direction = "DOWN"


    if not claimed_direction:

        return (
            "SKIPPED: Factual trend direction "
            "not clear in text"
        )


    # --------------------------------------------------------
    # Get Yahoo Finance market data
    # --------------------------------------------------------

    try:

        start_date = post_date.strftime(
            "%Y-%m-%d"
        )

        end_date = (
            post_date + timedelta(days=1)
        ).strftime("%Y-%m-%d")


        ticker_data = yf.Ticker(
            target_ticker
        )

        historical_df = ticker_data.history(
            start=start_date,
            end=end_date
        )


        if historical_df.empty:

            return (
                "SKIPPED: Market API data "
                "missing for this date"
            )


        # ----------------------------------------------------
        # Calculate daily price return
        # ----------------------------------------------------

        open_price = (
            historical_df["Open"].iloc[0]
        )

        close_price = (
            historical_df["Close"].iloc[0]
        )

        daily_return = (
            (close_price - open_price)
            / open_price
        ) * 100


        # ----------------------------------------------------
        # Determine actual market direction
        # ----------------------------------------------------

        if daily_return > 0.5:

            real_market_direction = "UP"

        elif daily_return < -0.5:

            real_market_direction = "DOWN"

        else:

            real_market_direction = "FLAT"


        # ----------------------------------------------------
        # Compare claim with actual market direction
        # ----------------------------------------------------

        if claimed_direction == real_market_direction:

            return (
                f"VERIFIED TRUTH "
                f"({target_coin_name.upper()} was indeed "
                f"{real_market_direction} by "
                f"{daily_return:.2f}%)"
            )

        else:

            return (
                f"MISINFORMATION DETECTED "
                f"({target_coin_name.upper()} text claimed "
                f"{claimed_direction}, but market was actually "
                f"{real_market_direction} by "
                f"{daily_return:.2f}%)"
            )


    except Exception as e:

        return (
            f"ERROR: Market check failed ({str(e)})"
        )


# ============================================================
# 6. RUN VERIFICATION
# ============================================================

print("\n========================================")
print("STARTING MARKET VERIFICATION")
print("========================================")

print("\nContacting Yahoo Finance...")

df_claims["VERIFICATION_VERDICT"] = (
    df_claims.apply(
        verify_claim,
        axis=1
    )
)


# ============================================================
# 7. SAVE AS CSV
# ============================================================

print("\nSaving CSV...")

df_claims.to_csv(
    output_csv,
    index=False
)

print("CSV saved:")
print(output_csv)


# ============================================================
# 8. SAVE AS NEW SQLITE DATABASE
# ============================================================

print("\nSaving database...")

db_conn = sqlite3.connect(output_db)

df_claims.to_sql(
    "verified_claims",
    db_conn,
    if_exists="replace",
    index=False
)

db_conn.close()

print("Database saved:")
print(output_db)


# ============================================================
# 9. SUMMARY
# ============================================================

verified_count = (
    df_claims["VERIFICATION_VERDICT"]
    .str.startswith("VERIFIED TRUTH")
    .sum()
)

misinformation_count = (
    df_claims["VERIFICATION_VERDICT"]
    .str.startswith("MISINFORMATION")
    .sum()
)

skipped_count = (
    df_claims["VERIFICATION_VERDICT"]
    .str.startswith("SKIPPED")
    .sum()
)

error_count = (
    df_claims["VERIFICATION_VERDICT"]
    .str.startswith("ERROR")
    .sum()
)


print("\n========================================")
print("FACT CHECKING COMPLETE")
print("========================================")

print("Total claims:", len(df_claims))
print("Verified truth:", verified_count)
print("Misinformation:", misinformation_count)
print("Skipped:", skipped_count)
print("Errors:", error_count)

print("\nCSV:")
print(output_csv)

print("\nDatabase:")
print(output_db)


# ============================================================
# 10. SHOW SAMPLE RESULTS
# ============================================================

print("\n========================================")
print("SAMPLE RESULTS")
print("========================================")

print(
    df_claims[
        [
            "POST_ID",
            "TIMESTAMP",
            "EXTRACTED_MAIN_PART",
            "VERIFICATION_VERDICT"
        ]
    ]
    .head(20)
    .to_string(index=False)
)