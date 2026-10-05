import re
import sqlite3
import pandas as pd
import yfinance as yf
from pathlib import Path
import os
from overview_charts_new import export_overview
from dashboard_export_new import export_market_charts

# ============================================================
# CONFIG
# ============================================================
BASE = str(Path(os.environ.get("CRYPTOTRUTH_DATA_DIR", Path(__file__).resolve().parent)).resolve())
claims_db = str(Path(BASE) / "filtered_claims.db")
reddit_db = str(Path(BASE) / "batch2.db")

output_csv = str(Path(BASE) / "verified_claims.csv")
output_db = str(Path(BASE) / "verified_claims.db")

HORIZON_DAYS = 1        # trend/percent window: 1 = same day, 3 or 7 = wider
FLAT_THRESHOLD = 0.5    # % move under which the market counts as FLAT
ACCURATE_PCT = 2        # numeric claims: within 2% (or 2 percentage points)
APPROX_PCT = 10         # ... within 10%
MIN_CLAIMS_USER = 3     # users with fewer scored claims get "not enough data"

COINS = {
    "bitcoin":  {"ticker": "BTC-USD",  "aliases": ["bitcoin", "btc"]},
    "ethereum": {"ticker": "ETH-USD",  "aliases": ["ethereum", "eth", "ether"]},
    "solana":   {"ticker": "SOL-USD",  "aliases": ["solana", "sol"]},
    "bnb":      {"ticker": "BNB-USD",  "aliases": ["binance coin", "bnb"]},
    "tether":   {"ticker": "USDT-USD", "aliases": ["tether", "usdt"]},
}
GENERIC_ALIASES = ["crypto", "cryptocurrency", "altcoin", "alt rally", "the market"]

UP_WORDS = ["uptrend", "upward", "positive trend", "bullish", "rally", "surge", "pump",
            "moon", "go up", "going up", "goes up", "up", "higher", "rise", "rising",
            "increase", "inflows", "recover", "gain", "gained", "soared", "jumped"]
DOWN_WORDS = ["downtrend", "downward", "negative trend", "bearish", "crash", "crashed", "drop",
              "dropped", "dump", "decline", "go lower", "going lower", "goes lower", "lower",
              "fall", "falling", "fell", "down", "decrease", "outflows", "sell-off", "selloff",
              "plunge", "plunged", "tanked"]
EVENT_WORDS = ["halt", "hack", "hacked", "exploit", "ban", "banned", "sec", "lawsuit", "sued",
               "approve", "approved", "approval", "etf", "delist", "listed", "announce",
               "announced", "launch", "upgrade", "fork", "regulation", "bankrupt", "exchange",
               "withdrawals", "fix", "fixes"]
HYPO_WORDS = ["if", "could", "might", "may", "would", "will", "gonna", "going to", "should",
              "probably", "predict", "target", "by end of", "next"]


def has_word(text, word):
    return re.search(r"\b" + re.escape(word) + r"\b", text) is not None


def any_word(text, words):
    return any(has_word(text, w) for w in words)


# ============================================================
# 1. LOAD CLAIMS + POST/COMMENT METADATA
# ============================================================
conn = sqlite3.connect(claims_db)
df = pd.read_sql("SELECT * FROM extracted_claims", conn)
conn.close()
print("Claim columns:", df.columns.tolist())

conn = sqlite3.connect(reddit_db)
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print("Reddit DB tables:", tables)


def cols_of(table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


post_cols = cols_of("POSTS")
print("POSTS columns:", post_cols)
sub_col = next((c for c in post_cols if "subreddit" in c.lower()), None)
select = "ID AS POST_ID, TIMESTAMP AS POST_TS" + (f", {sub_col} AS SUBREDDIT" if sub_col else "")
posts = pd.read_sql(f"SELECT {select} FROM POSTS", conn)
if not sub_col:
    print("!! No subreddit column in POSTS - subreddit summaries will be skipped.")

# if a comments table has its own timestamps, use them (claims are comments, t1_...)
comment_ts = None
for t in tables:
    if t.upper() == "COMMENTS":
        cc = cols_of(t)
        if "ID" in cc and "TIMESTAMP" in cc:
            comment_ts = pd.read_sql(f"SELECT ID, TIMESTAMP AS COMMENT_TS FROM {t}", conn)
            print("Using comment timestamps from", t)
conn.close()

df = df.merge(posts, on="POST_ID", how="left")
if comment_ts is not None:
    df = df.merge(comment_ts, on="ID", how="left")
    df["TS_USED"] = df["COMMENT_TS"].fillna(df["POST_TS"])
else:
    df["TS_USED"] = df["POST_TS"]

df["POST_DATE"] = pd.to_datetime(df["TS_USED"].astype(str).str[:10], errors="coerce")
df["WEEK"] = df["POST_DATE"] - pd.to_timedelta(df["POST_DATE"].dt.weekday, unit="D")
if "SUBREDDIT" not in df.columns:
    df["SUBREDDIT"] = "unknown"
sent_col = next((c for c in df.columns if "sentiment" in c.lower()
                 and pd.api.types.is_numeric_dtype(df[c])), None)
print("Claims:", len(df), "| missing dates:", df["POST_DATE"].isna().sum(),
      "| sentiment column:", sent_col)

# ============================================================
# 2. MARKET DATA (downloaded once per coin)
# ============================================================
start = df["POST_DATE"].min() - pd.Timedelta(days=3)
end = df["POST_DATE"].max() + pd.Timedelta(days=max(HORIZON_DAYS, 7) + 3)
prices = {}
for name, info in COINS.items():
    h = yf.Ticker(info["ticker"]).history(start=start, end=end)
    if h.empty:
        print("No data for", info["ticker"])
        continue
    h.index = h.index.tz_localize(None).normalize()
    prices[name] = h
    print(f"{info['ticker']}: {len(h)} days")


def window(coin, date, days):
    h = prices.get(coin)
    if h is None or pd.isna(date):
        return None
    w = h.loc[date: date + pd.Timedelta(days=days - 1)]
    return None if w.empty else w


def ret_pct(w):
    return (w["Close"].iloc[-1] - w["Open"].iloc[0]) / w["Open"].iloc[0] * 100


def direction_of(r):
    return "UP" if r > FLAT_THRESHOLD else "DOWN" if r < -FLAT_THRESHOLD else "FLAT"


# ============================================================
# 3. TEXT PARSING
# ============================================================
NUM = r"(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)"
PRICE_A = re.compile(r"\$\s?" + NUM + r"\s?([km])?\b", re.I)
PRICE_B = re.compile(r"\b" + NUM + r"\s?(k)\b", re.I)
PCT = re.compile(NUM + r"\s?%")


def to_number(num, suffix):
    v = float(num.replace(",", ""))
    s = (suffix or "").lower()
    return v * 1_000 if s == "k" else v * 1_000_000 if s == "m" else v


def extract_prices(text):
    out = [to_number(n, s) for n, s in PRICE_A.findall(text)]
    out += [to_number(n, s) for n, s in PRICE_B.findall(text)]
    return out


def extract_pcts(text):
    return [float(n.replace(",", "")) for n in PCT.findall(text)]


def detect_coin(extracted, original):
    for text in (extracted, original):
        for name, info in COINS.items():
            if any_word(text, info["aliases"]):
                return name, False
    for text in (extracted, original):
        if any_word(text, GENERIC_ALIASES):
            return "bitcoin", True
    return None, False


def detect_direction(text):
    up, down = any_word(text, UP_WORDS), any_word(text, DOWN_WORDS)
    if up and not down:
        return "UP"
    if down and not up:
        return "DOWN"
    return "MIXED" if up and down else None


# ============================================================
# 4. CLASSIFY + VERIFY EACH CLAIM
# ============================================================
def verify(row):
    ext = str(row["EXTRACTED_MAIN_PART"]).lower()
    orig = str(row["ORIGINAL_TEXT"]).lower()
    out = dict(CLAIM_TYPE="OPINION_OTHER", COIN=None, PROXY=False, CLAIMED_DIR=None,
               CLAIMED_VALUE=None, ACTUAL_DIR=None, ACTUAL_VALUE=None, RETURN_PCT=None,
               STATUS="NOT_CHECKABLE", REASON="")

    coin, proxy = detect_coin(ext, orig)
    out.update(COIN=coin, PROXY=proxy)
    if pd.isna(row["POST_DATE"]):
        out["REASON"] = "No usable timestamp"
        return pd.Series(out)
    if not coin:
        out["CLAIM_TYPE"] = "EVENT_NEWS" if any_word(ext, EVENT_WORDS) else "OPINION_OTHER"
        out["REASON"] = "No crypto asset mentioned"
        return pd.Series(out)

    direction = detect_direction(ext) or detect_direction(orig)
    if direction == "MIXED":
        direction = None
    out["CLAIMED_DIR"] = direction
    hypo = any_word(ext, HYPO_WORDS)
    pcts, prices_found = extract_pcts(ext), extract_prices(ext)
    w = window(coin, row["POST_DATE"], HORIZON_DAYS)
    day = window(coin, row["POST_DATE"], 1)
    suffix = " (generic 'crypto' claim checked against BTC)" if proxy else ""

    # ---- numeric percent claim: "ETH up 10%"
    if pcts and direction:
        out["CLAIM_TYPE"] = "NUMERIC_PERCENT"
        if w is None:
            out.update(STATUS="NO_MARKET_DATA", REASON="No price data for that date")
            return pd.Series(out)
        claimed = pcts[0] if direction == "UP" else -pcts[0]
        actual = ret_pct(w)
        diff = abs(claimed - actual)
        status = ("ACCURATE" if diff <= ACCURATE_PCT else
                  "APPROXIMATE" if diff <= APPROX_PCT else "INACCURATE")
        out.update(STATUS=status, CLAIMED_VALUE=claimed, ACTUAL_VALUE=round(actual, 2),
                   RETURN_PCT=round(actual, 2), ACTUAL_DIR=direction_of(actual),
                   REASON=f"Claimed {claimed:+.1f}%, {coin} moved {actual:+.2f}% "
                          f"(off by {diff:.1f} points){suffix}")
        return pd.Series(out)

    # ---- numeric price claim: "BTC hit $120k"
    if prices_found and not hypo and day is not None:
        claimed = prices_found[0]
        lo, hi, close = day["Low"].iloc[0], day["High"].iloc[0], day["Close"].iloc[0]
        if 0.2 <= claimed / close <= 5:          # plausible price for this coin
            out["CLAIM_TYPE"] = "NUMERIC_PRICE"
            if lo <= claimed <= hi:
                diff = 0.0
            else:
                diff = abs(claimed - (lo if claimed < lo else hi)) / close * 100
            status = ("ACCURATE" if diff <= ACCURATE_PCT else
                      "APPROXIMATE" if diff <= APPROX_PCT else "INACCURATE")
            out.update(STATUS=status, CLAIMED_VALUE=claimed, ACTUAL_VALUE=round(close, 2),
                       REASON=f"Claimed ${claimed:,.0f}, {coin} traded ${lo:,.0f}-${hi:,.0f} "
                              f"that day (off by {diff:.1f}%){suffix}")
            return pd.Series(out)
    if prices_found and hypo:
        out.update(CLAIM_TYPE="PREDICTION",
                   REASON="Price prediction (future-tense) - can't be scored at posting time")
        return pd.Series(out)

    # ---- trend claim: "downtrend", "bullish", "could go lower"
    if direction:
        out["CLAIM_TYPE"] = "TREND"
        if w is None:
            out.update(STATUS="NO_MARKET_DATA", REASON="No price data for that date")
            return pd.Series(out)
        actual = ret_pct(w)
        real = direction_of(actual)
        out.update(ACTUAL_DIR=real, RETURN_PCT=round(actual, 2), ACTUAL_VALUE=round(actual, 2))
        if direction == real:
            out.update(STATUS="VERIFIED",
                       REASON=f"{coin} was {real} {actual:+.2f}% over {HORIZON_DAYS}d{suffix}")
        elif real == "FLAT":
            out.update(STATUS="INCONCLUSIVE",
                       REASON=f"Claimed {direction}, market flat ({actual:+.2f}%){suffix}")
        else:
            out.update(STATUS="CONTRADICTED",
                       REASON=f"Claimed {direction}, market {real} {actual:+.2f}%{suffix}")
        return pd.Series(out)

    # ---- everything else
    if any_word(ext, EVENT_WORDS):
        out.update(CLAIM_TYPE="EVENT_NEWS",
                   REASON=f"{coin} news/event - not checkable with price data")
    else:
        out["REASON"] = f"{coin} mentioned but no price direction or number"
    return pd.Series(out)


res = df.apply(verify, axis=1)
df = pd.concat([df, res], axis=1)

# unified verdict for the dashboard
VERDICT_MAP = {"ACCURATE": "ACCURATE", "VERIFIED": "ACCURATE",
               "APPROXIMATE": "APPROXIMATE",
               "INACCURATE": "INACCURATE", "CONTRADICTED": "INACCURATE"}
df["DASHBOARD_VERDICT"] = df["STATUS"].map(VERDICT_MAP).fillna("NOT_SCORED")
df["SCORED"] = df["DASHBOARD_VERDICT"] != "NOT_SCORED"

# ============================================================
# 5. MARKET CONTEXT FOR *EVERY* POST (scored or not)
# ============================================================
def context(row):
    coin = row["COIN"] if row["COIN"] in prices else "bitcoin"
    d = window(coin, row["POST_DATE"], 1)
    f = window(coin, row["POST_DATE"], 7)
    if d is None:
        return pd.Series(dict(MKT_COIN=coin, MKT_DAY_RETURN=None, MKT_RANGE_PCT=None,
                              MKT_FWD_7D_RETURN=None))
    return pd.Series(dict(
        MKT_COIN=coin,
        MKT_DAY_RETURN=round(ret_pct(d), 2),
        MKT_RANGE_PCT=round((d["High"].iloc[0] - d["Low"].iloc[0]) / d["Open"].iloc[0] * 100, 2),
        MKT_FWD_7D_RETURN=round(ret_pct(f), 2) if f is not None and len(f) > 1 else None))


df = pd.concat([df, df.apply(context, axis=1)], axis=1)

# ============================================================
# 6. SUMMARY TABLES FOR THE DASHBOARD
# ============================================================
def trust_summary(group_cols):
    scored = df[df["SCORED"]]
    g = scored.groupby(group_cols)["DASHBOARD_VERDICT"]
    t = pd.DataFrame({
        "SCORED_CLAIMS": g.size(),
        "ACCURATE": g.apply(lambda s: (s == "ACCURATE").sum()),
        "APPROXIMATE": g.apply(lambda s: (s == "APPROXIMATE").sum()),
        "INACCURATE": g.apply(lambda s: (s == "INACCURATE").sum()),
    })
    t["TRUST_SCORE"] = ((t["ACCURATE"] + 0.5 * t["APPROXIMATE"]) / t["SCORED_CLAIMS"] * 100).round(1)
    total = df.groupby(group_cols).size().rename("TOTAL_CLAIMS")
    return t.join(total, how="right").fillna(0).reset_index()


by_sub = trust_summary(["SUBREDDIT"])
by_week = trust_summary(["WEEK"]).merge(
    df.groupby("WEEK")["MKT_RANGE_PCT"].mean().round(2).rename("AVG_VOLATILITY_PCT").reset_index(),
    on="WEEK", how="left")
by_user = trust_summary(["AUTHOR"])
by_user["TRUST_LABEL"] = by_user.apply(
    lambda r: "NOT ENOUGH DATA" if r["SCORED_CLAIMS"] < MIN_CLAIMS_USER else
    "RELIABLE" if r["TRUST_SCORE"] >= 70 else "CAUTION" if r["TRUST_SCORE"] >= 40 else "SPREADER",
    axis=1)
by_type = df.groupby(["CLAIM_TYPE", "STATUS"]).size().rename("COUNT").reset_index()

if sent_col:
    by_day = df.groupby("POST_DATE").agg(
        AVG_SENTIMENT=(sent_col, "mean"),
        BTC_DAY_RETURN=("MKT_DAY_RETURN", "mean"),
        POSTS=("ID", "size")).round(3).reset_index()
else:
    by_day = None

# ============================================================
# 7. SAVE
# ============================================================
df.to_csv(output_csv, index=False)
db = sqlite3.connect(output_db)
df.to_sql("verified_claims", db, if_exists="replace", index=False)
by_sub.to_sql("summary_by_subreddit", db, if_exists="replace", index=False)
by_week.to_sql("summary_by_week", db, if_exists="replace", index=False)
by_user.to_sql("summary_by_user", db, if_exists="replace", index=False)
by_type.to_sql("summary_by_type", db, if_exists="replace", index=False)
if by_day is not None:
    by_day.to_sql("summary_by_day", db, if_exists="replace", index=False)
db.close()

print("\n=== CLAIM TYPES ===")
print(df["CLAIM_TYPE"].value_counts().to_string())
print("\n=== STATUS ===")
print(df["STATUS"].value_counts().to_string())
print(f"\nScored claims: {df['SCORED'].sum()} of {len(df)}")
print("\n=== BY SUBREDDIT ===\n", by_sub.to_string(index=False))
print("\n=== BY WEEK ===\n", by_week.to_string(index=False))

# ============================================================
# 8. JSON OUTPUT (replaces PNG rendering only)
# ============================================================
output_json = Path(__file__).resolve().parents[1] / 'notebooks/Prototype/dashboard_data_new.json'
export_overview(df, by_sub, by_user, output_json, Path(output_db).name)
export_market_charts(df, by_day, prices, output_json)
print("Done. CSV, database tables and dashboard JSON saved.")
