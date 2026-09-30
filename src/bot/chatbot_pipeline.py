"""
Chatbot pipeline:
plain text in -> sentiment + claim verification out.

The chatbot supports two types of claim verification:

1. Numerical cryptocurrency price claims
   -> Verified directly against CoinGecko historical price data.

2. Other claims
   -> Verified using the existing ChromaDB + NLI ClaimVerifier.

Prerequisite for normal/textual claim verification:
    python src/verifier/populate_evidence.py

CoinGecko API key:
    Add COINGECKO_API_KEY to your .env file.
"""

import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Add src/ to Python path
# ---------------------------------------------------------------------------

sys.path.append(str(Path(__file__).resolve().parent.parent))


# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------

import html
import logging
import os
import re
import time
from datetime import datetime
from typing import Dict, Optional


# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------

import pandas as pd
import requests
from dotenv import load_dotenv

from sentiment.analyser import SentimentEngine


# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------

load_dotenv()

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

COINGECKO_HEADERS = {
    "accept": "application/json",
    "x-cg-demo-api-key": COINGECKO_API_KEY,
}


# ---------------------------------------------------------------------------
# Quiet down noisy third-party loggers
# ---------------------------------------------------------------------------

for noisy_logger in (
    "httpx",
    "huggingface_hub",
    "urllib3",
    "filelock",
    "chromadb",
):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Lazy-loaded models
# ---------------------------------------------------------------------------

_engine: Optional[SentimentEngine] = None
_verifier = None


# ---------------------------------------------------------------------------
# Verification tuning
# ---------------------------------------------------------------------------

# Textual claim verification
TOP_K = 5
RELEVANCE_MAX_DISTANCE = 0.6
VERDICT_THRESHOLD = 0.7

# Numerical claim verification
# A 0.1% tolerance allows for small differences caused by
# rounding/API precision.
NUMERICAL_TOLERANCE = 0.001


# ===========================================================================
# SENTIMENT ENGINE
# ===========================================================================

def get_engine() -> SentimentEngine:
    """
    Lazily initialise the sentiment engine so importing this module
    stays cheap.
    """
    global _engine

    if _engine is None:
        _engine = SentimentEngine(load_finbert=True)

    return _engine


# ===========================================================================
# TEXTUAL CLAIM VERIFIER
# ===========================================================================

def get_verifier():
    """
    Lazily load the teammate's ClaimVerifier.

    This reuses the existing:
        - ChromaDB evidence database
        - NLI model
    """
    global _verifier

    if _verifier is None:
        from verifier.claim_verifier import ClaimVerifier

        _verifier = ClaimVerifier()

    return _verifier


def _clean(text: str) -> str:
    """
    Strip HTML tags/entities left over from RSS summaries.
    """
    text = re.sub(r"<[^>]+>", " ", text or "")

    return re.sub(
        r"\s+",
        " ",
        html.unescape(text)
    ).strip()


def verify_claim(claim: str) -> Dict:
    """
    Verify a normal/textual claim using:

        ChromaDB -> relevant evidence -> NLI

    Returns:
        SUPPORTED
        REFUTED
        NOT ENOUGH INFO
    """

    v = get_verifier()

    res = v.collection.query(
        query_texts=[claim],
        n_results=TOP_K,
        include=[
            "documents",
            "metadatas",
            "distances",
        ],
    )

    docs = res["documents"][0]
    metas = res["metadatas"][0]
    dists = res["distances"][0]

    # -----------------------------------------------------------------------
    # 1. Keep only evidence that is sufficiently relevant
    # -----------------------------------------------------------------------

    evidence = [
        (_clean(d), m, dist)
        for d, m, dist in zip(docs, metas, dists)
        if dist <= RELEVANCE_MAX_DISTANCE
    ]

    if not evidence:
        return {
            "veracity": "NOT ENOUGH INFO",
            "confidence": 0.0,
            "evidence": None,
            "source": None,
            "note": "No relevant evidence found.",
        }

    # -----------------------------------------------------------------------
    # 2. Run NLI
    #
    # premise    = evidence
    # hypothesis = user's claim
    # -----------------------------------------------------------------------

    pairs = [
        {
            "text": e[:1500],
            "text_pair": claim,
        }
        for e, _, _ in evidence
    ]

    outs = v.nli(
        pairs,
        top_k=None,
        truncation=True,
    )

    # Handle a single result returned as a dictionary
    if outs and isinstance(outs[0], dict):
        outs = [outs]

    scored = []

    for (text, meta, dist), out in zip(evidence, outs):

        probs = {
            o["label"].lower(): float(o["score"])
            for o in out
        }

        scored.append(
            {
                "entailment": probs.get(
                    "entailment",
                    0.0
                ),
                "contradiction": probs.get(
                    "contradiction",
                    0.0
                ),
                "text": text,
                "meta": meta,
                "distance": round(dist, 3),
            }
        )

    # -----------------------------------------------------------------------
    # 3. Find strongest supporting/refuting evidence
    # -----------------------------------------------------------------------

    best_sup = max(
        scored,
        key=lambda s: s["entailment"]
    )

    best_ref = max(
        scored,
        key=lambda s: s["contradiction"]
    )

    # -----------------------------------------------------------------------
    # 4. Decide verdict
    # -----------------------------------------------------------------------

    if (
        best_sup["entailment"] >= VERDICT_THRESHOLD
        and
        best_sup["entailment"] >= best_ref["contradiction"]
    ):

        veracity = "SUPPORTED"
        best = best_sup
        conf = best_sup["entailment"]

    elif best_ref["contradiction"] >= VERDICT_THRESHOLD:

        veracity = "REFUTED"
        best = best_ref
        conf = best_ref["contradiction"]

    else:

        veracity = "NOT ENOUGH INFO"

        best = max(
            scored,
            key=lambda s: max(
                s["entailment"],
                s["contradiction"],
            ),
        )

        conf = 0.0

    return {
        "veracity": veracity,
        "confidence": round(conf, 3),
        "evidence": best["text"][:200] + "...",
        "source": best["meta"].get(
            "source",
            "Unknown"
        ),
        "url": best["meta"].get("url"),
        "distance": best["distance"],
    }


# ===========================================================================
# COINGECKO
# ===========================================================================

def _coingecko_get(
    endpoint: str,
    params: Optional[dict] = None,
) -> dict:
    """
    Make a GET request to the CoinGecko API.

    Includes basic handling for HTTP 429 rate limiting.
    """

    url = f"{COINGECKO_BASE_URL}/{endpoint}"

    response = requests.get(
        url,
        headers=COINGECKO_HEADERS,
        params=params,
        timeout=15,
    )

    # ---------------------------------------------------------------
    # Rate limit handling
    # ---------------------------------------------------------------

    if response.status_code == 429:

        print(
            "CoinGecko rate limit hit - "
            "waiting 60 seconds..."
        )

        time.sleep(60)

        response = requests.get(
            url,
            headers=COINGECKO_HEADERS,
            params=params,
            timeout=15,
        )

    response.raise_for_status()

    return response.json()


def get_historical_prices_range(
    coin_id: str,
    start_date: str,
    end_date: str,
    vs_currency: str = "usd",
) -> pd.DataFrame:
    """
    Fetch historical cryptocurrency price observations between
    two dates.

    Parameters
    ----------
    coin_id:
        CoinGecko coin ID, e.g. "bitcoin"

    start_date:
        YYYY-MM-DD

    end_date:
        YYYY-MM-DD

    vs_currency:
        Default = USD

    Returns
    -------
    DataFrame containing:

        datetime
        price
    """

    start = pd.Timestamp(
        start_date,
        tz="UTC",
    )

    end = pd.Timestamp(
        end_date,
        tz="UTC",
    )

    start_unix = int(
        start.timestamp()
    )

    end_unix = int(
        end.timestamp()
    )

    params = {
        "vs_currency": vs_currency,
        "from": start_unix,
        "to": end_unix,
    }

    data = _coingecko_get(
        f"coins/{coin_id}/market_chart/range",
        params,
    )

    prices = data.get(
        "prices",
        [],
    )

    df = pd.DataFrame(
        prices,
        columns=[
            "timestamp_ms",
            "price",
        ],
    )

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(
        df["timestamp_ms"],
        unit="ms",
        utc=True,
    )

    return df[
        [
            "datetime",
            "price",
        ]
    ]


# ===========================================================================
# NUMERICAL CLAIM PARSER
# ===========================================================================

def parse_price_claim(
    text: str,
) -> Optional[Dict]:
    """
    Detect historical cryptocurrency price claims.

    Example supported claim:

        Bitcoin's minimum (closing) price for the year 2026
        was $58,558.86 on 30-Jun-2026.

    Returns
    -------
    Dictionary containing:

        coin
        statistic
        year
        claimed_price
        claimed_date

    Returns None if the text does not look like a supported
    historical price claim.
    """

    pattern = re.compile(
        r"""
        (?P<coin>[A-Za-z0-9]+)

        .*?

        (?P<stat>
            minimum|
            maximum|
            lowest|
            highest
        )

        \s*

        (?:\([^)]*\))?

        \s*
        price

        .*?

        (?:year\s+)?

        (?P<year>20\d{2})

        .*?

        (?:was|is)

        \s*

        \$?
        (?P<price>[\d,]+(?:\.\d+)?)

        .*?

        (?:on\s+)?

        (?P<date>
            \d{1,2}
            -
            [A-Za-z]{3}
            -
            20\d{2}
        )
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    match = pattern.search(text)

    if not match:
        return None

    # -----------------------------------------------------------------------
    # Extract values
    # -----------------------------------------------------------------------

    coin = match.group(
        "coin"
    ).lower()

    stat = match.group(
        "stat"
    ).lower()

    if stat in (
        "minimum",
        "lowest",
    ):
        statistic = "minimum"

    else:
        statistic = "maximum"

    price = float(
        match.group("price")
        .replace(",", "")
    )

    date_text = match.group(
        "date"
    )

    try:
        claimed_date = datetime.strptime(
            date_text,
            "%d-%b-%Y",
        ).date()

    except ValueError:
        return None

    year = int(
        match.group("year")
    )

    return {
        "coin": coin,
        "statistic": statistic,
        "year": year,
        "claimed_price": price,
        "claimed_date": claimed_date,
    }


# ===========================================================================
# NUMERICAL CLAIM VERIFICATION
# ===========================================================================

def verify_numerical_claim(
    claim: Dict,
) -> Dict:
    """
    Verify a historical cryptocurrency price claim using CoinGecko.

    The process is:

        1. Request historical data for the claimed year.
        2. Convert raw observations into daily values.
        3. Treat the final observation for each UTC day as the
           daily closing price.
        4. Find the minimum or maximum.
        5. Compare it against the user's claim.
    """

    coin = claim["coin"]
    year = claim["year"]
    statistic = claim["statistic"]

    claimed_price = claim[
        "claimed_price"
    ]

    claimed_date = claim[
        "claimed_date"
    ]

    # -----------------------------------------------------------------------
    # Request the complete calendar year
    # -----------------------------------------------------------------------

    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"

    try:

        df = get_historical_prices_range(
            coin_id=coin,
            start_date=start_date,
            end_date=end_date,
            vs_currency="usd",
        )

    except requests.exceptions.HTTPError as exc:

        return {
            "veracity": "NOT ENOUGH INFO",
            "confidence": 0.0,
            "source": "CoinGecko",
            "error": str(exc),
            "note": (
                f"CoinGecko could not retrieve "
                f"data for {coin}."
            ),
        }

    except Exception as exc:

        return {
            "veracity": "NOT ENOUGH INFO",
            "confidence": 0.0,
            "source": "CoinGecko",
            "error": str(exc),
            "note": (
                "An error occurred while "
                "retrieving CoinGecko data."
            ),
        }

    # -----------------------------------------------------------------------
    # Check whether data was returned
    # -----------------------------------------------------------------------

    if df.empty:

        return {
            "veracity": "NOT ENOUGH INFO",
            "confidence": 0.0,
            "source": "CoinGecko",
            "note": (
                f"No historical price data "
                f"was found for {coin} in {year}."
            ),
        }

    # -----------------------------------------------------------------------
    # Keep only the requested year
    # -----------------------------------------------------------------------

    df["date"] = df[
        "datetime"
    ].dt.date

    year_data = df[
        df["datetime"].dt.year == year
    ].copy()

    if year_data.empty:

        return {
            "veracity": "NOT ENOUGH INFO",
            "confidence": 0.0,
            "source": "CoinGecko",
            "note": (
                f"No price data was found "
                f"for {coin} during {year}."
            ),
        }

    # -----------------------------------------------------------------------
    # Convert observations to daily closing prices
    #
    # The final CoinGecko observation for each UTC calendar day
    # is treated as that day's closing price.
    # -----------------------------------------------------------------------

    daily = (
        year_data
        .sort_values("datetime")
        .groupby("date")
        .last()
        .reset_index()
    )

    if daily.empty:

        return {
            "veracity": "NOT ENOUGH INFO",
            "confidence": 0.0,
            "source": "CoinGecko",
            "note": "Could not calculate daily prices.",
        }

    # -----------------------------------------------------------------------
    # Find actual minimum or maximum
    # -----------------------------------------------------------------------

    if statistic == "minimum":

        actual_row = daily.loc[
            daily["price"].idxmin()
        ]

    else:

        actual_row = daily.loc[
            daily["price"].idxmax()
        ]

    actual_price = float(
        actual_row["price"]
    )

    actual_date = actual_row[
        "date"
    ]

    # -----------------------------------------------------------------------
    # Compare claimed and actual values
    # -----------------------------------------------------------------------

    absolute_difference = abs(
        actual_price - claimed_price
    )

    relative_difference = (
        absolute_difference / actual_price
        if actual_price != 0
        else float("inf")
    )

    price_matches = (
        relative_difference
        <= NUMERICAL_TOLERANCE
    )

    date_matches = (
        actual_date == claimed_date
    )

    # -----------------------------------------------------------------------
    # Determine verdict
    # -----------------------------------------------------------------------

    if price_matches and date_matches:

        veracity = "SUPPORTED"

        confidence = 0.99

    elif price_matches:

        veracity = "PARTIALLY SUPPORTED"

        confidence = 0.85

    elif date_matches:

        veracity = "PARTIALLY SUPPORTED"

        confidence = 0.85

    else:

        veracity = "REFUTED"

        confidence = 0.99

    # -----------------------------------------------------------------------
    # Return detailed result
    # -----------------------------------------------------------------------

    return {
        "veracity": veracity,
        "confidence": confidence,

        "claimed_value": round(
            claimed_price,
            2,
        ),

        "actual_value": round(
            actual_price,
            2,
        ),

        "claimed_date": str(
            claimed_date
        ),

        "actual_date": str(
            actual_date
        ),

        "source": "CoinGecko",

        "url": "https://www.coingecko.com/",

        "statistic": statistic,

        "year": year,

        "price_difference": round(
            absolute_difference,
            2,
        ),

        "price_difference_percent": round(
            relative_difference * 100,
            4,
        ),

        "note": (
            f"CoinGecko {statistic} daily "
            f"closing price for {coin} in {year}."
        ),
    }


# ===========================================================================
# MAIN ANALYSIS PIPELINE
# ===========================================================================

def analyse_text(
    text: str,
) -> Dict:
    """
    Run one piece of text through:

        1. VADER sentiment
        2. FinBERT sentiment
        3. Claim verification

    Numerical cryptocurrency price claims are automatically
    sent to CoinGecko.

    Other claims are sent to the existing ChromaDB + NLI
    verifier.
    """

    engine = get_engine()

    # -----------------------------------------------------------------------
    # Check whether this is a numerical crypto price claim
    # -----------------------------------------------------------------------

    numerical_claim = parse_price_claim(
        text
    )

    # -----------------------------------------------------------------------
    # Select appropriate verification method
    # -----------------------------------------------------------------------

    if numerical_claim:

        verification = verify_numerical_claim(
            numerical_claim
        )

    else:

        verification = verify_claim(
            text
        )

    # -----------------------------------------------------------------------
    # Return complete analysis
    # -----------------------------------------------------------------------

    return {
        "text": text,

        "vader": engine.score_vader(
            text
        ),

        "finbert": engine.score_finbert_batch(
            [text]
        )[0],

        "verification": verification,
    }


# ===========================================================================
# TEST / COMMAND-LINE ENTRY POINT
# ===========================================================================

if __name__ == "__main__":

    import json

    test_claim = (
        "Bitcoin's minimum (closing) price for the year 2026 was $58,558.86 on 30-Jun-2026."
    )

    result = analyse_text(
        test_claim
    )

    print(
        json.dumps(
            result,
            indent=2,
            default=str,
        )
    )