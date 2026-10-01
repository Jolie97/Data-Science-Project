"""Chatbot pipeline: plain text in -> sentiment + claim verification out.

Prerequisite: build the evidence database once with
    python src/verifier/populate_evidence.py
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))  # adds src/ to the path

import html
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Optional

from sentiment.analyser import SentimentEngine

# quiet down noisy third-party loggers
for noisy_logger in ("httpx", "huggingface_hub", "urllib3", "filelock", "chromadb"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

_engine: Optional[SentimentEngine] = None
_verifier = None

# ---------------------------------------------------------------------------
# Verification tuning (adjust after testing)
# ---------------------------------------------------------------------------
TOP_K = 5  # articles retrieved per claim
RELEVANCE_MAX_DISTANCE = 0.35  # cosine distance; evidence further away is ignored
VERDICT_THRESHOLD = 0.7  # min NLI probability to call SUPPORTED
REFUTE_THRESHOLD = 0.85  # stricter for REFUTED: a wrong "false" verdict is the worst error

# Numeric claims (prices / market cap) are checked against CoinGecko instead
ACCURATE_TOL = 0.02  # within 2%  -> SUPPORTED
APPROX_TOL = 0.10  # within 10% -> APPROXIMATE, beyond -> REFUTED


def get_engine() -> SentimentEngine:
    """Lazily initialise the sentiment engine so importing this module stays cheap."""
    global _engine
    if _engine is None:
        _engine = SentimentEngine(load_finbert=True)
    return _engine


def get_verifier():
    """Lazily load the teammate's ClaimVerifier (we reuse its ChromaDB + NLI model)."""
    global _verifier
    if _verifier is None:
        from verifier.claim_verifier import ClaimVerifier

        _verifier = ClaimVerifier()
    return _verifier


def _clean(text: str) -> str:
    """Strip HTML tags/entities left over from RSS summaries."""
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _norm(text: str) -> str:
    """Lowercase and strip punctuation so quoted text compares equal."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9$%.]+", " ", text.lower())).strip(" .")


def _verify_with_evidence(claim: str) -> Dict:
    """Retrieve relevant evidence and decide SUPPORTED / REFUTED / NOT ENOUGH INFO."""
    v = get_verifier()
    res = v.collection.query(
        query_texts=[claim],
        n_results=TOP_K,
        include=["documents", "metadatas", "distances"],
    )
    docs, metas, dists = res["documents"][0], res["metadatas"][0], res["distances"][0]

    # 1. Keep only evidence that is actually about this claim
    evidence = [
        (_clean(d), m, dist) for d, m, dist in zip(docs, metas, dists)
        if dist <= RELEVANCE_MAX_DISTANCE
    ]
    if not evidence:
        return {"veracity": "NOT ENOUGH INFO", "confidence": 0.0, "evidence": None,
                "source": None, "method": "evidence_search",
                "explanation": "We couldn't find relevant reporting in our sources to confirm or dispute this.",
                "note": "No relevant evidence found."}

    # 2a. Verbatim match: the claim is quoted word-for-word in an article
    #     (exact only, so altered numbers or words never slip through)
    norm_claim = _norm(claim)
    if len(norm_claim.split()) >= 5:
        for text, meta, dist in evidence:
            if norm_claim in _norm(text):
                return {
                    "veracity": "SUPPORTED", "method": "evidence_search",
                    "explanation": f"This matches reporting from {meta.get('source', 'Unknown')}.",
                    "confidence": 1.0, "evidence": text[:200] + "...",
                    "source": meta.get("source", "Unknown"), "url": meta.get("url"),
                    "distance": round(dist, 3),
                }

    # 2b. NLI: hypothesis = claim. Premises are each sentence of the article
    #     (NLI models work best on short premises) plus the whole article.
    #     Support can come from any premise; refutation only from the whole
    #     article, which keeps false REFUTED verdicts rare.
    pairs, owners = [], []
    for i, (text, _, _) in enumerate(evidence):
        for sent in [x for x in re.split(r"(?<=[.!?])\s+", text) if len(x.split()) >= 4][:8]:
            pairs.append({"text": sent, "text_pair": claim})
            owners.append((i, False))
        pairs.append({"text": text[:1500], "text_pair": claim})
        owners.append((i, True))
    outs = v.nli(pairs, top_k=None, truncation=True)
    if outs and isinstance(outs[0], dict):  # single result, not a batch
        outs = [outs]

    scored = [{"entailment": 0.0, "contradiction": 0.0, "text": t, "meta": m,
               "distance": round(d, 3)} for t, m, d in evidence]
    for (i, whole), out in zip(owners, outs):
        probs = {o["label"].lower(): float(o["score"]) for o in out}
        scored[i]["entailment"] = max(scored[i]["entailment"], probs.get("entailment", 0.0))
        if whole:
            scored[i]["contradiction"] = probs.get("contradiction", 0.0)

    # 3. Decide from the strongest supporting / refuting evidence
    best_sup = max(scored, key=lambda s: s["entailment"])
    best_ref = max(scored, key=lambda s: s["contradiction"])

    if best_sup["entailment"] >= VERDICT_THRESHOLD and best_sup["entailment"] >= best_ref["contradiction"]:
        veracity, best, conf = "SUPPORTED", best_sup, best_sup["entailment"]
    elif best_ref["contradiction"] >= REFUTE_THRESHOLD:
        veracity, best, conf = "REFUTED", best_ref, best_ref["contradiction"]
    else:
        veracity, best = "NOT ENOUGH INFO", max(scored, key=lambda s: max(s["entailment"], s["contradiction"]))
        conf = 0.0

    source = best["meta"].get("source", "Unknown")
    explanation = {
        "SUPPORTED": f"This matches reporting from {source}.",
        "REFUTED": f"This conflicts with reporting from {source}.",
    }.get(veracity, "We found related reporting, but nothing that clearly confirms or disputes this.")

    return {
        "veracity": veracity,
        "method": "evidence_search",
        "explanation": explanation,
        "confidence": round(conf, 3),
        "evidence": best["text"][:200] + "...",
        "source": best["meta"].get("source", "Unknown"),
        "url": best["meta"].get("url"),
        "distance": best["distance"],  # for tuning; hide from end users
        "scores": {"entailment": round(best["entailment"], 3),  # for tuning; hide from end users
                   "contradiction": round(best["contradiction"], 3)},
    }


# ---------------------------------------------------------------------------
# Numeric claims (price / market cap) -> checked against CoinGecko
# ---------------------------------------------------------------------------
# CoinGecko's daily series is a snapshot at 00:00 UTC, so the *closing* price of
# day D is the snapshot stamped D+1. Small differences from other sites are
# absorbed by the 2% tolerance.

COINS = {  # CoinGecko id: (display name, case-insensitive names, UPPERCASE-only tickers)
    "bitcoin": ("Bitcoin", ["bitcoin", "btc"], []),
    "ethereum": ("Ethereum", ["ethereum", "ether", "eth"], []),
    "solana": ("Solana", ["solana"], ["SOL"]),
    "ripple": ("XRP", ["xrp", "ripple"], []),
    "dogecoin": ("Dogecoin", ["dogecoin", "doge"], []),
    "cardano": ("Cardano", ["cardano"], ["ADA"]),
    "binancecoin": ("BNB", ["bnb"], []),
}


def _coin_regex(names, upper):
    parts = [re.escape(n) for n in names] + [f"(?-i:{t})" for t in upper]
    return re.compile(r"\b(?:" + "|".join(parts) + r")\b", re.IGNORECASE)


_COIN_RES = {cid: _coin_regex(n, u) for cid, (_, n, u) in COINS.items()}

_MAGNITUDE = {"k": 1e3, "thousand": 1e3, "m": 1e6, "million": 1e6,
              "b": 1e9, "billion": 1e9, "t": 1e12, "trillion": 1e12}
_VALUE_RE = re.compile(
    r"(\$)?\s?(\d[\d,]*(?:\.\d+)?)\s?(trillion|billion|million|thousand|[kmbt](?![a-z]))?",
    re.IGNORECASE,
)
_MONTHS = {"january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
           "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
           "august": 8, "aug": 8, "september": 9, "sept": 9, "sep": 9,
           "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12}
_DATE_RES = [
    (re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"), "ymd"),
    (re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?[-\s/]+([A-Za-z]{3,9})\.?,?[-\s/]+(\d{4})\b"), "dmy"),
    (re.compile(r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b"), "mdy"),
]

FUTURE_RE = re.compile(
    r"\b(will|won't|going to|gonna|predict\w*|forecast\w*|expect\w*|could|might|tomorrow|"
    r"next (?:week|month|year)|by (?:the )?(?:end|eoy))\b", re.IGNORECASE)
NON_PRICE_RE = re.compile(
    r"\b(etfs?|inflows?|outflows?|volume|funds?|treasury|holdings?|sold|bought|purchase\w*|"
    r"revenue|fees?)\b", re.IGNORECASE)
PRICE_RE = re.compile(
    r"\b(price|trading|trades|traded|worth|valued|closed|closing|close|hit|hits|reached|"
    r"reaches|fell|dropped|rose|surged|lowest|highest|minimum|maximum|low|high|"
    r"was|is|stood|stands|sits|at|costs?)\b", re.IGNORECASE)
EVENT_RE = re.compile(
    r"\b(hit|hits|reached|reaches|touched|peaked|(?:fell|dropped|rose|surged|crashed|"
    r"climbed|jumped|tumbled) to)\b", re.IGNORECASE)
PAST_RE = re.compile(r"\b(was|were|had|closed|traded|dropped|fell|rose|surged|jumped|climbed)\b", re.IGNORECASE)
CURRENT_RE = re.compile(r"\b(current(?:ly)?|now|today|right now|at the moment)\b", re.IGNORECASE)
RELATIVE_RE = re.compile(r"\b(yesterday|last (?:week|month|year)|\d+ days? ago|ago|since|earlier|previously)\b", re.IGNORECASE)
ATH_RE = re.compile(r"\ball[- ]time[- ]?(high|low)\b|\b(ath|atl)\b", re.IGNORECASE)
BIG_RE = re.compile(r"\b(largest|biggest|greatest|highest|maximum|max|record|worst|steepest|sharpest)\b", re.IGNORECASE)
UP_RE = re.compile(r"\b(increase|gain|rise|jump|surge|rally|climb)\b", re.IGNORECASE)
DOWN_RE = re.compile(r"\b(decrease|drop|decline|fall|loss|crash|plunge|slide)\b", re.IGNORECASE)
DAILY_RE = re.compile(r"\b(daily|single[- ]day|one[- ]day|day)\b", re.IGNORECASE)
CHANGE_RE = re.compile(
    r"\b(increase[sd]?|decrease[sd]?|gain(?:s|ed)?|loss(?:es)?|lost|change[sd]?|average|mean|median|"
    r"range|volatility|difference|spread|swing|(?:up|down|rose|fell|dropped|jumped|climbed|surged|gained) by)\b",
    re.IGNORECASE)
MIN_RE = re.compile(r"\b(lowest|minimum|min|low|bottom)\b", re.IGNORECASE)
MAX_RE = re.compile(r"\b(highest|maximum|max|high|peak)\b", re.IGNORECASE)

_cache: Dict = {}


def _cached(key, ttl, fn):
    """Tiny TTL cache so repeated claims don't hammer CoinGecko's rate limit."""
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    value = fn()
    _cache[key] = (time.time(), value)
    return value


def _find_values(text: str):
    """Dollar amounts only: needs a $ sign, a k/M/B/T suffix, or 'USD'/'dollars'."""
    values = []
    for m in _VALUE_RE.finditer(text):
        dollar, num, suffix = m.group(1), m.group(2), m.group(3)
        has_unit = re.match(r"\s?(usd|dollars?)\b", text[m.end():], re.IGNORECASE)
        if not (dollar or suffix or has_unit):
            continue  # skips years, days, counts
        value = float(num.replace(",", ""))
        if suffix:
            value *= _MAGNITUDE[suffix.lower()]
        values.append(value)
    return values


def _find_date(text: str):
    """Return (date, (start, end)) for the first full date in the text."""
    for rx, order in _DATE_RES:
        for m in rx.finditer(text):
            try:
                if order == "ymd":
                    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                elif order == "dmy":
                    d, mo, y = int(m.group(1)), _MONTHS[m.group(2).lower()], int(m.group(3))
                else:
                    mo, d, y = _MONTHS[m.group(1).lower()], int(m.group(2)), int(m.group(3))
                return date(y, mo, d), m.span()
            except (KeyError, ValueError):
                continue
    return None, None


def parse_numeric_claim(text: str) -> Optional[Dict]:
    """Return a parsed numeric claim, or None if this isn't a price/market-cap claim.

    A dict with an 'unsupported' reason means: it IS a numeric claim, but one we
    can't check (e.g. averages), so we shouldn't fall back to news search.
    """
    if NON_PRICE_RE.search(text):
        return None
    coins = [cid for cid, rx in _COIN_RES.items() if rx.search(text)]
    values = _find_values(text)
    if len(coins) != 1 or not values:
        return None

    mentions_mcap = bool(re.search(r"market\s?cap", text, re.IGNORECASE))
    is_move = bool(DAILY_RE.search(text) and BIG_RE.search(text)
                   and bool(UP_RE.search(text)) != bool(DOWN_RE.search(text)))
    if not (mentions_mcap or is_move or PRICE_RE.search(text) or CHANGE_RE.search(text)):
        return None
    metric = "market_cap" if mentions_mcap else "price"

    d, span = _find_date(text)
    rest = text if span is None else text[:span[0]] + " " + text[span[1]:]
    years = re.findall(r"\b(20\d{2})\b", rest)
    year = int(years[0]) if len(years) == 1 else None

    parsed = {"coin_id": coins[0], "coin_name": COINS[coins[0]][0], "metric": metric,
              "claimed": values[0], "date": d, "year": year, "extreme": None}

    def unsupported(reason):
        parsed["unsupported"] = reason
        return parsed

    if len(values) > 1:  # ranges, "from X to Y"... news-search NLI is unreliable on numbers
        return unsupported("This claim contains several figures, which we can't check yet.")

    # all-time high / low (price only)
    ath = ATH_RE.search(text)
    if ath:
        if metric == "market_cap":
            return unsupported("We can only check all-time highs and lows for price, not market cap.")
        is_high = (ath.group(1) or ath.group(2)).lower() in ("high", "ath")
        parsed.update(kind="alltime", extreme="max" if is_high else "min")
        return parsed
    if re.search(r"all[- ]time", text, re.IGNORECASE):
        return unsupported("We can't tell which all-time record this refers to.")

    # largest daily increase / decrease within a year
    if is_move:
        parsed["year"] = year or (d.year if d else None)
        if parsed["year"] is None:
            return unsupported("We can't tell which time period this refers to.")
        parsed.update(kind="move", extreme="max" if UP_RE.search(text) else "min")
        return parsed

    # other changes / averages / percentages: don't guess
    if CHANGE_RE.search(text) or "%" in text or re.search(r"\bpercent", text, re.IGNORECASE):
        return unsupported("We can check prices, market caps and the largest daily moves, "
                           "but not other changes, averages or percentages.")

    mn, mx = MIN_RE.search(text), MAX_RE.search(text)
    if mn and mx:
        return None

    if mn or mx:
        parsed["extreme"] = "min" if mn else "max"
        parsed["year"] = year or (d.year if d else None)
        if parsed["year"] is None:
            return unsupported("We can't tell which time period this high/low refers to.")
        if metric == "market_cap":
            return unsupported("We can only check historical prices, not historical market caps.")
        parsed["kind"] = "extreme"
    elif d is not None:
        if metric == "market_cap":
            return unsupported("We can only check historical prices, not historical market caps.")
        parsed["kind"] = "date"
    elif year is not None:
        return unsupported("We can't tell which day in that year this refers to.")
    elif (EVENT_RE.search(text) or RELATIVE_RE.search(text)
          or (PAST_RE.search(text) and not CURRENT_RE.search(text))):
        return unsupported("We can't tell when this happened, so it can't be checked against price history.")
    else:
        parsed["kind"] = "current"
    return parsed


def _fmt_usd(x: float) -> str:
    ax = abs(x)
    if ax >= 1e12:
        return f"${x / 1e12:.2f} trillion"
    if ax >= 1e9:
        return f"${x / 1e9:.2f} billion"
    if ax >= 1e6:
        return f"${x / 1e6:.2f} million"
    return f"${x:,.2f}"


def _current(coin_id: str):
    from verifier.coingecko_client import get_current_price

    df = _cached(("cur", coin_id), 60, lambda: get_current_price([coin_id]))
    row = df.iloc[0]
    return float(row["price"]), float(row["market_cap"])


def _coin_info(coin_id: str) -> Dict:
    """CoinGecko coin details (includes all-time high / low and their dates)."""
    from verifier.coingecko_client import _get

    return _cached(("coin", coin_id), 600, lambda: _get(
        f"coins/{coin_id}",
        {"localization": "false", "tickers": "false", "community_data": "false",
         "developer_data": "false", "sparkline": "false"}))


def _year_covered(closes: Dict[date, float], y: int, today: date) -> bool:
    """True if the daily data spans the whole of year y (up to today for this year)."""
    if y > today.year or min(closes) > date(y, 1, 1):
        return False
    return y == today.year or max(closes) >= date(y, 12, 31)


def _daily_closes(coin_id: str) -> Dict[date, float]:
    """{date: closing price}. The 00:00 UTC snapshot stamped D+1 is the close of D."""
    from verifier.coingecko_client import get_historical_prices

    df = _cached(("hist", coin_id), 600, lambda: get_historical_prices(coin_id, days=365))
    midnight = df[(df["datetime"].dt.hour == 0) & (df["datetime"].dt.minute == 0)]
    return {(ts - timedelta(days=1)).date(): float(p)
            for ts, p in zip(midnight["datetime"], midnight["price"])}


def _numeric_result(veracity: str, explanation: str, **extra) -> Dict:
    return {"veracity": veracity, "method": "numeric", "source": "CoinGecko",
            "explanation": explanation, **extra}


def verify_numeric_claim(p: Dict) -> Dict:
    """Compare a parsed price / market-cap claim with CoinGecko data."""
    if p.get("unsupported"):
        return _numeric_result("NOT ENOUGH INFO", p["unsupported"])

    name, claimed = p["coin_name"], p["claimed"]
    label = "market cap" if p["metric"] == "market_cap" else "price"
    window_msg = "CoinGecko's free plan only covers about the past year of prices."
    actual_date = None
    try:
        today = datetime.now(timezone.utc).date()
        if p["kind"] == "current":
            price, mcap = _current(p["coin_id"])
            actual = mcap if p["metric"] == "market_cap" else price
            phrase = f"{name}'s {label} right now is {_fmt_usd(actual)}"
        elif p["kind"] == "alltime":
            info = _coin_info(p["coin_id"])["market_data"]
            key, word = ("ath", "all-time high") if p["extreme"] == "max" else ("atl", "all-time low")
            actual = float(info[key]["usd"])
            actual_date = datetime.fromisoformat(info[f"{key}_date"]["usd"].replace("Z", "+00:00")).date()
            phrase = f"{name}'s {word} is {_fmt_usd(actual)} (on {actual_date:%d %b %Y})"
        else:
            closes = _daily_closes(p["coin_id"])
            first = min(closes)
            if p["kind"] == "date":
                d = p["date"]
                if d > today:
                    return _numeric_result("NOT VERIFIABLE", "That date is in the future.")
                if d < first or d not in closes:
                    return _numeric_result("NOT ENOUGH INFO", window_msg)
                actual, actual_date = closes[d], d
                phrase = f"{name}'s closing price on {d:%d %b %Y} was {_fmt_usd(actual)}"
            else:
                y = p["year"]
                if not _year_covered(closes, y, today):
                    return _numeric_result("NOT ENOUGH INFO", window_msg)
                if p["kind"] == "extreme":
                    pool = {d: v for d, v in closes.items() if d.year == y}
                    pick, word = (min, "lowest") if p["extreme"] == "min" else (max, "highest")
                    actual_date = pick(pool, key=pool.get)
                    actual = pool[actual_date]
                    phrase = (f"{name}'s {word} daily closing price in {y} was {_fmt_usd(actual)} "
                              f"(on {actual_date:%d %b %Y})")
                else:  # "move": close-to-close change in USD
                    changes = {d: closes[d] - closes[d - timedelta(days=1)]
                               for d in closes if d.year == y and d - timedelta(days=1) in closes}
                    if not changes:
                        return _numeric_result("NOT ENOUGH INFO", window_msg)
                    pick, word = (max, "increase") if p["extreme"] == "max" else (min, "decrease")
                    actual_date = pick(changes, key=changes.get)
                    actual = abs(changes[actual_date])
                    phrase = (f"{name}'s largest daily {word} in {y} was {_fmt_usd(actual)} "
                              f"(on {actual_date:%d %b %Y})")
    except Exception as exc:
        logging.warning("CoinGecko check failed: %s", exc)
        return _numeric_result("NOT ENOUGH INFO", "We couldn't reach CoinGecko to check this number.")

    dev = abs(claimed - actual) / actual
    if dev <= ACCURATE_TOL:
        verdict, how = "SUPPORTED", f"accurate (within {ACCURATE_TOL:.0%})"
    elif dev <= APPROX_TOL:
        verdict, how = "APPROXIMATE", f"roughly right ({dev:.1%} off)"
    else:
        verdict, how = "REFUTED", f"inaccurate ({dev:.1%} off)"

    date_note = ""
    if p["kind"] in ("extreme", "move", "alltime") and p["date"] and actual_date and abs((actual_date - p["date"]).days) > 1:
        date_note = f" However, the claimed date ({p['date']:%d %b %Y}) doesn't match."
        if verdict == "SUPPORTED":
            verdict = "APPROXIMATE"

    return _numeric_result(
        verdict,
        f"{phrase}. The claim says {_fmt_usd(claimed)}, which is {how}.{date_note}",
        claimed=claimed, actual=round(actual, 2), deviation_pct=round(dev * 100, 2),
    )


def verify_claim(claim: str) -> Dict:
    """Route a claim: predictions -> not verifiable, numbers -> CoinGecko, rest -> news evidence."""
    if FUTURE_RE.search(claim):
        return {"veracity": "NOT VERIFIABLE", "method": "prediction",
                "explanation": "This is a prediction about the future, so it can't be checked yet."}
    numeric = parse_numeric_claim(claim)
    if numeric is not None:
        return verify_numeric_claim(numeric)
    return _verify_with_evidence(claim)


def analyse_text(text: str) -> Dict:
    """Run one piece of text through sentiment and claim verification."""
    engine = get_engine()
    return {
        "text": text,
        "vader": engine.score_vader(text),
        "finbert": engine.score_finbert_batch([text])[0],
        "verification": verify_claim(text),
    }


if __name__ == "__main__":
    import json

    # Usage: python chatbot_pipeline.py "your claim"   (or just run it for the default)
    claim = sys.argv[1] if len(sys.argv) > 1 else "18th September 2026 Price of ETH is up, trading at $2,634."
    print(json.dumps(analyse_text(claim), indent=2))