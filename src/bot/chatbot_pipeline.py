"""Chatbot pipeline: plain text in -> sentiment + claim verification out.

Prerequisite for the full local version: build the evidence database once with

    python src/verifier/populate_evidence.py

The Render deployment can run in lightweight mode using VADER + CoinGecko
without loading the heavy FinBERT / ChromaDB / NLI models.
"""
import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))  # adds src/ to the path
import html
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Optional
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
# ---------------------------------------------------------------------------
# Deployment mode
# ---------------------------------------------------------------------------
# Render Free uses limited memory, so it can run without the heavy
# FinBERT / ChromaDB / NLI models.
RENDER_LIGHTWEIGHT = os.getenv("RENDER_LIGHTWEIGHT", "").lower() == "true"
# ---------------------------------------------------------------------------
# Quiet down noisy third-party loggers
# ---------------------------------------------------------------------------
for noisy_logger in ( "httpx", "huggingface_hub", "urllib3", "filelock", "chromadb", ):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)
# ---------------------------------------------------------------------------
# Lazy-loaded models
# ---------------------------------------------------------------------------
_engine = None
_verifier = None
_vader = None
# ---------------------------------------------------------------------------
# Verification tuning
# ---------------------------------------------------------------------------
TOP_K = 5  # articles retrieved per claim
RELEVANCE_MAX_DISTANCE = 0.35  # cosine distance; evidence further away is ignored
VERDICT_THRESHOLD = 0.7  # min NLI probability to call SUPPORTED
REFUTE_THRESHOLD = 0.85  # stricter for REFUTED
# ---------------------------------------------------------------------------
# Numeric claims (prices / market cap) are checked against CoinGecko instead
# ---------------------------------------------------------------------------
ACCURATE_TOL = 0.02  # within 2% -> SUPPORTED
APPROX_TOL = 0.10  # within 10% -> APPROXIMATE, beyond -> REFUTED
# ---------------------------------------------------------------------------
# Sentiment
# ---------------------------------------------------------------------------
def get_engine():
    """Lazily initialise the full sentiment engine.

    This imports SentimentEngine only when the full local version is used.
    Therefore Render lightweight mode never imports PyTorch/Transformers.
    """
    global _engine
    if _engine is None:
        from sentiment.analyser import SentimentEngine
        _engine = SentimentEngine(load_finbert=True)
    return _engine
def get_lightweight_vader():
    """Lazily initialise VADER for Render's lightweight mode."""
    global _vader
    if _vader is None:
        try:
            nltk.data.find("sentiment/vader_lexicon.zip")
        except LookupError:
            nltk.download("vader_lexicon", quiet=True)
        _vader = SentimentIntensityAnalyzer()
        # Add crypto-specific terminology adjustments.
        crypto_lexicon_updates = {
            "bullish": 2.0,
            "bearish": -2.0,
            "moon": 2.5,
            "dump": -2.5,
            "pump": 2.0,
            "hodl": 1.5,
            "rugpull": -3.5,
            "scam": -3.0,
            "rekt": -3.0,
            "fud": -2.0,
        }
        _vader.lexicon.update(crypto_lexicon_updates)
    return _vader
# ---------------------------------------------------------------------------
# Full evidence verifier
# ---------------------------------------------------------------------------
def get_verifier():
    """Lazily load the full ClaimVerifier.

    This is only used in the full local version.
    """
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
    return re.sub( r"\s+", " ", re.sub(r"[^a-z0-9$%.]+", " ", text.lower()), ).strip(" .")
def _verify_with_evidence(claim: str) -> Dict:
    """Retrieve relevant evidence and decide SUPPORTED / REFUTED / NOT ENOUGH INFO."""
    v = get_verifier()
    res = v.collection.query( query_texts=[claim], n_results=TOP_K, include=["documents", "metadatas", "distances"], )
    docs = res["documents"][0]
    metas = res["metadatas"][0]
    dists = res["distances"][0]
    # 1. Keep only evidence that is actually about this claim.
    evidence = [ (_clean(d), m, dist) for d, m, dist in zip(docs, metas, dists) if dist <= RELEVANCE_MAX_DISTANCE ]
    if not evidence:
        return {
            "veracity": "NOT ENOUGH INFO",
            "confidence": 0.0,
            "evidence": None,
            "source": None,
            "method": "evidence_search",
            "explanation": ( "We couldn't find relevant reporting in our sources " "to confirm or dispute this." ),
            "note": "No relevant evidence found.",
        }
    # 2a. Verbatim match: the claim is quoted word-for-word in an article.
    # Exact only, so altered numbers or words never slip through.
    norm_claim = _norm(claim)
    if len(norm_claim.split()) >= 5:
        for text, meta, dist in evidence:
            if norm_claim in _norm(text):
                return {
                    "veracity": "SUPPORTED",
                    "method": "evidence_search",
                    "explanation": ( f"This matches reporting from " f"{meta.get('source', 'Unknown')}." ),
                    "confidence": 1.0,
                    "evidence": text[:200] + "...",
                    "source": meta.get("source", "Unknown"),
                    "url": meta.get("url"),
                    "distance": round(dist, 3),
                }
    # 2b. NLI: hypothesis = claim.
    # Premises are each sentence of the article plus the whole article.
    pairs = []
    owners = []
    for i, (text, _, _) in enumerate(evidence):
        sentences = [ x for x in re.split(r"(?<=[.!?])\s+", text) if len(x.split()) >= 4 ][:8]
        for sent in sentences:
            pairs.append( { "text": sent, "text_pair": claim, } )
            owners.append((i, False))
        pairs.append( { "text": text[:1500], "text_pair": claim, } )
        owners.append((i, True))
    outs = v.nli( pairs, top_k=None, truncation=True, )
    if outs and isinstance(outs[0], dict):
        outs = [outs]
    scored = [ { "entailment": 0.0, "contradiction": 0.0, "text": t, "meta": m, "distance": round(d, 3), } for t, m, d in evidence ]
    for (i, whole), out in zip(owners, outs):
        probs = { o["label"].lower(): float(o["score"]) for o in out }
        scored[i]["entailment"] = max( scored[i]["entailment"], probs.get("entailment", 0.0), )
        if whole:
            scored[i]["contradiction"] = probs.get( "contradiction", 0.0, )
    # 3. Decide from the strongest supporting / refuting evidence.
    best_sup = max( scored, key=lambda s: s["entailment"], )
    best_ref = max( scored, key=lambda s: s["contradiction"], )
    if ( best_sup["entailment"] >= VERDICT_THRESHOLD and best_sup["entailment"] >= best_ref["contradiction"] ):
        veracity = "SUPPORTED"
        best = best_sup
        conf = best_sup["entailment"]
    elif best_ref["contradiction"] >= REFUTE_THRESHOLD:
        veracity = "REFUTED"
        best = best_ref
        conf = best_ref["contradiction"]
    else:
        veracity = "NOT ENOUGH INFO"
        best = max( scored, key=lambda s: max( s["entailment"], s["contradiction"], ), )
        conf = 0.0
    source = best["meta"].get( "source", "Unknown", )
    explanation = {
        "SUPPORTED": f"This matches reporting from {source}.",
        "REFUTED": f"This conflicts with reporting from {source}.",
    }.get( veracity, "We found related reporting, but nothing that clearly confirms or disputes this.", )
    return {
        "veracity": veracity,
        "method": "evidence_search",
        "explanation": explanation,
        "confidence": round(conf, 3),
        "evidence": best["text"][:200] + "...",
        "source": source,
        "url": best["meta"].get("url"),
        "distance": best["distance"],
        "scores": { "entailment": round( best["entailment"], 3, ), "contradiction": round( best["contradiction"], 3, ), },
    }
# ---------------------------------------------------------------------------
# Numeric claims (price / market cap) -> checked against CoinGecko
# ---------------------------------------------------------------------------
# CoinGecko's daily series is a snapshot at 00:00 UTC, so the closing price
# of day D is the snapshot stamped D+1.
# Small differences from other sites are absorbed by the 2% tolerance.
COINS = {
    # CoinGecko id: (display name, case-insensitive names, UPPERCASE-only tickers)
    "bitcoin": ("Bitcoin", ["bitcoin", "btc"], []),
    "ethereum": ("Ethereum", ["ethereum", "ether", "eth"], []),
    "solana": ("Solana", ["solana"], ["SOL"]),
    "ripple": ("XRP", ["xrp", "ripple"], []),
    "dogecoin": ("Dogecoin", ["dogecoin", "doge"], []),
    "cardano": ("Cardano", ["cardano"], ["ADA"]),
    "binancecoin": ("BNB", ["bnb"], []),
}
def _coin_regex(names, upper):
    parts = [re.escape(n) for n in names] + [ f"(?-i:{t})" for t in upper ]
    return re.compile( r"\b(?:" + "|".join(parts) + r")\b", re.IGNORECASE, )
_COIN_RES = { cid: _coin_regex(n, u) for cid, (_, n, u) in COINS.items() }
_MAGNITUDE = { "k": 1e3, "thousand": 1e3, "m": 1e6, "million": 1e6, "b": 1e9, "billion": 1e9, "t": 1e12, "trillion": 1e12, }
_VALUE_RE = re.compile( r"(\$)?\s?(\d[\d,]*(?:\.\d+)?)\s?" r"(trillion|billion|million|thousand|[kmbt](?![a-z]))?", re.IGNORECASE, )
_MONTHS = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sept": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}
_DATE_RES = [
    ( re.compile( r"\b(\d{4})-(\d{2})-(\d{2})\b" ), "ymd", ),
    ( re.compile( r"\b(\d{1,2})(?:st|nd|rd|th)?[-\s/]+" r"([A-Za-z]{3,9})\.?,?[-\s/]+(\d{4})\b" ), "dmy", ),
    ( re.compile( r"\b([A-Za-z]{3,9})\.?\s+" r"(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b" ), "mdy", ),
]
FUTURE_RE = re.compile(
    r"\b("
    r"will|"
    r"won't|"
    r"going to|"
    r"gonna|"
    r"predict\w*|"
    r"forecast\w*|"
    r"expect\w*|"
    r"could|"
    r"might|"
    r"tomorrow|"
    r"next (?:week|month|year)|"
    r"by (?:the )?(?:end|eoy)"
    r")\b",
    re.IGNORECASE,
)
NON_PRICE_RE = re.compile(
    r"\b("
    r"etfs?|"
    r"inflows?|"
    r"outflows?|"
    r"volume|"
    r"funds?|"
    r"treasury|"
    r"holdings?|"
    r"sold|"
    r"bought|"
    r"purchase\w*|"
    r"revenue|"
    r"fees?"
    r")\b",
    re.IGNORECASE,
)
PRICE_RE = re.compile(
    r"\b("
    r"price|"
    r"trading|"
    r"trades|"
    r"traded|"
    r"worth|"
    r"valued|"
    r"closed|"
    r"closing|"
    r"close|"
    r"hit|"
    r"hits|"
    r"reached|"
    r"reaches|"
    r"fell|"
    r"dropped|"
    r"rose|"
    r"surged|"
    r"lowest|"
    r"highest|"
    r"minimum|"
    r"maximum|"
    r"low|"
    r"high|"
    r"was|"
    r"is|"
    r"stood|"
    r"stands|"
    r"sits|"
    r"at|"
    r"costs?"
    r")\b",
    re.IGNORECASE,
)
EVENT_RE = re.compile(
    r"\b("
    r"hit|"
    r"hits|"
    r"reached|"
    r"reaches|"
    r"touched|"
    r"peaked|"
    r"fell|"
    r"dropped|"
    r"rose|"
    r"surged|"
    r"crashed|"
    r"climbed|"
    r"jumped|"
    r"tumbled"
    r")\b",
    re.IGNORECASE,
)
PAST_RE = re.compile(
    r"\b("
    r"was|"
    r"were|"
    r"had|"
    r"closed|"
    r"traded|"
    r"dropped|"
    r"fell|"
    r"rose|"
    r"surged|"
    r"jumped|"
    r"climbed"
    r")\b",
    re.IGNORECASE,
)
CURRENT_RE = re.compile( r"\b(" r"current(?:ly)?|" r"now|" r"today|" r"right now|" r"at the moment" r")\b", re.IGNORECASE, )
RELATIVE_RE = re.compile(
    r"\b("
    r"yesterday|"
    r"last (?:week|month|year)|"
    r"\d+ days? ago|"
    r"ago|"
    r"since|"
    r"earlier|"
    r"previously"
    r")\b",
    re.IGNORECASE,
)
ATH_RE = re.compile( r"\ball[- ]time[- ]?(high|low)\b|" r"\b(ath|atl)\b", re.IGNORECASE, )
BIG_RE = re.compile(
    r"\b("
    r"largest|"
    r"biggest|"
    r"greatest|"
    r"highest|"
    r"maximum|"
    r"max|"
    r"record|"
    r"worst|"
    r"steepest|"
    r"sharpest"
    r")\b",
    re.IGNORECASE,
)
UP_RE = re.compile( r"\b(" r"increase|" r"gain|" r"rise|" r"jump|" r"surge|" r"rally|" r"climb" r")\b", re.IGNORECASE, )
DOWN_RE = re.compile( r"\b(" r"decrease|" r"drop|" r"decline|" r"fall|" r"loss|" r"crash|" r"plunge|" r"slide" r")\b", re.IGNORECASE, )
DAILY_RE = re.compile( r"\b(" r"daily|" r"single[- ]day|" r"one[- ]day|" r"day" r")\b", re.IGNORECASE, )
CHANGE_RE = re.compile(r"\b(increase[sd]?|decrease[sd]?|gain(?:s|ed)?|loss(?:es)?|lost|change[sd]?|average|mean|median|"
    r"range|volatility|difference|spread|swing|(?:up|down|rose|fell|dropped|jumped|climbed|surged|gained) by)\b",
    re.IGNORECASE,)
MIN_RE = re.compile( r"\b(lowest|minimum|min|low|bottom)\b", re.IGNORECASE, )
MAX_RE = re.compile( r"\b(highest|maximum|max|high|peak)\b", re.IGNORECASE, )
# ---------------------------------------------------------------------------
# CoinGecko cache
# ---------------------------------------------------------------------------
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
    """Dollar amounts only: needs a $ sign, suffix, or USD/dollars."""
    values = []
    for m in _VALUE_RE.finditer(text):
        dollar = m.group(1)
        num = m.group(2)
        suffix = m.group(3)
        has_unit = re.match( r"\s?(usd|dollars?)\b", text[m.end():], re.IGNORECASE, )
        if not (dollar or suffix or has_unit):
            continue
        # Skip bare numbers such as years, days and counts.
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
                    y = int(m.group(1))
                    mo = int(m.group(2))
                    d = int(m.group(3))
                elif order == "dmy":
                    d = int(m.group(1))
                    mo = _MONTHS[m.group(2).lower()]
                    y = int(m.group(3))
                else:
                    mo = _MONTHS[m.group(1).lower()]
                    d = int(m.group(2))
                    y = int(m.group(3))
                return date(y, mo, d), m.span()
            except (KeyError, ValueError):
                continue
    return None, None
def parse_numeric_claim(text: str) -> Optional[Dict]:
    """Return a parsed numeric claim, or None if this isn't a price/market-cap claim.

    A dict with an 'unsupported' reason means it IS a numeric claim, but one
    we can't check, so we shouldn't fall back to news search.
    """
    if NON_PRICE_RE.search(text):
        return None
    coins = [ cid for cid, rx in _COIN_RES.items() if rx.search(text) ]
    values = _find_values(text)
    if len(coins) != 1 or not values:
        return None
    mentions_mcap = bool( re.search( r"market\s?cap", text, re.IGNORECASE, ) )
    is_move = ( bool(DAILY_RE.search(text)) and bool(BIG_RE.search(text)) and bool(UP_RE.search(text)) != bool(DOWN_RE.search(text)) )
    if not ( mentions_mcap or is_move or PRICE_RE.search(text) or CHANGE_RE.search(text) ):
        return None
    metric = "market_cap" if mentions_mcap else "price"
    d, span = _find_date(text)
    rest = ( text if span is None else text[:span[0]] + " " + text[span[1]:] )
    years = re.findall( r"\b(20\d{2})\b", rest, )
    year = int(years[0]) if len(years) == 1 else None
    parsed = { "coin_id": coins[0], "coin_name": COINS[coins[0]][0], "metric": metric, "claimed": values[0], "date": d, "year": year, "extreme": None, }
    def unsupported(reason):
        parsed["unsupported"] = reason
        return parsed
    # Ranges, "from X to Y", etc.
    # News-search NLI is unreliable on multiple numbers.
    if len(values) > 1:
        return unsupported( "This claim contains several figures, which we can't check yet." )
    # All-time high / low (price only).
    ath = ATH_RE.search(text)
    if ath:
        if metric == "market_cap":
            return unsupported( "We can only check all-time highs and lows for price, " "not market cap." )
        matched_word = ( ath.group(1) or ath.group(2) ).lower()
        is_high = matched_word in ("high", "ath")
        parsed.update( kind="alltime", extreme="max" if is_high else "min", )
        return parsed
    if re.search( r"all[- ]time", text, re.IGNORECASE, ):
        return unsupported( "We can't tell which all-time record this refers to." )
    # Largest daily increase / decrease within a year.
    if is_move:
        parsed["year"] = year or ( d.year if d else None )
        if parsed["year"] is None:
            return unsupported( "We can't tell which time period this refers to." )
        parsed.update( kind="move", extreme="max" if UP_RE.search(text) else "min", )
        return parsed
    # Other changes / averages / percentages:
    # don't guess.
    if ( CHANGE_RE.search(text) or "%" in text or re.search( r"\bpercent", text, re.IGNORECASE, ) ):
        return unsupported( "We can check prices, market caps and the largest daily moves, " "but not other changes, averages or percentages." )
    mn = MIN_RE.search(text)
    mx = MAX_RE.search(text)
    if mn and mx:
        return None
    if mn or mx:
        parsed["extreme"] = "min" if mn else "max"
        parsed["year"] = year or ( d.year if d else None )
        if parsed["year"] is None:
            return unsupported( "We can't tell which time period this high/low refers to." )
        if metric == "market_cap":
            return unsupported( "We can only check historical prices, " "not historical market caps." )
        parsed["kind"] = "extreme"
    elif d is not None:
        if metric == "market_cap":
            return unsupported( "We can only check historical prices, " "not historical market caps." )
        parsed["kind"] = "date"
    elif year is not None:
        return unsupported( "We can't tell which day in that year this refers to." )
    elif ( EVENT_RE.search(text) or RELATIVE_RE.search(text) or ( PAST_RE.search(text) and not CURRENT_RE.search(text) ) ):
        return unsupported( "We can't tell when this happened, so it can't be " "checked against price history." )
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
    df = _cached( ("cur", coin_id), 60, lambda: get_current_price([coin_id]), )
    row = df.iloc[0]
    return ( float(row["price"]), float(row["market_cap"]), )
def _coin_info(coin_id: str) -> Dict:
    """CoinGecko coin details including all-time high/low and dates."""
    from verifier.coingecko_client import _get
    return _cached(
        ("coin", coin_id),
        600,
        lambda: _get(
            f"coins/{coin_id}",
            { "localization": "false", "tickers": "false", "community_data": "false", "developer_data": "false", "sparkline": "false", },
        ),
    )
def _year_covered( closes: Dict[date, float], y: int, today: date, ) -> bool:
    """True if daily data spans the whole of year y."""
    if y > today.year:
        return False
    if min(closes) > date(y, 1, 1):
        return False
    return ( y == today.year or max(closes) >= date(y, 12, 31) )
def _daily_closes(coin_id: str) -> Dict[date, float]:
    """Return {date: closing price}.

    The 00:00 UTC snapshot stamped D+1 is the close of D.
    """
    from verifier.coingecko_client import get_historical_prices
    df = _cached( ("hist", coin_id), 600, lambda: get_historical_prices( coin_id, days=365, ), )
    midnight = df[ (df["datetime"].dt.hour == 0) & (df["datetime"].dt.minute == 0) ]
    return { (ts - timedelta(days=1)).date(): float(p) for ts, p in zip( midnight["datetime"], midnight["price"], ) }
def _numeric_result( veracity: str, explanation: str, **extra, ) -> Dict:
    return { "veracity": veracity, "method": "numeric", "source": "CoinGecko", "explanation": explanation, **extra, }
def verify_numeric_claim(p: Dict) -> Dict:
    """Compare a parsed price / market-cap claim with CoinGecko data."""
    if p.get("unsupported"):
        return _numeric_result( "NOT ENOUGH INFO", p["unsupported"], )
    name = p["coin_name"]
    claimed = p["claimed"]
    label = ( "market cap" if p["metric"] == "market_cap" else "price" )
    window_msg = ( "CoinGecko's free plan only covers about " "the past year of prices." )
    actual_date = None
    try:
        today = datetime.now( timezone.utc ).date()
        if p["kind"] == "current":
            price, mcap = _current( p["coin_id"] )
            actual = ( mcap if p["metric"] == "market_cap" else price )
            phrase = ( f"{name}'s {label} right now is " f"{_fmt_usd(actual)}" )
        elif p["kind"] == "alltime":
            info = _coin_info( p["coin_id"] )["market_data"]
            key, word = ( ("ath", "all-time high") if p["extreme"] == "max" else ("atl", "all-time low") )
            actual = float( info[key]["usd"] )
            actual_date = datetime.fromisoformat( info[ f"{key}_date" ]["usd"].replace( "Z", "+00:00", ) ).date()
            phrase = ( f"{name}'s {word} is " f"{_fmt_usd(actual)} " f"(on {actual_date:%d %b %Y})" )
        else:
            closes = _daily_closes( p["coin_id"] )
            first = min(closes)
            if p["kind"] == "date":
                d = p["date"]
                if d > today:
                    return _numeric_result( "NOT VERIFIABLE", "That date is in the future.", )
                if d < first or d not in closes:
                    return _numeric_result( "NOT ENOUGH INFO", window_msg, )
                actual = closes[d]
                actual_date = d
                phrase = ( f"{name}'s closing price on " f"{d:%d %b %Y} was " f"{_fmt_usd(actual)}" )
            else:
                y = p["year"]
                if not _year_covered( closes, y, today, ):
                    return _numeric_result( "NOT ENOUGH INFO", window_msg, )
                if p["kind"] == "extreme":
                    pool = { d: v for d, v in closes.items() if d.year == y }
                    pick, word = ( (min, "lowest") if p["extreme"] == "min" else (max, "highest") )
                    actual_date = pick( pool, key=pool.get, )
                    actual = pool[ actual_date ]
                    phrase = ( f"{name}'s {word} daily closing " f"price in {y} was " f"{_fmt_usd(actual)} " f"(on {actual_date:%d %b %Y})" )
                else:
                    # "move": close-to-close change in USD.
                    changes = { d: closes[d] - closes[d - timedelta(days=1)] for d in closes if d.year == y and d - timedelta(days=1) in closes }
                    if not changes:
                        return _numeric_result( "NOT ENOUGH INFO", window_msg, )
                    pick, word = ( (max, "increase") if p["extreme"] == "max" else (min, "decrease") )
                    actual_date = pick( changes, key=changes.get, )
                    actual = abs( changes[actual_date] )
                    phrase = ( f"{name}'s largest daily " f"{word} in {y} was " f"{_fmt_usd(actual)} " f"(on {actual_date:%d %b %Y})" )
    except Exception as exc:
        logging.warning( "CoinGecko check failed: %s", exc, )
        return _numeric_result( "NOT ENOUGH INFO", "We couldn't reach CoinGecko to check this number.", )
    dev = abs( claimed - actual ) / actual
    if dev <= ACCURATE_TOL:
        verdict = "SUPPORTED"
        how = ( f"accurate (within {ACCURATE_TOL:.0%})" )
    elif dev <= APPROX_TOL:
        verdict = "APPROXIMATE"
        how = ( f"roughly right ({dev:.1%} off)" )
    else:
        verdict = "REFUTED"
        how = ( f"inaccurate ({dev:.1%} off)" )
    date_note = ""
    if ( p["kind"] in ( "extreme", "move", "alltime", ) and p["date"] and actual_date and abs( ( actual_date - p["date"] ).days ) > 1 ):
        date_note = ( f" However, the claimed date " f"({p['date']:%d %b %Y}) doesn't match." )
        if verdict == "SUPPORTED":
            verdict = "APPROXIMATE"
    return _numeric_result(
        verdict,
        ( f"{phrase}. The claim says " f"{_fmt_usd(claimed)}, which is " f"{how}.{date_note}" ),
        claimed=claimed,
        actual=round(actual, 2),
        deviation_pct=round( dev * 100, 2, ),
    )
# ---------------------------------------------------------------------------
# Main claim router
# ---------------------------------------------------------------------------
def verify_claim(claim: str) -> Dict:
    """Route a claim.

    Predictions -> not verifiable
    Numeric claims -> CoinGecko
    Other claims -> news evidence locally,
                    lightweight fallback on Render
    """
    # Future claims are never checked against current evidence.
    if FUTURE_RE.search(claim):
        return {
            "veracity": "NOT VERIFIABLE",
            "method": "prediction",
            "explanation": ( "This is a prediction about the future, " "so it can't be checked yet." ),
        }
    # Numeric claims go through CoinGecko.
    # This remains available in Render lightweight mode.
    numeric = parse_numeric_claim(claim)
    if numeric is not None:
        return verify_numeric_claim(numeric)
    # Render Free does not load ChromaDB, SentenceTransformer,
    # DeBERTa or the NLI model.
    if RENDER_LIGHTWEIGHT:
        return {
            "veracity": "NOT ENOUGH INFO",
            "method": "lightweight",
            "confidence": 0.0,
            "evidence": None,
            "source": None,
            "explanation": (
                "This claim does not contain a numeric value "
                "that can be checked against CoinGecko. "
                "The lightweight deployment does not load the "
                "full evidence verification model."
            ),
        }
    # Full local version.
    return _verify_with_evidence(claim)
# ---------------------------------------------------------------------------
# Main analysis pipeline
# ---------------------------------------------------------------------------
def analyse_text(text: str) -> Dict:
    """Run one piece of text through sentiment and claim verification."""
    # -----------------------------------------------------------------------
    # Render Free lightweight mode
    # -----------------------------------------------------------------------
    if RENDER_LIGHTWEIGHT:
        vader = get_lightweight_vader().polarity_scores( text )
        # Keep the same response structure expected by app.py.
        # FinBERT is not loaded in lightweight mode.
        finbert = { "finbert_positive": 0.0, "finbert_negative": 0.0, "finbert_neutral": 1.0, "finbert_net": None, }
        return { "text": text, "vader": vader, "finbert": finbert, "verification": verify_claim(text), }
    # -----------------------------------------------------------------------
    # Full local mode
    # -----------------------------------------------------------------------
    engine = get_engine()
    return { "text": text, "vader": engine.score_vader(text), "finbert": engine.score_finbert_batch( [text] )[0], "verification": verify_claim(text), }
# ---------------------------------------------------------------------------
# Command-line usage
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json
    # Usage:
    # python chatbot_pipeline.py "your claim"
    #
    # Or just run it for the default claim.
    claim = ( sys.argv[1] if len(sys.argv) > 1 else ( "18th September 2026 Price of ETH is up, " "trading at $2,634." ) )
    print( json.dumps( analyse_text(claim), indent=2, ) )
