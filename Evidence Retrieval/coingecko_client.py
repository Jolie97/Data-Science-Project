import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()  # reads COINGECKO_API_KEY from .env file

BASE_URL = "https://api.coingecko.com/api/v3"
API_KEY = os.getenv("COINGECKO_API_KEY", "")

HEADERS = {
    "accept": "application/json",
    "x-cg-demo-api-key": API_KEY,
}

# Path to your Reddit post data file
REDDIT_DATA_PATH = "reddit_posts.csv"  # <-- change to actual filename


# ---------------------------------------------------------------------------
# Helper: safe GET request with basic rate-limit handling
# ---------------------------------------------------------------------------


def _get(endpoint: str, params: dict = None) -> dict:
    """Make a GET request to the CoinGecko API and return parsed JSON."""
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, headers=HEADERS, params=params, timeout=10)

    if response.status_code == 429:
        print("Rate limit hit — waiting 60 seconds...")
        time.sleep(60)
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)

    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# CoinGecko data fetchers
# ---------------------------------------------------------------------------


def ping() -> bool:
    """Check that the API is reachable and the key is valid."""
    data = _get("ping")
    print("CoinGecko ping:", data)
    return "gecko_says" in data


def get_current_price(coin_ids: list[str], vs_currency: str = "usd") -> pd.DataFrame:
    """
    Fetch current price, market cap, and 24 h volume for a list of coins.

    Parameters
    ----------
    coin_ids    : list of CoinGecko coin IDs, e.g. ["bitcoin", "ethereum"]
    vs_currency : quote currency (default "usd")

    Returns
    -------
    pd.DataFrame with columns: coin, price, market_cap, volume_24h
    """
    params = {
        "ids": ",".join(coin_ids),
        "vs_currencies": vs_currency,
        "include_market_cap": "true",
        "include_24hr_vol": "true",
    }
    data = _get("simple/price", params)

    rows = []
    for coin, values in data.items():
        rows.append(
            {
                "coin": coin,
                "price": values.get(vs_currency),
                "market_cap": values.get(f"{vs_currency}_market_cap"),
                "volume_24h": values.get(f"{vs_currency}_24h_vol"),
            }
        )
    return pd.DataFrame(rows)


def get_historical_prices(
    coin_id: str,
    days: int = 30,
    vs_currency: str = "usd",
) -> pd.DataFrame:
    """
    Fetch daily closing prices for a single coin over the past `days` days.

    Granularity is automatic:
      1 day  → 5-minute intervals
      2-90 d → hourly
      > 90 d → daily

    Returns
    -------
    pd.DataFrame with columns: datetime, price
    """
    params = {
        "vs_currency": vs_currency,
        "days": days,
    }
    data = _get(f"coins/{coin_id}/market_chart", params)

    prices = data.get("prices", [])
    df = pd.DataFrame(prices, columns=["timestamp_ms", "price"])
    df["datetime"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df[["datetime", "price"]]
    return df

    # def get_trending_coins() -> pd.DataFrame:
    """
    Return the top trending coins on CoinGecko (last 24 hours).

    Returns
    -------
    pd.DataFrame with columns: name, symbol, market_cap_rank, score
    """
    data = _get("search/trending")
    rows = []
    for coin in data.get("coins", []):
        item = coin.get("item", {})
        rows.append(
            {
                "name": item.get("name"),
                "symbol": item.get("symbol"),
                "market_cap_rank": item.get("market_cap_rank"),
                "score": item.get("score"),
            }
        )
    return pd.DataFrame(rows)


def get_coins_by_market_cap(
    vs_currency: str = "usd",
    per_page: int = 50,
    page: int = 1,
) -> pd.DataFrame:
    """
    Return coins listed by market cap (descending) from CoinGecko.

    Parameters
    ----------
    vs_currency : str
        The target currency for market data (default: 'usd').
    per_page : int
        Number of results per page, max 250 (default: 50).
    page : int
        Page number for pagination (default: 1).

    Returns
    -------
    pd.DataFrame with columns:
        name, symbol, market_cap_rank, market_cap, current_price,
        price_change_percentage_24h, total_volume
    """
    data = _get(
        "coins/markets",
        params={
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": False,
        },
    )
    rows = []
    for coin in data:
        rows.append(
            {
                "name": coin.get("name"),
                "symbol": coin.get("symbol"),
                "market_cap_rank": coin.get("market_cap_rank"),
                "market_cap": coin.get("market_cap"),
                "current_price": coin.get("current_price"),
                "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
                "total_volume": coin.get("total_volume"),
            }
        )
    return pd.DataFrame(rows)

    # ---------------------------------------------------------------------------
    # Reddit data loader
    # ---------------------------------------------------------------------------

    # def load_reddit_data(path: str = REDDIT_DATA_PATH) -> pd.DataFrame:
    """
    Load Reddit post data from a local CSV file.

    Expected columns (adjust to match your actual file):
      - title      : post title
      - coin       : coin mentioned (e.g. "bitcoin")
      - score      : Reddit upvote score
      - created_utc: Unix timestamp of post creation
      - text       : post body text (optional)

    Returns
    -------
    pd.DataFrame
    """
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Reddit data file not found: {path}\n"
            "Place your CSV file next to coingecko_client.py and update REDDIT_DATA_PATH."
        )

    df = pd.read_csv(path)

    # Normalise coin names to lowercase so they match CoinGecko IDs
    if "coin" in df.columns:
        df["coin"] = df["coin"].str.lower().str.strip()

    # Convert timestamp if present
    if "created_utc" in df.columns:
        df["datetime"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)

    return df


# ---------------------------------------------------------------------------
# Comparison: Reddit sentiment vs CoinGecko price
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    # 1. Verify API connectivity
    if not ping():
        print("ERROR: Could not reach CoinGecko API. Check your key in .env.")
        return

    # 2. Show trending coins
    # print("\n--- Trending Coins ---")
    # trending = get_trending_coins()
    # print(trending.to_string(index=False))

    # 2.Show coins by market cap
    print("\n--- Coins by Market Cap ---")
    market_cap_coins = get_coins_by_market_cap(per_page=10)
    print(market_cap_coins.to_string(index=False))

    # 3. Fetch current prices for a few coins
    coins_of_interest = ["bitcoin", "ethereum", "dogecoin"]
    print("\n--- Current Prices ---")
    prices = get_current_price(coins_of_interest)
    print(prices.to_string(index=False))

    # 4. Historical price for Bitcoin (last 30 days)
    print("\n--- Bitcoin: Last Year (daily) ---")
    history = get_historical_prices("bitcoin", days=365)
    print(history.head(5).to_string(index=False))
    print(history.tail(5).to_string(index=False))

    # 5. Compare against Reddit data (only runs if the file exists)


if __name__ == "__main__":
    main()
