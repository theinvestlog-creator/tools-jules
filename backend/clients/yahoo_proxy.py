import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List

import requests

# --- Configuration from environment variables ---
YAHOO_PROXY_URL = os.environ.get("YAHOO_PROXY_URL")
YAHOO_PROXY_TOKEN = os.environ.get("YAHOO_PROXY_TOKEN")

# --- Constants ---
REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 8.0
USER_AGENT = "InvestLogFetcher/1.0"


def fetch_daily_bars(ticker: str) -> List[Dict[str, str]]:
    """
    Fetches daily historical data for a given ticker, with retries and backoff.

    Args:
        ticker: The stock ticker symbol to fetch.

    Returns:
        A list of dictionaries, each representing a daily bar.

    Raises:
        ValueError: If the required proxy URL or token are not configured.
        requests.exceptions.RequestException: On persistent network errors after all retries.
    """
    if not YAHOO_PROXY_URL or not YAHOO_PROXY_TOKEN:
        raise ValueError("YAHOO_PROXY_URL and YAHOO_PROXY_TOKEN must be set in the environment.")

    url = f"{YAHOO_PROXY_URL}?token={YAHOO_PROXY_TOKEN}&symbol={ticker}&interval=1d&range=max"
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}

    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()

            json_data = response.json()
            if not json_data.get("chart", {}).get("result"):
                return []  # Valid response but no data (e.g., for a delisted ticker)

            return _parse_yahoo_response(json_data)

        except requests.exceptions.RequestException as e:
            last_exception = e
            # Exponential backoff with full jitter
            backoff_duration = min(BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * (2**attempt))
            jitter = random.uniform(0, backoff_duration)
            time.sleep(jitter)
            continue  # Retry

    raise last_exception  # Raise the last captured exception if all retries fail


def _parse_yahoo_response(response: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Parses the JSON response from the Yahoo Finance v8 API.
    Stitches price data with dividend and split events.
    """
    result = response["chart"]["result"][0]
    timestamps = result.get("timestamp", [])

    if not timestamps:
        return []

    adjclose_list = result.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
    if len(timestamps) != len(adjclose_list):
        return []  # Data integrity issue

    events = result.get("events", {})
    dividends = {str(data['date']): data['amount'] for data in events.get("dividends", {}).values()}
    splits = {str(data['date']): data['splitRatio'] for data in events.get("splits", {}).values()}

    rows = []
    for i, ts in enumerate(timestamps):
        if adjclose_list[i] is None:
            continue  # Skip entries with no price data

        ts_str = str(ts)
        rows.append({
            "date": datetime.fromtimestamp(ts).strftime("%Y-%m-%d"),
            "close": f"{adjclose_list[i]:.6f}",
            "div": str(dividends.get(ts_str, "")),
            "split": splits.get(ts_str, ""),
        })

    return rows
