import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

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


def fetch_daily_bars(
    ticker: str, start_date: datetime, end_date: datetime
) -> List[Dict[str, str]]:
    """
    Fetches daily historical data for a given ticker within a specific date range.

    Args:
        ticker: The stock ticker symbol to fetch.
        start_date: The start of the date range (inclusive).
        end_date: The end of the date range (inclusive).

    Returns:
        A list of dictionaries, each representing a daily bar.

    Raises:
        ValueError: If the required proxy URL or token are not configured.
        requests.exceptions.RequestException: On persistent network errors after all retries.
    """
    if not YAHOO_PROXY_URL or not YAHOO_PROXY_TOKEN:
        raise ValueError(
            "YAHOO_PROXY_URL and YAHOO_PROXY_TOKEN must be set in the environment."
        )

    # Convert dates to UTC timestamps for the API
    period1 = int(start_date.timestamp())
    period2 = int(end_date.timestamp())

    url = (
        f"{YAHOO_PROXY_URL}?token={YAHOO_PROXY_TOKEN}&symbol={ticker}&interval=1d"
        f"&period1={period1}&period2={period2}"
    )
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}

    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()

            json_data = response.json()
            if not json_data.get("chart", {}).get("result"):
                return []

            return _parse_yahoo_response(json_data)

        except requests.exceptions.RequestException as e:
            last_exception = e
            backoff_duration = min(
                BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * (2**attempt)
            )
            jitter = random.uniform(0, backoff_duration)
            time.sleep(jitter)
            continue

    raise last_exception


def _parse_yahoo_response(response: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Parses the JSON response from the Yahoo Finance v8 API.
    Stitches price data with dividend and split events.
    """
    result = response["chart"]["result"][0]
    timestamps = result.get("timestamp", [])

    if not timestamps:
        return []

    indicators = result.get("indicators", {})
    quote = indicators.get("quote", [{}])[0]
    adjclose_list = indicators.get("adjclose", [{}])[0].get("adjclose", [])

    open_list = quote.get("open", [])
    high_list = quote.get("high", [])
    low_list = quote.get("low", [])
    close_list = quote.get("close", [])
    volume_list = quote.get("volume", [])

    if not (
        len(timestamps)
        == len(adjclose_list)
        == len(open_list)
        == len(high_list)
        == len(low_list)
        == len(close_list)
        == len(volume_list)
    ):
        return []

    events = result.get("events", {})
    dividends = {
        str(data["date"]): data["amount"] for data in events.get("dividends", {}).values()
    }
    splits = {
        str(data["date"]): data["splitRatio"]
        for data in events.get("splits", {}).values()
    }

    rows = []
    for i, ts in enumerate(timestamps):
        if adjclose_list[i] is None or close_list[i] is None:
            continue

        ts_str = str(ts)
        rows.append(
            {
                "date": datetime.fromtimestamp(ts).strftime("%Y-%m-%d"),
                "open": f"{open_list[i]:.6f}" if open_list[i] is not None else "",
                "high": f"{high_list[i]:.6f}" if high_list[i] is not None else "",
                "low": f"{low_list[i]:.6f}" if low_list[i] is not None else "",
                "close": f"{close_list[i]:.6f}" if close_list[i] is not None else "",
                "adj_close": f"{adjclose_list[i]:.6f}"
                if adjclose_list[i] is not None
                else "",
                "volume": str(volume_list[i]) if volume_list[i] is not None else "",
                "div": str(dividends.get(ts_str, "")),
                "split": splits.get(ts_str, ""),
            }
        )

    return rows
