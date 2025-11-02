import json
import os
import random
import time
from typing import Dict, List, Set

from backend.clients.yahoo_proxy import fetch_daily_bars
from backend.util.io_safe import atomic_write_text, read_csv_rows, merge_incremental
from backend.util.log import log_structured

# --- Constants ---
DATA_STORE_TICKERS_DIR = "data/store/tickers"
INDICATOR_REGISTRY_PATH = "public/data/indicators/registry.json"
PORTFOLIO_REGISTRY_PATH = "public/data/portfolios/registry.json"


def _get_unique_tickers_from_registries() -> Set[str]:
    """Extracts a unique set of tickers from all available registries."""
    tickers: Set[str] = set()

    # Load from indicator registry
    if os.path.exists(INDICATOR_REGISTRY_PATH):
        with open(INDICATOR_REGISTRY_PATH, "r") as f:
            indicator_registry = json.load(f)
            for item in indicator_registry.get("items", []):
                tickers.update(item.get("tickers", []))

    # Load from portfolio registry (if it exists)
    if os.path.exists(PORTFOLIO_REGISTRY_PATH):
        with open(PORTFOLIO_REGISTRY_PATH, "r") as f:
            portfolio_registry = json.load(f)
            # Assuming a similar structure for portfolios
            for item in portfolio_registry.get("items", []):
                tickers.update(item.get("tickers", []))

    return tickers


def _rows_to_csv_text(rows: List[Dict[str, str]]) -> str:
    """Converts a list of dictionaries to a CSV string with a header."""
    if not rows:
        return ""

    header = "date,open,high,low,close,adj_close,volume,div,split"
    lines = [header]
    for row in rows:
        lines.append(
            f"{row['date']},{row['open']},{row['high']},{row['low']},{row['close']},"
            f"{row['adj_close']},{row['volume']},{row['div']},{row['split']}"
        )

    return "\n".join(lines)


def run(max_failure_rate: float = 0.5, jitter_ms: tuple = (150, 450)):
    """
    Main pipeline to update all ticker CSV files from the Yahoo proxy.
    """
    os.makedirs(DATA_STORE_TICKERS_DIR, exist_ok=True)
    all_tickers = sorted(list(_get_unique_tickers_from_registries()))

    if not all_tickers:
        log_structured({"level": "warning", "message": "No tickers found in any registry."})
        return

    total_tickers = len(all_tickers)
    failures = 0
    summary = []

    for ticker in all_tickers:
        log_data = {"ticker": ticker}
        try:
            # Add jitter before the request
            time.sleep(random.uniform(jitter_ms[0] / 1000.0, jitter_ms[1] / 1000.0))

            new_rows = fetch_daily_bars(ticker)
            if not new_rows:
                log_data.update({"status": "no_data", "message": "No new data returned from client."})
                log_structured(log_data)
                summary.append(log_data)
                continue

            csv_path = os.path.join(DATA_STORE_TICKERS_DIR, f"{ticker}.csv")
            existing_rows = read_csv_rows(csv_path)

            merged_rows = merge_incremental(existing_rows, new_rows)

            if merged_rows:
                csv_text = _rows_to_csv_text(merged_rows)
                atomic_write_text(csv_path, csv_text)
                log_data.update({
                    "status": "ok",
                    "rows_written": len(merged_rows),
                    "new_rows": len(merged_rows) - len(existing_rows),
                })
            else:
                log_data["status"] = "no_rows_to_write"

        except Exception as e:
            failures += 1
            log_data.update({"status": "error", "message": str(e)})

        log_structured(log_data)
        summary.append(log_data)

    # --- Final Summary ---
    log_structured({
        "event": "run_summary",
        "total_tickers": total_tickers,
        "successes": total_tickers - failures,
        "failures": failures,
    })

    if total_tickers > 0 and (failures / total_tickers) > max_failure_rate:
        raise SystemExit(f"Failure rate ({failures / total_tickers:.2f}) exceeded the threshold of {max_failure_rate:.2f}.")

if __name__ == "__main__":
    run()
