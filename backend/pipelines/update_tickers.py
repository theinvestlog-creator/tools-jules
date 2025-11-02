import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set

from backend.clients.yahoo_proxy import fetch_daily_bars
from backend.util.io_safe import atomic_write_text, merge_incremental, read_csv_rows
from backend.util.log import log_structured

# --- Constants ---
DATA_STORE_TICKERS_DIR = "data/store/tickers"
INDICATOR_REGISTRY_PATH = "public/data/indicators/registry.json"
PORTFOLIO_REGISTRY_PATH = "public/data/portfolios/registry.json"
DEFAULT_START_YEAR = 1990
CHUNK_YEARS = 1 # How many years of data to fetch in a single request


def _get_unique_tickers_from_registries() -> Set[str]:
    """Extracts a unique set of tickers from all available registries."""
    tickers: Set[str] = set()
    for path in [INDICATOR_REGISTRY_PATH, PORTFOLIO_REGISTRY_PATH]:
        if os.path.exists(path):
            with open(path, "r") as f:
                registry = json.load(f)
                for item in registry.get("items", []):
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
            f"{row['date']},{row.get('open','')},{row.get('high','')},{row.get('low','')},"
            f"{row.get('close','')},{row.get('adj_close','')},{row.get('volume','')},"
            f"{row.get('div','')},{row.get('split','')}"
        )
    return "\n".join(lines)


def run(max_failure_rate: float = 0.5, jitter_ms: tuple = (150, 450)):
    """Main pipeline to update all ticker CSV files from the Yahoo proxy."""
    os.makedirs(DATA_STORE_TICKERS_DIR, exist_ok=True)
    all_tickers = sorted(list(_get_unique_tickers_from_registries()))

    if not all_tickers:
        log_structured({"level": "warning", "message": "No tickers found."})
        return

    failures = 0
    now_utc = datetime.now(timezone.utc)

    for ticker in all_tickers:
        log_data = {"ticker": ticker}
        try:
            csv_path = os.path.join(DATA_STORE_TICKERS_DIR, f"{ticker}.csv")
            existing_rows = read_csv_rows(csv_path)

            if existing_rows:
                last_date_str = existing_rows[-1]["date"]
                start_date_utc = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            else:
                start_date_utc = datetime(DEFAULT_START_YEAR, 1, 1, tzinfo=timezone.utc)

            all_new_rows = []

            # Fetch data in chunks from the start date until today
            current_start_utc = start_date_utc
            while current_start_utc < now_utc:
                end_date_utc = current_start_utc + timedelta(days=365 * CHUNK_YEARS)
                if end_date_utc > now_utc:
                    end_date_utc = now_utc

                time.sleep(random.uniform(jitter_ms[0] / 1000.0, jitter_ms[1] / 1000.0))

                chunk_rows = fetch_daily_bars(ticker, current_start_utc, end_date_utc)
                all_new_rows.extend(chunk_rows)

                current_start_utc = end_date_utc + timedelta(days=1)


            if not all_new_rows:
                log_data.update({"status": "no_new_data"})
                log_structured(log_data)
                continue

            merged_rows = merge_incremental(existing_rows, all_new_rows)
            csv_text = _rows_to_csv_text(merged_rows)
            atomic_write_text(csv_path, csv_text)

            log_data.update({
                "status": "ok",
                "rows_written": len(merged_rows),
                "new_rows": len(merged_rows) - len(existing_rows),
            })

        except Exception as e:
            failures += 1
            log_data.update({"status": "error", "message": str(e)})

        log_structured(log_data)

    total_tickers = len(all_tickers)
    if total_tickers > 0 and (failures / total_tickers) > max_failure_rate:
        raise SystemExit(f"Failure rate exceeded: {failures / total_tickers:.2f}")

if __name__ == "__main__":
    run()
