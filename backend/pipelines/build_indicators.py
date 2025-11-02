import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List

from backend.util.dates import get_berlin_now, to_iso_date
from backend.util.io_safe import atomic_write_text, read_csv_rows
from backend.util.log import log_structured

# --- Constants ---
DATA_STORE_TICKERS_DIR = "data/store/tickers"
INDICATOR_REGISTRY_PATH = "public/data/indicators/registry.json"
INDICATORS_OUTPUT_DIR = "public/data/indicators"


def _calculate_price_ratio(ticker1_rows: List[Dict], ticker2_rows: List[Dict]) -> List[Dict]:
    """Calculates the price ratio between two tickers based on their close prices."""

    # Create dictionaries for quick date lookups
    ticker1_map = {row["date"]: Decimal(row["close"]) for row in ticker1_rows}
    ticker2_map = {row["date"]: Decimal(row["close"]) for row in ticker2_rows}

    # Find common dates where both tickers have data
    common_dates = sorted(list(set(ticker1_map.keys()) & set(ticker2_map.keys())))

    series = []
    for date in common_dates:
        close1 = ticker1_map[date]
        close2 = ticker2_map[date]

        if close2 == Decimal(0):
            # Avoid division by zero, skip this data point
            continue

        ratio = close1 / close2
        # Quantize to 6 decimal places
        rounded_ratio = ratio.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

        series.append({"date": date, "ratio": float(rounded_ratio)})

    return series


def run():
    """
    Main pipeline to build all indicator JSON files from ticker CSVs.
    """
    if not os.path.exists(INDICATOR_REGISTRY_PATH):
        log_structured({"level": "error", "message": "Indicator registry not found.", "path": INDICATOR_REGISTRY_PATH})
        return

    with open(INDICATOR_REGISTRY_PATH, "r") as f:
        registry = json.load(f)

    last_updated = to_iso_date(get_berlin_now())

    for indicator in registry.get("items", []):
        slug = indicator.get("slug")
        indicator_type = indicator.get("type")
        tickers = indicator.get("tickers", [])

        log_data = {"slug": slug, "type": indicator_type}

        try:
            if indicator_type == "price_ratio":
                if len(tickers) != 2:
                    raise ValueError(f"price_ratio requires exactly 2 tickers, found {len(tickers)}")

                ticker1_path = os.path.join(DATA_STORE_TICKERS_DIR, f"{tickers[0]}.csv")
                ticker2_path = os.path.join(DATA_STORE_TICKERS_DIR, f"{tickers[1]}.csv")

                ticker1_rows = read_csv_rows(ticker1_path)
                ticker2_rows = read_csv_rows(ticker2_path)

                if not ticker1_rows or not ticker2_rows:
                    log_data.update({"status":"skipped", "reason": "Missing ticker data"})
                    log_structured(log_data)
                    continue

                series_data = _calculate_price_ratio(ticker1_rows, ticker2_rows)

                output_data = {
                    "meta": {
                        "slug": slug,
                        "title": indicator.get("title"),
                        "type": indicator_type,
                        "tickers": tickers,
                        "lastUpdated": last_updated,
                    },
                    "series": series_data,
                }

                output_dir = os.path.join(INDICATORS_OUTPUT_DIR, slug)
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, "series.json")

                atomic_write_text(output_path, json.dumps(output_data, indent=2))
                log_data.update({"status": "ok", "points": len(series_data)})

            else:
                log_data.update({"status": "unsupported_type"})

        except Exception as e:
            log_data.update({"status": "error", "message": str(e)})

        log_structured(log_data)


if __name__ == "__main__":
    run()
