# MyTools Data Pipeline

This repository contains the backend data pipeline for a future investment analysis website. It is designed to be deterministic, CI-first, and easy to maintain.

## Overview

The core of this repository is a nightly data pipeline that:
1.  Reads a registry of indicators from `public/data/indicators/registry.json`.
2.  Downloads daily financial data for the required tickers.
3.  Stores the ticker data in CSV files under `data/store/tickers/`.
4.  Builds indicator data and saves it as JSON files under `public/data/indicators/`.
5.  Commits the updated data back to the repository.

## Running Locally

To run the pipeline locally, you will need Python 3.12 and `requests`.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/mytools.git
    cd mytools
    ```

2.  **Set up a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install requests
    ```

4.  **Set environment variables:**
    You will need to set the following environment variables to access the data source:
    ```bash
    export YAHOO_PROXY_URL="your_proxy_url"
    export YAHOO_PROXY_TOKEN="your_proxy_token"
    ```

5.  **Run the pipeline:**
    ```bash
    python -m backend.cli
    ```
    This will run both the ticker update and indicator build pipelines. You can also run them individually:
    ```bash
    python -m backend.cli sync-indicator-tickers
    python -m backend.cli build-indicators
    ```

## Data Schemas

### Ticker CSV

-   **Path:** `data/store/tickers/<SYMBOL>.csv`
-   **Format:** CSV
-   **Header:** `date,open,high,low,close,adj_close,volume,div,split`
-   **Fields:**
    -   `date`: ISO format (`YYYY-MM-DD`)
    -   `open`, `high`, `low`, `close`: Standard daily prices
    -   `adj_close`: Adjusted closing price (accounts for dividends and splits)
    -   `volume`: Trading volume
    -   `div`: Cash dividend on that date (if any)
    -   `split`: Stock split ratio on that date (if any)

### Indicator JSON

-   **Path:** `public/data/indicators/<slug>/series.json`
-   **Format:** JSON
-   **Shape:**
    ```json
    {
      "meta": {
        "slug": "<slug>",
        "title": "<title>",
        "type": "<type>",
        "tickers": ["T1", "T2"],
        "lastUpdated": "YYYY-MM-DD"
      },
      "series": [
        { "date": "YYYY-MM-DD", "ratio": 1.2345 }
      ]
    }
    ```

## How to Add a New Indicator

To add a new indicator, you need to:

1.  **Update the registry:** Add a new entry to `public/data/indicators/registry.json`. Make sure to include a unique `slug`, a `title`, the `type` of indicator, and the required `tickers`.
2.  **Run the pipeline:** The next time the pipeline runs (either locally or via the nightly CI job), it will automatically pick up the new indicator, download the required ticker data, and build the indicator JSON file.

## CI/CD

The data pipeline is run automatically every night at 00:00 Europe/Berlin time. It can also be triggered manually through the GitHub Actions interface. The workflow is defined in `.github/workflows/data.yml`.

## Failure Policy

The ticker update pipeline is designed to be robust to partial failures. If a ticker fails to download, the pipeline will log the error and continue with the other tickers. The pipeline will only fail if more than 50% of the tickers fail to download. This threshold can be configured using the `--max-failure-rate` command-line flag.
