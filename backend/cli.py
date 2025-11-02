import argparse

from backend.pipelines import update_tickers, build_indicators
from backend.util.log import log_structured

def main():
    """
    Main entry point for the command-line interface.
    """
    parser = argparse.ArgumentParser(description="Data pipeline CLI for indicators and portfolios.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- `sync-indicator-tickers` command ---
    sync_parser = subparsers.add_parser(
        "sync-indicator-tickers",
        help="Downloads and updates all ticker data from the registries."
    )
    sync_parser.add_argument(
        "--max-failure-rate",
        type=float,
        default=0.5,
        help="The maximum allowed failure rate for ticker downloads (default: 0.5)."
    )
    sync_parser.add_argument(
        "--jitter-ms",
        type=int,
        nargs=2,
        default=[150, 450],
        metavar=("MIN", "MAX"),
        help="The min and max delay in milliseconds for request jitter (default: 150 450)."
    )

    # --- `build-indicators` command ---
    subparsers.add_parser(
        "build-indicators",
        help="Builds all indicator JSON files from the processed ticker data."
    )

    args = parser.parse_args()

    if args.command is None:
        # Default behavior: run both pipelines in order
        log_structured({"event": "cli_run", "command": "default"})
        update_tickers.run(
            max_failure_rate=0.5,
            jitter_ms=(150, 450)
        )
        build_indicators.run()

    elif args.command == "sync-indicator-tickers":
        log_structured({"event": "cli_run", "command": "sync-indicator-tickers", "params": vars(args)})
        update_tickers.run(
            max_failure_rate=args.max_failure_rate,
            jitter_ms=tuple(args.jitter_ms)
        )

    elif args.command == "build-indicators":
        log_structured({"event": "cli_run", "command": "build-indicators"})
        build_indicators.run()

if __name__ == "__main__":
    main()
