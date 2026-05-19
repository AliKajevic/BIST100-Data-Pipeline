"""CLI entry point for downloading and storing BIST100 stock data."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bist100_pipeline.downloader import download_stock_data
from bist100_pipeline.storage import MongoDBStorage

DEFAULT_SYMBOLS = ["^XU100"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download BIST100 market data and store in MongoDB")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
    parser.add_argument("--database", default="bist100")
    parser.add_argument("--collection", default="prices")
    parser.add_argument("--start", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def main() -> int:
    configure_logging()
    args = parse_args()

    storage = None
    try:
        storage = MongoDBStorage(args.mongo_uri, database=args.database, collection=args.collection)
        total_rows = 0
        for symbol in args.symbols:
            df = download_stock_data(symbol, start=args.start, end=args.end)
            stored = storage.store_prices(df)
            logging.info("Stored %s rows for %s", stored, symbol)
            total_rows += stored

        logging.info("Pipeline completed. Total stored rows: %s", total_rows)
        return 0
    except Exception:
        logging.exception("Pipeline failed")
        return 1
    finally:
        if storage is not None:
            storage.close()


if __name__ == "__main__":
    sys.exit(main())
