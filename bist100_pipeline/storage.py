"""MongoDB integration for storing BIST100 market data."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

LOGGER = logging.getLogger(__name__)


class MongoDBStorage:
    """Simple MongoDB storage layer with idempotent upserts."""

    def __init__(self, uri: str, database: str = "bist100", collection: str = "prices") -> None:
        self._client = MongoClient(uri)
        self._collection = self._client[database][collection]
        self._collection.create_index([("symbol", 1), ("Date", 1)], unique=True)

    def store_prices(self, df: pd.DataFrame) -> int:
        """Store a dataframe in MongoDB and return affected document count."""
        if df.empty:
            LOGGER.info("Received empty dataframe; nothing to store")
            return 0

        operations = []
        for record in df.to_dict(orient="records"):
            payload: dict[str, Any] = {
                "Date": record.get("Date"),
                "Open": record.get("Open"),
                "High": record.get("High"),
                "Low": record.get("Low"),
                "Close": record.get("Close"),
                "Adj Close": record.get("Adj Close"),
                "Volume": record.get("Volume"),
                "ingested_at": record.get("ingested_at"),
            }
            symbol = record.get("symbol")
            if symbol is None or payload["Date"] is None:
                continue

            operations.append(
                UpdateOne(
                    {"symbol": symbol, "Date": payload["Date"]},
                    {"$set": {"symbol": symbol, **payload}},
                    upsert=True,
                )
            )

        if not operations:
            LOGGER.warning("No valid records found for storage")
            return 0

        try:
            result = self._collection.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except PyMongoError as exc:
            LOGGER.exception("MongoDB write failed")
            raise RuntimeError("Failed to persist price data") from exc

    def close(self) -> None:
        self._client.close()
