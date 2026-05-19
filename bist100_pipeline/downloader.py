"""Download market data for BIST100 symbols using yfinance."""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger(__name__)


def download_stock_data(symbol: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Download OHLCV data for a symbol and return a normalized DataFrame."""
    try:
        history = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=False)
    except Exception as exc:
        LOGGER.exception("yfinance failed for symbol '%s'", symbol)
        raise RuntimeError(f"Failed to download data for {symbol}") from exc

    if history.empty:
        LOGGER.warning("No data received for symbol '%s'", symbol)
        return pd.DataFrame()

    history = history.reset_index()
    history["Date"] = pd.to_datetime(history["Date"], utc=True, errors="coerce")
    history = history.dropna(subset=["Date"])  # defensive parsing
    history["symbol"] = symbol
    history["ingested_at"] = datetime.utcnow()
    return history
