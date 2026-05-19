"""
BIST100 Historical Stock Data Downloader → MongoDB
=====================================================
Downloads full historical daily OHLCV data for all BIST100 companies
from Yahoo Finance and stores them in MongoDB with deduplication.

Requirements:
    pip install yfinance pymongo requests pandas

Usage:
    python bist100_downloader.py

Environment variables (optional):
    MONGO_URI  — defaults to mongodb://localhost:27017
"""

import os
import time
import logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME         = "bist100_data"
COLLECTION_NAME = "daily_prices"

BATCH_SIZE      = 10        # tickers per yfinance call
BATCH_DELAY_SEC = 2         # polite delay between batches
MAX_RETRIES     = 3
RETRY_DELAY_SEC = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# BIST100 ticker list  (Yahoo Finance format, .IS suffix)
# ─────────────────────────────────────────────
BIST100_TICKERS = [
    "ACSEL.IS","ADEL.IS","AGESA.IS","AGHOL.IS","AKBNK.IS","AKCNS.IS",
    "AKENR.IS","AKFEN.IS","AKGRT.IS","AKSA.IS","AKSEN.IS","AKSGY.IS",
    "AKSUE.IS","AKTIN.IS","ALARK.IS","ALBRK.IS","ALFAS.IS","ALKIM.IS",
    "ALKLC.IS","ANELE.IS","ANHYT.IS","ANSGR.IS","ARCLK.IS","ARDYZ.IS",
    "ARENA.IS","ASELS.IS","ASUZU.IS","AYDEM.IS","AYEN.IS","BERA.IS",
    "BIENY.IS","BIMAS.IS","BIOEN.IS","BINHO.IS","BJKAS.IS","BRISA.IS",
    "BRYAT.IS","BUCIM.IS","CANTE.IS","CCOLA.IS","CEMAS.IS","CEMTS.IS",
    "CIMSA.IS","CLEBI.IS","CMENT.IS","COKCR.IS","CSGYO.IS","CWENE.IS",
    "DESA.IS","DEVA.IS","DOAS.IS","DOHOL.IS","DYOBY.IS","ECILC.IS",
    "ECZYT.IS","EGEEN.IS","EGGUB.IS","EGPRO.IS","EKGYO.IS","ELITE.IS",
    "ENJSA.IS","ENKAI.IS","EREGL.IS","EUPWR.IS","EUREN.IS","EVOTR.IS",
    "FENER.IS","FLAP.IS","FMIZP.IS","FROTO.IS","GARAN.IS","GLYHO.IS",
    "GOLTS.IS","GOODY.IS","GOZDE.IS","GRSEL.IS","GSDHO.IS","GUBRF.IS",
    "GWIND.IS","HALKB.IS","HATEK.IS","HEKTS.IS","HLGYO.IS","ISCTR.IS",
    "ISFIN.IS","ISGYO.IS","ISMEN.IS","IZENR.IS","JANTS.IS","KARSN.IS",
    "KCAER.IS","KCHOL.IS","KENT.IS","KMPUR.IS","KONTR.IS","KONYA.IS",
    "KORDS.IS","KOZAA.IS","KOZAL.IS","KRDMD.IS","KRPLS.IS","KTLEV.IS",
    "KUYAS.IS","LOGO.IS","LRSHO.IS","MAVI.IS","MGROS.IS","MIPAZ.IS",
    "MMCAS.IS","MPARK.IS","NETAS.IS","NTHOL.IS","NUGYO.IS","OBASE.IS",
    "ODAS.IS","ONCSM.IS","ORGE.IS","OTKAR.IS","OYAKC.IS","OZRDN.IS",
    "PEKGY.IS","PGSUS.IS","PINSU.IS","PKENT.IS","POLHO.IS","PRKAB.IS",
    "PRKME.IS","QUAGR.IS","REEDR.IS","RGYAS.IS","SAHOL.IS","SARKY.IS",
    "SELEC.IS","SISE.IS","SKBNK.IS","SKYLP.IS","SMART.IS","SMRTG.IS",
    "SOKM.IS","SRVGY.IS","TATGD.IS","TAVHL.IS","TBORG.IS","TCELL.IS",
    "THYAO.IS","TKFEN.IS","TKNSA.IS","TLMAN.IS","TOASO.IS","TSKB.IS",
    "TTKOM.IS","TTRAK.IS","TUCLK.IS","TUPRS.IS","TURSG.IS","ULUUN.IS",
    "ULKER.IS","VAKBN.IS","VERUS.IS","VESBE.IS","VESTL.IS","VKGYO.IS",
    "WINTE.IS","YKBNK.IS","YYLGD.IS","ZOREN.IS","ZRGYO.IS",
]
BIST100_TICKERS = sorted(set(BIST100_TICKERS))


# ─────────────────────────────────────────────
# MongoDB helpers
# ─────────────────────────────────────────────
def get_collection():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    log.info("Connected to MongoDB: %s", MONGO_URI)
    db  = client[DB_NAME]
    col = db[COLLECTION_NAME]

    # Remove any null-ticker garbage left from previous failed runs
    deleted = col.delete_many({"ticker": None})
    if deleted.deleted_count:
        log.warning("Cleaned up %d null-ticker documents from previous runs.",
                    deleted.deleted_count)

    # Create unique index if not already present
    existing = col.index_information()
    if "ticker_date_unique" not in existing:
        col.create_index(
            [("ticker", ASCENDING), ("date", ASCENDING)],
            unique=True,
            name="ticker_date_unique",
        )
        log.info("Created unique compound index: ticker + date.")
    else:
        log.info("Unique index already exists — skipping creation.")

    return col


# ─────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────
def _safe_float(val) -> float:
    try:
        f = float(val)
        return round(f, 6) if not pd.isna(f) else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _df_to_docs(df: pd.DataFrame, ticker: str) -> list[dict]:
    """
    Convert a flat single-ticker DataFrame (DatetimeIndex, OHLCV columns) to
    a list of MongoDB documents. Always sets ticker explicitly — never None.
    """
    if df is None or df.empty:
        return []

    df = df.copy()
    # Flatten MultiIndex columns if present (shouldn't happen here but be safe)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]

    close_col = "close" if "close" in df.columns else None
    if close_col is None:
        log.warning("  %s — no 'close' column, skipping.", ticker)
        return []

    df = df.dropna(subset=[close_col])
    if df.empty:
        return []

    adj_col = next((c for c in df.columns if "adj" in c), None)

    docs = []
    for date_idx, row in df.iterrows():
        try:
            dt = datetime(date_idx.year, date_idx.month, date_idx.day, tzinfo=timezone.utc)
        except AttributeError:
            continue

        docs.append({
            "ticker":    ticker,   # explicit string, never None
            "date":      dt,
            "open":      _safe_float(row.get("open")),
            "high":      _safe_float(row.get("high")),
            "low":       _safe_float(row.get("low")),
            "close":     _safe_float(row.get("close")),
            "adj_close": _safe_float(row.get(adj_col) if adj_col else row.get("close")),
            "volume":    int(row.get("volume", 0) or 0),
        })
    return docs


def download_single(ticker: str) -> list[dict]:
    """Download one ticker individually — fallback when batch parsing fails."""
    try:
        df = yf.download(
            tickers=ticker,
            period="max",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return _df_to_docs(df, ticker)
    except Exception as exc:
        log.error("  %s — individual download failed: %s", ticker, exc)
        return []


def extract_from_batch(raw: pd.DataFrame, tickers: list[str]) -> dict:
    """
    Parse a multi-ticker yfinance DataFrame.
    Returns {ticker: docs_list | None}
    None means: parse failed, caller should fall back to individual download.
    """
    result = {}

    if not isinstance(raw.columns, pd.MultiIndex):
        # Flat frame — only valid if exactly one ticker was requested
        if len(tickers) == 1:
            result[tickers[0]] = _df_to_docs(raw, tickers[0])
        else:
            # Unexpected — mark all for individual fallback
            for t in tickers:
                result[t] = None
        return result

    # yfinance MultiIndex: level 0 = field, level 1 = ticker
    available = set(raw.columns.get_level_values(1).unique())

    for ticker in tickers:
        if ticker not in available:
            log.warning("  %s — missing from MultiIndex, will retry individually.", ticker)
            result[ticker] = None
            continue
        try:
            sub = raw.xs(ticker, axis=1, level=1)
            result[ticker] = _df_to_docs(sub, ticker)
        except Exception as exc:
            log.warning("  %s — xs() failed (%s), will retry individually.", ticker, exc)
            result[ticker] = None

    return result


# ─────────────────────────────────────────────
# Bulk upsert
# ─────────────────────────────────────────────
def upsert_docs(col, docs: list[dict]) -> tuple[int, int]:
    # Hard guard: drop any doc that somehow still has ticker=None
    docs = [d for d in docs if d.get("ticker") is not None]
    if not docs:
        return 0, 0

    ops = [
        UpdateOne(
            {"ticker": d["ticker"], "date": d["date"]},
            {"$set": d},
            upsert=True,
        )
        for d in docs
    ]
    try:
        res = col.bulk_write(ops, ordered=False)
        return res.upserted_count, res.modified_count
    except BulkWriteError as exc:
        log.error("BulkWriteError: %s", exc.details.get("writeErrors", [])[:3])
        return 0, 0


# ─────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────
def main():
    col = get_collection()
    total = len(BIST100_TICKERS)
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    log.info("Starting download for %d BIST100 tickers in %d batches …", total, total_batches)

    ins_total = upd_total = fail_total = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = BIST100_TICKERS[batch_start: batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        log.info("── Batch %d/%d: %s", batch_num, total_batches, batch)

        # Download with retries
        raw = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                raw = yf.download(
                    tickers=batch,
                    period="max",
                    interval="1d",
                    group_by="ticker",
                    auto_adjust=False,
                    progress=False,
                    threads=True,
                )
                break
            except Exception as exc:
                log.warning("  Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_SEC * attempt)

        if raw is None or raw.empty:
            log.error("  Batch %d — all attempts failed, marking tickers as failed.", batch_num)
            fail_total += len(batch)
            continue

        parsed = extract_from_batch(raw, batch)

        for ticker in batch:
            docs = parsed.get(ticker)

            # None = batch parse failed → individual fallback
            if docs is None:
                log.info("  %s — individual fallback …", ticker)
                docs = download_single(ticker)

            if not docs:
                log.warning("  %-15s → 0 records", ticker)
                fail_total += 1
                continue

            ins, upd = upsert_docs(col, docs)
            ins_total += ins
            upd_total += upd
            log.info("  %-15s → %4d rows  (inserted=%d, updated=%d)",
                     ticker, len(docs), ins, upd)

        if batch_start + BATCH_SIZE < total:
            time.sleep(BATCH_DELAY_SEC)

    log.info("=" * 60)
    log.info("DONE  |  Inserted: %d  |  Updated: %d  |  Failed tickers: %d",
             ins_total, upd_total, fail_total)
    log.info("DB: %s  |  Collection: %s", DB_NAME, COLLECTION_NAME)


if __name__ == "__main__":
    main()