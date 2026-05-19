# BIST100-Data-Pipeline
A robust Python data pipeline that downloads historical OHLCV data for BIST100 stocks using yfinance and stores it efficiently in MongoDB with built-in deduplication.

## Project structure

```text
bist100_pipeline/
  downloader.py      # yfinance download layer
  storage.py         # MongoDB integration layer
scripts/
  download_bist100.py
requirements.txt
```

## Run

```bash
pip install -r requirements.txt
python scripts/download_bist100.py --symbols ^XU100 --start 2024-01-01 --end 2024-12-31
```
