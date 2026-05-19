# BIST100-Data-Pipeline
A robust Python data pipeline that downloads historical OHLCV data for BIST100 stocks using yfinance and stores it efficiently in MongoDB with built-in deduplication.

# BIST100 Historical Stock Data Downloader to MongoDB

A professional-grade data pipeline designed to fetch full historical daily OHLCV (Open, High, Low, Close, Volume) data for all BIST100 companies and store them in a MongoDB instance with high integrity.

## 🚀 Key Features
- **Dynamic Data Fetching:** Utilizes `yfinance` to retrieve historical data for the entire BIST100 index.
- **Smart Storage:** Implements a **Unique Compound Index** (ticker + date) in MongoDB to prevent duplicate records.
- **Batch Processing:** Downloads data in configurable batches to ensure stability and respect API rate limits.
- **Fail-Safe Mechanism:** Includes an individual fallback mode that automatically retries single tickers if batch downloads fail.
- **Professional Logging:** Full execution transparency with timestamped logs for monitoring insertion and update counts.

## 🛠️ Technical Stack
- **Language:** Python
- **Database:** MongoDB
- **Libraries:** `yfinance`, `pymongo`, `pandas`, `requests`

## 📋 Installation & Usage

1. **Prerequisites:**
   ```bash
   pip install yfinance pymongo pandas requests
