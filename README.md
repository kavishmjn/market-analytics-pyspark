# Market Analytics — PySpark

End-to-end crypto market analytics pipeline built with PySpark, analyzing BTC/USDT 1-minute candlestick data to detect market patterns and trading signals.

## Project Structure

crypto-spark/
├── data/
│   └── raw/              # Binance OHLCV CSVs (not tracked in git)
├── src/
│   ├── session.py        # SparkSession setup
│   ├── ingestion.py      # Data loading and schema definition
│   ├── transformations.py # Core metrics derivation
│   └── windows.py        # Rolling and lag metrics
├── output/               # Pipeline outputs
├── main.py               # Pipeline orchestration
└── requirements.txt


## Tech Stack

- Python 3.12
- PySpark 3.5.3
- Java 11

## Dataset

Binance public 1-minute OHLCV candlestick data for BTC/USDT
Source: https://data.binance.vision/?prefix=data/spot/monthly/klines/BTCUSDT/1m/

## Milestones

- [x] M1 — Ingestion & Schema definition
- [x] M2 — Core transformations (returns, VWAP, taker ratio)
- [x] M3 — Window functions (rolling metrics, lag, momentum)
- [ ] M4 — Pump & dump detection
- [ ] M5 — Performance tuning & multi-coin analysis

## How to Run

# Create and activate virtual environment
python -m venv SparkVenv
SparkVenv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Add Binance CSVs to data/raw/ then run
python main.py

## Metrics Computed

| Metric | Description |
|---|---|
| `returns` | % price change per candle |
| `price_range` | High minus low per candle |
| `typical_price` | Average of high, low, close |
| `taker_ratio` | Buying vs selling pressure (0-1) |
| `vwap_simple` | Volume weighted average price |
| `rolling_avg_volume` | 20 candle rolling average volume |
| `rolling_volatility` | 20 candle rolling std dev of returns |
| `rolling_vwap` | 7 candle rolling VWAP |
| `prev_close` | Previous candle close price |
| `price_momentum` | Close price 5 candles ago |
| `consecutive_green` | Both current and previous candle green |