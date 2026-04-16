import requests
import json
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# Constants
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
INTERVAL = "1m"
EXPECTED_FIELDS = 12
RAW_DIR = Path("data/raw_responses")
STREAMING_DIR = Path("data/streaming")


def fetch_raw(symbol: str) -> list | None:
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": INTERVAL, "limit": 1}
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            print(f"[ERROR] {symbol} — HTTP {response.status_code}")
            return None
        data = response.json()
        if not data:
            print(f"[ERROR] {symbol} — empty response")
            return None
        return data[0]  # raw list, no parsing yet
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] {symbol} — request failed: {e}")
        return None


def save_raw(symbol: str, raw: list, timestamp: str):
    folder = RAW_DIR / symbol
    folder.mkdir(parents=True, exist_ok=True)
    filepath = folder / f"{symbol}_{timestamp}.json"
    with open(filepath, "w") as f:
        json.dump(raw, f)


def validate_raw(raw: list) -> bool:
    if len(raw) != EXPECTED_FIELDS:
        print(f"[VALIDATION FAILED] Expected {EXPECTED_FIELDS} fields, got {len(raw)}")
        return False
    try:
        open_time = int(raw[0])
        low       = float(raw[3])
        close     = float(raw[4])
        high      = float(raw[2])
        volume    = float(raw[5])
        trades    = int(raw[8])
        if open_time <= 0:
            print("[VALIDATION FAILED] open_time must be positive")
            return False
        if not (low <= close <= high):
            print("[VALIDATION FAILED] price relationship broken: low <= close <= high")
            return False
        if volume < 0:
            print("[VALIDATION FAILED] volume cannot be negative")
            return False
        if trades < 0:
            print("[VALIDATION FAILED] number_of_trades cannot be negative")
            return False
    except (ValueError, TypeError) as e:
        print(f"[VALIDATION FAILED] type error during validation: {e}")
        return False
    return True


def parse_to_dict(raw: list) -> dict:
    return {
        "open_time":            int(raw[0]),
        "open":                 float(raw[1]),
        "high":                 float(raw[2]),
        "low":                  float(raw[3]),
        "close":                float(raw[4]),
        "volume":               float(raw[5]),
        "close_time":           int(raw[6]),
        "quote_volume":         float(raw[7]),
        "trade_count":          int(raw[8]),
        "taker_buy_volume":       float(raw[9]),
        "taker_buy_quote":      float(raw[10]),
        "ignore":               raw[11],
    }


def save_csv(symbol: str, candle: dict, timestamp: str):
    folder = STREAMING_DIR / symbol
    folder.mkdir(parents=True, exist_ok=True)
    filepath = folder / f"{symbol}_{timestamp}.csv"
    pd.DataFrame([candle]).to_csv(filepath, index=False)
    print(f"[SAVED] {symbol} → {filepath}")


def run():
    print("Binance feed starting...")
    while True:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        for symbol in SYMBOLS:
            raw = fetch_raw(symbol)
            if raw is None:
                continue
            save_raw(symbol, raw, timestamp)       # save ground truth first
            if not validate_raw(raw):
                continue
            candle = parse_to_dict(raw)
            save_csv(symbol, candle, timestamp)
        time.sleep(60)


if __name__ == "__main__":
    run()