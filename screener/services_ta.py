# screener/services_ta.py
from typing import Dict
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

def compute_signals(df: pd.DataFrame) -> Dict:
    close = df["Close"].astype(float)
    high = df["High"].astype(float)
    low = df["Low"].astype(float)

    rsi = float(RSIIndicator(close, window=14).rsi().iloc[-1])
    atr = AverageTrueRange(high, low, close, window=14).average_true_range().iloc[-1]
    last = float(close.iloc[-1]) if len(close) else 0.0
    atr_pct = float(round(100 * atr / last, 2)) if last else 0.0

    sma20 = close.rolling(20).mean()
    sma200 = close.rolling(200).mean()

    def slope(series, lookback=5):
        y = series.dropna().iloc[-lookback:]
        if len(y) < lookback: return 0.0
        x = np.arange(len(y))
        m = np.polyfit(x, y, 1)[0]
        return float(m / (y.iloc[-1] if y.iloc[-1] else 1.0))

    return {
        "rsi": round(rsi, 1),
        "atr_pct": atr_pct,
        "slope20": round(100 * slope(sma20), 3),
        "slope200": round(100 * slope(sma200), 3),
    }
