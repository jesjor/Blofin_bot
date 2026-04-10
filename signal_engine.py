"""
signal_engine.py — Technical indicator library.
All indicators calculated manually from raw OHLCV data.
No external TA library dependencies = no version breakage risk.
Every function is deterministic and stateless.
"""

import math
import logging
from typing import List, Optional, Tuple

import numpy as np

log = logging.getLogger("signals")


# ── EMA ───────────────────────────────────────────────────────────────────────

def ema(prices: List[float], period: int) -> List[float]:
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(prices[:period]) / period]
    for p in prices[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result


def ema_value(prices: List[float], period: int) -> Optional[float]:
    vals = ema(prices, period)
    return vals[-1] if vals else None


def ema_slope(prices: List[float], period: int, lookback: int = 3) -> float:
    """Positive = trending up, negative = trending down."""
    vals = ema(prices, period)
    if len(vals) < lookback + 1:
        return 0.0
    return (vals[-1] - vals[-1 - lookback]) / vals[-1 - lookback]


# ── SMA ───────────────────────────────────────────────────────────────────────

def sma(prices: List[float], period: int) -> List[float]:
    if len(prices) < period:
        return []
    return [
        sum(prices[i:i + period]) / period
        for i in range(len(prices) - period + 1)
    ]


def sma_value(prices: List[float], period: int) -> Optional[float]:
    vals = sma(prices, period)
    return vals[-1] if vals else None


# ── RSI ───────────────────────────────────────────────────────────────────────

def rsi(prices: List[float], period: int = 14) -> List[float]:
    if len(prices) < period + 1:
        return []
    deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
    gains = [max(0, d) for d in deltas]
    losses = [max(0, -d) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    result = []
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            result.append(100.0)
        else:
            rs = avg_gain / avg_loss
            result.append(100 - 100 / (1 + rs))

    return result


def rsi_value(prices: List[float], period: int = 14) -> Optional[float]:
    vals = rsi(prices, period)
    return vals[-1] if vals else None


# ── ATR ───────────────────────────────────────────────────────────────────────

def atr(highs: List[float], lows: List[float], closes: List[float],
         period: int = 14) -> List[float]:
    if len(highs) < period + 1:
        return []
    trs = []
    for i in range(1, len(highs)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)

    # Wilder smoothing
    result = [sum(trs[:period]) / period]
    for tr in trs[period:]:
        result.append((result[-1] * (period - 1) + tr) / period)
    return result


def atr_value(highs: List[float], lows: List[float], closes: List[float],
               period: int = 14) -> Optional[float]:
    vals = atr(highs, lows, closes, period)
    return vals[-1] if vals else None


# ── Bollinger Bands ───────────────────────────────────────────────────────────

def bollinger_bands(prices: List[float], period: int = 20,
                     std_dev: float = 2.0) -> Tuple[List[float], List[float], List[float]]:
    """Returns (upper, middle, lower) bands."""
    if len(prices) < period:
        return [], [], []
    middles, uppers, lowers = [], [], []
    for i in range(period - 1, len(prices)):
        window = prices[i - period + 1:i + 1]
        mid = sum(window) / period
        std = math.sqrt(sum((p - mid) ** 2 for p in window) / period)
        middles.append(mid)
        uppers.append(mid + std_dev * std)
        lowers.append(mid - std_dev * std)
    return uppers, middles, lowers


def bb_values(prices: List[float], period: int = 20, std_dev: float = 2.0
               ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    u, m, l = bollinger_bands(prices, period, std_dev)
    return (u[-1] if u else None,
            m[-1] if m else None,
            l[-1] if l else None)


# ── ADX ───────────────────────────────────────────────────────────────────────

def adx(highs: List[float], lows: List[float], closes: List[float],
         period: int = 14) -> Optional[float]:
    """Returns current ADX value. Higher = stronger trend."""
    if len(highs) < period * 2:
        return None

    pos_dms, neg_dms, trs = [], [], []
    for i in range(1, len(highs)):
        up   = highs[i]   - highs[i - 1]
        down = lows[i - 1] - lows[i]
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i - 1]),
                 abs(lows[i]  - closes[i - 1]))
        pos_dms.append(up   if up > down and up > 0 else 0)
        neg_dms.append(down if down > up and down > 0 else 0)
        trs.append(tr)

    # Wilder smooth
    def wilder_smooth(vals, p):
        if len(vals) < p:
            return []
        s = [sum(vals[:p])]
        for v in vals[p:]:
            s.append(s[-1] - s[-1] / p + v)
        return s

    atr_sm  = wilder_smooth(trs, period)
    pdm_sm  = wilder_smooth(pos_dms, period)
    ndm_sm  = wilder_smooth(neg_dms, period)

    if not atr_sm:
        return None

    dxs = []
    for a, p, n in zip(atr_sm, pdm_sm, ndm_sm):
        pdi = 100 * p / a if a else 0
        ndi = 100 * n / a if a else 0
        dx  = 100 * abs(pdi - ndi) / (pdi + ndi) if (pdi + ndi) else 0
        dxs.append(dx)

    if len(dxs) < period:
        return None

    # Final Wilder smooth of DX
    adx_val = sum(dxs[-period:]) / period
    return adx_val


# ── Volume ────────────────────────────────────────────────────────────────────

def volume_avg(volumes: List[float], period: int = 20) -> Optional[float]:
    if len(volumes) < period:
        return None
    return sum(volumes[-period:]) / period


def volume_ratio(volumes: List[float], period: int = 20) -> float:
    """Current bar volume / N-bar average."""
    avg = volume_avg(volumes[:-1], period)   # exclude current bar from avg
    if not avg or avg == 0:
        return 0.0
    return volumes[-1] / avg


# ── EMA cross detection ───────────────────────────────────────────────────────

def ema_crossed_above(prices: List[float], fast: int, slow: int,
                       within_bars: int = 5) -> bool:
    """True if fast EMA crossed above slow EMA within the last N bars."""
    if len(prices) < slow + within_bars + 1:
        return False
    fast_vals = ema(prices, fast)
    slow_vals = ema(prices, slow)
    if len(fast_vals) < within_bars + 1 or len(slow_vals) < within_bars + 1:
        return False
    for i in range(-within_bars, 0):
        if fast_vals[i - 1] <= slow_vals[i - 1] and fast_vals[i] > slow_vals[i]:
            return True
    return False


def ema_crossed_below(prices: List[float], fast: int, slow: int,
                       within_bars: int = 5) -> bool:
    if len(prices) < slow + within_bars + 1:
        return False
    fast_vals = ema(prices, fast)
    slow_vals = ema(prices, slow)
    if len(fast_vals) < within_bars + 1 or len(slow_vals) < within_bars + 1:
        return False
    for i in range(-within_bars, 0):
        if fast_vals[i - 1] >= slow_vals[i - 1] and fast_vals[i] < slow_vals[i]:
            return True
    return False


# ── Consolidation / box detection ─────────────────────────────────────────────

def detect_consolidation(highs: List[float], lows: List[float],
                           period: int = 20, max_range_pct: float = 2.5
                           ) -> Tuple[bool, float, float, float]:
    """
    Returns (is_consolidating, box_high, box_low, range_pct).
    is_consolidating = True if range over `period` bars < max_range_pct%.
    """
    if len(highs) < period:
        return False, 0, 0, 0
    window_highs = highs[-period:]
    window_lows  = lows[-period:]
    box_high = max(window_highs)
    box_low  = min(window_lows)
    if box_low == 0:
        return False, 0, 0, 0
    range_pct = (box_high - box_low) / box_low * 100
    return range_pct <= max_range_pct, box_high, box_low, range_pct


# ── Momentum score (for Bot 7) ────────────────────────────────────────────────

def momentum_score(closes: List[float], atr_vals: List[float],
                    period: int = 20) -> float:
    """
    (Close - Close[N]) / ATR[N]
    Higher = stronger upward momentum.
    """
    if len(closes) < period + 1 or len(atr_vals) < 1:
        return 0.0
    price_change = closes[-1] - closes[-(period + 1)]
    current_atr  = atr_vals[-1]
    return price_change / current_atr if current_atr else 0.0


# ── Realized volatility ───────────────────────────────────────────────────────

def realized_vol(closes: List[float], period: int = 20) -> float:
    """Annualized realized volatility as a fraction (not percent)."""
    if len(closes) < period + 1:
        return 0.0
    returns = [math.log(closes[i] / closes[i - 1]) for i in range(-period, 0)]
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    return math.sqrt(variance * 365)


# ── Candle helpers ────────────────────────────────────────────────────────────

def parse_candles(raw: list) -> dict:
    """
    Convert raw BloFin candle list [[ts, o, h, l, c, vol, ...], ...]
    to typed arrays.
    """
    opens, highs, lows, closes, volumes, timestamps = [], [], [], [], [], []
    for row in raw:
        timestamps.append(int(row[0]))
        opens.append(float(row[1]))
        highs.append(float(row[2]))
        lows.append(float(row[3]))
        closes.append(float(row[4]))
        volumes.append(float(row[5]))
    return {
        "timestamps": timestamps,
        "opens":      opens,
        "highs":      highs,
        "lows":       lows,
        "closes":     closes,
        "volumes":    volumes,
    }
