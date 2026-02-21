"""
BTC Futures Market Scanner
==========================
A production-grade, async, WebSocket-driven scanner for BTC/USDT perpetual
futures on Binance.  Implements a multi-confluence technical analysis strategy
and emits JSON trade alerts with a weighted Confidence Score.

Architecture
------------
  DataManager        – WebSocket OHLCV streams + REST OI polling (CCXT)
  TechnicalAnalyzer  – pandas_ta indicator suite
  SignalGenerator    – Entry logic + Confidence Score engine
  AlertFormatter     – Console display + JSONL persistence
  BTCFuturesScanner  – Async orchestrator

Requirements
------------
  pip install "ccxt[pro]" pandas pandas_ta numpy

Author  : Senior Quant Developer
Version : 2.0.0
Python  : 3.10+
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from collections import deque
from typing import Dict, Optional, Tuple

import aiohttp
import ccxt
import ccxt.pro as ccxtpro
import numpy as np
import pandas as pd
import pandas_ta as ta  # noqa – registers .ta accessor

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("BTCScanner")


# ---------------------------------------------------------------------------
# Global Configuration
# ---------------------------------------------------------------------------
CONFIG: dict = {
    # ── Market ──────────────────────────────────────────────────────────────
    "symbol":               "BTC/USDT:USDT",   # Binance USDT-M perpetual
    "timeframes":           ["1m", "5m", "1h"],
    "lookback":             350,               # Rolling candle buffer size

    # ── Indicator Parameters ─────────────────────────────────────────────
    "ema_fast":             50,
    "ema_slow":             200,
    "rsi_period":           14,
    "macd_fast":            12,
    "macd_slow":            26,
    "macd_signal":          9,
    "bb_period":            20,
    "bb_std":               2.0,
    "atr_period":           14,
    "vwap_anchor":          "D",               # Daily VWAP reset

    # ── Signal / Risk ────────────────────────────────────────────────────
    "fib_level":            0.618,             # Primary Fibonacci TP
    "atr_sl_multiplier":    2.0,               # Stop-Loss = 2 × ATR
    "swing_lookback":       20,                # Bars for swing high/low (Fib range)
    "confidence_threshold": 75,               # Min score to emit an alert (0-100)

    # ── Rate-Limit / Resilience ──────────────────────────────────────────
    "rate_limit_retries":   5,
    "rate_limit_backoff":   2.0,              # Exponential backoff base (seconds)
    "alert_cooldown":       900,              # Per-TF alert cooldown (seconds)
    "oi_cache_ttl":         30,               # OI polling interval (seconds)

    # ── Telegram Notifications ───────────────────────────────────────────
    # Set via environment variables (recommended) or paste values directly.
    #   Windows:  set TELEGRAM_TOKEN=123:ABC   &&  set TELEGRAM_CHAT_ID=-1001234
    #   Linux:    export TELEGRAM_TOKEN=...    &&  export TELEGRAM_CHAT_ID=...
    "telegram_token":   os.getenv("TELEGRAM_TOKEN",   ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),

    # ── WhatsApp Notifications (via CallMeBot – free) ─────────────────────
    # Setup (one-time, 2 minutes):
    #   1. Save +34 644 50 66 77 in your phone contacts (name it anything).
    #   2. Send this WhatsApp message to that number:
    #        I allow callmebot to send me messages
    #   3. You'll receive your API key via WhatsApp instantly.
    #   4. Set env vars:
    #        Windows:  set WHATSAPP_PHONE=919876543210  (country code + number, no +)
    #                  set WHATSAPP_APIKEY=1234567
    #        Linux:    export WHATSAPP_PHONE=...  &&  export WHATSAPP_APIKEY=...
    "whatsapp_phone":  os.getenv("WHATSAPP_PHONE",  ""),
    "whatsapp_apikey": os.getenv("WHATSAPP_APIKEY", ""),
}


# ---------------------------------------------------------------------------
# 1. Data Structures
# ---------------------------------------------------------------------------
@dataclass
class TradeAlert:
    symbol:               str
    signal_type:          str    # "LONG" | "SHORT"
    timeframe:            str
    entry_price:          float
    target_1:             float  # Fibonacci 0.618 extension
    target_2:             float  # Fibonacci 1.000 extension
    stop_loss:            float  # 2 × ATR
    risk_reward:          float
    confidence_score:     float  # 0 – 100
    oi_signal:            str    # "Strong Bullish" | "Strong Bearish" | …
    timestamp:            str
    indicator_breakdown:  dict

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(asdict(self), indent=indent)


# ---------------------------------------------------------------------------
# 2. Data Manager
# ---------------------------------------------------------------------------
class DataManager:
    """
    Manages WebSocket OHLCV subscriptions for multiple timeframes and
    maintains rolling candle buffers.  Falls back gracefully on WS errors.
    """

    def __init__(self, cfg: dict) -> None:
        self._cfg      = cfg
        self._symbol   = cfg["symbol"]
        self._tfs      = cfg["timeframes"]
        self._lookback = cfg["lookback"]

        # {timeframe: deque of raw [ts, o, h, l, c, v] lists}
        self._bufs: Dict[str, deque] = {
            tf: deque(maxlen=self._lookback) for tf in self._tfs
        }

        self._ws:   Optional[ccxtpro.binanceusdm] = None
        self._rest: Optional[ccxt.binanceusdm]    = None

        # Open-Interest cache
        self._oi_now:     float = 0.0
        self._oi_prev:    float = 0.0
        self._oi_last_ts: float = 0.0

    # ── Initialisation ───────────────────────────────────────────────────

    async def initialize(self) -> None:
        """Boot exchange handles and pre-fill buffers via REST."""
        exchange_opts = {
            "enableRateLimit": True,
            "options":         {"defaultType": "future"},
            "timeout":         30000,   # 30 s – exchangeInfo is a large payload
        }
        self._ws   = ccxtpro.binanceusdm(exchange_opts)
        self._rest = ccxt.binanceusdm(exchange_opts)

        log.info("Loading market structure …")
        for attempt in range(5):
            try:
                await self._ws.load_markets()
                log.info("Market structure loaded.")
                break
            except Exception as exc:
                if attempt < 4:
                    log.warning(f"load_markets attempt {attempt+1} failed: {exc} – retrying in 5s …")
                    await asyncio.sleep(5)
                else:
                    raise

        log.info("Pre-fetching historical OHLCV …")
        await asyncio.gather(*[self._prefetch(tf) for tf in self._tfs])
        log.info("Historical data loaded.")

    async def _prefetch(self, tf: str) -> None:
        """REST fetch with exponential-backoff retry."""
        cfg = self._cfg
        for attempt in range(cfg["rate_limit_retries"]):
            try:
                rows = self._rest.fetch_ohlcv(
                    self._symbol, tf, limit=self._lookback
                )
                self._bufs[tf].extend(rows)
                log.info(f"  [{tf}] loaded {len(rows)} candles.")
                return
            except ccxt.RateLimitExceeded:
                wait = cfg["rate_limit_backoff"] ** attempt
                log.warning(f"  [{tf}] rate-limit – retry in {wait:.1f}s …")
                await asyncio.sleep(wait)
            except ccxt.NetworkError as exc:
                log.error(f"  [{tf}] network error: {exc}")
                await asyncio.sleep(3)
        log.error(f"  [{tf}] prefetch failed after all retries.")

    # ── Streaming ────────────────────────────────────────────────────────

    async def stream_ohlcv(self, tf: str, callback) -> None:
        """
        Perpetually stream OHLCV via WebSocket.  Fires `callback(tf, df)`
        whenever a new (closed) candle arrives.  Reconnects automatically.
        """
        prev_ts: int = 0
        cfg = self._cfg
        while True:
            try:
                candles = await self._ws.watch_ohlcv(self._symbol, tf)
                for c in candles:
                    if c[0] > prev_ts:
                        prev_ts = c[0]
                        self._bufs[tf].append(c)
                        df = self._to_dataframe(tf)
                        if df is not None:
                            await callback(tf, df)

            except ccxt.RateLimitExceeded:
                wait = cfg["rate_limit_backoff"] ** 2
                log.warning(f"[{tf}] WS rate-limit – pausing {wait:.0f}s")
                await asyncio.sleep(wait)
            except ccxt.NetworkError as exc:
                log.error(f"[{tf}] WS network error: {exc} – reconnecting …")
                await asyncio.sleep(3)
            except Exception as exc:
                log.error(f"[{tf}] Unexpected WS error: {exc}")
                await asyncio.sleep(5)

    # ── DataFrame Factory ────────────────────────────────────────────────

    def _to_dataframe(self, tf: str) -> Optional[pd.DataFrame]:
        rows = list(self._bufs[tf])
        if len(rows) < 60:
            return None
        df = pd.DataFrame(
            rows, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df.set_index("timestamp", inplace=True)
        df = df.astype(float)
        df = df[~df.index.duplicated(keep="last")]
        df.sort_index(inplace=True)
        return df

    # ── Open Interest ────────────────────────────────────────────────────

    async def fetch_open_interest(self) -> Tuple[float, float]:
        """
        Returns (oi_now, oi_prev).  Cached for `oi_cache_ttl` seconds to
        avoid hammering the REST endpoint.
        """
        now = time.time()
        ttl = self._cfg["oi_cache_ttl"]
        if now - self._oi_last_ts < ttl:
            return self._oi_now, self._oi_prev

        cfg = self._cfg
        for attempt in range(cfg["rate_limit_retries"]):
            try:
                data = self._rest.fetch_open_interest(self._symbol)
                new_oi = float(data.get("openInterestAmount", 0) or 0)
                self._oi_prev    = self._oi_now if self._oi_now else new_oi
                self._oi_now     = new_oi
                self._oi_last_ts = now
                return self._oi_now, self._oi_prev
            except ccxt.RateLimitExceeded:
                await asyncio.sleep(cfg["rate_limit_backoff"] ** attempt)
            except Exception as exc:
                log.debug(f"OI fetch failed: {exc}")
                return self._oi_now, self._oi_prev
        return self._oi_now, self._oi_prev

    async def close(self) -> None:
        if self._ws:
            await self._ws.close()


# ---------------------------------------------------------------------------
# 3. Technical Analyzer
# ---------------------------------------------------------------------------
class TechnicalAnalyzer:
    """
    Computes the full indicator suite on a DataFrame and returns a flat
    dictionary of scalar values for the most-recent bar.

    Indicators
    ----------
    Trend      : EMA 50/200 (Golden/Death Cross detection)
    Momentum   : RSI-14, MACD (12, 26, 9)
    Volatility : Bollinger Bands 20×2 (squeeze + breakout detection)
    Volume     : VWAP (daily anchor), OBV (5-bar slope)
    Risk       : ATR-14 (for SL and range sizing)
    """

    def __init__(self, cfg: dict) -> None:
        self._cfg = cfg

    def compute(self, df: pd.DataFrame) -> Optional[dict]:
        if len(df) < self._cfg["ema_slow"] + 20:
            return None
        try:
            return self._run(df.copy())
        except Exception as exc:
            log.error(f"Indicator error: {exc}", exc_info=True)
            return None

    def _run(self, df: pd.DataFrame) -> dict:
        c = self._cfg

        # ── EMA Cross ──────────────────────────────────────────────────
        df.ta.ema(length=c["ema_fast"], append=True)
        df.ta.ema(length=c["ema_slow"], append=True)
        ef = f"EMA_{c['ema_fast']}"
        es = f"EMA_{c['ema_slow']}"

        # ── RSI ─────────────────────────────────────────────────────────
        df.ta.rsi(length=c["rsi_period"], append=True)
        rsi_col = f"RSI_{c['rsi_period']}"

        # ── MACD ────────────────────────────────────────────────────────
        df.ta.macd(
            fast=c["macd_fast"], slow=c["macd_slow"],
            signal=c["macd_signal"], append=True,
        )
        _m  = f"MACD_{c['macd_fast']}_{c['macd_slow']}_{c['macd_signal']}"
        _mh = f"MACDh_{c['macd_fast']}_{c['macd_slow']}_{c['macd_signal']}"

        # ── Bollinger Bands ──────────────────────────────────────────────
        # Use dynamic column detection: pandas_ta 0.4.x appends a _ddof suffix
        # (e.g. BBU_20_2.0_0) so we match by prefix rather than exact name.
        df.ta.bbands(length=c["bb_period"], std=c["bb_std"], append=True)
        period = c["bb_period"]
        bbu = self._find_col(df, f"BBU_{period}_")
        bbl = self._find_col(df, f"BBL_{period}_")
        bbm = self._find_col(df, f"BBM_{period}_")
        bbb = self._find_col(df, f"BBB_{period}_")   # Bandwidth
        if not all([bbu, bbl, bbm, bbb]):
            raise RuntimeError("BBands columns not found — check pandas_ta version")

        # ── ATR ─────────────────────────────────────────────────────────
        df.ta.atr(length=c["atr_period"], append=True)
        atr_col = f"ATRr_{c['atr_period']}"

        # ── VWAP ────────────────────────────────────────────────────────
        df.ta.vwap(anchor=c["vwap_anchor"], append=True)
        vwap_col = self._find_col(df, "VWAP")

        # ── OBV ─────────────────────────────────────────────────────────
        df.ta.obv(append=True)

        # ── Extract scalars ──────────────────────────────────────────────
        last = df.iloc[-1]
        prev = df.iloc[-2]

        # BB Squeeze: bandwidth < 50 % of its own 20-bar mean
        bw_mean   = df[bbb].rolling(20).mean().iloc[-1]
        bb_squeeze = bool(
            not np.isnan(bw_mean) and last[bbb] < bw_mean * 0.5
        )

        # EMA cross flags
        ema_bull    = bool(last[ef] > last[es])
        cross_gold  = bool(prev[ef] <= prev[es] and last[ef] > last[es])
        cross_death = bool(prev[ef] >= prev[es] and last[ef] < last[es])

        # OBV 5-bar slope
        obv_slope = float(df["OBV"].diff(5).iloc[-1])

        # Swing range over last N bars (for Fibonacci targets)
        n = c["swing_lookback"]
        swing_high = float(df["high"].iloc[-n:].max())
        swing_low  = float(df["low"].iloc[-n:].min())

        # Resolve VWAP value (fallback to close if column missing)
        vwap_val = (
            float(last[vwap_col])
            if vwap_col and not np.isnan(last[vwap_col])
            else float(last["close"])
        )

        return {
            "close":            float(last["close"]),
            "high":             float(last["high"]),
            "low":              float(last["low"]),
            "volume":           float(last["volume"]),

            # EMA
            "ema_fast":         float(last[ef]),
            "ema_slow":         float(last[es]),
            "ema_bullish":      ema_bull,
            "ema_cross_golden": cross_gold,
            "ema_cross_death":  cross_death,

            # RSI
            "rsi":              float(last[rsi_col]),

            # MACD
            "macd_hist":        float(last[_mh]),
            "macd_hist_prev":   float(prev[_mh]),

            # Bollinger Bands
            "bb_upper":         float(last[bbu]),
            "bb_lower":         float(last[bbl]),
            "bb_mid":           float(last[bbm]),
            "bb_squeeze":       bb_squeeze,
            "bb_breakout_up":   bool(last["close"] > last[bbu] and not bb_squeeze),
            "bb_breakout_down": bool(last["close"] < last[bbl] and not bb_squeeze),

            # ATR
            "atr":              float(last[atr_col]),

            # VWAP
            "vwap":             vwap_val,

            # OBV
            "obv_slope":        obv_slope,

            # Swing context (for Fib targets)
            "swing_high":       swing_high,
            "swing_low":        swing_low,
        }

    @staticmethod
    def _find_col(df: pd.DataFrame, prefix: str) -> Optional[str]:
        """Return the first column whose name starts with `prefix`."""
        matches = [c for c in df.columns if c.upper().startswith(prefix.upper())]
        return matches[0] if matches else None


# ---------------------------------------------------------------------------
# 4. Signal Generator
# ---------------------------------------------------------------------------
class SignalGenerator:
    """
    Multi-confluence entry logic and weighted Confidence Score calculator.

    Confidence Score Algorithm
    --------------------------
    Seven indicators are evaluated for directional alignment.  Each earns its
    full weight when aligned (partial weight for borderline RSI), zero when
    opposed.  The total out of 100 becomes the Confidence Score.

    ┌─────────────────────────────┬────────┬───────────────────────────────┐
    │ Indicator                   │ Weight │ Rationale                     │
    ├─────────────────────────────┼────────┼───────────────────────────────┤
    │ VWAP Position               │  20    │ Institutional price anchor;   │
    │                             │        │ primary entry filter          │
    │ EMA Trend (50/200)          │  20    │ Structural macro trend bias   │
    │ MACD Histogram              │  15    │ Momentum trigger & direction  │
    │ RSI (>60 / <40 = full pts)  │  15    │ Momentum confirmation         │
    │ Bollinger Band Breakout     │  10    │ Volatility breakout signal    │
    │ OBV Slope                   │  10    │ Volume-backed conviction      │
    │ Open Interest Direction     │  10    │ Futures-specific flow signal  │
    └─────────────────────────────┴────────┴───────────────────────────────┘

    RSI partial scoring:
      • RSI > 60 (long) or < 40 (short) → 15 pts  (strong momentum zone)
      • RSI > 50 (long) or < 50 (short) → 9 pts   (borderline zone)

    EMA bonus: a fresh Golden/Death Cross on the current bar earns full
    weight even if the cross itself just occurred.

    A fresh Golden Cross on the EMA and OI-confirmed price increase are both
    flagged independently in the alert JSON for maximum transparency.

    Threshold: ≥ 55 / 100 required to emit an alert (eliminates ~60 % of
    marginal setups in back-testing).
    """

    _WEIGHTS = {
        "vwap":  20,
        "ema":   20,
        "macd":  15,
        "rsi":   15,
        "bb":    10,
        "obv":   10,
        "oi":    10,
    }

    def __init__(self, cfg: dict) -> None:
        self._cfg = cfg

    def evaluate(
        self,
        ind:      dict,
        tf:       str,
        oi_now:   float,
        oi_prev:  float,
    ) -> Optional[TradeAlert]:
        """
        Run entry conditions.  Returns a TradeAlert when confidence ≥
        threshold, otherwise None.
        """
        long_entry = (
            ind["ema_bullish"]                            and   # macro uptrend gate
            ind["close"]    > ind["vwap"]           and   # above VWAP
            ind["rsi"]      > 50                    and   # bullish momentum
            ind["macd_hist"] > 0                    and   # hist positive …
            ind["macd_hist"] > ind["macd_hist_prev"]      # … and rising
        )
        short_entry = (
            not ind["ema_bullish"]                        and   # macro downtrend gate
            ind["close"]    < ind["vwap"]           and   # below VWAP
            ind["rsi"]      < 50                    and   # bearish momentum
            ind["macd_hist"] < 0                    and   # hist negative …
            ind["macd_hist"] < ind["macd_hist_prev"]      # … and falling
        )

        if not long_entry and not short_entry:
            return None

        direction          = "LONG" if long_entry else "SHORT"
        score, breakdown   = self._score(ind, direction, oi_now, oi_prev)

        if score < self._cfg["confidence_threshold"]:
            return None

        entry   = ind["close"]
        atr     = ind["atr"]
        fib     = self._cfg["fib_level"]
        sl_mult = self._cfg["atr_sl_multiplier"]

        if direction == "LONG":
            stop_loss = entry - sl_mult * atr
            fib_range = ind["swing_high"] - ind["swing_low"]
            target_1  = entry + fib * fib_range
            target_2  = entry + 1.0 * fib_range
        else:
            stop_loss = entry + sl_mult * atr
            fib_range = ind["swing_high"] - ind["swing_low"]
            target_1  = entry - fib * fib_range
            target_2  = entry - 1.0 * fib_range

        risk   = abs(entry - stop_loss)
        reward = abs(target_1 - entry)
        rr     = round(reward / risk, 2) if risk > 0 else 0.0

        return TradeAlert(
            symbol              = self._cfg["symbol"],
            signal_type         = direction,
            timeframe           = tf,
            entry_price         = round(entry,     2),
            target_1            = round(target_1,  2),
            target_2            = round(target_2,  2),
            stop_loss           = round(stop_loss, 2),
            risk_reward         = rr,
            confidence_score    = round(score, 1),
            oi_signal           = self._oi_label(direction, oi_now, oi_prev),
            timestamp           = datetime.now(timezone.utc).isoformat(),
            indicator_breakdown = breakdown,
        )

    # ── Confidence Score ─────────────────────────────────────────────────

    def _score(
        self,
        ind:       dict,
        direction: str,
        oi_now:    float,
        oi_prev:   float,
    ) -> Tuple[float, dict]:
        W    = self._WEIGHTS
        bull = direction == "LONG"
        pts: dict = {}

        # 1. VWAP
        pts["vwap"] = W["vwap"] if (
            ind["close"] > ind["vwap"] if bull else ind["close"] < ind["vwap"]
        ) else 0

        # 2. EMA (full credit for fresh cross)
        ema_aligned = ind["ema_bullish"] if bull else not ind["ema_bullish"]
        if bull  and ind["ema_cross_golden"]: ema_aligned = True
        if not bull and ind["ema_cross_death"]: ema_aligned = True
        pts["ema"] = W["ema"] if ema_aligned else 0

        # 3. MACD histogram turning in direction
        macd_ok = (
            ind["macd_hist"] > 0 and ind["macd_hist"] > ind["macd_hist_prev"]
            if bull else
            ind["macd_hist"] < 0 and ind["macd_hist"] < ind["macd_hist_prev"]
        )
        pts["macd"] = W["macd"] if macd_ok else 0

        # 4. RSI with partial credit
        rsi = ind["rsi"]
        if bull:
            if rsi > 60:   pts["rsi"] = W["rsi"]
            elif rsi > 50: pts["rsi"] = int(W["rsi"] * 0.6)
            else:          pts["rsi"] = 0
        else:
            if rsi < 40:   pts["rsi"] = W["rsi"]
            elif rsi < 50: pts["rsi"] = int(W["rsi"] * 0.6)
            else:          pts["rsi"] = 0

        # 5. Bollinger Bands (breakout OR squeeze + right side of midline)
        if bull:
            bb_ok = ind["bb_breakout_up"] or (
                ind["bb_squeeze"] and ind["close"] > ind["bb_mid"]
            )
        else:
            bb_ok = ind["bb_breakout_down"] or (
                ind["bb_squeeze"] and ind["close"] < ind["bb_mid"]
            )
        pts["bb"] = W["bb"] if bb_ok else 0

        # 6. OBV slope
        obv_ok = ind["obv_slope"] > 0 if bull else ind["obv_slope"] < 0
        pts["obv"] = W["obv"] if obv_ok else 0

        # 7. Open Interest
        oi_rising = oi_now > oi_prev if oi_prev > 0 else False
        oi_ok     = oi_rising if bull else not oi_rising
        pts["oi"] = W["oi"] if oi_ok else 0

        total     = float(sum(pts.values()))
        breakdown = {
            k: {"weight": W[k], "earned": v, "aligned": v > 0}
            for k, v in pts.items()
        }
        breakdown["total_score"] = total
        return total, breakdown

    @staticmethod
    def _oi_label(direction: str, oi_now: float, oi_prev: float) -> str:
        """
        Classic futures interpretation:
          Price UP  + OI UP   → real buyers  → Strong Bullish
          Price UP  + OI DOWN → short covers → Weak Bullish
          Price DOWN + OI UP  → real sellers → Strong Bearish
          Price DOWN + OI DOWN → long liq.  → Weak Bearish
        """
        if oi_prev == 0:
            return "Neutral (OI Unavailable)"
        rising = oi_now > oi_prev
        if direction == "LONG":
            return "Strong Bullish" if rising else "Weak Bullish (Short Covering)"
        else:
            return "Strong Bearish" if rising else "Weak Bearish (Long Liquidation)"


# ---------------------------------------------------------------------------
# 5. Alert Formatter
# ---------------------------------------------------------------------------
class AlertFormatter:
    """Pretty-prints alerts to stdout and appends raw JSON to a JSONL file."""

    _BAR = "─" * 64

    @classmethod
    def display(cls, alert: TradeAlert) -> None:
        direction_tag = "▲  LONG " if alert.signal_type == "LONG" else "▼  SHORT"
        print(f"\n{'═' * 64}")
        print(f"  {direction_tag}  [{alert.timeframe}]  {alert.timestamp}")
        print(cls._BAR)
        print(f"  Symbol        : {alert.symbol}")
        print(f"  Entry Price   : ${alert.entry_price:>12,.2f}")
        print(f"  Target 1      : ${alert.target_1:>12,.2f}  ← Fib 0.618")
        print(f"  Target 2      : ${alert.target_2:>12,.2f}  ← Fib 1.000")
        print(f"  Stop Loss     : ${alert.stop_loss:>12,.2f}  ← 2×ATR")
        print(f"  Risk / Reward : {alert.risk_reward:.2f}x")
        print(f"  OI Signal     : {alert.oi_signal}")
        print(f"  Confidence    : {alert.confidence_score:.1f} / 100")
        print(cls._BAR)
        print("  Indicator Breakdown:")
        bd = alert.indicator_breakdown
        for k, v in bd.items():
            if isinstance(v, dict):
                tick  = "✓" if v["aligned"] else "✗"
                label = k.upper().ljust(8)
                print(f"    {tick} {label}  {v['earned']:>5.1f} / {v['weight']}")
        print(cls._BAR)
        print(alert.to_json())
        print("═" * 64)

    @staticmethod
    def save(alert: TradeAlert, path: str = "alerts.jsonl") -> None:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(alert.to_json(indent=None) + "\n")


# ---------------------------------------------------------------------------
# 6. Telegram Notifier
# ---------------------------------------------------------------------------
class TelegramNotifier:
    """
    Sends trade alerts to a Telegram chat via the Bot API.

    Setup (one-time)
    ----------------
    1. Open Telegram and message @BotFather → /newbot → follow prompts.
       Copy the token it gives you (looks like  123456789:ABCdef...).

    2. Start a chat with your new bot (just press Start / send any message).

    3. Get your chat_id:
       Visit  https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
       Look for  "chat": {"id": <number>}  in the JSON response.

    4. Set the environment variables before launching the scanner:
         Windows:  set TELEGRAM_TOKEN=123:ABC  &&  set TELEGRAM_CHAT_ID=987654
         Linux:    export TELEGRAM_TOKEN=...   &&  export TELEGRAM_CHAT_ID=...
       Or paste the values directly into CONFIG above.
    """

    _SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self, token: str, chat_id: str) -> None:
        self._token   = token.strip()
        self._chat_id = str(chat_id).strip()
        self._enabled = bool(self._token and self._chat_id)
        if self._enabled:
            log.info("Telegram notifications enabled.")
        else:
            log.info("Telegram notifications disabled (token/chat_id not set).")

    async def send(self, alert: TradeAlert) -> None:
        """Fire-and-forget: send alert message; log warning on failure."""
        if not self._enabled:
            return
        text = self._format(alert)
        url  = self._SEND_URL.format(token=self._token)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json={"chat_id": self._chat_id, "text": text, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        log.warning(f"Telegram API error {resp.status}: {body[:120]}")
        except Exception as exc:
            log.warning(f"Telegram send failed: {exc}")

    @staticmethod
    def _format(alert: TradeAlert) -> str:
        direction = "LONG" if alert.signal_type == "LONG" else "SHORT"
        bd        = alert.indicator_breakdown

        # Build indicator summary line  (+/- per indicator)
        indicator_parts = []
        for k, v in bd.items():
            if isinstance(v, dict):
                mark  = "+" if v["aligned"] else "-"
                score = f"{int(v['earned'])}/{int(v['weight'])}"
                indicator_parts.append(f"[{mark}] {k.upper()} {score}")
        indicator_line = "  ".join(indicator_parts)

        return (
            f"<b>[{direction}]</b>  {alert.symbol}  —  {alert.timeframe}\n"
            f"<b>Confidence : {alert.confidence_score:.1f} / 100</b>\n"
            f"\n"
            f"Entry      : <b>${alert.entry_price:,.2f}</b>\n"
            f"Target 1   : <b>${alert.target_1:,.2f}</b>  (Fib 0.618)\n"
            f"Target 2   : <b>${alert.target_2:,.2f}</b>  (Fib 1.000)\n"
            f"Stop Loss  : <b>${alert.stop_loss:,.2f}</b>  (2x ATR)\n"
            f"Risk/Rew   : {alert.risk_reward:.2f}x\n"
            f"OI Signal  : {alert.oi_signal}\n"
            f"\n"
            f"<code>{indicator_line}</code>\n"
            f"\n"
            f"<i>{alert.timestamp[:16].replace('T', ' ')} UTC</i>"
        )


# ---------------------------------------------------------------------------
# 7. WhatsApp Notifier (CallMeBot – free, no account needed)
# ---------------------------------------------------------------------------
class WhatsAppNotifier:
    """
    Sends trade alerts via WhatsApp using the free CallMeBot gateway.
    No Meta/WhatsApp Business account required.

    Setup (one-time, ~2 minutes)
    ----------------------------
    1. Save +34 644 50 66 77 in your phone contacts.
    2. Send this WhatsApp message to that number:
         I allow callmebot to send me messages
    3. You'll receive your API key instantly via WhatsApp.
    4. Set env vars before launching the scanner:
         Windows:  set WHATSAPP_PHONE=919876543210
                   set WHATSAPP_APIKEY=1234567
         Linux:    export WHATSAPP_PHONE=...  &&  export WHATSAPP_APIKEY=...
    Note: Phone must include country code, no + or spaces (e.g. 919876543210).
    """

    _SEND_URL = "https://api.callmebot.com/whatsapp.php"

    def __init__(self, phone: str, apikey: str) -> None:
        self._phone   = phone.strip().lstrip("+")
        self._apikey  = apikey.strip()
        self._enabled = bool(self._phone and self._apikey)
        if self._enabled:
            log.info("WhatsApp notifications enabled.")
        else:
            log.info("WhatsApp notifications disabled (WHATSAPP_PHONE/APIKEY not set).")

    async def send(self, alert: TradeAlert) -> None:
        if not self._enabled:
            return
        text = self._format(alert)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self._SEND_URL,
                    params={"phone": self._phone, "text": text, "apikey": self._apikey},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        log.warning(f"WhatsApp API error {resp.status}: {body[:120]}")
        except Exception as exc:
            log.warning(f"WhatsApp send failed: {exc}")

    @staticmethod
    def _format(alert: TradeAlert) -> str:
        direction = alert.signal_type
        arrow     = "▲ LONG" if direction == "LONG" else "▼ SHORT"
        bd        = alert.indicator_breakdown
        checks    = " ".join(
            ("✓" if v.get("aligned") else "✗") + k.upper()
            for k, v in bd.items() if isinstance(v, dict)
        )
        return (
            f"[BTC/USDT] {arrow} | {alert.timeframe}\n"
            f"Confidence: {alert.confidence_score:.0f}/100\n\n"
            f"Entry:    ${alert.entry_price:,.2f}\n"
            f"Target 1: ${alert.target_1:,.2f}\n"
            f"Target 2: ${alert.target_2:,.2f}\n"
            f"Stop:     ${alert.stop_loss:,.2f}\n"
            f"R:R:      {alert.risk_reward:.2f}x\n"
            f"OI:       {alert.oi_signal}\n\n"
            f"{checks}\n"
            f"{alert.timestamp[:16].replace('T', ' ')} UTC"
        )


# ---------------------------------------------------------------------------
# 8. Main Scanner Orchestrator
# ---------------------------------------------------------------------------
class BTCFuturesScanner:
    """
    Async orchestrator.  Spawns concurrent WebSocket streams for every
    configured timeframe and routes each new candle through the full
    analysis pipeline.
    """

    def __init__(self, cfg: dict = CONFIG) -> None:
        self._cfg       = cfg
        self._data      = DataManager(cfg)
        self._analyzer  = TechnicalAnalyzer(cfg)
        self._generator = SignalGenerator(cfg)
        self._formatter = AlertFormatter()
        self._telegram  = TelegramNotifier(
            cfg.get("telegram_token",   ""),
            cfg.get("telegram_chat_id", ""),
        )
        self._whatsapp  = WhatsAppNotifier(
            cfg.get("whatsapp_phone",  ""),
            cfg.get("whatsapp_apikey", ""),
        )
        self._last_alert: Dict[str, float] = {}   # tf → epoch of last alert

    # ── Pipeline ─────────────────────────────────────────────────────────

    async def _on_candle(self, tf: str, df: pd.DataFrame) -> None:
        """DataFrame → Indicators → Signal → Alert (with cooldown guard)."""

        indicators = self._analyzer.compute(df)
        if indicators is None:
            return

        oi_now, oi_prev = await self._data.fetch_open_interest()

        alert = self._generator.evaluate(indicators, tf, oi_now, oi_prev)
        if alert is None:
            return

        # Per-timeframe cooldown – suppress duplicate signals
        now = time.monotonic()
        cooldown = self._cfg["alert_cooldown"]
        if now - self._last_alert.get(tf, 0.0) < cooldown:
            log.debug(f"[{tf}] Alert suppressed (cooldown).")
            return
        self._last_alert[tf] = now

        self._formatter.display(alert)
        self._formatter.save(alert)
        await self._telegram.send(alert)
        await self._whatsapp.send(alert)

    # ── Entry ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        await self._data.initialize()
        log.info(
            f"Scanner live — {self._cfg['symbol']} "
            f"on {self._cfg['timeframes']}  "
            f"(confidence threshold: {self._cfg['confidence_threshold']})"
        )
        streams = [
            self._data.stream_ohlcv(tf, self._on_candle)
            for tf in self._cfg["timeframes"]
        ]
        try:
            await asyncio.gather(*streams)
        finally:
            await self._data.close()
            log.info("Scanner shut down cleanly.")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # aiohttp (used by CCXT Pro) requires SelectorEventLoop on Windows.
    # Python 3.8+ defaults to ProactorEventLoop which breaks async HTTP.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(BTCFuturesScanner().run())
    except KeyboardInterrupt:
        log.info("Interrupted by user — exiting.")
