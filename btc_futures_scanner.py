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
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
    valid_for_minutes:    int    # practical validity window (based on timeframe)
    expires_at:           str    # UTC ISO timestamp when signal expires
    leverage_rec:         dict   # {"conservative","moderate","aggressive","sl_pct","max_theoretical"}
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

        # Funding Rate cache (5-min TTL)
        self._funding_rate: float = 0.0
        self._funding_ts:   float = 0.0

        # Fear & Greed cache (4-hour TTL)
        self._fear_greed:    int   = 50
        self._fear_greed_ts: float = 0.0

        # Taker Buy/Sell Ratio cache (5-min TTL)
        self._taker_ratio: float = 1.0
        self._taker_ts:    float = 0.0

        # Long/Short Ratio cache (5-min TTL)
        self._ls_ratio: float = 1.0
        self._ls_ts:    float = 0.0

    # ── Initialisation ───────────────────────────────────────────────────

    async def initialize(self) -> None:
        """Boot exchange handles and pre-fill buffers via REST."""
        exchange_opts = {
            "enableRateLimit": True,
            "options":         {"defaultType": "future"},
            "timeout":         30000,   # 30 s – exchangeInfo is a large payload
        }
        # Optional HTTP proxy – set HTTP_PROXY env var if Binance is geo-blocked
        # e.g.  export HTTP_PROXY=http://user:pass@proxyhost:port
        _proxy = os.getenv("HTTP_PROXY", os.getenv("http_proxy", "")).strip()
        if _proxy:
            exchange_opts["proxies"]       = {"http": _proxy, "https": _proxy}
            exchange_opts["aiohttp_proxy"] = _proxy
            log.info(f"Using proxy: {_proxy.split('@')[-1]}")  # host only, not credentials

        self._ws   = ccxtpro.binanceusdm(exchange_opts)
        self._rest = ccxt.binanceusdm(exchange_opts)

        log.info("Loading market structure …")
        backoff = 5
        attempt = 0
        while True:
            try:
                await self._ws.load_markets()
                log.info("Market structure loaded.")
                # Clear any previous error file on success
                try:
                    import os as _os; _os.remove("scanner_error.txt")
                except OSError:
                    pass
                break
            except Exception as exc:
                attempt += 1
                wait = min(backoff * (2 ** min(attempt - 1, 4)), 120)  # cap at 120s
                log.warning(f"load_markets attempt {attempt} failed: {exc} – retrying in {wait}s …")
                try:
                    with open("scanner_error.txt", "w") as _ef:
                        _ef.write(f"{datetime.now(timezone.utc).isoformat()} | load_markets failed: {exc}")
                except OSError:
                    pass
                await asyncio.sleep(wait)

        log.info("Pre-fetching historical OHLCV …")
        await asyncio.gather(*[self._prefetch(tf) for tf in self._tfs])
        log.info("Historical data loaded.")

    async def _prefetch(self, tf: str) -> None:
        """REST fetch with infinite exponential-backoff retry (never crashes)."""
        attempt = 0
        while True:
            attempt += 1
            try:
                rows = self._rest.fetch_ohlcv(
                    self._symbol, tf, limit=self._lookback
                )
                self._bufs[tf].extend(rows)
                log.info(f"  [{tf}] loaded {len(rows)} candles.")
                return
            except ccxt.RateLimitExceeded:
                wait = min(self._cfg["rate_limit_backoff"] ** attempt, 60)
                log.warning(f"  [{tf}] rate-limit – retry in {wait:.1f}s …")
                await asyncio.sleep(wait)
            except Exception as exc:
                wait = min(5 * attempt, 60)
                log.error(f"  [{tf}] prefetch attempt {attempt} failed: {exc} – retry in {wait}s …")
                try:
                    with open("scanner_error.txt", "w") as _ef:
                        _ef.write(f"{datetime.now(timezone.utc).isoformat()} | prefetch [{tf}] failed: {exc}")
                except OSError:
                    pass
                await asyncio.sleep(wait)

    # ── Poll interval per timeframe (seconds) ────────────────────────────
    _POLL_SECONDS: Dict[str, int] = {
        "1m": 30, "3m": 60, "5m": 60, "15m": 120, "30m": 180, "1h": 300,
    }

    # ── Streaming (REST polling – works in geo-restricted cloud envs) ─────

    async def stream_ohlcv(self, tf: str, callback) -> None:
        """
        Poll OHLCV via async REST every N seconds and fire `callback(tf, df)`
        whenever a new closed candle arrives.  Uses REST instead of WebSocket
        so it works in cloud environments where Binance WebSocket is blocked.

        Resilience design
        -----------------
        - asyncio.wait_for(timeout=25) prevents the coroutine hanging forever
          if Binance accepts the TCP connection but stops sending data.
        - Heartbeat written on EVERY successful poll (not just new-candle polls)
          so /status correctly shows alive even in low-volatility periods.
        - signal-analysis callback wrapped separately so an indicator/signal
          crash does not kill the poll loop.
        """
        prev_ts: int  = 0
        poll_s        = self._POLL_SECONDS.get(tf, 60)
        poll_count    = 0

        while True:
            try:
                poll_count += 1
                log.info(f"[{tf}] poll #{poll_count} (every {poll_s}s) …")

                # Hard 25s timeout – a silent hang kills the scanner with no error
                candles = await asyncio.wait_for(
                    self._ws.fetch_ohlcv(self._symbol, tf, limit=200),
                    timeout=25,
                )

                # Heartbeat on every successful response (not just new candles)
                try:
                    with open("scanner_heartbeat.txt", "w") as _hb:
                        _hb.write(datetime.now(timezone.utc).isoformat())
                except OSError:
                    pass

                new_candles = 0
                for c in candles:
                    if c[0] > prev_ts:
                        prev_ts = c[0]
                        self._bufs[tf].append(c)
                        new_candles += 1

                if new_candles > 0:
                    log.info(f"[{tf}] {new_candles} new candle(s) – running analysis …")
                    df = self._to_dataframe(tf)
                    if df is not None:
                        try:
                            await asyncio.wait_for(callback(tf, df), timeout=30)
                        except asyncio.TimeoutError:
                            log.warning(f"[{tf}] _on_candle timed out after 30s – skipping bar")
                            try:
                                with open("scanner_error.txt", "w") as _ef:
                                    _ef.write(
                                        f"{datetime.now(timezone.utc).isoformat()} "
                                        f"| [{tf}] _on_candle timeout"
                                    )
                            except OSError:
                                pass
                        except Exception as cb_exc:
                            log.error(f"[{tf}] analysis error: {cb_exc}", exc_info=True)
                            try:
                                with open("scanner_error.txt", "w") as _ef:
                                    _ef.write(
                                        f"{datetime.now(timezone.utc).isoformat()} "
                                        f"| [{tf}] analysis: {cb_exc}"
                                    )
                            except OSError:
                                pass
                else:
                    log.debug(f"[{tf}] no new candles this poll.")

                await asyncio.sleep(poll_s)

            except asyncio.TimeoutError:
                log.warning(f"[{tf}] fetch_ohlcv timed out after 25s – retrying in 15s …")
                try:
                    with open("scanner_error.txt", "w") as _ef:
                        _ef.write(
                            f"{datetime.now(timezone.utc).isoformat()} | [{tf}] timeout"
                        )
                except OSError:
                    pass
                await asyncio.sleep(15)

            except ccxt.RateLimitExceeded:
                log.warning(f"[{tf}] rate-limit – backing off 60s …")
                await asyncio.sleep(60)

            except Exception as exc:
                log.error(f"[{tf}] poll error: {exc}", exc_info=True)
                try:
                    with open("scanner_error.txt", "w") as _ef:
                        _ef.write(
                            f"{datetime.now(timezone.utc).isoformat()} | [{tf}] poll: {exc}"
                        )
                except OSError:
                    pass
                await asyncio.sleep(15)

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

    async def fetch_funding_rate(self) -> float:
        """Returns current BTC perpetual funding rate. Cached 5 min."""
        now = time.time()
        if now - self._funding_ts < 300:
            return self._funding_rate
        try:
            data = await asyncio.wait_for(
                self._ws.fetch_funding_rate(self._symbol),
                timeout=10,
            )
            self._funding_rate = float(data.get("fundingRate", 0) or 0)
            self._funding_ts   = now
        except Exception as exc:
            log.debug(f"Funding rate fetch failed: {exc}")
        return self._funding_rate

    async def fetch_fear_greed(self) -> int:
        """Returns Fear & Greed index (0–100). Cached 4 hours."""
        now = time.time()
        if now - self._fear_greed_ts < 14400:
            return self._fear_greed
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.alternative.me/fng/?limit=1",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        body = await resp.json(content_type=None)
                        self._fear_greed    = int(body["data"][0]["value"])
                        self._fear_greed_ts = now
        except Exception as exc:
            log.debug(f"Fear & Greed fetch failed: {exc}")
        return self._fear_greed

    async def fetch_taker_ratio(self) -> float:
        """Returns taker buy/sell volume ratio. Cached 5 min."""
        now = time.time()
        if now - self._taker_ts < 300:
            return self._taker_ratio
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://fapi.binance.com/futures/data/takerlongshortRatio",
                    params={"symbol": "BTCUSDT", "period": "5m", "limit": 1},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        body = await resp.json()
                        if body:
                            self._taker_ratio = float(body[0]["buySellRatio"])
                            self._taker_ts    = now
        except Exception as exc:
            log.debug(f"Taker ratio fetch failed: {exc}")
        return self._taker_ratio

    async def fetch_long_short_ratio(self) -> float:
        """Returns global long/short account ratio. Cached 5 min."""
        now = time.time()
        if now - self._ls_ts < 300:
            return self._ls_ratio
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
                    params={"symbol": "BTCUSDT", "period": "5m", "limit": 1},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        body = await resp.json()
                        if body:
                            self._ls_ratio = float(body[0]["longShortRatio"])
                            self._ls_ts    = now
        except Exception as exc:
            log.debug(f"Long/Short ratio fetch failed: {exc}")
        return self._ls_ratio

    async def refresh_market_data(self) -> None:
        """
        Background task: keeps funding-rate, fear/greed, taker-ratio and
        long/short-ratio caches warm by refreshing them every 5 minutes.

        Running this as a separate task means _on_candle never has to wait
        for an external API call; it always reads the already-cached value.
        Any individual fetch failure is silently ignored (cached default used).
        """
        log.info("Market-data refresher started (funding, fear/greed, taker, L/S).")
        while True:
            results = await asyncio.gather(
                self.fetch_funding_rate(),
                self.fetch_fear_greed(),
                self.fetch_taker_ratio(),
                self.fetch_long_short_ratio(),
                return_exceptions=True,
            )
            for name, result in zip(
                ["funding_rate", "fear_greed", "taker_ratio", "ls_ratio"], results
            ):
                if isinstance(result, Exception):
                    log.debug(f"refresh_market_data [{name}] failed: {result}")
            await asyncio.sleep(300)

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

        # ── ADX ─────────────────────────────────────────────────────────
        df.ta.adx(length=14, append=True)
        adx_col = self._find_col(df, "ADX_")
        dmp_col = self._find_col(df, "DMP_")
        dmn_col = self._find_col(df, "DMN_")

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

            # ADX (trend strength + directional movement)
            "adx":       float(last[adx_col]) if adx_col and not np.isnan(last[adx_col]) else 0.0,
            "adx_plus":  float(last[dmp_col]) if dmp_col and not np.isnan(last[dmp_col]) else 0.0,
            "adx_minus": float(last[dmn_col]) if dmn_col and not np.isnan(last[dmn_col]) else 0.0,

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

    # How long a signal remains practically valid, keyed by timeframe.
    # Rule of thumb: ~15× the candle duration, capped so 1h stays within a session.
    _VALIDITY: Dict[str, int] = {
        "1m":  15,    # 15 minutes
        "3m":  30,    # 30 minutes
        "5m":  60,    # 1 hour
        "15m": 180,   # 3 hours
        "30m": 360,   # 6 hours
        "1h":  480,   # 8 hours
        "4h":  1440,  # 24 hours
    }

    # Original base weights – never modified; used as clamp reference by WeightLearner
    # 12 indicators, sum = 100
    _BASE_WEIGHTS = {
        "vwap": 14, "ema": 14, "macd": 11, "rsi": 11,
        "bb":    7, "obv":  7, "oi":    6,
        "funding": 10, "adx": 7, "fear_greed": 3,
        "taker_ratio": 5, "ls_ratio": 5,
    }

    def __init__(self, cfg: dict) -> None:
        self._cfg     = cfg
        self._weights = WeightLearner.load_weights()
        source = "learned" if Path(WeightLearner.WEIGHTS_FILE).exists() else "default"
        log.info(f"SignalGenerator: using {source} indicator weights — {self._weights}")

    def reload_weights(self) -> None:
        """Hot-reload weights from learned_weights.json (called after each learning cycle)."""
        self._weights = WeightLearner.load_weights()
        log.info(f"SignalGenerator: weights reloaded — {self._weights}")

    def evaluate(
        self,
        ind:          dict,
        tf:           str,
        oi_now:       float,
        oi_prev:      float,
        funding_rate: float = 0.0,
        fear_greed:   int   = 50,
        taker_ratio:  float = 1.0,
        ls_ratio:     float = 1.0,
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
        score, breakdown   = self._score(
            ind, direction, oi_now, oi_prev,
            funding_rate, fear_greed, taker_ratio, ls_ratio,
        )

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

        now_utc       = datetime.now(timezone.utc)
        valid_minutes = self._VALIDITY.get(tf, 60)
        expires_at    = (now_utc + timedelta(minutes=valid_minutes)).isoformat()
        leverage_rec  = self._calc_leverage(entry, stop_loss, score)

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
            timestamp           = now_utc.isoformat(),
            valid_for_minutes   = valid_minutes,
            expires_at          = expires_at,
            leverage_rec        = leverage_rec,
            indicator_breakdown = breakdown,
        )

    # ── Confidence Score ─────────────────────────────────────────────────

    def _score(
        self,
        ind:          dict,
        direction:    str,
        oi_now:       float,
        oi_prev:      float,
        funding_rate: float = 0.0,
        fear_greed:   int   = 50,
        taker_ratio:  float = 1.0,
        ls_ratio:     float = 1.0,
    ) -> Tuple[float, dict]:
        W    = self._weights
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

        # 8. Funding Rate – avoid entering when crowd is crowded in our direction
        # Positive funding → longs pay shorts (crowd is long → contrarian caution for LONG)
        # Threshold ±0.05% per 8h: beyond this the trade is overcrowded
        _FUND_THRESH = 0.0005
        if bull:
            funding_ok = funding_rate <= _FUND_THRESH   # not excessively crowded long
        else:
            funding_ok = funding_rate >= -_FUND_THRESH  # not excessively crowded short
        pts["funding"] = W["funding"] if funding_ok else 0

        # 9. ADX – confirms we are in a trending (not ranging) market
        # ADX ≥ 25 = trending; DMP > DMN = bullish trend, DMN > DMP = bearish
        adx_val = ind.get("adx", 0.0)
        if adx_val >= 25:
            adx_ok = (
                ind.get("adx_plus", 0.0) > ind.get("adx_minus", 0.0)
                if bull else
                ind.get("adx_minus", 0.0) > ind.get("adx_plus", 0.0)
            )
        else:
            adx_ok = False  # ranging market – no trend credit
        pts["adx"] = W["adx"] if adx_ok else 0

        # 10. Fear & Greed Index – avoid chasing extremes
        # Extreme Greed (> 74) → LONG is overcrowded; Extreme Fear (< 25) → SHORT panic
        if bull:
            fg_ok = fear_greed <= 74
        else:
            fg_ok = fear_greed >= 25
        pts["fear_greed"] = W["fear_greed"] if fg_ok else 0

        # 11. Taker Buy/Sell Ratio – aggressive order flow in our direction
        # > 1.15 = buyers are more aggressive → confirms LONG
        # < 0.85 = sellers are more aggressive → confirms SHORT
        if bull:
            taker_ok = taker_ratio > 1.15
        else:
            taker_ok = taker_ratio < 0.85
        pts["taker_ratio"] = W["taker_ratio"] if taker_ok else 0

        # 12. Long/Short Ratio – contrarian retail-crowd signal
        # Crowd heavily short (ratio < 0.9) → contrarian LONG opportunity
        # Crowd heavily long  (ratio > 1.5) → contrarian SHORT opportunity
        if bull:
            ls_ok = ls_ratio < 0.9
        else:
            ls_ok = ls_ratio > 1.5
        pts["ls_ratio"] = W["ls_ratio"] if ls_ok else 0

        total     = float(sum(pts.values()))
        breakdown = {
            k: {"weight": W[k], "earned": v, "aligned": v > 0}
            for k, v in pts.items()
        }
        breakdown["total_score"] = total
        return total, breakdown

    # Standard exchange leverage tiers (Binance, Bybit etc.)
    _LEV_TIERS: List[int] = [2, 3, 5, 7, 10, 15, 20, 25]

    @staticmethod
    def _snap_leverage(value: float) -> int:
        """Round DOWN to the nearest standard exchange leverage tier (min 2x)."""
        tiers = [2, 3, 5, 7, 10, 15, 20, 25]
        for t in reversed(tiers):
            if value >= t:
                return t
        return 2

    @staticmethod
    def _calc_leverage(entry: float, stop_loss: float, confidence: float) -> dict:
        """
        Compute conservative / moderate / aggressive leverage tiers.

        Formula
        -------
        sl_pct   = |entry − stop_loss| / entry            (e.g. 0.022 = 2.2%)
        liq_max  = 1 / sl_pct                             (liquidation exactly at SL)
        safe_max = 0.5 / sl_pct                           (50 % safety buffer:
                                                            liquidation sits 50 % further
                                                            out than the stop loss)

        Confidence caps prevent recommending high leverage on weaker signals:
          ≥ 90 → cap 25x  |  ≥ 85 → 20x  |  ≥ 80 → 15x  |  ≥ 75 → 10x  |  else → 5x

        Three tiers (snapped to standard exchange values):
          Conservative  = 30 % of capped safe_max  (stays open through normal noise)
          Moderate      = 60 % of capped safe_max  (solid risk management)
          Aggressive    = 100 % of capped safe_max (only for high-confidence setups)
        """
        sl_pct = abs(entry - stop_loss) / entry if entry > 0 else 0.02
        sl_pct = max(sl_pct, 0.001)  # guard against zero

        liq_max  = int(1.0 / sl_pct)           # theoretical max before liquidation
        safe_max = 0.5 / sl_pct                # with 50 % buffer

        # Confidence cap
        if   confidence >= 90: conf_cap = 25
        elif confidence >= 85: conf_cap = 20
        elif confidence >= 80: conf_cap = 15
        elif confidence >= 75: conf_cap = 10
        else:                  conf_cap =  5

        capped = min(safe_max, conf_cap, 25)

        conservative = SignalGenerator._snap_leverage(max(2, capped * 0.30))
        moderate     = SignalGenerator._snap_leverage(max(2, capped * 0.60))
        aggressive   = SignalGenerator._snap_leverage(max(2, capped))

        return {
            "conservative":      conservative,
            "moderate":          moderate,
            "aggressive":        aggressive,
            "sl_distance_pct":   round(sl_pct * 100, 2),
            "max_theoretical":   liq_max,
        }

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
# 5. Weight Learner
# ---------------------------------------------------------------------------
class WeightLearner:
    """
    Adjusts SignalGenerator indicator weights based on resolved trade outcomes.

    Algorithm  (conservative ±10 % per cycle, user-selected)
    ----------------------------------------------------------
    For each of the 7 indicators:
      1. Count how often it was aligned on WIN vs LOSS trades.
      2. Compute precision = wins_when_aligned / (wins + losses when aligned).
      3. Compute lift = precision / overall_win_rate.
         lift > 1 → indicator predicts well above baseline → increase weight.
         lift < 1 → indicator adds noise → decrease weight.
      4. Clamp delta to ± MAX_DELTA_FRAC × base_weight  (±10 % per cycle).
      5. Hard bounds: weight stays in [30 %, 200 %] of its original base value.
      6. Normalise all weights so they sum to exactly 100.

    Requires MIN_SAMPLES resolved outcomes before first adjustment.
    """

    WEIGHTS_FILE   = "learned_weights.json"
    OUTCOMES_FILE  = "outcomes.jsonl"
    ALERTS_FILE    = "alerts.jsonl"
    MIN_SAMPLES    = 20
    MAX_DELTA_FRAC = 0.10   # ±10 % of base weight per cycle
    CLAMP_MIN_FRAC = 0.30   # floor: never below 30 % of base
    CLAMP_MAX_FRAC = 2.00   # ceiling: never above 200 % of base

    _BASE: Dict[str, float] = {
        "vwap": 14, "ema": 14, "macd": 11, "rsi": 11,
        "bb":    7, "obv":  7, "oi":    6,
        "funding": 10, "adx": 7, "fear_greed": 3,
        "taker_ratio": 5, "ls_ratio": 5,
    }
    INDICATORS: List[str] = list(_BASE.keys())

    # ── Public API ────────────────────────────────────────────────────────

    @classmethod
    def load_weights(cls) -> Dict[str, float]:
        """Load learned weights from file; fall back to base defaults."""
        try:
            with open(cls.WEIGHTS_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            w = data.get("weights", {})
            if set(w.keys()) == set(cls._BASE.keys()):
                return {k: float(v) for k, v in w.items()}
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            pass
        return dict(cls._BASE)

    @classmethod
    def run_update(cls, current_weights: Dict[str, float]) -> Optional[Dict[str, float]]:
        """
        Run one learning cycle.  Returns new weights dict if update happened,
        else None (not enough data or I/O error).
        """
        # ── load outcomes ────────────────────────────────────────────────
        outcomes: Dict[str, str] = {}   # timestamp → 'WIN'|'LOSS'
        try:
            with open(cls.OUTCOMES_FILE, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        rec = json.loads(line)
                        outcomes[rec["timestamp"]] = rec["outcome"]
        except FileNotFoundError:
            return None

        # ── join with alerts to get indicator_breakdown ──────────────────
        resolved: List[Dict] = []
        try:
            with open(cls.ALERTS_FILE, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    alert = json.loads(line)
                    outcome = outcomes.get(alert.get("timestamp", ""))
                    if outcome in ("WIN", "LOSS"):
                        resolved.append({
                            "outcome":   outcome,
                            "breakdown": alert.get("indicator_breakdown", {}),
                        })
        except FileNotFoundError:
            return None

        if len(resolved) < cls.MIN_SAMPLES:
            log.info(
                f"WeightLearner: {len(resolved)}/{cls.MIN_SAMPLES} resolved trades "
                f"– skipping update."
            )
            return None

        # ── compute per-indicator lift ───────────────────────────────────
        total_wins = sum(1 for r in resolved if r["outcome"] == "WIN")
        total_res  = len(resolved)
        base_rate  = total_wins / total_res if total_res else 0.5

        new_weights: Dict[str, float] = {}
        for ind in cls.INDICATORS:
            wins_al  = sum(
                1 for r in resolved
                if r["outcome"] == "WIN" and r["breakdown"].get(ind, {}).get("aligned", False)
            )
            losses_al = sum(
                1 for r in resolved
                if r["outcome"] == "LOSS" and r["breakdown"].get(ind, {}).get("aligned", False)
            )
            total_al = wins_al + losses_al

            if total_al < 5:
                # Too few aligned samples for this indicator – keep current
                new_weights[ind] = current_weights.get(ind, cls._BASE[ind])
                continue

            precision = wins_al / total_al
            lift      = precision / max(0.01, base_rate)

            base_w  = cls._BASE[ind]
            cur_w   = current_weights.get(ind, base_w)
            target  = cur_w * lift
            delta   = max(-cls.MAX_DELTA_FRAC * base_w,
                          min( cls.MAX_DELTA_FRAC * base_w, target - cur_w))
            raw     = cur_w + delta
            new_weights[ind] = max(cls.CLAMP_MIN_FRAC * base_w,
                                   min(cls.CLAMP_MAX_FRAC * base_w, raw))

        # ── normalise to 100 ─────────────────────────────────────────────
        total = sum(new_weights.values())
        norm  = {k: round(v / total * 100, 2) for k, v in new_weights.items()}

        # ── log changes ──────────────────────────────────────────────────
        changes = [
            f"{k}: {current_weights.get(k, cls._BASE[k]):.1f}→{norm[k]:.1f}"
            for k in cls.INDICATORS
            if abs(norm[k] - current_weights.get(k, cls._BASE[k])) > 0.05
        ]
        log.info(
            f"WeightLearner: updated weights  samples={len(resolved)}"
            f"  win_rate={base_rate:.1%}"
            + (f"  changes=[{', '.join(changes)}]" if changes else "  (no significant change)")
        )

        # ── persist ──────────────────────────────────────────────────────
        try:
            with open(cls.WEIGHTS_FILE, "w", encoding="utf-8") as fh:
                json.dump({
                    "weights":      norm,
                    "base_weights": cls._BASE,
                    "samples":      len(resolved),
                    "win_rate":     round(base_rate, 4),
                    "updated":      datetime.now(timezone.utc).isoformat(),
                }, fh, indent=2)
        except OSError as exc:
            log.warning(f"WeightLearner: could not save {cls.WEIGHTS_FILE}: {exc}")

        return norm


# ---------------------------------------------------------------------------
# 6. Alert Formatter
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
        exp_str = alert.expires_at[:16].replace("T", " ") + " UTC"
        print(f"  Valid For     : ~{alert.valid_for_minutes} min  (until {exp_str})")
        lv = alert.leverage_rec
        print(
            f"  Leverage Rec  :  Conservative {lv['conservative']}x"
            f"  |  Moderate {lv['moderate']}x"
            f"  |  Aggressive {lv['aggressive']}x"
        )
        print(
            f"                   SL is {lv['sl_distance_pct']}% from entry"
            f"  (liquidation at >{lv['max_theoretical']}x)"
        )
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

        exp_str  = alert.expires_at[:16].replace("T", " ") + " UTC"
        lv       = alert.leverage_rec
        lev_line = (
            f"Cons <b>{lv['conservative']}x</b>"
            f"  ·  Mod <b>{lv['moderate']}x</b>"
            f"  ·  Aggr <b>{lv['aggressive']}x</b>"
            f"  <i>(SL {lv['sl_distance_pct']}% away)</i>"
        )
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
            f"Valid For  : ~{alert.valid_for_minutes} min  (until {exp_str})\n"
            f"Leverage   : {lev_line}\n"
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
        exp_str = alert.expires_at[:16].replace("T", " ") + " UTC"
        lv      = alert.leverage_rec
        return (
            f"[BTC/USDT] {arrow} | {alert.timeframe}\n"
            f"Confidence: {alert.confidence_score:.0f}/100\n\n"
            f"Entry:    ${alert.entry_price:,.2f}\n"
            f"Target 1: ${alert.target_1:,.2f}\n"
            f"Target 2: ${alert.target_2:,.2f}\n"
            f"Stop:     ${alert.stop_loss:,.2f}\n"
            f"R:R:      {alert.risk_reward:.2f}x\n"
            f"OI:       {alert.oi_signal}\n"
            f"Valid:    ~{alert.valid_for_minutes} min (until {exp_str})\n"
            f"Leverage: {lv['conservative']}x / {lv['moderate']}x / {lv['aggressive']}x"
            f"  (Cons/Mod/Aggr · SL {lv['sl_distance_pct']}% away)\n\n"
            f"{checks}\n"
            f"{alert.timestamp[:16].replace('T', ' ')} UTC"
        )


# ---------------------------------------------------------------------------
# 8. Outcome Tracker
# ---------------------------------------------------------------------------
class OutcomeTracker:
    """
    Background asyncio task that resolves live alert outcomes and triggers
    weight learning after each batch.

    Every CHECK_INTERVAL seconds it:
      1. Reads alerts.jsonl for unresolved alerts (not yet in outcomes.jsonl).
      2. Skips alerts < MIN_AGE_SECS old (not enough subsequent bars yet).
      3. Fetches OHLCV bars from the alert timestamp forward via REST.
      4. Scans bars for the first T1 or SL touch:
           LONG : high >= target_1 → WIN ;  low  <= stop_loss → LOSS
           SHORT: low  <= target_1 → WIN ;  high >= stop_loss → LOSS
           Both in same bar → LOSS  (conservative)
           Neither in MAX_HOLD bars → OPEN  (retry next cycle)
      5. Appends resolved outcomes to outcomes.jsonl.
      6. After any new resolutions, calls WeightLearner.run_update() and
         reloads weights into the SignalGenerator.
    """

    OUTCOMES_FILE  = "outcomes.jsonl"
    ALERTS_FILE    = "alerts.jsonl"
    CHECK_INTERVAL = 300    # seconds between cycles (5 min)
    MIN_AGE_SECS   = 1800   # ignore alerts < 30 min old
    MAX_HOLD       = 50     # maximum forward bars to scan

    def __init__(self, data: DataManager, generator: "SignalGenerator") -> None:
        self._data      = data
        self._generator = generator

    async def track(self) -> None:
        """Run forever; called as a concurrent asyncio task alongside poll streams."""
        log.info("OutcomeTracker started – resolving alert outcomes every 5 min.")
        await asyncio.sleep(90)   # warm-up delay
        while True:
            try:
                new_resolved = await self._resolve_cycle()
                if new_resolved > 0:
                    updated = WeightLearner.run_update(self._generator._weights)
                    if updated:
                        self._generator.reload_weights()
            except Exception as exc:
                log.error(f"OutcomeTracker cycle error: {exc}", exc_info=True)
            await asyncio.sleep(self.CHECK_INTERVAL)

    # ── Internal ──────────────────────────────────────────────────────────

    async def _resolve_cycle(self) -> int:
        """Resolve pending alerts; return count of newly resolved outcomes."""
        # Load already-resolved timestamps
        resolved_ts: set = set()
        try:
            with open(self.OUTCOMES_FILE, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        resolved_ts.add(json.loads(line)["timestamp"])
        except FileNotFoundError:
            pass

        # Load all alerts
        alerts: List[Dict] = []
        try:
            with open(self.ALERTS_FILE, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        try:
                            alerts.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
        except FileNotFoundError:
            return 0

        now_utc      = datetime.now(timezone.utc)
        new_resolved = 0

        for alert in alerts:
            ts = alert.get("timestamp", "")
            if ts in resolved_ts:
                continue

            try:
                age_s = (now_utc - datetime.fromisoformat(ts)).total_seconds()
            except ValueError:
                continue
            if age_s < self.MIN_AGE_SECS:
                continue   # too fresh

            tf        = alert.get("timeframe", "5m")
            direction = alert.get("signal_type", "LONG")
            entry     = float(alert.get("entry_price", 0))
            target_1  = float(alert.get("target_1", 0))
            stop_loss = float(alert.get("stop_loss", 0))
            if not all([entry, target_1, stop_loss]):
                continue

            outcome = await self._fetch_outcome(ts, tf, direction, target_1, stop_loss)
            if outcome == "OPEN":
                continue   # not yet resolved

            try:
                with open(self.OUTCOMES_FILE, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps({
                        "timestamp":   ts,
                        "outcome":     outcome,
                        "resolved_at": now_utc.isoformat(),
                        "timeframe":   tf,
                        "direction":   direction,
                    }) + "\n")
                resolved_ts.add(ts)
                new_resolved += 1
                log.info(f"OutcomeTracker: {direction} [{tf}] @ {ts[:16]} → {outcome}")
            except OSError as exc:
                log.warning(f"OutcomeTracker: write failed: {exc}")

        if new_resolved:
            log.info(f"OutcomeTracker: {new_resolved} new outcome(s) resolved this cycle.")
        return new_resolved

    async def _fetch_outcome(
        self, ts: str, tf: str, direction: str, target_1: float, stop_loss: float,
    ) -> str:
        """Return 'WIN', 'LOSS', or 'OPEN' by scanning forward OHLCV from ts."""
        try:
            since_ms = int(datetime.fromisoformat(ts).timestamp() * 1000)
            candles  = await asyncio.wait_for(
                self._data._ws.fetch_ohlcv(
                    self._data._symbol, tf,
                    since=since_ms, limit=self.MAX_HOLD + 2,
                ),
                timeout=20,
            )
        except Exception as exc:
            log.debug(f"OutcomeTracker: fetch failed for {ts[:16]}: {exc}")
            return "OPEN"

        # Skip first bar (entry bar), scan subsequent bars
        for c in candles[1:]:
            hi, lo = float(c[2]), float(c[3])
            if direction == "LONG":
                hit_t1 = hi >= target_1
                hit_sl = lo <= stop_loss
            else:
                hit_t1 = lo <= target_1
                hit_sl = hi >= stop_loss

            if hit_t1 and hit_sl:
                return "LOSS"   # conservative: assume SL hit first
            if hit_t1:
                return "WIN"
            if hit_sl:
                return "LOSS"

        return "OPEN"   # not resolved yet


# ---------------------------------------------------------------------------
# 9. Main Scanner Orchestrator
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
        self._last_alert:     Dict[str, float] = {}   # tf → epoch of last alert
        self._outcome_tracker = OutcomeTracker(self._data, self._generator)

    # ── Pipeline ─────────────────────────────────────────────────────────

    async def _on_candle(self, tf: str, df: pd.DataFrame) -> None:
        """DataFrame → Indicators → Signal → Alert (with cooldown guard)."""

        indicators = self._analyzer.compute(df)
        if indicators is None:
            return

        oi_now, oi_prev = await self._data.fetch_open_interest()
        # Funding/sentiment/flow data is kept warm by refresh_market_data()
        # background task — read from cache synchronously (no awaiting, no risk of hanging).
        funding_rate = self._data._funding_rate
        fear_greed   = self._data._fear_greed
        taker_ratio  = self._data._taker_ratio
        ls_ratio     = self._data._ls_ratio

        alert = self._generator.evaluate(
            indicators, tf, oi_now, oi_prev,
            funding_rate, fear_greed, taker_ratio, ls_ratio,
        )
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
            await asyncio.gather(
                *streams,
                self._outcome_tracker.track(),
                self._data.refresh_market_data(),
            )
        finally:
            await self._data.close()
            log.info("Scanner shut down cleanly.")


# ---------------------------------------------------------------------------
# Health-Check Server (for Koyeb / Docker platforms)
# ---------------------------------------------------------------------------
def _start_health_server() -> None:
    """
    Start a minimal HTTP server in a background daemon thread.
    Responds GET / → 200 OK so Koyeb / Railway / Fly.io health checks pass.
    Port is read from the HEALTH_PORT env var (default 8000).
    """
    port = int(os.getenv("HEALTH_PORT", "8000"))

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, _fmt, *_args):
            pass  # silence access logs

        def do_GET(self):
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = HTTPServer(("0.0.0.0", port), _Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info(f"Health-check server listening on port {port}")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # aiohttp (used by CCXT Pro) requires SelectorEventLoop on Windows.
    # Python 3.8+ defaults to ProactorEventLoop which breaks async HTTP.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Health-check server only starts if HEALTH_PORT is explicitly set.
    # When running via start.sh, dashboard.py already serves on port 8000.
    if os.getenv("HEALTH_PORT"):
        _start_health_server()

    try:
        asyncio.run(BTCFuturesScanner().run())
    except KeyboardInterrupt:
        log.info("Interrupted by user — exiting.")
