"""
BTC Futures Scanner - Backtester
=================================
Fetches historical OHLCV from Binance USDM and replays the exact
TechnicalAnalyzer + SignalGenerator logic from btc_futures_scanner.py
to measure historical signal accuracy.

No look-ahead bias: each bar's indicator window contains only data up
to and including that bar.  Outcome is determined by scanning the next
MAX_HOLD_BARS candles for the first target or stop-loss touch.

Usage
-----
    py -3.13 -E backtest.py
    py -3.13 -E backtest.py --tf 1m 5m 1h --bars 1500
    py -3.13 -E backtest.py --tf 5m --bars 2000 --max-hold 30
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from typing import List, Optional

import ccxt
import numpy as np
import pandas as pd

# Import strategy components directly - no duplication
from btc_futures_scanner import CONFIG, TechnicalAnalyzer, SignalGenerator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MIN_WARMUP    = 220    # Bars before first valid indicator (EMA-200 + BB-20 + margin)
MAX_HOLD_BARS = 50     # Bars to scan after signal for T1 / SL touch

# Cooldown in seconds (mirrors live scanner CONFIG["alert_cooldown"])
COOLDOWN_SEC  = CONFIG["alert_cooldown"]   # 300 s

TF_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900,
    "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400,
}


# ---------------------------------------------------------------------------
# Data Fetch
# ---------------------------------------------------------------------------

def fetch_ohlcv(symbol: str, tf: str, limit: int) -> pd.DataFrame:
    exchange = ccxt.binanceusdm({
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
        "timeout": 30000,
    })
    print(f"  Fetching {limit} × {tf} candles for {symbol} …")
    rows = exchange.fetch_ohlcv(symbol, tf, limit=min(limit, 1500))
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    df = df.astype(float)
    df = df[~df.index.duplicated(keep="last")]
    df.sort_index(inplace=True)
    print(f"  [{tf}] {len(df)} candles: {df.index[0].date()} -> {df.index[-1].date()}")
    return df


# ---------------------------------------------------------------------------
# Outcome Determination
# ---------------------------------------------------------------------------

def determine_outcome(
    df: pd.DataFrame,
    signal_bar: int,
    direction: str,
    entry: float,
    target_1: float,
    stop_loss: float,
    max_hold: int,
) -> str:
    """
    Scan forward from signal_bar+1.
    Returns 'WIN' | 'LOSS' | 'OPEN' (neither touched within max_hold bars).

    When both target and stop are inside the same bar's range (gap or spike),
    LOSS is returned (conservative: assumes adverse fill first).
    """
    future = df.iloc[signal_bar + 1 : signal_bar + 1 + max_hold]
    for _, bar in future.iterrows():
        hi, lo = bar["high"], bar["low"]
        if direction == "LONG":
            hit_t  = hi >= target_1
            hit_sl = lo <= stop_loss
        else:
            hit_t  = lo <= target_1
            hit_sl = hi >= stop_loss

        if hit_t and hit_sl:
            return "LOSS"   # conservative: SL assumed to fill first
        if hit_t:
            return "WIN"
        if hit_sl:
            return "LOSS"
    return "OPEN"


# ---------------------------------------------------------------------------
# Walk-Forward Simulation
# ---------------------------------------------------------------------------

def backtest_timeframe(
    df: pd.DataFrame,
    tf: str,
    cfg: dict,
    max_hold: int,
) -> List[dict]:
    analyzer  = TechnicalAnalyzer(cfg)
    generator = SignalGenerator(cfg)

    bar_sec      = TF_SECONDS.get(tf, 300)
    cooldown_bars = max(1, COOLDOWN_SEC // bar_sec)
    last_signal  = -cooldown_bars   # bar index of last fired signal

    trades: List[dict] = []
    total_bars = len(df)

    for i in range(MIN_WARMUP, total_bars - max_hold - 1):
        # Enforce cooldown
        if i - last_signal < cooldown_bars:
            continue

        window = df.iloc[: i + 1]   # only data up to bar i (inclusive)

        indicators = analyzer.compute(window)
        if indicators is None:
            continue

        # OI is unavailable for historical data - pass neutral zeros
        # (this sets OI weight to 0, capping max possible score at 90)
        alert = generator.evaluate(indicators, tf, oi_now=0.0, oi_prev=0.0)
        if alert is None:
            continue

        outcome = determine_outcome(
            df,
            signal_bar=i,
            direction=alert.signal_type,
            entry=alert.entry_price,
            target_1=alert.target_1,
            stop_loss=alert.stop_loss,
            max_hold=max_hold,
        )

        trades.append({
            "tf":         tf,
            "bar_index":  i,
            "timestamp":  df.index[i].isoformat(),
            "direction":  alert.signal_type,
            "entry":      round(alert.entry_price, 2),
            "target_1":   round(alert.target_1,    2),
            "stop_loss":  round(alert.stop_loss,   2),
            "rr":         alert.risk_reward,
            "confidence": alert.confidence_score,
            "outcome":    outcome,
        })
        last_signal = i

    return trades


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(trades: List[dict], tf: str, max_hold: int) -> None:
    SEP   = "=" * 62
    DIV   = "-" * 62

    total    = len(trades)
    resolved = [t for t in trades if t["outcome"] != "OPEN"]
    wins     = [t for t in resolved if t["outcome"] == "WIN"]
    losses   = [t for t in resolved if t["outcome"] == "LOSS"]
    opens    = [t for t in trades   if t["outcome"] == "OPEN"]

    win_rate  = len(wins) / len(resolved) * 100 if resolved else 0.0
    avg_conf  = float(np.mean([t["confidence"] for t in trades])) if trades else 0.0
    avg_conf_w = float(np.mean([t["confidence"] for t in wins])) if wins else float("nan")
    avg_rr    = float(np.mean([t["rr"] for t in trades])) if trades else 0.0

    print(f"\n{SEP}")
    print(f"  BACKTEST  -  {tf.upper()}  -  max-hold {max_hold} bars")
    print(DIV)
    print(f"  Total signals  : {total}")
    print(f"  Resolved       : {len(resolved)}   Win {len(wins)}  -  Loss {len(losses)}")
    print(f"  Still open     : {len(opens)}  (no T1/SL touch within {max_hold} bars)")
    print(f"  Win Rate       : {win_rate:.1f}%  (of resolved)")
    print(f"  Avg R:R        : {avg_rr:.2f}x")
    print(f"  Avg Confidence : {avg_conf:.1f}  (winners: {avg_conf_w:.1f})")
    print(DIV)

    # Win rate by confidence bucket
    print("  Win Rate by Confidence Band:")
    buckets = [(55, 65, "55-64"), (65, 75, "65-74"), (75, 85, "75-84"), (85, 101, "85-90")]
    for lo, hi, label in buckets:
        bt = [t for t in resolved if lo <= t["confidence"] < hi]
        if bt:
            bw = sum(1 for t in bt if t["outcome"] == "WIN")
            print(f"    {label}  ->  {bw/len(bt)*100:>5.1f}%   ({bw}/{len(bt)} resolved)")
        else:
            print(f"    {label}  ->  no signals")

    # Long vs Short
    print(DIV)
    print("  Win Rate by Direction:")
    for d in ("LONG", "SHORT"):
        dt = [t for t in resolved if t["direction"] == d]
        if dt:
            dw = sum(1 for t in dt if t["outcome"] == "WIN")
            print(f"    {d:5}  ->  {dw/len(dt)*100:>5.1f}%   ({dw}/{len(dt)} resolved)")
        else:
            print(f"    {d:5}  ->  no signals")

    print(SEP)


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------

def save_csv(trades: List[dict], path: str = "backtest_results.csv") -> None:
    if not trades:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=trades[0].keys())
        writer.writeheader()
        writer.writerows(trades)
    print(f"\n  Trades saved -> {path}")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="BTC Futures Scanner - Backtester")
    parser.add_argument("--tf",       nargs="+", default=["5m", "1h"],
                        help="Timeframe(s) to test  (default: 5m 1h)")
    parser.add_argument("--bars",     type=int, default=1500,
                        help="Historical candles to fetch per TF  (max 1500, default: 1500)")
    parser.add_argument("--max-hold", type=int, default=MAX_HOLD_BARS, dest="max_hold",
                        help=f"Max bars to hold before marking OPEN  (default: {MAX_HOLD_BARS})")
    parser.add_argument("--no-csv",   action="store_true",
                        help="Skip saving results to CSV")
    args = parser.parse_args()

    cfg = dict(CONFIG)   # shallow copy - don't mutate the live config
    all_trades: List[dict] = []

    print(f"\n{'='*62}")
    print(f"  BTC Futures Scanner  -  Backtest")
    print(f"  Symbol : {cfg['symbol']}")
    print(f"  TF(s)  : {args.tf}   Bars: {args.bars}   Max-hold: {args.max_hold}")
    print(f"  Confidence threshold : {cfg['confidence_threshold']}")
    print(f"  NOTE: OI weight (10 pts) excluded - no historical OI data")
    print(f"{'='*62}")

    for tf in args.tf:
        df     = fetch_ohlcv(cfg["symbol"], tf, args.bars)
        trades = backtest_timeframe(df, tf, cfg, args.max_hold)
        print_report(trades, tf, args.max_hold)
        all_trades.extend(trades)

    # Combined summary
    if len(args.tf) > 1 and all_trades:
        resolved = [t for t in all_trades if t["outcome"] != "OPEN"]
        wins     = sum(1 for t in resolved if t["outcome"] == "WIN")
        if resolved:
            print(f"\n  COMBINED  -  All Timeframes")
            print(f"  Win Rate : {wins/len(resolved)*100:.1f}%   ({wins}/{len(resolved)} resolved)\n")

    if not args.no_csv and all_trades:
        save_csv(all_trades)


if __name__ == "__main__":
    main()
