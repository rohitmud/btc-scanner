"""
BTC Futures Scanner - One-Shot Mode
====================================
Designed for scheduled execution: GitHub Actions, cron, Task Scheduler.

Fetches latest OHLCV via REST (no WebSocket needed), checks for high-confidence
trade signals across configured timeframes, and sends a Telegram notification
when a signal is found.  Respects the same cooldown as the live scanner to
avoid duplicate alerts.

Usage
-----
    python scanner_oneshot.py

Environment Variables
---------------------
    TELEGRAM_TOKEN    – Bot token from BotFather
    TELEGRAM_CHAT_ID  – Your Telegram chat / channel ID
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import ccxt
import pandas as pd
import requests

# Reuse strategy logic from the main scanner (no WebSocket is started)
from btc_futures_scanner import CONFIG, TechnicalAnalyzer, SignalGenerator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MIN_WARMUP  = 220           # bars needed for EMA-200 + margin
TIMEFRAMES  = ["5m", "15m", "1h"]
ALERTS_FILE = Path(__file__).parent / "alerts.jsonl"
COOLDOWN_S  = int(CONFIG["alert_cooldown"])   # 900 s


# ---------------------------------------------------------------------------
# Data Fetch (REST)
# ---------------------------------------------------------------------------
def fetch_candles(symbol: str, tf: str, limit: int = 250) -> pd.DataFrame:
    exchange = ccxt.binanceusdm({
        "enableRateLimit": True,
        "timeout": 30000,
    })
    rows = exchange.fetch_ohlcv(symbol, tf, limit=limit)
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    return df.astype(float)


# ---------------------------------------------------------------------------
# Cooldown Check
# ---------------------------------------------------------------------------
def seconds_since_last_alert(tf: str) -> float:
    """Return seconds elapsed since the last saved alert for this TF."""
    if not ALERTS_FILE.exists():
        return float("inf")
    now = datetime.now(timezone.utc)
    for raw in reversed(ALERTS_FILE.read_text(encoding="utf-8").splitlines()):
        try:
            a = json.loads(raw)
            if a.get("timeframe") == tf:
                ts = datetime.fromisoformat(a["timestamp"].replace("Z", "+00:00"))
                return (now - ts).total_seconds()
        except Exception:
            continue
    return float("inf")


# ---------------------------------------------------------------------------
# Persist Alert
# ---------------------------------------------------------------------------
def save_alert(alert, tf: str) -> None:
    with ALERTS_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "timeframe":   tf,
            "signal":      alert.signal_type,
            "entry":       round(alert.entry_price, 2),
            "target_1":    round(alert.target_1,    2),
            "target_2":    round(alert.target_2,    2),
            "stop_loss":   round(alert.stop_loss,   2),
            "rr":          round(alert.risk_reward, 2),
            "confidence":  alert.confidence_score,
        }) + "\n")


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------
def send_telegram(token: str, chat_id: str, text: str) -> None:
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(
        url,
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"  Telegram error {resp.status_code}: {resp.text[:200]}")
    else:
        print("  Telegram notification sent.")


def format_alert(alert, tf: str) -> str:
    arrow = "^ LONG" if alert.signal_type == "LONG" else "v SHORT"
    return (
        f"<b>[BTCUSDT Perp] {arrow} | {tf.upper()}</b>\n\n"
        f"Entry      <code>${alert.entry_price:>10,.2f}</code>\n"
        f"Target 1   <code>${alert.target_1:>10,.2f}</code>\n"
        f"Target 2   <code>${alert.target_2:>10,.2f}</code>\n"
        f"Stop Loss  <code>${alert.stop_loss:>10,.2f}</code>\n\n"
        f"R:R        <code>{alert.risk_reward:.2f}x</code>\n"
        f"Confidence <code>{alert.confidence_score:.0f} / 100</code>\n\n"
        f"<i>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    token   = os.getenv("TELEGRAM_TOKEN",   CONFIG.get("telegram_token",   "")).strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", CONFIG.get("telegram_chat_id", "")).strip()

    if not token or not chat_id:
        print("WARNING: TELEGRAM_TOKEN / TELEGRAM_CHAT_ID not set — alerts will be logged only.")

    cfg       = dict(CONFIG)
    analyzer  = TechnicalAnalyzer(cfg)
    generator = SignalGenerator(cfg)

    found = 0

    for tf in TIMEFRAMES:
        print(f"[{tf}] Checking ...")
        try:
            # Cooldown guard
            elapsed = seconds_since_last_alert(tf)
            if elapsed < COOLDOWN_S:
                remaining = int(COOLDOWN_S - elapsed)
                print(f"[{tf}] Cooldown active — {remaining}s remaining, skipping.")
                continue

            df = fetch_candles(cfg["symbol"], tf, limit=MIN_WARMUP + 20)
            df = df.iloc[:-1]   # drop last bar (may be incomplete)

            if len(df) < MIN_WARMUP:
                print(f"[{tf}] Not enough bars ({len(df)}), skipping.")
                continue

            indicators = analyzer.compute(df)
            if indicators is None:
                print(f"[{tf}] Indicators not ready.")
                continue

            alert = generator.evaluate(indicators, tf, oi_now=0.0, oi_prev=0.0)
            if alert is None:
                print(f"[{tf}] No signal.")
                continue

            print(f"[{tf}] >>> SIGNAL: {alert.signal_type}  confidence={alert.confidence_score:.0f}")
            found += 1
            save_alert(alert, tf)

            if token and chat_id:
                send_telegram(token, chat_id, format_alert(alert, tf))

        except Exception as exc:
            print(f"[{tf}] Error: {exc}")

    if found == 0:
        print("No signals found across all timeframes.")
    else:
        print(f"\n{found} signal(s) sent.")


if __name__ == "__main__":
    main()
