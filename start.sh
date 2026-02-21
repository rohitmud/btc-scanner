#!/bin/bash
# Entrypoint for cloud deployment (Koyeb, Fly.io, Railway, etc.)
# Runs the BTC scanner and live dashboard together in one container.
#
# Environment variables:
#   PORT             – dashboard port (Koyeb sets this automatically, default 8000)
#   TELEGRAM_TOKEN   – Telegram bot token
#   TELEGRAM_CHAT_ID – Telegram chat / group ID
#   WHATSAPP_PHONE   – Phone number for CallMeBot WhatsApp alerts
#   WHATSAPP_APIKEY  – CallMeBot API key
#   HTTP_PROXY       – (optional) proxy URL if Binance is geo-blocked

PORT="${PORT:-8000}"

echo "Starting BTC Futures Scanner ..."
python -u btc_futures_scanner.py &
SCANNER_PID=$!

echo "Starting Dashboard on port $PORT ..."
python -u dashboard.py --serve --port "$PORT" --interval 30 &
DASHBOARD_PID=$!

# If either process exits, kill the other and exit
wait -n
kill $SCANNER_PID $DASHBOARD_PID 2>/dev/null
