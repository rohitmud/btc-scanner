@echo off
REM Launch the BTC Futures Scanner with PYTHONPATH isolation.
REM The -E flag tells Python to ignore the PYTHONPATH env var,
REM preventing stale Python 3.8 site-packages from polluting the import chain.
py -3.13 -E "%~dp0btc_futures_scanner.py" %*
