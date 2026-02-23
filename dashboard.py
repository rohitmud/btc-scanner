"""
BTC Futures Scanner - Dashboard Generator
==========================================
Reads backtest_results.csv and alerts.jsonl, computes stats,
and generates a rich dark-theme HTML dashboard opened in your browser.

Usage
-----
    # Static one-shot (open file:// in browser, no auto-refresh)
    py -3.13 -E dashboard.py

    # Live server (auto-refreshes alerts every 60 s)
    py -3.13 -E dashboard.py --serve
    py -3.13 -E dashboard.py --serve --interval 30 --port 5000
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Data Readers
# ---------------------------------------------------------------------------

def read_backtest_csv(path: str) -> List[Dict]:
    if not Path(path).exists():
        return []
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for t in rows:
        t["confidence"] = float(t["confidence"])
        t["rr"]         = float(t["rr"])
        t["entry"]      = float(t["entry"])
        t["target_1"]   = float(t["target_1"])
        t["stop_loss"]  = float(t["stop_loss"])
        t["bar_index"]  = int(t["bar_index"])
    return rows


def read_alerts_jsonl(path: str) -> List[Dict]:
    if not Path(path).exists():
        return []
    alerts = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    alerts.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return alerts[-50:]


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def compute_stats(trades: List[Dict]) -> Dict:
    if not trades:
        return {}

    resolved = [t for t in trades if t["outcome"] != "OPEN"]
    wins     = [t for t in resolved if t["outcome"] == "WIN"]
    losses   = [t for t in resolved if t["outcome"] == "LOSS"]
    opens    = [t for t in trades   if t["outcome"] == "OPEN"]

    win_rate = len(wins) / len(resolved) * 100 if resolved else 0.0
    avg_rr   = sum(t["rr"] for t in trades) / len(trades)
    ev       = (win_rate / 100 * avg_rr) - ((1 - win_rate / 100) * 1.0)

    # Per-timeframe
    tfs    = sorted(set(t["tf"] for t in trades))
    by_tf  = {}
    for tf in tfs:
        tf_t  = [t for t in trades   if t["tf"] == tf]
        tf_r  = [t for t in tf_t     if t["outcome"] != "OPEN"]
        tf_w  = [t for t in tf_r     if t["outcome"] == "WIN"]
        by_tf[tf] = {
            "total":    len(tf_t),
            "wins":     len(tf_w),
            "losses":   len(tf_r) - len(tf_w),
            "opens":    len(tf_t) - len(tf_r),
            "win_rate": round(len(tf_w) / len(tf_r) * 100, 1) if tf_r else 0.0,
            "avg_rr":   round(sum(t["rr"] for t in tf_t) / len(tf_t), 2),
        }

    # By confidence band (75+ only since threshold is 75)
    bands   = [(75, 80, "75-79"), (80, 85, "80-84"), (85, 90, "85-89"), (90, 101, "90+")]
    by_conf = {}
    for lo, hi, label in bands:
        bt = [t for t in resolved if lo <= t["confidence"] < hi]
        bw = [t for t in bt if t["outcome"] == "WIN"]
        by_conf[label] = {
            "total":    len(bt),
            "wins":     len(bw),
            "win_rate": round(len(bw) / len(bt) * 100, 1) if bt else 0.0,
        }

    # By direction
    by_dir = {}
    for d in ("LONG", "SHORT"):
        dt = [t for t in resolved if t["direction"] == d]
        dw = [t for t in dt if t["outcome"] == "WIN"]
        by_dir[d] = {
            "total":    len([t for t in trades if t["direction"] == d]),
            "resolved": len(dt),
            "wins":     len(dw),
            "win_rate": round(len(dw) / len(dt) * 100, 1) if dt else 0.0,
        }

    # Cumulative P&L (R multiples), sorted chronologically per TF then by bar
    sorted_trades = sorted(trades, key=lambda x: (x["timestamp"], x["bar_index"]))
    cumulative = []
    running    = 0.0
    for t in sorted_trades:
        if t["outcome"] == "WIN":
            running += t["rr"]
        elif t["outcome"] == "LOSS":
            running -= 1.0
        cumulative.append({
            "ts":      t["timestamp"],
            "val":     round(running, 3),
            "tf":      t["tf"],
            "outcome": t["outcome"],
        })

    return {
        "total":          len(trades),
        "resolved":       len(resolved),
        "wins":           len(wins),
        "losses":         len(losses),
        "opens":          len(opens),
        "win_rate":       round(win_rate, 1),
        "avg_rr":         round(avg_rr, 2),
        "ev":             round(ev, 3),
        "by_tf":          by_tf,
        "by_conf":        by_conf,
        "by_dir":         by_dir,
        "cumulative_pnl": cumulative,
        "tfs":            tfs,
    }


# ---------------------------------------------------------------------------
# HTML Template
# ---------------------------------------------------------------------------
# Data is injected via .replace() – no f-string escaping needed.
# Placeholders: __STATS__, __TRADES__, __ALERTS__

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BTC Futures Scanner - Dashboard</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0d1117; --card: #161b22; --border: #30363d; --text: #e6edf3;
    --muted: #8b949e; --green: #3fb950; --red: #f85149;
    --yellow: #d29922; --blue: #58a6ff; --purple: #bc8cff;
  }
  * { box-sizing: border-box; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; min-height: 100vh; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; }
  .stat-card { text-align: center; padding: 1.1rem 0.75rem; }
  .stat-value { font-size: 1.75rem; font-weight: 700; line-height: 1.1; }
  .stat-label { font-size: 0.72rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; margin-top: 5px; }
  .stat-sub { font-size: 0.7rem; color: var(--muted); margin-top: 3px; }
  .section-title { font-size: 0.78rem; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.9rem; }
  .win { color: var(--green); }
  .loss { color: var(--red); }
  .badge-pill { display: inline-block; padding: 2px 9px; border-radius: 10px; font-size: 0.72rem; font-weight: 600; }
  .bp-win    { background: rgba(63,185,80,0.15);  color: var(--green);  border: 1px solid rgba(63,185,80,0.35); }
  .bp-loss   { background: rgba(248,81,73,0.15);  color: var(--red);    border: 1px solid rgba(248,81,73,0.35); }
  .bp-open   { background: rgba(210,153,34,0.15); color: var(--yellow); border: 1px solid rgba(210,153,34,0.35); }
  .bp-long   { background: rgba(88,166,255,0.15); color: var(--blue);   border: 1px solid rgba(88,166,255,0.35); }
  .bp-short  { background: rgba(188,140,255,0.15);color: var(--purple); border: 1px solid rgba(188,140,255,0.35); }
  table { color: var(--text); }
  thead th { background: var(--card); border-color: var(--border) !important; color: var(--muted); font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 500; position: sticky; top: 0; z-index: 2; }
  tbody tr { border-color: var(--border) !important; transition: background 0.1s; }
  tbody tr:hover { background: rgba(255,255,255,0.03); }
  td, th { border-color: var(--border) !important; font-size: 0.85rem; vertical-align: middle; padding: 0.45rem 0.65rem !important; }
  .header-bar { background: var(--card); border-bottom: 1px solid var(--border); padding: 0.9rem 1.5rem; }
  .alert-card { background: var(--card); border: 1px solid var(--border); border-radius: 6px; padding: 0.9rem 1rem; margin-bottom: 0.65rem; }
  .alert-long  { border-left: 3px solid var(--blue); }
  .alert-short { border-left: 3px solid var(--purple); }
  .chip { display: inline-block; font-size: 0.68rem; padding: 2px 7px; border-radius: 3px; margin: 2px; }
  .chip-ok  { background: rgba(63,185,80,0.15);  color: var(--green); }
  .chip-no  { background: rgba(255,255,255,0.05); color: var(--muted); text-decoration: line-through; }
  select.form-select { background-color: var(--card) !important; color: var(--text) !important; border-color: var(--border) !important; font-size: 0.82rem; }
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
</head>
<body>

<!-- Header -->
<div class="header-bar d-flex align-items-center justify-content-between mb-3">
  <div>
    <span style="font-weight:700;font-size:1.05rem;">BTC Futures Scanner</span>
    <span style="color:var(--muted);margin-left:0.75rem;font-size:0.85rem;">Backtest Dashboard</span>
  </div>
  <div class="d-flex align-items-center gap-3">
    <span id="refresh-status" style="display:none;font-size:0.75rem;color:var(--muted)"></span>
    <span style="color:var(--muted);font-size:0.78rem;" id="gen-ts"></span>
  </div>
</div>
<!-- New-alert toast -->
<div id="toast" style="display:none;position:fixed;top:14px;right:20px;z-index:9999;background:var(--card);border:1px solid var(--green);border-radius:6px;padding:8px 16px;font-size:0.82rem;color:var(--green);box-shadow:0 4px 16px rgba(0,0,0,.5)">
  New alert received
</div>

<div class="container-fluid px-3 pb-4">

  <!-- Live Alerts (top) -->
  <div class="row g-3 mb-3" id="alertsSection" style="display:none">
    <div class="col-12">
      <div class="card p-3" style="border-color:rgba(88,166,255,0.35)">
        <div class="d-flex align-items-center justify-content-between mb-2">
          <div class="d-flex align-items-center gap-2">
            <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green)" id="live-dot"></span>
            <span class="section-title mb-0">Live Scanner Alerts</span>
          </div>
          <span style="color:var(--muted);font-size:0.75rem" id="alert-count"></span>
        </div>
        <div id="alertsList" style="max-height:340px;overflow-y:auto"></div>
      </div>
    </div>
  </div>

  <!-- Summary Cards -->
  <div class="row g-2 mb-3" id="summary-cards"></div>

  <!-- Charts Row 1 -->
  <div class="row g-3 mb-3">
    <div class="col-lg-5">
      <div class="card p-3 h-100">
        <div class="section-title">Win Rate by Confidence Band</div>
        <div style="position:relative;height:210px"><canvas id="confChart"></canvas></div>
      </div>
    </div>
    <div class="col-lg-3">
      <div class="card p-3 h-100">
        <div class="section-title">Outcome Split</div>
        <div style="position:relative;height:210px"><canvas id="outcomeChart"></canvas></div>
      </div>
    </div>
    <div class="col-lg-4">
      <div class="card p-3 h-100">
        <div class="section-title">Win Rate by Direction</div>
        <div style="position:relative;height:210px"><canvas id="dirChart"></canvas></div>
      </div>
    </div>
  </div>

  <!-- Cumulative P&L -->
  <div class="row g-3 mb-3">
    <div class="col-12">
      <div class="card p-3">
        <div class="section-title">Cumulative P&L  (R Multiples)</div>
        <div style="position:relative;height:190px"><canvas id="pnlChart"></canvas></div>
      </div>
    </div>
  </div>

  <!-- Trades Table -->
  <div class="row g-3 mb-3">
    <div class="col-12">
      <div class="card p-3">
        <div class="d-flex justify-content-between align-items-center mb-2">
          <div class="section-title mb-0">All Trades</div>
          <div class="d-flex gap-2">
            <select id="fTf"  class="form-select form-select-sm" style="width:auto"><option value="">All TFs</option></select>
            <select id="fOut" class="form-select form-select-sm" style="width:auto">
              <option value="">All Outcomes</option>
              <option>WIN</option><option>LOSS</option><option>OPEN</option>
            </select>
            <select id="fDir" class="form-select form-select-sm" style="width:auto">
              <option value="">All Directions</option>
              <option>LONG</option><option>SHORT</option>
            </select>
          </div>
        </div>
        <div style="overflow:auto;max-height:360px">
          <table class="table table-sm table-hover mb-0">
            <thead>
              <tr>
                <th>TF</th><th>Timestamp</th><th>Dir</th>
                <th class="text-end">Entry</th><th class="text-end">Target 1</th>
                <th class="text-end">Stop Loss</th><th class="text-end">R:R</th>
                <th class="text-end">Conf</th><th>Outcome</th>
              </tr>
            </thead>
            <tbody id="tradesBody"></tbody>
          </table>
        </div>
        <div id="tradeCount" style="color:var(--muted);font-size:0.75rem;margin-top:6px"></div>
      </div>
    </div>
  </div>


</div><!-- /container -->

<script>
const STATS        = __STATS__;
const TRADES       = __TRADES__;
const ALERTS       = __ALERTS__;
const SERVE_MODE   = __SERVE_MODE__;
const POLL_INTERVAL = __POLL_INTERVAL__; // seconds

// Chart global defaults
Chart.defaults.color       = '#8b949e';
Chart.defaults.borderColor = '#30363d';
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
Chart.defaults.font.size   = 11;

const C = { green:'#3fb950', red:'#f85149', yellow:'#d29922', blue:'#58a6ff', purple:'#bc8cff' };

// Convert UTC timestamp string to IST (UTC+5:30)
// Uses manual offset so result is correct regardless of server location or browser locale
function toIST(ts) {
  const d   = new Date(ts);
  const ist = new Date(d.getTime() + 330 * 60 * 1000); // add 5h 30m
  const pad = n => String(n).padStart(2, '0');
  return ist.getUTCFullYear() + '-' + pad(ist.getUTCMonth() + 1) + '-' + pad(ist.getUTCDate()) +
         ' ' + pad(ist.getUTCHours()) + ':' + pad(ist.getUTCMinutes()) + ' IST';
}

document.getElementById('gen-ts').textContent = 'Generated ' + new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });

// ─── Summary Cards ────────────────────────────────────────────────────────
(function renderCards() {
  const s = STATS;
  const wr_c = s.win_rate >= 55 ? C.green : s.win_rate >= 45 ? C.yellow : C.red;
  const ev_c = s.ev >= 0 ? C.green : C.red;
  let cards = [
    { v: s.total,              l: 'Total Signals',   c: C.blue },
    { v: s.resolved,           l: 'Resolved',        c: '#e6edf3' },
    { v: s.win_rate + '%',     l: 'Win Rate',        c: wr_c },
    { v: s.avg_rr + 'x',      l: 'Avg R:R',         c: C.blue },
    { v: (s.ev>=0?'+':'')+s.ev+'R', l: 'Expected Value', c: ev_c },
    { v: s.opens,              l: 'Still Open',      c: C.yellow },
  ];
  for (const [tf, d] of Object.entries(s.by_tf || {})) {
    const c = d.win_rate >= 55 ? C.green : d.win_rate >= 45 ? C.yellow : C.red;
    cards.push({ v: d.win_rate.toFixed(1)+'%', l: tf.toUpperCase()+' Win Rate', c, sub: d.wins+'/'+(d.wins+d.losses)+' resolved' });
  }
  document.getElementById('summary-cards').innerHTML = cards.map(c => `
    <div class="col-6 col-sm-4 col-md-3 col-xl-2">
      <div class="card stat-card">
        <div class="stat-value" style="color:${c.c}">${c.v}</div>
        <div class="stat-label">${c.l}</div>
        ${c.sub ? `<div class="stat-sub">${c.sub}</div>` : ''}
      </div>
    </div>`).join('');
})();

// ─── Confidence Band Chart ────────────────────────────────────────────────
(function renderConfChart() {
  const bands = STATS.by_conf || {};
  const labels = Object.keys(bands);
  const rates  = labels.map(k => bands[k].win_rate);
  const counts = labels.map(k => bands[k].total);
  new Chart(document.getElementById('confChart'), {
    data: {
      labels,
      datasets: [
        { type:'bar', label:'Win Rate %', data: rates,
          backgroundColor: rates.map(v => v>=55?'rgba(63,185,80,0.75)':v>=45?'rgba(210,153,34,0.75)':'rgba(248,81,73,0.75)'),
          borderRadius:4, yAxisID:'y' },
        { type:'bar', label:'Signals',    data: counts,
          backgroundColor:'rgba(88,166,255,0.2)', borderColor:'rgba(88,166,255,0.5)', borderWidth:1,
          borderRadius:4, yAxisID:'y2' },
      ]
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins: { legend:{ position:'bottom', labels:{ boxWidth:10, padding:10 } },
                 tooltip:{ callbacks:{ label: ctx => ctx.dataset.label+': '+ctx.raw+(ctx.datasetIndex===0?'%':'') } } },
      scales: {
        y:  { position:'left',  grid:{ color:'#21262d' }, ticks:{ callback:v=>v+'%' }, max:100, min:0 },
        y2: { position:'right', grid:{ display:false } },
        x:  { grid:{ display:false } }
      }
    }
  });
})();

// ─── Outcome Doughnut ─────────────────────────────────────────────────────
(function renderOutcomeChart() {
  const s = STATS;
  new Chart(document.getElementById('outcomeChart'), {
    type:'doughnut',
    data: {
      labels:['WIN','LOSS','OPEN'],
      datasets:[{ data:[s.wins,s.losses,s.opens], backgroundColor:[C.green,C.red,C.yellow], borderWidth:0, hoverOffset:5 }]
    },
    options: {
      responsive:true, maintainAspectRatio:false, cutout:'66%',
      plugins: {
        legend:{ position:'bottom', labels:{ boxWidth:10, padding:10 } },
        tooltip:{ callbacks:{ label: ctx => ctx.label+': '+ctx.raw+' ('+((ctx.raw/s.total)*100).toFixed(1)+'%)' } }
      }
    }
  });
})();

// ─── Direction Bar Chart ──────────────────────────────────────────────────
(function renderDirChart() {
  const d = STATS.by_dir || {};
  const keys = Object.keys(d);
  new Chart(document.getElementById('dirChart'), {
    type:'bar',
    data: {
      labels: keys,
      datasets:[{
        label:'Win Rate %',
        data: keys.map(k => d[k].win_rate),
        backgroundColor: [C.blue, C.purple],
        borderRadius:6
      }]
    },
    options: {
      responsive:true, maintainAspectRatio:false, indexAxis:'y',
      plugins: {
        legend:{ display:false },
        tooltip:{ callbacks:{ label: ctx => ctx.raw+'%  ('+d[keys[ctx.dataIndex]].wins+'/'+d[keys[ctx.dataIndex]].resolved+' resolved)' } }
      },
      scales: {
        x:{ grid:{ color:'#21262d' }, ticks:{ callback:v=>v+'%' }, max:100, min:0 },
        y:{ grid:{ display:false } }
      }
    }
  });
})();

// ─── Cumulative P&L ───────────────────────────────────────────────────────
(function renderPnlChart() {
  const pnl = STATS.cumulative_pnl || [];
  if (!pnl.length) return;
  const step   = Math.max(1, Math.floor(pnl.length / 24));
  const labels = pnl.map((p,i) => i % step === 0 ? toIST(p.ts).slice(5, 16) : '');
  const data   = pnl.map(p => p.val);
  const final  = data[data.length-1];
  new Chart(document.getElementById('pnlChart'), {
    type:'line',
    data: {
      labels,
      datasets:[{
        data,
        borderColor: final >= 0 ? C.green : C.red,
        borderWidth: 1.5,
        pointRadius: 0,
        pointHoverRadius: 4,
        fill: { target:'origin', above:'rgba(63,185,80,0.07)', below:'rgba(248,81,73,0.07)' },
        tension: 0.25,
      }]
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins: {
        legend:{ display:false },
        tooltip:{ callbacks:{
          title: items => toIST(pnl[items[0].dataIndex].ts),
          label: ctx  => (ctx.raw>=0?'+':'')+ctx.raw.toFixed(3)+'R  ['+pnl[ctx.dataIndex].tf+' '+pnl[ctx.dataIndex].outcome+']'
        }}
      },
      scales: {
        x:{ grid:{ display:false }, ticks:{ maxRotation:0 } },
        y:{ grid:{ color:'#21262d' }, ticks:{ callback:v=>(v>=0?'+':'')+v+'R' } }
      }
    }
  });
})();

// ─── Trades Table ─────────────────────────────────────────────────────────
function bp(cls, txt) { return `<span class="badge-pill bp-${cls}">${txt}</span>`; }
function fmt(n) { return parseFloat(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}); }
function confColor(c) { return c>=90?'#3fb950':c>=85?'#58a6ff':c>=80?'#d29922':'#8b949e'; }

(function initTable() {
  const tfs = [...new Set(TRADES.map(t=>t.tf))].sort();
  const sel = document.getElementById('fTf');
  tfs.forEach(tf => { const o=document.createElement('option'); o.value=o.textContent=tf; sel.appendChild(o); });
  ['fTf','fOut','fDir'].forEach(id => document.getElementById(id).addEventListener('change', renderTable));
  renderTable();
})();

function renderTable() {
  const tf  = document.getElementById('fTf').value;
  const out = document.getElementById('fOut').value;
  const dir = document.getElementById('fDir').value;
  const filtered = TRADES.filter(t =>
    (!tf  || t.tf === tf) &&
    (!out || t.outcome === out) &&
    (!dir || t.direction === dir)
  );
  document.getElementById('tradesBody').innerHTML = filtered.map(t => `
    <tr>
      <td><code style="color:var(--muted);font-size:0.78rem">${t.tf}</code></td>
      <td style="color:var(--muted);white-space:nowrap;font-size:0.78rem">${toIST(t.timestamp)}</td>
      <td>${bp(t.direction.toLowerCase(), t.direction)}</td>
      <td class="text-end">${fmt(t.entry)}</td>
      <td class="text-end win">${fmt(t.target_1)}</td>
      <td class="text-end loss">${fmt(t.stop_loss)}</td>
      <td class="text-end">${parseFloat(t.rr).toFixed(2)}x</td>
      <td class="text-end"><span style="color:${confColor(t.confidence)};font-weight:600">${t.confidence}</span></td>
      <td>${bp(t.outcome.toLowerCase(), t.outcome)}</td>
    </tr>`).join('');
  document.getElementById('tradeCount').textContent =
    `Showing ${filtered.length} of ${TRADES.length} trades`;
}

// ─── Leverage Recommendation Helper ──────────────────────────────────────
function leverageRow(a) {
  let lv = a.leverage_rec;
  // Fall back: compute from entry_price + stop_loss so old alerts also show leverage
  if (!lv) {
    const entry = parseFloat(a.entry_price || a.entry || 0);
    const sl    = parseFloat(a.stop_loss   || 0);
    const conf  = parseFloat(a.confidence_score || 75);
    if (entry > 0 && sl > 0) {
      const slPct  = Math.abs(entry - sl) / entry;
      const safeMax = slPct > 0 ? (0.5 / slPct) : 2;
      const cap     = conf >= 90 ? 25 : conf >= 85 ? 20 : conf >= 80 ? 15 : conf >= 75 ? 10 : 5;
      const capped  = Math.min(safeMax, cap, 25);
      const tiers   = [2, 3, 5, 7, 10, 15, 20, 25];
      const snap    = v => tiers.slice().reverse().find(t => v >= t) || 2;
      lv = {
        conservative:    snap(Math.max(2, capped * 0.30)),
        moderate:        snap(Math.max(2, capped * 0.60)),
        aggressive:      snap(Math.max(2, capped)),
        sl_distance_pct: (slPct * 100).toFixed(2),
        max_theoretical: Math.floor(slPct > 0 ? 1.0 / slPct : 1),
      };
    }
  }
  if (!lv) return '';
  const badge = (label, val, color) =>
    `<span style="background:${color};color:#000;font-weight:700;border-radius:4px;padding:1px 6px;font-size:0.75rem;margin-right:4px">${label} ${val}x</span>`;
  return `<div style="margin-top:5px">` +
    badge('Conservative', lv.conservative, '#4ade80') +
    badge('Moderate',     lv.moderate,     '#facc15') +
    badge('Aggressive',   lv.aggressive,   '#f97316') +
    `<span style="color:var(--muted);font-size:0.72rem">&nbsp;SL ${lv.sl_distance_pct}% away &bull; liq &gt;${lv.max_theoretical}x</span>` +
    `</div>`;
}

// ─── Signal Validity Helper ───────────────────────────────────────────────
function validityRow(a) {
  if (!a.expires_at) return '';
  const exp     = new Date(a.expires_at);
  const expired = exp < new Date();
  // Convert expires_at to HH:MM IST
  const ist  = new Date(exp.getTime() + 330 * 60 * 1000);
  const pad  = n => String(n).padStart(2, '0');
  const time = pad(ist.getUTCHours()) + ':' + pad(ist.getUTCMinutes()) + ' IST';
  const mins = a.valid_for_minutes || '?';
  if (expired) {
    return `<div style="margin-top:5px;font-size:0.78rem;color:#ffffff;opacity:0.7">` +
           `&#8987; Expired &nbsp;&bull;&nbsp; was valid ~${mins} min</div>`;
  }
  return `<div style="margin-top:5px;font-size:0.78rem;color:#4ade80">` +
         `&#9200; Active &nbsp;&bull;&nbsp; valid ~${mins} min &nbsp;&bull;&nbsp; until ${time}</div>`;
}

// ─── Live Alerts ──────────────────────────────────────────────────────────
(function renderAlerts() {
  if (!ALERTS.length) return;
  document.getElementById('alertsSection').style.display = '';
  document.getElementById('alert-count').textContent = ALERTS.length + ' alert' + (ALERTS.length !== 1 ? 's' : '') + ' recorded';
  // Pulse animation on live dot
  const dot = document.getElementById('live-dot');
  dot.style.animation = 'pulse 2s infinite';
  const style = document.createElement('style');
  style.textContent = '@keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.4;transform:scale(1.4)} }';
  document.head.appendChild(style);
  const list = document.getElementById('alertsList');
  list.innerHTML = [...ALERTS].reverse().slice(0, 20).map(a => {
    const bd = a.indicator_breakdown || {};
    const chips = Object.entries(bd)
      .filter(([k]) => k !== 'total_score')
      .map(([k,v]) => `<span class="chip ${v.aligned?'chip-ok':'chip-no'}">${k.toUpperCase()} ${v.earned}/${v.weight}</span>`)
      .join('');
    return `
    <div class="alert-card alert-${a.signal_type.toLowerCase()}">
      <div class="d-flex justify-content-between align-items-center">
        <div>
          ${bp(a.signal_type.toLowerCase(), a.signal_type)}
          <span style="color:var(--muted);font-size:0.78rem;margin-left:8px">${a.timeframe} &nbsp; ${toIST(a.timestamp)}</span>
        </div>
        <span style="font-weight:700;color:${confColor(a.confidence_score)}">${a.confidence_score}/100</span>
      </div>
      <div style="margin-top:8px;font-size:0.85rem;">
        <span style="color:var(--muted)">Entry</span> <b style="color:#ffffff">$${parseFloat(a.entry_price||a.entry).toLocaleString(undefined,{minimumFractionDigits:2})}</b>
        &nbsp;&nbsp;<span style="color:var(--muted)">T1</span> <b class="win">$${parseFloat(a.target_1).toLocaleString(undefined,{minimumFractionDigits:2})}</b>
        &nbsp;&nbsp;<span style="color:var(--muted)">T2</span> <b class="win">$${parseFloat(a.target_2||a.target_1).toLocaleString(undefined,{minimumFractionDigits:2})}</b>
        &nbsp;&nbsp;<span style="color:var(--muted)">SL</span> <b class="loss">$${parseFloat(a.stop_loss).toLocaleString(undefined,{minimumFractionDigits:2})}</b>
        &nbsp;&nbsp;<span style="color:var(--muted)">R:R</span> <b>${parseFloat(a.risk_reward||a.rr||0).toFixed(2)}x</b>
        ${a.oi_signal ? `&nbsp;&nbsp;<span style="color:var(--muted);font-size:0.78rem">${a.oi_signal}</span>` : ''}
      </div>
      <div style="margin-top:6px">${chips}</div>
      ${leverageRow(a)}
      ${validityRow(a)}
    </div>`;
  }).join('');
})();

// ─── Live Polling (serve mode only) ──────────────────────────────────────
if (SERVE_MODE) {
  document.getElementById('refresh-status').style.display = '';

  // Track latest alert by timestamp so we detect new alerts even if count stays same
  let latestTs = ALERTS.length ? ALERTS[ALERTS.length - 1].timestamp : '';
  let countdown = POLL_INTERVAL;

  function buildAlertHTML(alertList) {
    return [...alertList].reverse().slice(0, 20).map(a => {
      const bd = a.indicator_breakdown || {};
      const chips = Object.entries(bd)
        .filter(([k]) => k !== 'total_score')
        .map(([k,v]) => `<span class="chip ${v.aligned?'chip-ok':'chip-no'}">${k.toUpperCase()} ${v.earned}/${v.weight}</span>`)
        .join('');
      return `
      <div class="alert-card alert-${a.signal_type.toLowerCase()}">
        <div class="d-flex justify-content-between align-items-center">
          <div>
            ${bp(a.signal_type.toLowerCase(), a.signal_type)}
            <span style="color:var(--muted);font-size:0.78rem;margin-left:8px">${a.timeframe} &nbsp; ${toIST(a.timestamp)}</span>
          </div>
          <span style="font-weight:700;color:${confColor(a.confidence_score)}">${a.confidence_score}/100</span>
        </div>
        <div style="margin-top:8px;font-size:0.85rem;">
          <span style="color:var(--muted)">Entry</span> <b style="color:#ffffff">$${parseFloat(a.entry_price).toLocaleString(undefined,{minimumFractionDigits:2})}</b>
          &nbsp;&nbsp;<span style="color:var(--muted)">T1</span> <b class="win">$${parseFloat(a.target_1).toLocaleString(undefined,{minimumFractionDigits:2})}</b>
          &nbsp;&nbsp;<span style="color:var(--muted)">T2</span> <b class="win">$${parseFloat(a.target_2||a.target_1).toLocaleString(undefined,{minimumFractionDigits:2})}</b>
          &nbsp;&nbsp;<span style="color:var(--muted)">SL</span> <b class="loss">$${parseFloat(a.stop_loss).toLocaleString(undefined,{minimumFractionDigits:2})}</b>
          &nbsp;&nbsp;<span style="color:var(--muted)">R:R</span> <b style="color:#ffffff">${parseFloat(a.risk_reward||a.rr||0).toFixed(2)}x</b>
          &nbsp;&nbsp;<span style="color:var(--muted);font-size:0.78rem">${a.oi_signal||''}</span>
        </div>
        <div style="margin-top:6px">${chips}</div>
        ${leverageRow(a)}
        ${validityRow(a)}
      </div>`;
    }).join('');
  }

  function showToast() {
    const t = document.getElementById('toast');
    t.style.display = '';
    setTimeout(() => { t.style.display = 'none'; }, 5000);
  }

  async function pollAlerts() {
    try {
      const resp = await fetch('/api/alerts');
      if (!resp.ok) return;
      const fresh = await resp.json();
      const newTs = fresh.length ? fresh[fresh.length - 1].timestamp : '';

      // Update alerts panel whenever newest alert timestamp changes
      if (newTs !== latestTs) {
        if (fresh.length > 0 && newTs > latestTs) showToast();
        latestTs = newTs;
        document.getElementById('alertsSection').style.display = '';
        document.getElementById('alert-count').textContent =
          fresh.length + ' alert' + (fresh.length !== 1 ? 's' : '') + ' recorded';
        document.getElementById('alertsList').innerHTML = buildAlertHTML(fresh);
      }
      document.getElementById('refresh-status').textContent =
        'Alerts updated ' + new Date().toLocaleTimeString();
    } catch(e) { /* server may be busy */ }
    countdown = POLL_INTERVAL;
  }

  // Countdown ticker every 1 s
  setInterval(() => {
    countdown = Math.max(0, countdown - 1);
    document.getElementById('refresh-status').textContent =
      countdown > 0 ? 'Next alert check in ' + countdown + 's'
                    : 'Checking alerts...';
  }, 1000);

  // Poll alerts on interval
  setInterval(pollAlerts, POLL_INTERVAL * 1000);
  pollAlerts(); // run immediately on load

  // Auto full-page reload every 5 min to refresh stats, charts, and trades table
  setTimeout(() => location.reload(), 5 * 60 * 1000);
}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

def generate_html(
    stats: Dict,
    trades: List[Dict],
    alerts: List[Dict],
    serve_mode: bool = False,
    poll_interval: int = 60,
) -> str:
    return (HTML_TEMPLATE
            .replace("__STATS__",         json.dumps(stats,   ensure_ascii=False))
            .replace("__TRADES__",        json.dumps(trades,  ensure_ascii=False))
            .replace("__ALERTS__",        json.dumps(alerts,  ensure_ascii=False))
            .replace("__SERVE_MODE__",    "true" if serve_mode else "false")
            .replace("__POLL_INTERVAL__", str(poll_interval)))


# ---------------------------------------------------------------------------
# HTTP Server (serve mode)
# ---------------------------------------------------------------------------

def make_handler(alerts_path: str, backtest_path: str, poll_interval: int):
    """Return a request handler that regenerates the dashboard on every page load."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # silence per-request logs

        def _send(self, code: int, ctype: str, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                # Re-read ALL data on every page load so stats/charts are always fresh
                trades = read_backtest_csv(backtest_path)
                alerts = read_alerts_jsonl(alerts_path)
                stats  = compute_stats(trades) if trades else {}
                fresh  = generate_html(stats, trades, alerts,
                                       serve_mode=True, poll_interval=poll_interval)
                self._send(200, "text/html; charset=utf-8", fresh.encode("utf-8"))

            elif self.path == "/api/alerts":
                alerts = read_alerts_jsonl(alerts_path)
                body   = json.dumps(alerts, ensure_ascii=False).encode("utf-8")
                self._send(200, "application/json", body)

            elif self.path == "/status":
                # Scanner health check — visit /status in browser to see live state
                now_utc = datetime.now(timezone.utc).isoformat()

                # Heartbeat file written by scanner on every candle
                hb_path = Path("scanner_heartbeat.txt")
                if hb_path.exists():
                    hb_ts  = hb_path.read_text().strip()
                    try:
                        hb_dt  = datetime.fromisoformat(hb_ts)
                        age_s  = int((datetime.now(timezone.utc) - hb_dt).total_seconds())
                        scanner_alive = age_s < 300  # alive if heartbeat < 5 min ago
                    except ValueError:
                        hb_ts, age_s, scanner_alive = "invalid", -1, False
                else:
                    hb_ts, age_s, scanner_alive = "never", -1, False

                # Alerts summary
                all_alerts = read_alerts_jsonl(alerts_path)
                last_alert = all_alerts[-1].get("timestamp") if all_alerts else None

                # Last error written by scanner (if any)
                err_path = Path("scanner_error.txt")
                last_error = err_path.read_text().strip() if err_path.exists() else None

                # Learned weights info (if self-learning has run)
                lw_path = Path("learned_weights.json")
                learned = None
                if lw_path.exists():
                    try:
                        learned = json.loads(lw_path.read_text(encoding="utf-8"))
                    except (json.JSONDecodeError, OSError):
                        pass

                # Outcomes count
                oc_path = Path("outcomes.jsonl")
                outcomes_count = 0
                if oc_path.exists():
                    try:
                        outcomes_count = sum(
                            1 for ln in oc_path.read_text(encoding="utf-8").splitlines()
                            if ln.strip()
                        )
                    except OSError:
                        pass

                status = {
                    "server_time_utc":        now_utc,
                    "scanner_alive":          scanner_alive,
                    "scanner_last_heartbeat": hb_ts,
                    "heartbeat_age_seconds":  age_s,
                    "alerts_count":           len(all_alerts),
                    "last_alert_timestamp":   last_alert,
                    "scanner_last_error":     last_error,
                    "outcomes_resolved":      outcomes_count,
                    "learned_weights":        learned,
                }
                body = json.dumps(status, indent=2).encode("utf-8")
                self._send(200, "application/json", body)

            else:
                self._send(404, "text/plain", b"Not found")

    return Handler


def serve(alerts_path: str, backtest_path: str, port: int, poll_interval: int) -> None:
    # In cloud environments bind to all interfaces; locally bind to loopback only
    host = "0.0.0.0" if os.getenv("PORT") or os.getenv("KOYEB_APP_NAME") else "127.0.0.1"
    handler = make_handler(alerts_path, backtest_path, poll_interval)
    server  = HTTPServer((host, port), handler)
    url     = f"http://localhost:{port}/"
    print(f"Dashboard server -> {url}  (Ctrl+C to stop)")
    print(f"Bound to {host}:{port}. Alerts panel polls every {poll_interval}s.")
    if host == "127.0.0.1":
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="BTC Futures Scanner - Dashboard Generator")
    parser.add_argument("--backtest",  default="backtest_results.csv",
                        help="Backtest CSV file  (default: backtest_results.csv)")
    parser.add_argument("--alerts",    default="alerts.jsonl",
                        help="Live alerts JSONL  (default: alerts.jsonl)")
    parser.add_argument("--out",       default="dashboard.html",
                        help="Output HTML file for static mode  (default: dashboard.html)")
    parser.add_argument("--serve",     action="store_true",
                        help="Start a live HTTP server instead of writing a static file")
    parser.add_argument("--port",      type=int, default=int(os.getenv("PORT", "5000")),
                        help="Port for --serve mode  (default: PORT env var or 5000)")
    parser.add_argument("--interval",  type=int, default=30,
                        help="Alert poll interval in seconds for --serve mode  (default: 30)")
    args = parser.parse_args()

    trades = read_backtest_csv(args.backtest)
    if not trades:
        print(f"No backtest data at '{args.backtest}'.")
        print("Run first:  py -3.13 -E backtest.py")
        sys.exit(1)

    alerts = read_alerts_jsonl(args.alerts)
    stats  = compute_stats(trades)

    if args.serve:
        serve(args.alerts, args.backtest, args.port, args.interval)
    else:
        html = generate_html(stats, trades, alerts)
        out  = Path(args.out).resolve()
        out.write_text(html, encoding="utf-8")
        print(f"Dashboard -> {out}")
        webbrowser.open(out.as_uri())


if __name__ == "__main__":
    main()
