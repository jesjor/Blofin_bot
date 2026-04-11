"""
dashboard.py — Full trading dashboard for the BloFin 7-Bot system.

Serves on port $PORT (Railway) or 8080 (default).
Routes:
  GET /           → dashboard HTML
  GET /api/state  → full system state JSON (polled every 5s by frontend)
  GET /health     → Railway health check
  GET /metrics    → extended metrics

The dashboard shows:
  • Live balance, daily P&L, drawdown gauge
  • 7 bot status cards
  • Open positions with full entry commentary + forward strategy
  • Trade history with entry/exit commentary
  • P&L chart (30-day)
  • Signal log
  • Risk events
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import psutil
from aiohttp import web

log = logging.getLogger("dashboard")

_start_time = time.time()

# ── HTML template ─────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BloFin Bot Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d1117;--bg2:#161b22;--bg3:#21262d;
  --border:#30363d;--text:#e6edf3;--muted:#8b949e;
  --green:#3fb950;--red:#f85149;--yellow:#d29922;
  --blue:#58a6ff;--purple:#bc8cff;--teal:#39d353;
  --card-r:8px;--font:'SF Mono','Fira Code',monospace;
}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;min-height:100vh}
a{color:var(--blue);text-decoration:none}

/* Header */
.header{background:var(--bg2);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;gap:24px;flex-wrap:wrap}
.logo{font-size:18px;font-weight:700;color:var(--text);letter-spacing:-0.5px}
.logo span{color:var(--blue)}
.mode-badge{background:#0d4a1a;color:var(--green);border:1px solid #238636;padding:3px 10px;border-radius:20px;font-size:12px;font-weight:600}
.mode-badge.live{background:#4a0d0d;color:var(--red);border-color:#da3633}
.header-metric{display:flex;flex-direction:column;gap:2px}
.header-metric .label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px}
.header-metric .value{font-size:16px;font-weight:600;font-family:var(--font)}
.halted-banner{background:#4a0d0d;border:1px solid #da3633;color:#ff7b72;padding:8px 16px;border-radius:var(--card-r);font-weight:600;font-size:13px}
.status-dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px;animation:pulse 2s infinite}
.status-dot.running{background:var(--green)}
.status-dot.halted{background:var(--red);animation:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* Layout */
.container{padding:20px 24px;max-width:1600px;margin:0 auto}
.grid-4{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}
.grid-7{display:grid;grid-template-columns:repeat(7,1fr);gap:10px;margin-bottom:20px}
@media(max-width:1200px){.grid-4{grid-template-columns:repeat(2,1fr)}.grid-7{grid-template-columns:repeat(4,1fr)}}
@media(max-width:600px){.grid-4{grid-template-columns:1fr}.grid-7{grid-template-columns:repeat(2,1fr)}}

/* Cards */
.card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--card-r);padding:16px}
.card-title{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:12px;font-weight:600}
.metric-big{font-size:28px;font-weight:700;font-family:var(--font);line-height:1}
.metric-sub{font-size:12px;color:var(--muted);margin-top:4px}

/* Bot cards */
.bot-card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--card-r);padding:12px;transition:border-color .2s}
.bot-card.running{border-color:#238636}
.bot-card.paused{border-color:var(--yellow)}
.bot-card.halted,.bot-card.stopped{border-color:#da3633}
.bot-name{font-size:11px;font-weight:600;color:var(--text);margin-bottom:4px}
.bot-status{font-size:11px;color:var(--muted);display:flex;align-items:center;gap:4px}
.bot-status .dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.dot-green{background:var(--green)}
.dot-yellow{background:var(--yellow)}
.dot-red{background:var(--red)}
.dot-gray{background:var(--muted)}
.bot-trades{font-size:11px;color:var(--muted);margin-top:4px}

/* Tables */
.section{margin-bottom:24px}
.section-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.section-title{font-size:15px;font-weight:600;color:var(--text)}
.section-count{background:var(--bg3);border:1px solid var(--border);color:var(--muted);padding:2px 8px;border-radius:20px;font-size:12px}
.table-wrap{overflow-x:auto;border-radius:var(--card-r);border:1px solid var(--border)}
table{width:100%;border-collapse:collapse;font-size:13px}
th{background:var(--bg3);color:var(--muted);font-weight:600;padding:10px 14px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:0.4px;white-space:nowrap;border-bottom:1px solid var(--border)}
td{padding:10px 14px;border-bottom:1px solid var(--border);vertical-align:top}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(255,255,255,.025)}
.tag{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;white-space:nowrap}
.tag-long{background:#0d4a1a;color:var(--green)}
.tag-short{background:#4a0d0d;color:var(--red)}
.pnl-pos{color:var(--green);font-family:var(--font);font-weight:600}
.pnl-neg{color:var(--red);font-family:var(--font);font-weight:600}
.pnl-zero{color:var(--muted);font-family:var(--font)}
.price{font-family:var(--font);color:var(--text)}
.muted{color:var(--muted)}
.inst{font-weight:700;color:var(--text)}
.bot-tag{font-size:11px;color:var(--blue);font-family:var(--font)}

/* Commentary */
.commentary{font-size:12px;color:var(--muted);line-height:1.6;max-width:480px}
.commentary .trigger{color:var(--text);margin-bottom:6px}
.commentary .forward{color:var(--muted)}
.commentary-badge{display:inline-block;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:var(--blue);border:1px solid var(--blue);padding:1px 6px;border-radius:3px;margin-bottom:4px}

/* Chart */
.chart-wrap{position:relative;height:220px}

/* Tabs */
.tabs{display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid var(--border);padding-bottom:-1px}
.tab{padding:8px 16px;font-size:13px;font-weight:500;color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;transition:color .15s,border-color .15s}
.tab.active{color:var(--blue);border-bottom-color:var(--blue)}
.tab-content{display:none}
.tab-content.active{display:block}

/* Drawdown bar */
.drawdown-bar{height:6px;background:var(--bg3);border-radius:3px;overflow:hidden;margin-top:8px}
.drawdown-fill{height:100%;border-radius:3px;transition:width .5s}

/* Update indicator */
.update-time{font-size:11px;color:var(--muted);margin-left:auto}

/* Empty state */
.empty{text-align:center;padding:40px;color:var(--muted)}
.empty .icon{font-size:32px;margin-bottom:8px}

/* Responsive table columns */
.hide-sm{display:table-cell}
@media(max-width:900px){.hide-sm{display:none}}

/* Risk event chip */
.chip{font-size:11px;padding:2px 8px;border-radius:4px;border:1px solid}
.chip-halt{background:#4a0d0d;color:#ff7b72;border-color:#da3633}
.chip-block{background:#2d2209;color:var(--yellow);border-color:#9e6a03}
.chip-resume{background:#0d4a1a;color:var(--green);border-color:#238636}
.chip-reconcile{background:#1a1a2e;color:var(--blue);border-color:var(--blue)}
</style>
</head>
<body>

<!-- Header -->
<div class="header" id="header">
  <div class="logo">BloFin<span>Bot</span></div>
  <div class="mode-badge" id="mode-badge">PAPER</div>
  <div class="header-metric">
    <div class="label">Balance</div>
    <div class="value" id="h-balance">—</div>
  </div>
  <div class="header-metric">
    <div class="label">Daily P&L</div>
    <div class="value" id="h-daily-pnl">—</div>
  </div>
  <div class="header-metric">
    <div class="label">Drawdown</div>
    <div class="value" id="h-drawdown">—</div>
  </div>
  <div class="header-metric">
    <div class="label">Open Positions</div>
    <div class="value" id="h-positions">—</div>
  </div>
  <div class="header-metric">
    <div class="label">Uptime</div>
    <div class="value" id="h-uptime">—</div>
  </div>
  <div id="halt-banner" style="display:none" class="halted-banner">🚨 SYSTEM HALTED</div>
  <div class="update-time" id="last-update">Connecting...</div>
</div>

<div class="container">

  <!-- Summary metrics -->
  <div class="grid-4" style="margin-top:16px">
    <div class="card">
      <div class="card-title">Total Realized P&L</div>
      <div class="metric-big" id="m-total-pnl">—</div>
      <div class="metric-sub" id="m-total-pnl-sub">all time</div>
    </div>
    <div class="card">
      <div class="card-title">Win Rate</div>
      <div class="metric-big" id="m-winrate">—</div>
      <div class="metric-sub" id="m-winrate-sub">—</div>
    </div>
    <div class="card">
      <div class="card-title">Total Trades</div>
      <div class="metric-big" id="m-trades">—</div>
      <div class="metric-sub" id="m-trades-sub">since start</div>
    </div>
    <div class="card">
      <div class="card-title">Today's Trades</div>
      <div class="metric-big" id="m-today-trades">—</div>
      <div class="metric-sub">today</div>
    </div>
  </div>

  <!-- Bot status grid -->
  <div class="section-header">
    <div class="section-title">Bot Status</div>
  </div>
  <div class="grid-7" id="bot-grid">
    <!-- populated by JS -->
  </div>

  <!-- Main tabs -->
  <div class="tabs">
    <div class="tab active" onclick="switchTab('positions')">Open Positions</div>
    <div class="tab" onclick="switchTab('trades')">Trade History</div>
    <div class="tab" onclick="switchTab('signals')">Signal Log</div>
    <div class="tab" onclick="switchTab('pnl')">P&L Chart</div>
    <div class="tab" onclick="switchTab('risk')">Risk Events</div>
    <div class="tab" onclick="switchTab('bots')">Bot Stats</div>
  </div>

  <!-- Open Positions -->
  <div id="tab-positions" class="tab-content active section">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Asset</th>
            <th>Bot</th>
            <th>Side</th>
            <th>Entry</th>
            <th>Current</th>
            <th>Size</th>
            <th>Unreal. P&L</th>
            <th>TP</th>
            <th>SL</th>
            <th>Since</th>
            <th class="hide-sm">Entry Commentary</th>
            <th class="hide-sm">Forward Strategy</th>
          </tr>
        </thead>
        <tbody id="positions-body">
          <tr><td colspan="12" class="empty"><div class="icon">📊</div>No open positions</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- Trade History -->
  <div id="tab-trades" class="tab-content section">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Closed</th>
            <th>Asset</th>
            <th>Bot</th>
            <th>Side</th>
            <th>Entry</th>
            <th>Exit</th>
            <th>Net P&L</th>
            <th>Duration</th>
            <th>Reason</th>
            <th class="hide-sm">Exit Commentary</th>
          </tr>
        </thead>
        <tbody id="trades-body">
          <tr><td colspan="10" class="empty"><div class="icon">📈</div>No completed trades yet</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- Signal Log -->
  <div id="tab-signals" class="tab-content section">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Bot</th>
            <th>Asset</th>
            <th>Signal</th>
            <th>Direction</th>
            <th>Price</th>
            <th>Risk Check</th>
          </tr>
        </thead>
        <tbody id="signals-body">
          <tr><td colspan="7" class="empty"><div class="icon">📡</div>Waiting for signals...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- P&L Chart -->
  <div id="tab-pnl" class="tab-content section">
    <div class="card">
      <div class="card-title">30-Day Cumulative P&L</div>
      <div class="chart-wrap">
        <canvas id="pnl-chart"></canvas>
      </div>
    </div>
  </div>

  <!-- Risk Events -->
  <div id="tab-risk" class="tab-content section">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Type</th>
            <th>Bot</th>
            <th>Reason</th>
          </tr>
        </thead>
        <tbody id="risk-body">
          <tr><td colspan="4" class="empty"><div class="icon">🛡️</div>No risk events</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- Bot Stats -->
  <div id="tab-bots" class="tab-content section">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Bot</th>
            <th>Trades</th>
            <th>Wins</th>
            <th>Losses</th>
            <th>Win Rate</th>
            <th>Total P&L</th>
            <th>Avg P&L</th>
            <th>Avg Duration</th>
          </tr>
        </thead>
        <tbody id="bots-stats-body">
          <tr><td colspan="8" class="empty"><div class="icon">🤖</div>No bot data yet</td></tr>
        </tbody>
      </table>
    </div>
  </div>

</div>

<script>
// ── State ────────────────────────────────────────────────────────────────────
let pnlChart = null;
let currentPrices = {};
let activeTab = 'positions';

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab').forEach((t,i) => {
    const tabs = ['positions','trades','signals','pnl','risk','bots'];
    t.classList.toggle('active', tabs[i] === name);
  });
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  activeTab = name;
}

// ── Formatters ────────────────────────────────────────────────────────────────
const fmt = n => n === null || n === undefined ? '—' : parseFloat(n).toFixed(4);
const fmt2 = n => n === null || n === undefined ? '—' : parseFloat(n).toFixed(2);
const fmtPnl = n => {
  if (n === null || n === undefined) return '<span class="muted">—</span>';
  const v = parseFloat(n);
  const cls = v > 0 ? 'pnl-pos' : v < 0 ? 'pnl-neg' : 'pnl-zero';
  return `<span class="${cls}">${v >= 0 ? '+' : ''}${v.toFixed(2)} USDT</span>`;
};
const fmtTime = s => {
  if (!s) return '—';
  const d = new Date(s);
  return d.toLocaleString('en-GB',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
};
const fmtDur = s => {
  if (!s) return '—';
  if (s < 60) return s + 's';
  if (s < 3600) return Math.floor(s/60) + 'm';
  if (s < 86400) return Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm';
  return Math.floor(s/86400) + 'd ' + Math.floor((s%86400)/3600) + 'h';
};
const timeAgo = s => {
  if (!s) return '—';
  const d = (Date.now() - new Date(s)) / 1000;
  if (d < 60) return Math.floor(d) + 's ago';
  if (d < 3600) return Math.floor(d/60) + 'm ago';
  if (d < 86400) return Math.floor(d/3600) + 'h ago';
  return Math.floor(d/86400) + 'd ago';
};
const fmtUptime = s => {
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
};
const botColors = {
  bot1_scalper:'#f78166',bot2_trend:'#79c0ff',bot3_meanrev:'#d2a8ff',
  bot4_mm:'#ffa657',bot5_breakout:'#7ee787',bot6_funding:'#a5d6ff',
  bot7_momentum:'#ffa8a8'
};
const botNames = {
  bot1_scalper:'Scalper',bot2_trend:'Trend',bot3_meanrev:'MeanRev',
  bot4_mm:'MM',bot5_breakout:'Breakout',bot6_funding:'FundArb',
  bot7_momentum:'Momentum'
};

// ── Render functions ──────────────────────────────────────────────────────────

function renderBotGrid(botStates, botStats) {
  const statsMap = {};
  (botStats||[]).forEach(b => statsMap[b.bot_id] = b);

  const bots = [
    {id:'bot6_funding',name:'Bot 6\nFund Arb'},
    {id:'bot2_trend',name:'Bot 2\nTrend'},
    {id:'bot5_breakout',name:'Bot 5\nBreakout'},
    {id:'bot3_meanrev',name:'Bot 3\nMeanRev'},
    {id:'bot1_scalper',name:'Bot 1\nScalper'},
    {id:'bot4_mm',name:'Bot 4\nMkt Maker'},
    {id:'bot7_momentum',name:'Bot 7\nMomentum'},
  ];

  const grid = document.getElementById('bot-grid');
  grid.innerHTML = bots.map(b => {
    const state = (botStates||{})[b.id] || {status:'OFFLINE'};
    const stats = statsMap[b.id];
    const status = (state.status||'OFFLINE').toLowerCase();
    const dotCls = status==='running'?'dot-green':status==='paused'?'dot-yellow':status==='stopped'?'dot-red':'dot-gray';
    const cardCls = status==='running'?'running':status==='paused'?'paused':status==='stopped'?'stopped':'';
    const wr = stats && stats.total_trades > 0
      ? ((stats.wins / stats.total_trades)*100).toFixed(0)+'%'
      : '—';
    const pnlColor = stats && stats.total_pnl > 0 ? 'var(--green)' : stats && stats.total_pnl < 0 ? 'var(--red)' : 'var(--muted)';
    const pnlStr = stats ? (stats.total_pnl >= 0 ? '+' : '') + parseFloat(stats.total_pnl).toFixed(2) : '—';
    return `
      <div class="bot-card ${cardCls}">
        <div class="bot-name">${b.name.replace('\n','<br>')}</div>
        <div class="bot-status"><span class="dot ${dotCls}"></span>${state.status||'OFFLINE'}</div>
        <div class="bot-trades" style="margin-top:6px">
          <span style="color:var(--muted)">Trades: </span>${stats ? stats.total_trades : '—'}<br>
          <span style="color:var(--muted)">WR: </span>${wr}<br>
          <span style="color:${pnlColor};font-family:var(--font);font-size:12px">${pnlStr}</span>
        </div>
      </div>`;
  }).join('');
}

function renderPositions(positions, prices) {
  const tbody = document.getElementById('positions-body');
  if (!positions || positions.length === 0) {
    tbody.innerHTML = '<tr><td colspan="12" class="empty"><div class="icon">📊</div>No open positions</td></tr>';
    return;
  }
  tbody.innerHTML = positions.map(p => {
    const meta = typeof p.metadata === 'string' ? JSON.parse(p.metadata||'{}') : (p.metadata||{});
    const commentary = meta.commentary || {};
    const currPrice = prices[p.inst_id] || p.current_price || p.entry_price;
    const entry = parseFloat(p.entry_price);
    const size  = parseFloat(p.size);
    const upnl  = p.side === 'LONG'
      ? (currPrice - entry) * size
      : (entry - currPrice) * size;
    const tp = p.tp_price ? fmt(p.tp_price) : '—';
    const sl = p.sl_price ? fmt(p.sl_price) : '—';
    const botColor = botColors[p.bot_id] || '#8b949e';
    const since = p.opened_at ? timeAgo(p.opened_at) : '—';
    return `<tr>
      <td><span class="inst">${p.inst_id.replace('-USDT','')}<span style="color:var(--muted)">/USDT</span></span></td>
      <td><span class="bot-tag" style="color:${botColor}">${botNames[p.bot_id]||p.bot_id}</span></td>
      <td><span class="tag ${p.side==='LONG'?'tag-long':'tag-short'}">${p.side}</span></td>
      <td class="price">${fmt(p.entry_price)}</td>
      <td class="price">${fmt(currPrice)}</td>
      <td class="price">${fmt2(size)}</td>
      <td>${fmtPnl(upnl)}</td>
      <td class="price" style="color:var(--green)">${tp}</td>
      <td class="price" style="color:var(--red)">${sl}</td>
      <td class="muted">${since}</td>
      <td class="hide-sm">
        ${commentary.trigger ? `
        <div class="commentary">
          <div class="commentary-badge">TRIGGER</div>
          <div class="trigger">${commentary.trigger}</div>
        </div>` : '<span class="muted">—</span>'}
      </td>
      <td class="hide-sm">
        ${commentary.forward_strategy ? `
        <div class="commentary">
          <div class="commentary-badge" style="color:var(--green);border-color:var(--green)">STRATEGY</div>
          <div class="forward">${commentary.forward_strategy}</div>
        </div>` : '<span class="muted">—</span>'}
      </td>
    </tr>`;
  }).join('');
}

function renderTrades(trades) {
  const tbody = document.getElementById('trades-body');
  if (!trades || trades.length === 0) {
    tbody.innerHTML = '<tr><td colspan="10" class="empty"><div class="icon">📈</div>No completed trades yet — run in paper mode to collect data</td></tr>';
    return;
  }
  tbody.innerHTML = trades.map(t => {
    const botColor = botColors[t.bot_id] || '#8b949e';
    const reason = (t.close_reason||'').replace(/_/g,' ');
    return `<tr>
      <td class="muted">${fmtTime(t.closed_at)}</td>
      <td><span class="inst">${(t.inst_id||'').replace('-USDT','')}</span></td>
      <td><span class="bot-tag" style="color:${botColor}">${botNames[t.bot_id]||t.bot_id}</span></td>
      <td><span class="tag ${t.side==='LONG'?'tag-long':'tag-short'}">${t.side}</span></td>
      <td class="price">${fmt(t.entry_price)}</td>
      <td class="price">${fmt(t.exit_price)}</td>
      <td>${fmtPnl(t.net_pnl)}</td>
      <td class="muted">${fmtDur(t.duration_seconds)}</td>
      <td class="muted" style="font-size:12px">${reason}</td>
      <td class="hide-sm">
        ${t.exit_commentary ? `<div class="commentary"><div class="forward">${t.exit_commentary}</div></div>` : '<span class="muted">—</span>'}
      </td>
    </tr>`;
  }).join('');
}

function renderSignals(signals) {
  const tbody = document.getElementById('signals-body');
  if (!signals || signals.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty"><div class="icon">📡</div>Waiting for signals...</td></tr>';
    return;
  }
  tbody.innerHTML = signals.map(s => {
    const passed = s.passed_risk;
    const botColor = botColors[s.bot_id] || '#8b949e';
    return `<tr>
      <td class="muted">${fmtTime(s.created_at)}</td>
      <td><span class="bot-tag" style="color:${botColor}">${botNames[s.bot_id]||s.bot_id}</span></td>
      <td><span class="inst">${(s.inst_id||'').replace('-USDT','')}</span></td>
      <td class="muted" style="font-size:12px">${(s.signal_type||'').replace(/_/g,' ')}</td>
      <td>${s.direction ? `<span class="tag ${s.direction==='LONG'?'tag-long':'tag-short'}">${s.direction}</span>` : '<span class="muted">—</span>'}</td>
      <td class="price">${s.price ? fmt(s.price) : '—'}</td>
      <td>${passed
        ? '<span style="color:var(--green);font-weight:600">✓ APPROVED</span>'
        : '<span style="color:var(--red)">✗ BLOCKED</span>'}</td>
    </tr>`;
  }).join('');
}

function renderPnlChart(timeseries) {
  const labels = (timeseries||[]).map(d => d.date);
  let cumPnl = 0;
  const data = (timeseries||[]).map(d => {
    cumPnl += parseFloat(d.realized_pnl||0);
    return parseFloat(cumPnl.toFixed(2));
  });

  if (pnlChart) pnlChart.destroy();
  const ctx = document.getElementById('pnl-chart').getContext('2d');
  pnlChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Cumulative P&L (USDT)',
        data,
        borderColor: data[data.length-1] >= 0 ? '#3fb950' : '#f85149',
        backgroundColor: data[data.length-1] >= 0
          ? 'rgba(63,185,80,0.08)' : 'rgba(248,81,73,0.08)',
        borderWidth: 2,
        pointRadius: 3,
        fill: true,
        tension: 0.3,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#8b949e', maxTicksLimit: 10 }, grid: { color: '#21262d' } },
        y: { ticks: { color: '#8b949e', callback: v => v + ' USDT' }, grid: { color: '#21262d' } }
      }
    }
  });
}

function renderRiskEvents(events) {
  const tbody = document.getElementById('risk-body');
  if (!events || events.length === 0) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty"><div class="icon">🛡️</div>No risk events logged</td></tr>';
    return;
  }
  tbody.innerHTML = events.map(e => {
    const chipMap = {HALT:'chip-halt',KILL_SWITCH:'chip-halt',BLOCK:'chip-block',RESUME:'chip-resume',RECONCILE:'chip-reconcile',API_OUTAGE:'chip-halt'};
    const chip = chipMap[e.event_type] || 'chip-block';
    return `<tr>
      <td class="muted">${fmtTime(e.created_at)}</td>
      <td><span class="chip ${chip}">${e.event_type}</span></td>
      <td class="muted">${e.bot_id || 'system'}</td>
      <td class="muted" style="font-size:12px">${(e.reason||'').replace(/_/g,' ')}</td>
    </tr>`;
  }).join('');
}

function renderBotStats(stats) {
  const tbody = document.getElementById('bots-stats-body');
  if (!stats || stats.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty"><div class="icon">🤖</div>No trades completed yet</td></tr>';
    return;
  }
  tbody.innerHTML = stats.map(s => {
    const wr = s.total_trades > 0 ? ((s.wins/s.total_trades)*100).toFixed(1) : '—';
    const botColor = botColors[s.bot_id] || '#8b949e';
    return `<tr>
      <td><span class="bot-tag" style="color:${botColor}">${botNames[s.bot_id]||s.bot_id}</span></td>
      <td class="price">${s.total_trades}</td>
      <td style="color:var(--green)">${s.wins}</td>
      <td style="color:var(--red)">${s.losses}</td>
      <td>${wr !== '—' ? `<span style="color:${parseFloat(wr)>=50?'var(--green)':'var(--red)'}">${wr}%</span>` : '—'}</td>
      <td>${fmtPnl(s.total_pnl)}</td>
      <td>${fmtPnl(s.avg_pnl)}</td>
      <td class="muted">${fmtDur(Math.round(parseFloat(s.avg_duration_s||0)))}</td>
    </tr>`;
  }).join('');
}

// ── Poll & update ─────────────────────────────────────────────────────────────
async function poll() {
  try {
    const r = await fetch('/api/state');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();

    // Header
    const isHalted = d.system_halted;
    document.getElementById('mode-badge').textContent = d.mode;
    document.getElementById('mode-badge').className = 'mode-badge' + (d.mode==='LIVE' ? ' live' : '');
    document.getElementById('h-balance').textContent = d.balance_usdt ? '$' + parseFloat(d.balance_usdt).toFixed(2) : '—';

    const dp = d.daily_pnl;
    const dpEl = document.getElementById('h-daily-pnl');
    if (dp !== undefined) {
      dpEl.textContent = (dp >= 0 ? '+' : '') + parseFloat(dp).toFixed(2) + ' USDT';
      dpEl.style.color = dp >= 0 ? 'var(--green)' : 'var(--red)';
    }

    const ddEl = document.getElementById('h-drawdown');
    if (d.drawdown_pct !== undefined) {
      ddEl.textContent = parseFloat(d.drawdown_pct).toFixed(2) + '%';
      ddEl.style.color = d.drawdown_pct > 3 ? 'var(--red)' : d.drawdown_pct > 1 ? 'var(--yellow)' : 'var(--green)';
    }

    document.getElementById('h-positions').textContent = (d.open_positions||[]).length;
    document.getElementById('h-uptime').textContent = fmtUptime(d.uptime_s || 0);
    document.getElementById('halt-banner').style.display = isHalted ? 'block' : 'none';

    // Summary cards
    const stats = d.bot_stats || [];
    const totalPnl = stats.reduce((a, b) => a + parseFloat(b.total_pnl||0), 0);
    const totalTrades = stats.reduce((a, b) => a + parseInt(b.total_trades||0), 0);
    const totalWins = stats.reduce((a, b) => a + parseInt(b.wins||0), 0);
    document.getElementById('m-total-pnl').innerHTML = fmtPnl(totalPnl);
    document.getElementById('m-trades').textContent = totalTrades;
    if (totalTrades > 0) {
      const wr = (totalWins/totalTrades*100).toFixed(1);
      document.getElementById('m-winrate').textContent = wr + '%';
      document.getElementById('m-winrate').style.color = parseFloat(wr) >= 50 ? 'var(--green)' : 'var(--red)';
      document.getElementById('m-winrate-sub').textContent = `${totalWins}W / ${totalTrades-totalWins}L`;
    }
    document.getElementById('m-today-trades').textContent = d.today_trades || 0;

    // Bot grid
    renderBotGrid(d.bot_states || {}, stats);

    // Tabs
    renderPositions(d.open_positions, d.current_prices || {});
    renderTrades(d.recent_trades);
    renderSignals(d.signals);
    renderBotStats(stats);
    renderRiskEvents(d.risk_events);

    // P&L chart (only redraw if active)
    if (activeTab === 'pnl' && d.pnl_timeseries) {
      renderPnlChart(d.pnl_timeseries);
    }

    document.getElementById('last-update').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) {
    document.getElementById('last-update').textContent = 'Error: ' + e.message;
  }
}

// Initial render + poll every 5s
poll();
setInterval(poll, 5000);
</script>
</body>
</html>"""

# ── API handlers ──────────────────────────────────────────────────────────────

async def state_handler(request: web.Request) -> web.Response:
    """GET /api/state — full system state for dashboard polling."""
    try:
        from config import TRADING_MODE, IS_LIVE
        from database import (
            get_all_states, get_open_positions, get_recent_trades,
            get_pnl_timeseries, get_signals_recent, get_bot_stats,
            get_today_pnl,
        )
        from database import get_pool
        from blofin_client import get_client

        pool = await get_pool()

        # Fetch everything concurrently
        client   = await get_client()
        balance  = await client.get_usdt_balance()
        states   = await get_all_states()
        open_pos = await get_open_positions()
        trades   = await get_recent_trades(100)
        signals  = await get_signals_recent(200)
        pnl_ts   = await get_pnl_timeseries(30)
        bot_stats= await get_bot_stats()
        today    = await get_today_pnl()

        # Risk events
        async with pool.acquire() as conn:
            risk_rows = await conn.fetch(
                "SELECT * FROM risk_events ORDER BY created_at DESC LIMIT 50"
            )
            risk_events = [dict(r) for r in risk_rows]

        # Bot states from DB
        async with pool.acquire() as conn:
            bs_rows = await conn.fetch("SELECT * FROM bot_state")
            bot_states = {r["bot_id"]: dict(r) for r in bs_rows}

        # Current prices for open positions
        current_prices = {}
        for pos in open_pos:
            try:
                price = await client.get_mid_price(pos["inst_id"])
                current_prices[pos["inst_id"]] = price
            except Exception:
                pass

        # Daily P&L and drawdown
        daily_pnl    = float(today.get("realized_pnl", 0)) + float(today.get("unrealized_pnl", 0)) if today else 0
        starting_bal = float(today.get("starting_balance", balance)) if today else balance
        drawdown_pct = abs(min(0, daily_pnl)) / starting_bal * 100 if starting_bal else 0

        # Today's trade count
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) as n FROM trades WHERE closed_at >= CURRENT_DATE"
            )
            today_trades = int(row["n"]) if row else 0

        def serialise(obj: Any):
            if hasattr(obj, "isoformat"):
                return obj.isoformat()
            return str(obj)

        payload = {
            "mode":          "LIVE" if IS_LIVE else "PAPER",
            "system_halted": states.get("system_halted", "0") == "1",
            "halt_reason":   states.get("halt_reason", ""),
            "balance_usdt":  round(balance, 2),
            "daily_pnl":     round(daily_pnl, 2),
            "drawdown_pct":  round(drawdown_pct, 2),
            "uptime_s":      int(time.time() - _start_time),
            "open_positions": [dict(p) for p in open_pos],
            "current_prices": current_prices,
            "recent_trades":  [dict(t) for t in trades],
            "signals":        [dict(s) for s in signals],
            "pnl_timeseries": [dict(p) for p in pnl_ts],
            "bot_stats":      [dict(b) for b in bot_stats],
            "bot_states":     {k: {kk: vv for kk, vv in v.items() if kk != "metadata"}
                                for k, v in bot_states.items()},
            "risk_events":    risk_events,
            "today_trades":   today_trades,
        }

        return web.Response(
            content_type="application/json",
            text=json.dumps(payload, default=serialise),
        )
    except Exception as e:
        log.error("State API error: %s", e, exc_info=True)
        return web.Response(status=500, content_type="application/json",
                             text=json.dumps({"error": str(e)}))


async def index_handler(request: web.Request) -> web.Response:
    return web.Response(content_type="text/html", text=DASHBOARD_HTML)


async def health_handler(request: web.Request) -> web.Response:
    try:
        from database import get_state
        is_halted = await get_state("system_halted", "0") == "1"
        payload = {
            "status":    "halted" if is_halted else "running",
            "uptime_s":  int(time.time() - _start_time),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return web.Response(
            status=503 if is_halted else 200,
            content_type="application/json",
            text=json.dumps(payload),
        )
    except Exception as e:
        return web.Response(status=500, text=str(e))


async def start_dashboard() -> None:
    """Start the dashboard + health server. Railway uses $PORT env var."""
    port = int(os.getenv("PORT", "8080"))
    app  = web.Application()
    app.router.add_get("/",          index_handler)
    app.router.add_get("/api/state", state_handler)
    app.router.add_get("/health",    health_handler)
    app.router.add_get("/metrics",   health_handler)   # alias

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Dashboard running on http://0.0.0.0:%d", port)
