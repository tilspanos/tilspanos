"use strict";

const $ = (id) => document.getElementById(id);
const PREVIEW = new URLSearchParams(location.search).has("preview");
const CAREER_KEY = "rhLighterCareer.v1";
const SCOPE_KEY = "rhLighterScope.v1";

let universe = [];
let enabledIds = new Set();
let lastSnapshot = null;
let logInit = false;
let scope = localStorage.getItem(SCOPE_KEY) === "all" ? "all" : "session";
let lastCardBlob = null;
let lastCaption = "";

const DEMO = {
  stats: {
    running: true,
    paused: false,
    view_only: false,
    session_start: Date.now() / 1000 - 3720,
    session_seconds: 3720,
    markets_enabled: 1,
    markets_total: 65,
    volume_usd: 180216,
    fills: 482,
    pnl: 49.42,
    net_pnl: 49.42,
    open_orders: 2,
    margin_used: 0.18,
    halted: false,
    sent_tx: 1204,
    balance: 2140.55,
    cost_per_million: -274,
    fees_usd: 0,
  },
  config: {
    strategy: "mid",
    order_size_usd: 75,
    spread_bps: -0.15,
    requote_bps: 0.14,
    refresh_ms: 3000,
    leverage: 5,
    max_concurrent_markets: 4,
    daily_loss_usd: 30,
    max_hold_s: 180,
    adverse_stop_bps: 6,
  },
  markets: [
    {
      symbol: "QQQ",
      market_id: 25,
      type: "perp",
      mid: 709.82,
      our_bid: 709.81,
      our_ask: 709.82,
      position: 0.12,
      fills: 482,
      volume_usd: 180216,
      pnl: 49.42,
      cost_per_million: -274,
    },
  ],
  fills: [
    { ts: Date.now() / 1000 - 12, symbol: "QQQ", side: "BUY", size: 0.11, price: 709.81, notional: 78.08, role: "maker" },
    { ts: Date.now() / 1000 - 11, symbol: "QQQ", side: "SELL", size: 0.11, price: 709.82, notional: 78.08, role: "maker" },
  ],
  universe: [
    { symbol: "QQQ", market_id: 25, group: "stocks" },
    { symbol: "SPY", market_id: 26, group: "stocks" },
    { symbol: "BTC", market_id: 1, group: "crypto" },
  ],
  enabled_ids: [25],
  venue: {
    name: "Lighter",
    partner: "Robinhood",
    chain_id: 466324,
    wallet: "0x48d77047ab05d2564a7b7b0e504a5e70c6e82f1b",
    collateral: "USDG",
    host: "api.rh.lighter.xyz",
  },
};

const post = async (path, body) => {
  if (PREVIEW) return { ok: true, json: async () => ({ ok: true, config: DEMO.config }) };
  const resp = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  if (!resp.ok) {
    const d = await resp.json().catch(() => ({}));
    if (d.error) alert(d.error);
  }
  return resp;
};

$("btnStart").onclick = () => post("/api/start");
$("btnPause").onclick = () => {
  const paused = lastSnapshot && lastSnapshot.stats.paused;
  post(paused ? "/api/resume" : "/api/pause");
};
$("btnStop").onclick = () => {
  if (confirm("Stop fleet? Cancels all quotes and flattens positions.")) post("/api/stop");
};
$("btnFlattenAll").onclick = () => {
  if (confirm("Flatten ALL positions and cancel every order?")) post("/api/flatten", { all: true });
};
$("btnGear").onclick = () => $("settings").scrollIntoView({ behavior: "smooth", block: "start" });

const CFG_FIELDS = {
  cfgSize: "order_size_usd",
  cfgSpread: "spread_bps",
  cfgRequote: "requote_bps",
  cfgRefresh: "refresh_ms",
  cfgLev: "leverage",
  cfgMax: "max_concurrent_markets",
  cfgLoss: "daily_loss_usd",
  cfgHold: "max_hold_s",
  cfgStop: "adverse_stop_bps",
};
const CFG_SELECTS = { cfgStrategy: "strategy" };
let cfgLoaded = false;

function fillConfig(cfg, force) {
  if (!cfg || (cfgLoaded && !force)) return;
  for (const [id, key] of Object.entries(CFG_FIELDS)) {
    const el = $(id);
    if (document.activeElement !== el) el.value = cfg[key];
  }
  for (const [id, key] of Object.entries(CFG_SELECTS)) {
    const el = $(id);
    if (document.activeElement !== el && cfg[key] != null) el.value = cfg[key];
  }
  cfgLoaded = true;
}

$("btnApplyCfg").onclick = async () => {
  const body = {};
  for (const [id, key] of Object.entries(CFG_FIELDS)) {
    const v = $(id).value;
    if (v !== "") body[key] = Number(v);
  }
  for (const [id, key] of Object.entries(CFG_SELECTS)) body[key] = $(id).value;
  const resp = await post("/api/config", body);
  if (resp.ok) {
    const d = await resp.json();
    fillConfig(d.config, true);
    $("btnApplyCfg").textContent = "Applied ✓";
    setTimeout(() => { $("btnApplyCfg").textContent = "Apply"; }, 1500);
  }
};

document.querySelectorAll(".groupbtn").forEach((el) => {
  el.onclick = async () => {
    const group = el.dataset.group;
    if (group === "clear") {
      for (const m of universe) if (enabledIds.has(m.market_id)) await post("/api/toggle", { symbol: m.symbol, enabled: false });
      return;
    }
    for (const m of universe) if (m.group === group && !enabledIds.has(m.market_id)) await post("/api/toggle", { symbol: m.symbol, enabled: true });
  };
});

document.querySelectorAll(".scope button").forEach((btn) => {
  btn.classList.toggle("on", btn.dataset.scope === scope);
  btn.onclick = () => {
    scope = btn.dataset.scope;
    localStorage.setItem(SCOPE_KEY, scope);
    document.querySelectorAll(".scope button").forEach((b) => b.classList.toggle("on", b === btn));
    if (lastSnapshot) render(lastSnapshot);
  };
});

function fmtUsd(v, dp = 0) {
  if (v == null || Number.isNaN(v)) return "–";
  const sign = v < 0 ? "−" : "";
  return sign + "$" + Math.abs(v).toLocaleString(undefined, { maximumFractionDigits: dp, minimumFractionDigits: dp });
}
function fmtUsdSigned(v, dp = 2) {
  if (v == null || Number.isNaN(v)) return "–";
  const sign = v > 0 ? "+" : v < 0 ? "−" : "";
  return sign + "$" + Math.abs(v).toLocaleString(undefined, { maximumFractionDigits: dp, minimumFractionDigits: dp });
}
function fmtNum(v, dp = 6) {
  if (v == null || Number.isNaN(v)) return "–";
  return Number(v).toLocaleString(undefined, { maximumFractionDigits: dp });
}
function fmtTime(ts) {
  return new Date(ts * 1000).toTimeString().slice(0, 8);
}
function fmtDur(s) {
  const h = String(Math.floor(s / 3600)).padStart(2, "0");
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, "0");
  const x = String(Math.floor(s % 60)).padStart(2, "0");
  return `${h}:${m}:${x}`;
}
function shortWallet(addr) {
  if (!addr || addr.length < 12) return addr || "—";
  return addr.slice(0, 6) + "…" + addr.slice(-4);
}
function strategyLabel(id) {
  const map = { avellaneda: "Avellaneda", mid: "Mid", grid: "Grid", rgrid: "RGrid", dgrid: "DGrid", signal: "Signal" };
  return map[id] || id || "—";
}

function emptyPast() {
  return { volume: 0, fills: 0, realised: 0, fees: 0, sessions: 0, since: null };
}
function loadCareer() {
  try {
    return JSON.parse(localStorage.getItem(CAREER_KEY) || "null") || { past: emptyPast(), lastLive: null };
  } catch {
    return { past: emptyPast(), lastLive: null };
  }
}
function saveCareer(c) {
  localStorage.setItem(CAREER_KEY, JSON.stringify(c));
}
function journalOf(snap) {
  const st = snap.stats || {};
  const fills = Number(st.fills) || 0;
  const volume = Number(st.volume_usd) || 0;
  return {
    volume,
    fills,
    realised: Number(st.net_pnl != null ? st.net_pnl : st.pnl) || 0,
    fees: Number(st.fees_usd) || 0,
    sessionStart: st.session_start || 0,
    ready: !!(snap.universe && snap.universe.length) || Number(st.markets_total) > 0,
    running: !!st.running,
  };
}
function isEmptyBoot(j, snap) {
  return !j.ready && j.volume === 0 && j.fills === 0 && !(snap.markets || []).length;
}
function approxSame(a, b) {
  return !!(a && b && a.volume > 0 && Math.abs(a.volume - b.volume) < 0.51 && a.fills === b.fills);
}
function updateCareer(snap) {
  const career = loadCareer();
  const live = journalOf(snap);
  if (isEmptyBoot(live, snap)) return career;
  if (approxSame(career.past, live)) career.past = { ...emptyPast(), since: career.past.since };
  const prev = career.lastLive;
  const reset =
    prev &&
    prev.volume > 0 &&
    ((live.sessionStart && prev.sessionStart && live.sessionStart !== prev.sessionStart && live.volume + 1 < prev.volume) ||
      (live.fills < prev.fills && live.volume + 1 < prev.volume));
  if (reset && !approxSame(career.past, prev)) {
    career.past.volume += prev.volume;
    career.past.fills += prev.fills;
    career.past.realised += prev.realised;
    career.past.fees += prev.fees;
    career.past.sessions += 1;
    if (!career.past.since) career.past.since = Date.now();
  }
  career.lastLive = live;
  if (!career.past.since && (live.volume > 0 || live.fills > 0)) career.past.since = Date.now();
  saveCareer(career);
  return career;
}
function scopedTotals(career, live) {
  if (scope === "session") {
    return {
      volume: live.volume,
      fills: live.fills,
      realised: live.realised,
      fees: live.fees,
      sessions: 1,
      since: live.sessionStart ? live.sessionStart * 1000 : Date.now(),
      cpmOverride: null,
    };
  }
  return {
    volume: career.past.volume + live.volume,
    fills: career.past.fills + live.fills,
    realised: career.past.realised + live.realised,
    fees: career.past.fees + live.fees,
    sessions: career.past.sessions + (live.volume > 0 || live.fills > 0 || live.running ? 1 : 0),
    since: career.past.since || (live.sessionStart ? live.sessionStart * 1000 : Date.now()),
    cpmOverride: null,
  };
}
function cpmOf(totals, snap) {
  if (scope === "session" && snap.stats && snap.stats.cost_per_million != null) return snap.stats.cost_per_million;
  if (totals.volume < 100) return null;
  return (-totals.realised / totals.volume) * 1_000_000;
}

function setBadge(el, live) {
  el.textContent = live ? "LIVE" : "STANDBY";
  el.className = "badge " + (live ? "live" : "standby");
}

function marketLine(snap) {
  const strat = strategyLabel(snap.config && snap.config.strategy);
  const rows = (snap.markets || []).filter((m) => enabledIds.has(m.market_id) || m.enabled !== false);
  const names = (rows.length ? rows : snap.markets || []).map((m) => m.symbol);
  const mkt = names.length === 1 ? names[0] : names.length ? names.length + " mkts" : "—";
  return `${strat} · Both · ${mkt}`;
}

function renderPills() {
  const box = $("pills");
  const groups = [
    ["crypto", "Crypto Perps"],
    ["stocks", "Stock / Index / RWA Perps"],
    ["spot", "Spot USDG"],
  ];
  let html = "";
  for (const [g, title] of groups) {
    const markets = universe.filter((m) => m.group === g);
    if (!markets.length) continue;
    html += `<div class="pill-section">${title} (${markets.length})</div><div class="pillbar">`;
    for (const m of markets) {
      const on = enabledIds.has(m.market_id) ? " on" : "";
      html += `<span class="mkt-pill${on}" data-sym="${m.symbol}" data-id="${m.market_id}">${on ? "✓ " : ""}${m.symbol}</span>`;
    }
    html += "</div>";
  }
  box.innerHTML = html || '<div class="empty">Discovering markets…</div>';
  box.querySelectorAll(".mkt-pill").forEach((el) => {
    el.onclick = () => post("/api/toggle", { symbol: el.dataset.sym, enabled: !enabledIds.has(Number(el.dataset.id)) });
  });
}

function marketRowHtml(r) {
  return `
    <tr>
      <td class="sym">${r.symbol}</td>
      <td><span class="tag">${r.type}</span></td>
      <td>${fmtNum(r.mid)}</td>
      <td>${fmtNum(r.our_bid)}</td>
      <td>${fmtNum(r.our_ask)}</td>
      <td class="${r.position > 0 ? "pos-txt" : r.position < 0 ? "neg-txt" : "muted"}">${r.position ? (r.position > 0 ? "+" : "") + fmtNum(r.position) : "0"}</td>
      <td>${r.fills}</td>
      <td>${fmtUsd(r.volume_usd, 0)}</td>
      <td class="${r.pnl >= 0 ? "pos-txt" : "neg-txt"}">${fmtUsdSigned(r.pnl, 2)}</td>
      <td class="${r.cost_per_million == null ? "muted" : r.cost_per_million <= 0 ? "pos-txt" : "neg-txt"}">${r.cost_per_million == null ? "–" : fmtUsd(r.cost_per_million, 0)}</td>
      <td><button class="btn danger flatbtn" onclick="post('/api/flatten',{symbol:'${r.symbol}'})">Flatten</button></td>
    </tr>`;
}

function render(snap) {
  lastSnapshot = snap;
  fillConfig(snap.config, false);
  const st = snap.stats || {};
  universe = snap.universe || universe;
  enabledIds = new Set(snap.enabled_ids || []);
  const career = updateCareer(snap);
  const liveJ = journalOf(snap);
  const totals = scopedTotals(career, liveJ);
  const cpm = cpmOf(totals, snap);
  const live = !!(st.running && !st.paused);
  const quoting = live && (st.open_orders > 0 || st.markets_enabled > 0);

  $("navDot").className = "dot" + (live ? " on" : st.running || st.view_only ? " idle" : "");
  $("navLive").textContent = live ? "LIVE" : st.view_only ? "VIEW" : "STANDBY";
  $("btnPause").textContent = st.paused ? "Resume" : "Pause";
  $("fleetCount").textContent = `${st.markets_enabled || 0}/${st.markets_total || 0}`;
  $("sessionTime").textContent = st.running ? fmtDur(st.session_seconds) : "00:00:00";

  const venue = snap.venue || {};
  $("walletPill").textContent = shortWallet(venue.wallet);
  $("chainPill").textContent = `RH · ${venue.chain_id || 466324}`;

  setBadge($("eqBadge"), live);
  setBadge($("pulseBadge"), live);
  $("eqHero").textContent = st.balance == null ? "—" : fmtUsd(st.balance, 2);
  const imr = st.margin_used == null ? null : Math.max(0, Math.min(1, st.margin_used));
  $("imrPct").textContent = imr == null ? "—" : Math.round(imr * 100) + "%";
  $("imrFill").style.width = imr == null ? "0%" : Math.round(imr * 100) + "%";

  $("pulseState").textContent = !st.running ? (st.view_only ? "View only" : "Idle") : st.paused ? "Paused" : quoting ? "Quoting" : "Idle";
  $("pulseSub").textContent = st.running
    ? `${st.markets_enabled || 0} market${st.markets_enabled === 1 ? "" : "s"} · ${st.open_orders || 0} open orders`
    : "Fleet not quoting";
  $("pulseMeter").className = "pulse-meter" + (quoting ? " quoting" : "");

  const enabled = (universe || []).filter((m) => enabledIds.has(m.market_id)).map((m) => m.symbol);
  $("cfgMarket").textContent = enabled.length ? enabled.join(" · ") : "None";

  $("stVolume").textContent = fmtUsd(totals.volume, totals.volume < 1000 ? 2 : 0);
  $("stFees").textContent = fmtUsd(totals.fees, 2);
  const realEl = $("stRealised");
  realEl.textContent = fmtUsdSigned(totals.realised, 2);
  realEl.className = "hero-num " + (totals.realised > 0 ? "pos" : totals.realised < 0 ? "neg" : "");
  const cpmEl = $("stCpm");
  if (cpm == null) {
    cpmEl.textContent = "—";
    cpmEl.className = "hero-num";
  } else {
    cpmEl.textContent = fmtUsd(cpm, 0) + " / $1M";
    cpmEl.className = "hero-num " + (cpm <= 0 ? "pos" : "neg");
  }

  $("volbarLabel").textContent = fmtUsd(totals.volume, 0);
  const pct = Math.max(4, Math.min(100, Math.log10(Math.max(totals.volume, 1)) / 6 * 100));
  $("volbarFill").style.width = pct + "%";

  renderPills();

  const rows = snap.markets || [];
  const open = rows.filter((r) => Number(r.position));
  $("posRows").innerHTML = open.length ? open.map(marketRowHtml).join("") : '<tr><td colspan="11" class="empty">No open positions</td></tr>';
  $("marketRows").innerHTML = rows.length ? rows.map(marketRowHtml).join("") : '<tr><td colspan="11" class="empty">Fleet not started</td></tr>';

  const fills = snap.fills || [];
  $("fillRows").innerHTML = fills.length
    ? fills.slice(0, 20).map((f) => `
      <tr>
        <td class="muted">${fmtTime(f.ts)}</td>
        <td class="sym">${f.symbol}</td>
        <td class="side-${String(f.side).toLowerCase()}">${f.side}</td>
        <td>${fmtNum(f.size)}</td>
        <td>${fmtNum(f.price)}</td>
        <td>${fmtUsd(f.notional, 2)}</td>
        <td><span class="tag">${f.role}</span></td>
      </tr>`).join("")
    : '<tr><td colspan="7" class="empty">No fills yet</td></tr>';
}

function appendEvents(events) {
  if (!events || !events.length) return;
  const box = $("logBox");
  if (!logInit) { box.innerHTML = ""; logInit = true; }
  const icons = { ok: "✓", warn: "⚠", err: "✗" };
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  for (const e of events) {
    const row = document.createElement("div");
    row.className = "row";
    row.innerHTML = `<span class="t">${fmtTime(e.ts)}</span><span class="${e.level}">${icons[e.level] || "·"}</span><span class="${e.level}" style="min-width:44px">${e.tag}</span><span style="min-width:66px;font-weight:600">${e.symbol || ""}</span><span class="msg">${e.message}</span>`;
    box.appendChild(row);
  }
  while (box.children.length > 400) box.removeChild(box.firstChild);
  if (atBottom) box.scrollTop = box.scrollHeight;
}

function loadImage(src) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.crossOrigin = "anonymous";
    img.onload = () => resolve(img);
    img.onerror = reject;
    img.src = src;
  });
}

function punchNearBlack(ctx, x, y, w, h) {
  const data = ctx.getImageData(x, y, w, h);
  const px = data.data;
  for (let i = 0; i < px.length; i += 4) {
    if (px[i] + px[i + 1] + px[i + 2] < 36) px[i + 3] = 0;
  }
  ctx.putImageData(data, x, y);
}

function drawStar(ctx, x, y, r) {
  ctx.save();
  ctx.translate(x, y);
  ctx.beginPath();
  for (let i = 0; i < 4; i++) {
    const a = (i * Math.PI) / 2 - Math.PI / 4;
    ctx.lineTo(Math.cos(a) * r, Math.sin(a) * r);
    ctx.lineTo(Math.cos(a + Math.PI / 4) * r * 0.34, Math.sin(a + Math.PI / 4) * r * 0.34);
  }
  ctx.closePath();
  const g = ctx.createLinearGradient(-r, -r, r, r);
  g.addColorStop(0, "rgba(61,255,154,0.16)");
  g.addColorStop(1, "rgba(139,124,255,0.14)");
  ctx.fillStyle = g;
  ctx.fill();
  ctx.restore();
}

function cardCaption(totals, cpm, snap) {
  const bps = cpm == null ? "—" : (cpm / 100).toFixed(2) + " bps";
  const cpmTxt = cpm == null ? "—" : fmtUsd(cpm, 0) + " / $1M";
  return `${fmtUsd(totals.volume, 0)} · CPM ${cpmTxt} · ${bps} · realised ${fmtUsdSigned(totals.realised, 2)}`;
}

function fitFont(ctx, text, family, maxPx, minPx, maxWidth) {
  let size = maxPx;
  while (size > minPx) {
    ctx.font = `500 ${size}px ${family}`;
    if (ctx.measureText(text).width <= maxWidth) return size;
    size -= 2;
  }
  return minPx;
}

async function paintCard(snap) {
  const career = updateCareer(snap);
  const liveJ = journalOf(snap);
  const totals = scopedTotals(career, liveJ);
  const cpm = cpmOf(totals, snap);
  const venue = snap.venue || {};
  const W = 1080;
  const H = 1350;
  const canvas = document.createElement("canvas");
  canvas.width = W;
  canvas.height = H;
  const ctx = canvas.getContext("2d");

  await Promise.all([
    document.fonts.load("600 32px Outfit"),
    document.fonts.load("500 28px Outfit"),
    document.fonts.load('500 108px "IBM Plex Mono"'),
    document.fonts.load('500 64px "IBM Plex Mono"'),
  ]).catch(() => {});

  ctx.fillStyle = "#070707";
  ctx.fillRect(0, 0, W, H);

  const wash = ctx.createRadialGradient(540, 200, 20, 540, 160, 520);
  wash.addColorStop(0, "rgba(255,255,255,0.10)");
  wash.addColorStop(0.35, "rgba(61,255,154,0.10)");
  wash.addColorStop(1, "rgba(7,7,7,0)");
  ctx.fillStyle = wash;
  ctx.fillRect(0, 0, W, 640);
  const corner = ctx.createRadialGradient(980, 80, 10, 980, 80, 280);
  corner.addColorStop(0, "rgba(139,124,255,0.16)");
  corner.addColorStop(1, "rgba(7,7,7,0)");
  ctx.fillStyle = corner;
  ctx.fillRect(700, 0, 380, 360);

  drawStar(ctx, 540, 188, 100);

  ctx.strokeStyle = "rgba(255,255,255,0.16)";
  ctx.lineWidth = 2;
  roundRect(ctx, 36, 36, W - 72, H - 72, 36);
  ctx.stroke();
  ctx.strokeStyle = "rgba(61,255,154,0.28)";
  ctx.lineWidth = 1.5;
  roundRect(ctx, 48, 48, W - 96, H - 96, 30);
  ctx.stroke();

  const [lgWord, lgIcon, rhWord] = await Promise.all([
    loadImage("/assets/lighter-wordmark.svg"),
    loadImage("/assets/lighter-icon.svg"),
    loadImage("/assets/robinhood.svg"),
  ]);

  const iconH = 70;
  const iconW = iconH * (lgIcon.width / lgIcon.height);
  const wordH = 48;
  const wordW = wordH * (lgWord.width / lgWord.height);
  const rhH = 42;
  const rhW = rhH * (rhWord.width / rhWord.height);
  const lockW = iconW + 16 + wordW + 24 + rhW;
  let x = (W - lockW) / 2;
  const y = 96;
  ctx.drawImage(lgIcon, x, y - 4, iconW, iconH);
  punchNearBlack(ctx, x, y - 4, iconW, iconH);
  x += iconW + 16;
  ctx.drawImage(lgWord, x, y + 8, wordW, wordH);
  punchNearBlack(ctx, x, y + 8, wordW, wordH);
  x += wordW + 24;
  ctx.drawImage(rhWord, x, y + 12, rhW, rhH);

  ctx.fillStyle = "#b8b8b8";
  ctx.font = "600 32px Outfit";
  ctx.letterSpacing = "0.16em";
  ctx.textAlign = "center";
  ctx.fillText("MARKET MAKER", W / 2, 204);
  ctx.letterSpacing = "0px";

  const vol = fmtUsd(Math.round(totals.volume), 0);
  ctx.fillStyle = "#f3f3f3";
  ctx.textAlign = "center";
  const volSize = fitFont(ctx, vol, '"IBM Plex Mono"', 108, 64, W - 160);
  ctx.font = `500 ${volSize}px "IBM Plex Mono"`;
  ctx.fillText(vol, W / 2, 360);

  ctx.fillStyle = "#c4c4c4";
  ctx.font = "600 28px Outfit";
  ctx.letterSpacing = "0.14em";
  ctx.fillText("TOTAL VOLUME", W / 2, 408);
  ctx.letterSpacing = "0px";
  ctx.font = "500 28px Outfit";
  ctx.fillStyle = "#e8e8e8";
  ctx.fillText(marketLine(snap), W / 2, 452);

  const tiles = [
    { k: "CPM", v: cpm == null ? "—" : fmtUsd(cpm, 0), s: cpm == null ? "" : "/ $1M", ok: cpm != null && cpm <= 0, fill: false },
    { k: "BPS", v: cpm == null ? "—" : (cpm / 100).toFixed(2), s: cpm == null ? "" : "bps", ok: cpm != null && cpm <= 0, fill: false },
    { k: "Realised", v: fmtUsdSigned(totals.realised, 2), s: "", ok: totals.realised >= 0, fill: false },
    { k: "Fills", v: String(totals.fills), s: "", ok: true, fill: true },
  ];
  const gap = 22;
  const tw = (W - 72 - 48 - gap) / 2;
  const th = 232;
  tiles.forEach((t, i) => {
    const col = i % 2;
    const row = Math.floor(i / 2);
    const tx = 60 + col * (tw + gap);
    const ty = 500 + row * (th + gap);
    ctx.fillStyle = "#101010";
    roundRect(ctx, tx, ty, tw, th, 24);
    ctx.fill();
    ctx.strokeStyle = "rgba(255,255,255,0.10)";
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.fillStyle = "#c4c4c4";
    ctx.font = "600 26px Outfit";
    ctx.textAlign = "left";
    ctx.letterSpacing = "0.10em";
    ctx.fillText(t.k.toUpperCase(), tx + 28, ty + 52);
    ctx.letterSpacing = "0px";
    ctx.fillStyle = t.fill ? "#f3f3f3" : t.ok ? "#3dff9a" : "#ff5d6c";
    const valueSize = fitFont(ctx, t.v, '"IBM Plex Mono"', 64, 36, tw - 56);
    ctx.font = `500 ${valueSize}px "IBM Plex Mono"`;
    ctx.fillText(t.v, tx + 28, ty + 132);
    if (t.s) {
      ctx.fillStyle = "#d0d0d0";
      ctx.font = "500 28px Outfit";
      ctx.fillText(t.s, tx + 28, ty + 180);
    }
  });

  const since = totals.since ? new Date(totals.since).toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" }) : "—";
  const footer = scope === "all"
    ? `ALL RUNS · ${totals.sessions} session${totals.sessions === 1 ? "" : "s"} · since ${since}`
    : "THIS SESSION · since last Start";
  ctx.fillStyle = "#d0d0d0";
  ctx.font = "600 24px Outfit";
  ctx.textAlign = "left";
  ctx.letterSpacing = "0.06em";
  ctx.fillText(footer, 72, 1088);
  ctx.letterSpacing = "0px";
  ctx.font = '500 24px "IBM Plex Mono"';
  ctx.textAlign = "right";
  ctx.fillStyle = "#f3f3f3";
  ctx.fillText(shortWallet(venue.wallet), W - 72, 1088);

  ctx.fillStyle = "rgba(255,255,255,0.08)";
  roundRect(ctx, 72, 1130, W - 144, 16, 999);
  ctx.fill();
  const bar = Math.max(0.08, Math.min(1, Math.log10(Math.max(totals.volume, 1)) / 6));
  const vg = ctx.createLinearGradient(72, 0, 72 + (W - 144) * bar, 0);
  vg.addColorStop(0, "#3dff9a");
  vg.addColorStop(1, "#8b7cff");
  ctx.fillStyle = vg;
  roundRect(ctx, 72, 1130, (W - 144) * bar, 16, 999);
  ctx.fill();

  lastCaption = cardCaption(totals, cpm, snap);
  return new Promise((resolve) => canvas.toBlob((b) => resolve(b), "image/png"));
}

function roundRect(ctx, x, y, w, h, r) {
  const rr = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
}

async function openShare() {
  if (!lastSnapshot) return;
  lastCardBlob = await paintCard(lastSnapshot);
  $("shareImg").src = URL.createObjectURL(lastCardBlob);
  $("shareOverlay").classList.add("on");
}
$("btnShare").onclick = openShare;
$("btnCloseShare").onclick = () => $("shareOverlay").classList.remove("on");
$("shareOverlay").addEventListener("click", (e) => {
  if (e.target === $("shareOverlay")) $("shareOverlay").classList.remove("on");
});
$("btnCopyCard").onclick = async () => {
  if (!lastCardBlob) return;
  try {
    await navigator.clipboard.write([new ClipboardItem({ "image/png": lastCardBlob })]);
    $("btnCopyCard").textContent = "Copied";
    setTimeout(() => { $("btnCopyCard").textContent = "Copy card"; }, 1400);
  } catch {
    alert("Clipboard blocked — use Download PNG.");
  }
};
$("btnDlCard").onclick = () => {
  if (!lastCardBlob) return;
  const a = document.createElement("a");
  a.href = URL.createObjectURL(lastCardBlob);
  a.download = "lighter-robinhood-mm.png";
  a.click();
};
$("btnPostX").onclick = async () => {
  if (!lastCardBlob) return;
  const file = new File([lastCardBlob], "lighter-robinhood-mm.png", { type: "image/png" });
  if (navigator.canShare && navigator.canShare({ files: [file] })) {
    try { await navigator.share({ files: [file], text: lastCaption }); return; } catch {}
  }
  window.open("https://twitter.com/intent/tweet?text=" + encodeURIComponent(lastCaption), "_blank");
};

let ws = null, pollTimer = null, reconnectPending = false, reconnectDelay = 3000;
function startPolling() {
  if (pollTimer || PREVIEW) return;
  pollTimer = setInterval(async () => {
    try { render(await (await fetch("/api/status")).json()); } catch {}
  }, 3000);
}
function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}
function connect() {
  if (PREVIEW) return;
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/api/stream`);
  ws.onopen = () => {
    $("connBadge").textContent = "stream connected";
    $("connBadge").className = "conn-badge";
    stopPolling();
    reconnectDelay = 3000;
  };
  ws.onmessage = (ev) => {
    const d = JSON.parse(ev.data);
    if (d.snapshot) render(d.snapshot);
    appendEvents(d.events);
  };
  ws.onerror = () => { try { ws.close(); } catch {} };
  ws.onclose = () => {
    $("connBadge").textContent = "stream lost — polling";
    $("connBadge").className = "conn-badge err";
    startPolling();
    if (reconnectPending) return;
    reconnectPending = true;
    setTimeout(() => { reconnectPending = false; connect(); }, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 30000);
  };
}

if (PREVIEW) {
  $("connBadge").textContent = "preview";
  render(DEMO);
  if (new URLSearchParams(location.search).has("card")) {
    openShare();
  }
} else {
  fetch("/api/status").then((r) => r.json()).then(render).catch(() => {});
  connect();
}

window.post = post;
