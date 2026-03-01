"""
🇮🇳 India Market Tracker — Sensex | Nifty 50 | India VIX
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
  - Live data via yfinance (^BSESN, ^NSEI, ^INDIAVIX)
  - 1%+ drop alerts for Sensex & Nifty 50
  - India VIX safety zone analysis
  - RSI-based momentum signal
  - Trend detection (above/below key MAs)
  - CSV history update in GitHub repo
  - Telegram formatted alerts
  - Market hours guard (IST 9:15–15:30, Mon–Fri)
"""

import os
import csv
import json
import math
import time
import datetime
import pytz
import requests
import yfinance as yf
import pandas as pd

# ─────────────────────────────────────────────
# CONFIG — set via GitHub Secrets or .env
# ─────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
CSV_PATH           = os.environ.get("CSV_PATH", "data/market_history.csv")
FORCE_RUN          = os.environ.get("FORCE_RUN", "false").lower() == "true"

IST = pytz.timezone("Asia/Kolkata")

# ─────────────────────────────────────────────
# INDIA VIX ZONES
# ─────────────────────────────────────────────
VIX_ZONES = [
    (0,   13,   "🟢 ULTRA LOW",   "Extreme complacency — reversal risk possible"),
    (13,  15,   "🟢 LOW",         "Calm market — bullish bias, low volatility"),
    (15,  18,   "🟡 MODERATE",    "Normal range — markets stable, mild caution"),
    (18,  20,   "🟠 ELEVATED",    "Caution advised — volatility picking up"),
    (20,  25,   "🔴 HIGH",        "Fear in market — high volatility, be defensive"),
    (25,  30,   "🔴 VERY HIGH",   "Panic zone — sharp swings likely, reduce exposure"),
    (30, 999,   "⛔ EXTREME",     "Crisis/Black Swan territory — extreme caution"),
]

def get_vix_zone(vix: float) -> tuple:
    for lo, hi, label, desc in VIX_ZONES:
        if lo <= vix < hi:
            return label, desc
    return "❓ UNKNOWN", "VIX out of expected range"

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def is_market_open() -> bool:
    """Returns True if current IST time is within NSE trading hours (Mon–Fri 9:15–15:30)."""
    now = datetime.datetime.now(IST)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close

def is_pre_open() -> bool:
    """8:45–9:15 pre-open session."""
    now = datetime.datetime.now(IST)
    if now.weekday() >= 5:
        return False
    pre_open_start = now.replace(hour=8, minute=45, second=0, microsecond=0)
    market_open    = now.replace(hour=9, minute=15, second=0, microsecond=0)
    return pre_open_start <= now < market_open

def fmt_num(val: float, decimals: int = 2) -> str:
    return f"{val:,.{decimals}f}"

def pct_arrow(pct: float) -> str:
    if pct >= 1:    return "🚀"
    if pct >= 0.3:  return "📈"
    if pct > 0:     return "🔼"
    if pct > -0.3:  return "🔽"
    if pct > -1:    return "📉"
    return "🔻"

def sentiment_label(pct: float) -> str:
    if pct >= 1.5:  return "STRONG BULL 🐂"
    if pct >= 0.5:  return "MILD BULL 🐂"
    if pct > -0.5:  return "NEUTRAL ⚖️"
    if pct > -1.5:  return "MILD BEAR 🐻"
    return "STRONG BEAR 🐻"

# ─────────────────────────────────────────────
# RSI CALCULATION
# ─────────────────────────────────────────────
def compute_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return float("nan")
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas[-period:]]
    losses = [abs(min(d, 0)) for d in deltas[-period:]]
    avg_gain = sum(gains)  / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def rsi_signal(rsi: float) -> str:
    if math.isnan(rsi): return "N/A"
    if rsi >= 70: return f"⚠️ Overbought ({rsi})"
    if rsi <= 30: return f"🔔 Oversold ({rsi})"
    return f"Normal ({rsi})"

# ─────────────────────────────────────────────
# TREND ANALYSIS
# ─────────────────────────────────────────────
def trend_analysis(closes: list) -> str:
    if len(closes) < 21:
        return "Insufficient data"
    ma5  = sum(closes[-5:])  / 5
    ma20 = sum(closes[-20:]) / 20
    cur  = closes[-1]
    parts = []
    parts.append("above MA5 ✅" if cur > ma5  else "below MA5 ❌")
    parts.append("above MA20 ✅" if cur > ma20 else "below MA20 ❌")
    if cur > ma5 > ma20:
        trend = "📈 Strong Uptrend"
    elif cur < ma5 < ma20:
        trend = "📉 Strong Downtrend"
    elif cur > ma20:
        trend = "🔼 Above key MA — Bullish bias"
    else:
        trend = "🔽 Below key MA — Bearish bias"
    return f"{trend} | {' | '.join(parts)}"

# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────
def fetch_ticker(symbol: str, period: str = "30d", interval: str = "1d") -> dict:
    """Fetch ticker data and return structured dict."""
    tk = yf.Ticker(symbol)
    hist = tk.history(period=period, interval=interval)
    if hist.empty:
        raise ValueError(f"No data for {symbol}")

    closes    = hist["Close"].tolist()
    current   = closes[-1]
    prev_day  = closes[-2] if len(closes) >= 2 else current
    day_high  = hist["High"].iloc[-1]
    day_low   = hist["Low"].iloc[-1]
    day_open  = hist["Open"].iloc[-1]
    volume    = hist["Volume"].iloc[-1] if "Volume" in hist.columns else 0

    change     = current - prev_day
    change_pct = (change / prev_day) * 100 if prev_day else 0

    # Also get intraday for live price
    intra = tk.history(period="1d", interval="5m")
    if not intra.empty:
        current  = intra["Close"].iloc[-1]
        day_high = max(day_high, intra["High"].max())
        day_low  = min(day_low,  intra["Low"].min())
        change     = current - prev_day
        change_pct = (change / prev_day) * 100 if prev_day else 0

    rsi = compute_rsi(closes)
    trend = trend_analysis(closes)

    # Week & month metrics
    week_ago  = closes[-6]  if len(closes) >= 6  else closes[0]
    month_ago = closes[-22] if len(closes) >= 22 else closes[0]
    week_chg  = ((current - week_ago)  / week_ago)  * 100 if week_ago else 0
    month_chg = ((current - month_ago) / month_ago) * 100 if month_ago else 0

    return {
        "symbol":     symbol,
        "current":    round(current, 2),
        "prev_day":   round(prev_day, 2),
        "change":     round(change, 2),
        "change_pct": round(change_pct, 2),
        "day_open":   round(day_open, 2),
        "day_high":   round(day_high, 2),
        "day_low":    round(day_low, 2),
        "volume":     int(volume),
        "rsi":        rsi,
        "trend":      trend,
        "week_chg":   round(week_chg, 2),
        "month_chg":  round(month_chg, 2),
        "closes":     closes,
    }

# ─────────────────────────────────────────────
# CSV UPDATE
# ─────────────────────────────────────────────
def update_csv(sensex: dict, nifty: dict, vix: dict):
    now_ist = datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
    row = {
        "timestamp":        now_ist,
        "sensex":           sensex["current"],
        "sensex_chg_pct":   sensex["change_pct"],
        "nifty50":          nifty["current"],
        "nifty50_chg_pct":  nifty["change_pct"],
        "india_vix":        vix["current"],
        "vix_zone":         get_vix_zone(vix["current"])[0],
        "nifty_rsi":        nifty["rsi"],
        "nifty_trend":      nifty["trend"].split("|")[0].strip(),
        "week_chg_nifty":   nifty["week_chg"],
        "month_chg_nifty":  nifty["month_chg"],
    }

    fieldnames = list(row.keys())
    file_exists = os.path.isfile(CSV_PATH)
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    print(f"✅ CSV updated → {CSV_PATH}")

# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️  Telegram not configured — skipping send")
        print("─" * 50)
        print(message)
        print("─" * 50)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        print("✅ Telegram message sent")
    except Exception as e:
        print(f"❌ Telegram error: {e}")

# ─────────────────────────────────────────────
# BUILD MESSAGE
# ─────────────────────────────────────────────
def build_market_message(sensex: dict, nifty: dict, vix: dict,
                          alerts: list, session_note: str = "") -> str:
    now_ist   = datetime.datetime.now(IST).strftime("%d %b %Y  %I:%M %p IST")
    vix_label, vix_desc = get_vix_zone(vix["current"])

    # VIX safety meter (visual bar)
    vix_val = vix["current"]
    filled  = min(int(vix_val / 3), 10)  # max bar at VIX=30
    bar     = "█" * filled + "░" * (10 - filled)

    # Nifty RSI signal
    rsi_sig = rsi_signal(nifty["rsi"])

    # Overall market mood
    avg_chg = (sensex["change_pct"] + nifty["change_pct"]) / 2
    mood    = sentiment_label(avg_chg)

    # ── Alert section ──
    alert_section = ""
    if alerts:
        alert_lines = "\n".join(f"  🚨 {a}" for a in alerts)
        alert_section = f"\n\n⚠️ <b>ALERTS</b>\n{alert_lines}"

    # ── Session note ──
    session_str = f"\n📋 <i>{session_note}</i>" if session_note else ""

    msg = (
        f"<b>🇮🇳 India Market Pulse</b>  |  {now_ist}"
        f"{session_str}"
        f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        # SENSEX
        f"\n<b>📊 SENSEX</b>  {pct_arrow(sensex['change_pct'])}\n"
        f"  Price : <b>{fmt_num(sensex['current'])}</b>\n"
        f"  Change: {'+' if sensex['change'] >= 0 else ''}{fmt_num(sensex['change'])} "
        f"({'+' if sensex['change_pct'] >= 0 else ''}{fmt_num(sensex['change_pct'])}%)\n"
        f"  H/L   : {fmt_num(sensex['day_high'])} / {fmt_num(sensex['day_low'])}\n"
        f"  1W    : {'+' if sensex['week_chg'] >= 0 else ''}{fmt_num(sensex['week_chg'])}%  "
        f"  1M: {'+' if sensex['month_chg'] >= 0 else ''}{fmt_num(sensex['month_chg'])}%\n"

        # NIFTY 50
        f"\n<b>📈 NIFTY 50</b>  {pct_arrow(nifty['change_pct'])}\n"
        f"  Price : <b>{fmt_num(nifty['current'])}</b>\n"
        f"  Change: {'+' if nifty['change'] >= 0 else ''}{fmt_num(nifty['change'])} "
        f"({'+' if nifty['change_pct'] >= 0 else ''}{fmt_num(nifty['change_pct'])}%)\n"
        f"  H/L   : {fmt_num(nifty['day_high'])} / {fmt_num(nifty['day_low'])}\n"
        f"  1W    : {'+' if nifty['week_chg'] >= 0 else ''}{fmt_num(nifty['week_chg'])}%  "
        f"  1M: {'+' if nifty['month_chg'] >= 0 else ''}{fmt_num(nifty['month_chg'])}%\n"
        f"  RSI   : {rsi_sig}\n"
        f"  Trend : {nifty['trend']}\n"

        # INDIA VIX
        f"\n<b>🌡️ INDIA VIX</b>\n"
        f"  Level : <b>{fmt_num(vix['current'])}</b>  {vix_label}\n"
        f"  Meter : [{bar}] {fmt_num(vix_val)}\n"
        f"  Signal: {vix_desc}\n"
        f"  Change: {'+' if vix['change_pct'] >= 0 else ''}{fmt_num(vix['change_pct'])}%\n"

        # SUMMARY
        f"\n<b>🧭 Market Mood:</b> {mood}\n"

        # ALERTS
        f"{alert_section}"

        f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>🤖 Auto-update every 5 min | Data: Yahoo Finance</i>"
    )
    return msg


def build_alert_message(sensex: dict, nifty: dict, alerts: list) -> str:
    """Dedicated loud alert message for 1%+ drops."""
    now_ist = datetime.datetime.now(IST).strftime("%d %b %Y  %I:%M %p IST")
    alert_lines = "\n".join(f"• {a}" for a in alerts)

    msg = (
        f"🚨🚨 <b>MARKET DROP ALERT</b> 🚨🚨\n"
        f"📅 {now_ist}\n\n"
        f"{alert_lines}\n\n"
        f"📊 Sensex: <b>{fmt_num(sensex['current'])}</b> "
        f"({'+' if sensex['change_pct'] >= 0 else ''}{fmt_num(sensex['change_pct'])}%)\n"
        f"📈 Nifty : <b>{fmt_num(nifty['current'])}</b> "
        f"({'+' if nifty['change_pct'] >= 0 else ''}{fmt_num(nifty['change_pct'])}%)\n\n"
        f"⚠️ <i>Possible support break — review your positions!</i>"
    )
    return msg

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print(f"\n{'='*55}")
    print(f"  🇮🇳 India Market Tracker  {datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}")
    print(f"{'='*55}")

    # ── Market hours check ──
    session_note = ""
    if not FORCE_RUN:
        if not is_market_open():
            if is_pre_open():
                session_note = "⏰ Pre-open session (8:45–9:15 AM)"
                print("ℹ️  Pre-open session — running anyway")
            else:
                print("⏸️  Market closed — skipping run (set FORCE_RUN=true to override)")
                return
        else:
            session_note = "🟢 Live Market Session"
    else:
        session_note = "🔧 Force-run mode (outside hours)"

    # ── Fetch data ──
    print("\n📡 Fetching data...")
    try:
        sensex = fetch_ticker("^BSESN")
        print(f"  ✅ Sensex  : {fmt_num(sensex['current'])} ({sensex['change_pct']:+.2f}%)")
    except Exception as e:
        print(f"  ❌ Sensex fetch failed: {e}")
        return

    try:
        nifty = fetch_ticker("^NSEI")
        print(f"  ✅ Nifty50 : {fmt_num(nifty['current'])} ({nifty['change_pct']:+.2f}%)")
    except Exception as e:
        print(f"  ❌ Nifty50 fetch failed: {e}")
        return

    try:
        vix = fetch_ticker("^INDIAVIX")
        print(f"  ✅ India VIX: {fmt_num(vix['current'])} ({vix['change_pct']:+.2f}%)")
    except Exception as e:
        print(f"  ❌ India VIX fetch failed: {e}")
        return

    # ── Drop alerts (1%+ from yesterday's close) ──
    alerts = []
    if sensex["change_pct"] <= -1.0:
        alerts.append(
            f"SENSEX dropped {abs(sensex['change_pct']):.2f}% from yesterday's close "
            f"({fmt_num(sensex['prev_day'])} → {fmt_num(sensex['current'])})"
        )
    if nifty["change_pct"] <= -1.0:
        alerts.append(
            f"NIFTY 50 dropped {abs(nifty['change_pct']):.2f}% from yesterday's close "
            f"({fmt_num(nifty['prev_day'])} → {fmt_num(nifty['current'])})"
        )

    # Bonus: VIX spike alert
    if vix["change_pct"] >= 15:
        alerts.append(
            f"INDIA VIX spiked +{vix['change_pct']:.1f}% — volatility surge detected!"
        )

    # Bonus: RSI extreme alerts
    if not math.isnan(nifty["rsi"]):
        if nifty["rsi"] >= 75:
            alerts.append(f"Nifty RSI at {nifty['rsi']} — extreme overbought condition")
        elif nifty["rsi"] <= 25:
            alerts.append(f"Nifty RSI at {nifty['rsi']} — extreme oversold, potential bounce zone")

    # ── Update CSV ──
    try:
        update_csv(sensex, nifty, vix)
    except Exception as e:
        print(f"  ⚠️  CSV update failed: {e}")

    # ── Send Telegram messages ──
    market_msg = build_market_message(sensex, nifty, vix, alerts, session_note)
    send_telegram(market_msg)

    # If there are drop/critical alerts — send a separate loud alert too
    if alerts and any("dropped" in a or "spiked" in a for a in alerts):
        time.sleep(1)
        send_telegram(build_alert_message(sensex, nifty, alerts))

    print(f"\n✅ Done — {datetime.datetime.now(IST).strftime('%H:%M:%S IST')}\n")


if __name__ == "__main__":
    main()
