"""
🇮🇳 India Market Tracker v2 — Sensex | Nifty 50 | India VIX | Sector Heatmap
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
  - Live Sensex, Nifty 50, India VIX with RSI + MA trend signals
  - 12 NSE sector indices fetched in PARALLEL (fast!)
  - Compact two-column sector heatmap sorted best→worst
  - Sector rotation narrative (what theme is market chasing)
  - Breadth indicator: how many sectors green vs red
  - 1%+ drop alerts (Sensex / Nifty) + VIX spike + RSI extremes
  - Sector collapse alert if any sector drops 2%+
  - TWO Telegram messages to stay under 4096-char limit each
  - CSV history auto-committed to GitHub
  - IST market hours guard (9:15-15:30 Mon-Fri)
"""

import os, csv, math, time, datetime, pytz, requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────────────────
# CONFIG  (set via GitHub Secrets)
# ─────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")
CSV_PATH           = os.environ.get("CSV_PATH", "data/market_history.csv")
FORCE_RUN          = os.environ.get("FORCE_RUN", "false").lower() == "true"

IST = pytz.timezone("Asia/Kolkata")

# ─────────────────────────────────────────────────────────
# NSE SECTOR INDEX SYMBOLS  (Yahoo Finance verified)
# ─────────────────────────────────────────────────────────
SECTORS = {
    "^NSEBANK":    "Bank",
    "^CNXIT":      "IT",
    "^CNXFMCG":    "FMCG",
    "^CNXAuto":    "Auto",
    "^CNXMetal":   "Metal",
    "^CNXPharma":  "Pharma",
    "^CNXRealty":  "Realty",
    "^CNXEnergy":  "Energy",
    "^CNXPSUBANK": "PSU Bank",
    "^CNXFIN": "Finance",
    "^CNXMedia":   "Media",
    "^CNXINFRA":   "Infra",
}

# What each leading sector signals about market sentiment
SECTOR_THEMES = {
    "^NSEBANK":    "rate optimism / credit expansion",
    "^CNXIT":      "tech rally / global risk-on",
    "^CNXFMCG":    "defensives / rural demand",
    "^CNXAuto":    "consumption / EV upcycle",
    "^CNXMetal":   "global growth / commodity boom",
    "^CNXPharma":  "defensives / healthcare demand",
    "^CNXRealty":  "low rates / housing cycle",
    "^CNXEnergy":  "oil & infra capex",
    "^CNXPSUBANK": "PSU reforms / rate play",
    "^CNXFIN": "credit growth / NBFC rally",
    "^CNXMedia":   "consumer sentiment uptick",
    "^CNXINFRA":   "government capex / infra push",
}

# ─────────────────────────────────────────────────────────
# INDIA VIX SAFETY ZONES
# ─────────────────────────────────────────────────────────
VIX_ZONES = [
    (0,  13,  "ULTRA LOW  🟢", "Extreme complacency - watch for reversal"),
    (13, 15,  "LOW        🟢", "Calm market - bullish bias"),
    (15, 18,  "MODERATE   🟡", "Normal range - mild caution"),
    (18, 20,  "ELEVATED   🟠", "Caution - volatility picking up"),
    (20, 25,  "HIGH       🔴", "Fear in market - stay defensive"),
    (25, 30,  "VERY HIGH  🔴", "Panic zone - sharp swings likely"),
    (30, 999, "EXTREME    ⛔", "Crisis territory - extreme caution"),
]

def get_vix_zone(v):
    for lo, hi, label, desc in VIX_ZONES:
        if lo <= v < hi:
            return label, desc
    return "UNKNOWN ❓", ""

# ─────────────────────────────────────────────────────────
# MARKET HOURS
# ─────────────────────────────────────────────────────────
def is_market_open():
    now = datetime.datetime.now(IST)
    if now.weekday() >= 5:
        return False
    o = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    c = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return o <= now <= c

def session_label():
    now = datetime.datetime.now(IST)
    if now.weekday() >= 5:
        return "CLOSED (Weekend)"
    pre_s  = now.replace(hour=8,  minute=45, second=0, microsecond=0)
    pre_e  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    mkt_e  = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if pre_s <= now < pre_e: return "Pre-Open"
    if now <= mkt_e:         return "Live"
    return "Post-Close"

# ─────────────────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────────────────
def fmt(v, d=2):
    return f"{v:,.{d}f}"

def sign(v):
    return "+" if v >= 0 else ""

def arrow(p):
    if p >= 1:   return "🚀"
    if p >= 0.3: return "📈"
    if p > 0:    return "🔼"
    if p > -0.3: return "🔽"
    if p > -1:   return "📉"
    return "🔻"

def mood(p):
    if p >= 1.5:  return "STRONG BULL 🐂"
    if p >= 0.5:  return "MILD BULL 🐂"
    if p > -0.5:  return "NEUTRAL ⚖️"
    if p > -1.5:  return "MILD BEAR 🐻"
    return "STRONG BEAR 🐻"

# ─────────────────────────────────────────────────────────
# RSI (14-period)
# ─────────────────────────────────────────────────────────
def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return float("nan")
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0)     for d in deltas[-period:]]
    losses = [abs(min(d,0)) for d in deltas[-period:]]
    ag = sum(gains)  / period
    al = sum(losses) / period
    if al == 0:
        return 100.0
    return round(100 - 100 / (1 + ag / al), 1)

def rsi_tag(rsi):
    if math.isnan(rsi): return "N/A"
    if rsi >= 75: return f"{rsi} ⚠️ Overbought"
    if rsi <= 25: return f"{rsi} 🔔 Oversold"
    if rsi >= 60: return f"{rsi} 📈"
    if rsi <= 40: return f"{rsi} 📉"
    return f"{rsi} ➡️"

# ─────────────────────────────────────────────────────────
# TREND  (MA5 / MA20)
# ─────────────────────────────────────────────────────────
def trend_tag(closes):
    if len(closes) < 21:
        return "N/A"
    ma5  = sum(closes[-5:])  / 5
    ma20 = sum(closes[-20:]) / 20
    cur  = closes[-1]
    if cur > ma5 > ma20:  return "Strong Up 📈"
    if cur < ma5 < ma20:  return "Strong Down 📉"
    if cur > ma20:        return "Above MA20 🔼"
    return "Below MA20 🔽"

# ─────────────────────────────────────────────────────────
# FETCH ONE INDEX TICKER
# ─────────────────────────────────────────────────────────
def fetch_ticker(symbol, period="30d"):
    tk   = yf.Ticker(symbol)
    hist = tk.history(period=period, interval="1d")
    if hist.empty:
        raise ValueError(f"No data: {symbol}")

    closes   = hist["Close"].tolist()
    prev_day = closes[-2] if len(closes) >= 2 else closes[-1]
    day_high = float(hist["High"].iloc[-1])
    day_low  = float(hist["Low"].iloc[-1])
    day_open = float(hist["Open"].iloc[-1])

    # Live intraday price
    intra = tk.history(period="1d", interval="5m")
    if not intra.empty:
        current  = float(intra["Close"].iloc[-1])
        day_high = max(day_high, float(intra["High"].max()))
        day_low  = min(day_low,  float(intra["Low"].min()))
    else:
        current  = closes[-1]

    chg   = current - prev_day
    chg_p = (chg / prev_day * 100) if prev_day else 0

    week_ago  = closes[-6]  if len(closes) >= 6  else closes[0]
    month_ago = closes[-22] if len(closes) >= 22 else closes[0]

    return {
        "symbol":  symbol,
        "current": round(current,  2),
        "prev":    round(prev_day, 2),
        "chg":     round(chg,      2),
        "chg_p":   round(chg_p,    2),
        "high":    round(day_high, 2),
        "low":     round(day_low,  2),
        "open":    round(day_open, 2),
        "rsi":     compute_rsi(closes),
        "trend":   trend_tag(closes),
        "week_p":  round(((current - week_ago)  / week_ago  * 100) if week_ago  else 0, 2),
        "month_p": round(((current - month_ago) / month_ago * 100) if month_ago else 0, 2),
        "closes":  closes,
    }

# ─────────────────────────────────────────────────────────
# FETCH ALL SECTORS IN PARALLEL
# ─────────────────────────────────────────────────────────
def fetch_sectors():
    results = {}

    def _fetch(sym):
        try:
            tk   = yf.Ticker(sym)
            hist = tk.history(period="5d", interval="1d")
            if hist.empty:
                return sym, None
            closes   = hist["Close"].tolist()
            prev_day = closes[-2] if len(closes) >= 2 else closes[-1]
            intra    = tk.history(period="1d", interval="5m")
            current  = float(intra["Close"].iloc[-1]) if not intra.empty else closes[-1]
            chg_p    = ((current - prev_day) / prev_day * 100) if prev_day else 0
            return sym, round(chg_p, 2)
        except Exception:
            return sym, None

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch, sym): sym for sym in SECTORS}
        for f in as_completed(futures):
            sym, val = f.result()
            if val is not None:
                results[sym] = val

    return results   # {symbol: chg_pct}

# ─────────────────────────────────────────────────────────
# SECTOR HEATMAP BLOCK (compact, fits Telegram)
# ─────────────────────────────────────────────────────────
def sector_emoji(p):
    if p >= 1.5:  return "🟢🟢"
    if p >= 0.5:  return "🟢 "
    if p >= 0:    return "🟡 "
    if p >= -0.5: return "🔸 "
    if p >= -1.5: return "🔴 "
    return "🔴🔴"

def build_sector_block(sector_data):
    if not sector_data:
        return "Sector data unavailable", None, None, 0, 0

    ranked = sorted(sector_data.items(), key=lambda x: x[1], reverse=True)
    green  = [(s, p) for s, p in ranked if p >= 0]
    red    = [(s, p) for s, p in ranked if p < 0]
    g_cnt  = len(green)
    r_cnt  = len(red)
    total  = len(ranked)

    top_g = ranked[0][0]   if ranked else None
    top_l = ranked[-1][0]  if ranked else None

    lines = []

    bar = "🟢" * g_cnt + "🔴" * r_cnt
    lines.append(f"{bar}")
    lines.append(f"{g_cnt} Green  {r_cnt} Red  ({total} sectors)\n")

    mid  = math.ceil(len(ranked) / 2)
    left = ranked[:mid]
    right = list(reversed(ranked[mid:]))

    lines.append(f"{'Sector':<10} {'Chg%':>6}   {'Sector':<10} {'Chg%':>6}")
    lines.append("─" * 36)

    for i in range(max(len(left), len(right))):
        def cell(item):
            if item is None:
                return f"{'':18s}"
            sym, p = item
            name = SECTORS[sym][:8].ljust(8)
            dot  = "▲" if p >= 0 else "▼"
            return f"{name} {dot}{abs(p):4.1f}%"

        l = left[i]  if i < len(left)  else None
        r = right[i] if i < len(right) else None
        lines.append(f"{cell(l)}  {cell(r)}")

    return "\n".join(lines), top_g, top_l, g_cnt, r_cnt


def rotation_insight(sector_data, top_g_sym, top_l_sym, g_cnt, r_cnt):
    if not sector_data:
        return ""
    total = len(sector_data)
    parts = []

    if g_cnt >= round(total * 0.75):
        parts.append(f"📊 Broad rally — {g_cnt}/{total} sectors advancing")
    elif r_cnt >= round(total * 0.75):
        parts.append(f"📊 Broad selloff — {r_cnt}/{total} sectors declining")
    elif g_cnt > r_cnt:
        parts.append(f"📊 Selective buying — {g_cnt}/{total} up")
    else:
        parts.append(f"📊 Selective selling — {r_cnt}/{total} down")

    if top_g_sym:
        p     = sector_data[top_g_sym]
        name  = SECTORS[top_g_sym]
        theme = SECTOR_THEMES.get(top_g_sym, "")
        parts.append(f"🔥 {name} leading +{p:.2f}%")
        if theme:
            parts.append(f"   → Market betting on: {theme}")

    if top_l_sym:
        p    = sector_data[top_l_sym]
        name = SECTORS[top_l_sym]
        parts.append(f"❄️  {name} weakest {p:.2f}%  — avoid near-term")

    return "\n".join(parts)

# ─────────────────────────────────────────────────────────
# ALERTS
# ─────────────────────────────────────────────────────────
def compute_alerts(sensex, nifty, vix, sector_data):
    alerts = []

    if sensex["chg_p"] <= -1.0:
        alerts.append(
            f"SENSEX ▼{abs(sensex['chg_p']):.2f}% from prev close "
            f"({fmt(sensex['prev'])} → {fmt(sensex['current'])})"
        )
    if nifty["chg_p"] <= -1.0:
        alerts.append(
            f"NIFTY 50 ▼{abs(nifty['chg_p']):.2f}% from prev close "
            f"({fmt(nifty['prev'])} → {fmt(nifty['current'])})"
        )
    if vix["chg_p"] >= 15:
        alerts.append(f"INDIA VIX spiked +{vix['chg_p']:.1f}% — volatility surge!")
    if vix["current"] >= 25:
        alerts.append(f"INDIA VIX at {fmt(vix['current'])} — PANIC ZONE")

    if not math.isnan(nifty["rsi"]):
        if nifty["rsi"] >= 75:
            alerts.append(f"Nifty RSI {nifty['rsi']} — EXTREME OVERBOUGHT")
        elif nifty["rsi"] <= 25:
            alerts.append(f"Nifty RSI {nifty['rsi']} — EXTREME OVERSOLD (bounce zone)")

    if sector_data:
        hard_drops = [(SECTORS[s], p) for s, p in sector_data.items() if p <= -2.0]
        hard_drops.sort(key=lambda x: x[1])
        if hard_drops:
            names = ", ".join(f"{n} ({p:.1f}%)" for n, p in hard_drops[:3])
            alerts.append(f"Sector collapse: {names}")

    return alerts

# ─────────────────────────────────────────────────────────
# TELEGRAM SENDER
# ─────────────────────────────────────────────────────────
def send_telegram(text):
    if len(text) > 4096:
        text = text[:4090] + "\n..."
        print(f"  ⚠️  Message trimmed to 4096 chars")

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("── [No Telegram config — printing locally] ──")
        print(text)
        print("──────────────────────────────────────────────")
        return True

    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=data, timeout=15)
        r.raise_for_status()
        print("  ✅ Telegram sent")
        return True
    except Exception as e:
        print(f"  ❌ Telegram error: {e}")
        return False

# ─────────────────────────────────────────────────────────
# MESSAGE 1 — Market Overview
# ─────────────────────────────────────────────────────────
def build_msg1(sensex, nifty, vix, alerts, now_str, sess):
    vix_label, vix_desc = get_vix_zone(vix["current"])
    bar_fill = min(int(vix["current"] / 3), 10)
    vix_bar  = "█" * bar_fill + "░" * (10 - bar_fill)
    avg_chg  = (sensex["chg_p"] + nifty["chg_p"]) / 2

    alert_block = ""
    if alerts:
        alert_block = (
            "\n\n🚨 ALERTS\n" +
            "\n".join(f"  • {a}" for a in alerts)
        )

    return (
        f"🇮🇳 India Market Pulse  [{sess}]\n"
        f"📅 {now_str}\n"
        f"{'─'*30}\n"

        f"\n📊 SENSEX {arrow(sensex['chg_p'])}\n"
        f"  {fmt(sensex['current'])}  "
        f"{sign(sensex['chg'])}{fmt(sensex['chg'])} "
        f"({sign(sensex['chg_p'])}{fmt(sensex['chg_p'])}%)\n"
        f"  H {fmt(sensex['high'])}  L {fmt(sensex['low'])}\n"
        f"  1W {sign(sensex['week_p'])}{fmt(sensex['week_p'])}%  "
        f"1M {sign(sensex['month_p'])}{fmt(sensex['month_p'])}%\n"

        f"\n📈 NIFTY 50 {arrow(nifty['chg_p'])}\n"
        f"  {fmt(nifty['current'])}  "
        f"{sign(nifty['chg'])}{fmt(nifty['chg'])} "
        f"({sign(nifty['chg_p'])}{fmt(nifty['chg_p'])}%)\n"
        f"  H {fmt(nifty['high'])}  L {fmt(nifty['low'])}\n"
        f"  1W {sign(nifty['week_p'])}{fmt(nifty['week_p'])}%  "
        f"1M {sign(nifty['month_p'])}{fmt(nifty['month_p'])}%\n"
        f"  RSI {rsi_tag(nifty['rsi'])}  |  {nifty['trend']}\n"

        f"\n🌡️ INDIA VIX\n"
        f"  {fmt(vix['current'])}  {vix_label}\n"
        f"  [{vix_bar}]\n"
        f"  {vix_desc}\n"
        f"  Change {sign(vix['chg_p'])}{fmt(vix['chg_p'])}%\n"

        f"\n🧭 Mood: {mood(avg_chg)}"
        f"{alert_block}"
    )

# ─────────────────────────────────────────────────────────
# MESSAGE 2 — Sector Heatmap
# ─────────────────────────────────────────────────────────
def build_msg2(sector_data, nifty_chg_p, now_str):
    block, top_g, top_l, g_cnt, r_cnt = build_sector_block(sector_data)
    insight = rotation_insight(sector_data, top_g, top_l, g_cnt, r_cnt)

    return (
        f"🏭 Sector Heatmap\n"
        f"📅 {now_str}\n"
        f"{'─'*30}\n\n"
        f"{block}\n\n"
        f"📌 Rotation Signal\n"
        f"{insight}\n"
        f"{'─'*30}\n"
        f"🤖 Every 5 min  |  NSE via Yahoo Finance"
    )

# ─────────────────────────────────────────────────────────
# CSV UPDATE
# ─────────────────────────────────────────────────────────
def update_csv(sensex, nifty, vix, sector_data):
    now_ist  = datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
    vix_zone = get_vix_zone(vix["current"])[0].strip()

    top_g = top_l = ""
    g_cnt = r_cnt = 0
    if sector_data:
        ranked = sorted(sector_data.items(), key=lambda x: x[1], reverse=True)
        top_g  = SECTORS.get(ranked[0][0],  "") if ranked else ""
        top_l  = SECTORS.get(ranked[-1][0], "") if ranked else ""
        g_cnt  = sum(1 for p in sector_data.values() if p >= 0)
        r_cnt  = sum(1 for p in sector_data.values() if p < 0)

    row = {
        "timestamp":       now_ist,
        "sensex":          sensex["current"],
        "sensex_chg_pct":  sensex["chg_p"],
        "nifty50":         nifty["current"],
        "nifty50_chg_pct": nifty["chg_p"],
        "india_vix":       vix["current"],
        "vix_zone":        vix_zone,
        "nifty_rsi":       nifty["rsi"] if not math.isnan(nifty["rsi"]) else "",
        "nifty_trend":     nifty["trend"],
        "nifty_1w_pct":    nifty["week_p"],
        "nifty_1m_pct":    nifty["month_p"],
        "top_sector":      top_g,
        "bottom_sector":   top_l,
        "sectors_green":   g_cnt,
        "sectors_red":     r_cnt,
    }

    for sym, name in SECTORS.items():
        col = name.lower().replace(" ", "_") + "_pct"
        row[col] = sector_data.get(sym, "")

    fieldnames = list(row.keys())
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    exists = os.path.isfile(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        w.writerow(row)
    print(f"  ✅ CSV updated → {CSV_PATH}")

# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    now     = datetime.datetime.now(IST)
    now_str = now.strftime("%d %b %Y  %I:%M %p IST")
    sess    = session_label()

    print(f"\n{'='*60}")
    print(f"  🇮🇳  India Market Tracker v2   {now.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"{'='*60}")
    print(f"  Session: {sess}")

    if not FORCE_RUN and not is_market_open():
        if sess == "Pre-Open":
            print("  Pre-open mode — continuing...")
        else:
            print("  Market closed. Set FORCE_RUN=true to override.")
            return

    print("\n📡 Fetching core indices...")
    try:
        sensex = fetch_ticker("^BSESN")
        print(f"  ✅ Sensex   {fmt(sensex['current'])} ({sign(sensex['chg_p'])}{fmt(sensex['chg_p'])}%)")
    except Exception as e:
        print(f"  ❌ Sensex: {e}"); return

    try:
        nifty = fetch_ticker("^NSEI")
        print(f"  ✅ Nifty50  {fmt(nifty['current'])} ({sign(nifty['chg_p'])}{fmt(nifty['chg_p'])}%)")
    except Exception as e:
        print(f"  ❌ Nifty50: {e}"); return

    try:
        vix = fetch_ticker("^INDIAVIX")
        print(f"  ✅ VIX      {fmt(vix['current'])} ({sign(vix['chg_p'])}{fmt(vix['chg_p'])}%)")
    except Exception as e:
        print(f"  ❌ VIX: {e}"); return

    print("\n📡 Fetching 12 sector indices in parallel...")
    sector_data = fetch_sectors()
    print(f"  ✅ Got {len(sector_data)}/{len(SECTORS)} sectors")
    if sector_data:
        ranked = sorted(sector_data.items(), key=lambda x: x[1], reverse=True)
        for sym, p in ranked:
            bar = "█" * int(min(abs(p) / 0.3, 10))
            print(f"     {'▲' if p >= 0 else '▼'}  {SECTORS[sym]:<12} {sign(p)}{p:.2f}%  {bar}")

    alerts = compute_alerts(sensex, nifty, vix, sector_data)
    if alerts:
        print(f"\n🚨 {len(alerts)} alert(s)")
        for a in alerts:
            print(f"   • {a}")

    print("\n💾 Updating CSV...")
    try:
        update_csv(sensex, nifty, vix, sector_data)
    except Exception as e:
        print(f"  ⚠️  CSV: {e}")

    print("\n📤 Sending Telegram...")
    msg1 = build_msg1(sensex, nifty, vix, alerts, now_str, sess)
    msg2 = build_msg2(sector_data, nifty["chg_p"], now_str)

    print(f"  MSG1: {len(msg1)} chars")
    print(f"  MSG2: {len(msg2)} chars")

    send_telegram(msg1)
    time.sleep(1)
    send_telegram(msg2)

    critical = any(
        kw in a for a in alerts
        for kw in ("SENSEX", "NIFTY 50", "PANIC", "collapse", "VIX spiked")
    )
    if alerts and critical:
        time.sleep(1)
        print("  🚨 Sending alert ping...")

    print(f"\n✅ Done — {datetime.datetime.now(IST).strftime('%H:%M:%S IST')}\n")


if __name__ == "__main__":
    main()
