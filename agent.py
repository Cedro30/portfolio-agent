"""
Portfolio Agent — Factor Portfolio 18 PIE
Monitoraggio real-time con notifiche Telegram
"""

import os
import time
import json
import logging
import hashlib
import sqlite3
from datetime import datetime, timedelta
import threading
import schedule
import requests
import yfinance as yf
import anthropic

# ── CONFIG ──────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")
NEWS_API_KEY      = os.environ.get("NEWS_API_KEY", "")

# Soglie alert
ALERT_MOVE_PCT    = 3.0   # movimento % per alert immediato
DIGEST_MOVE_PCT   = 1.5   # movimento % per digest orario
VOLUME_MULTIPLIER = 2.0   # volume anomalo rispetto alla media

# Orari (formato 24h, fuso orario server = UTC)
MARKET_OPEN  = 8    # 08:00 UTC = 09:00 CET circa
MARKET_CLOSE = 18   # 18:00 UTC = 19:00 CET circa
DIGEST_HOUR  = [10, 12, 14, 16]  # ore digest durante la sessione
REPORT_HOUR  = 18   # report serale

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── PORTAFOGLIO COMPLETO — 18 PIE ───────────────────────────
PORTFOLIO = {
    # TIER 1 — Dividend Growth Core (40%)
    "PIE01_Aristocrats_USA": {
        "tier": 1, "peso": "8%",
        "tickers": ["PG", "JNJ", "KO", "PEP", "ABT", "MDT", "WMT", "EMR"]
    },
    "PIE02_Aristocrats_EU": {
        "tier": 1, "peso": "7%",
        "tickers": ["AI.PA", "NESN.SW", "OR.PA", "SIKA.SW", "WKL.AS", "DSY.PA", "LIN"]
    },
    "PIE03_Aristocrats_Asia": {
        "tier": 1, "peso": "7%",
        "tickers": ["D05.SI", "O39.SI", "U11.SI", "7203.T", "8766.T", "CBA.AX"]
    },
    "PIE04_Champions_Energia": {
        "tier": 1, "peso": "6%",
        "tickers": ["XOM", "CVX", "WMB", "ENB", "TTE", "EOG", "CEG"]
    },
    "PIE05_Champions_Finanza": {
        "tier": 1, "peso": "6%",
        "tickers": ["HSBA.L", "CS.PA", "ALV.DE", "AIG", "UCG.MI", "BNP.PA", "MQG.AX"]
    },
    "PIE06_REIT_Growth": {
        "tier": 1, "peso": "6%",
        "tickers": ["O", "PLD", "AMT", "EQIX", "WPC", "AWK"]
    },
    # TIER 2 — Quality Compounders (30%)
    "PIE07_Quality_Tech": {
        "tier": 2, "peso": "6%",
        "tickers": ["ASML", "MSFT", "TXN", "AAPL", "SAP", "AVGO"]
    },
    "PIE08_Quality_Lusso": {
        "tier": 2, "peso": "6%",
        "tickers": ["MC.PA", "RMS.PA", "CFR.SW", "RACE", "MONC.MI", "EL"]
    },
    "PIE09_Quality_Healthcare": {
        "tier": 2, "peso": "5%",
        "tickers": ["JNJ", "LLY", "NOVOB.CO", "AZN", "TMO", "CSL.AX", "UNH"]
    },
    "PIE10_Quality_Difesa": {
        "tier": 2, "peso": "5%",
        "tickers": ["LMT", "NOC", "RHM.DE", "BAESY", "AIR.PA", "BWXT", "GD"]
    },
    "PIE11_Quality_Chip": {
        "tier": 2, "peso": "4%",
        "tickers": ["TSM", "005930.KS", "000660.KS", "2454.TW", "8035.T"]
    },
    "PIE12_Quality_Infrastrutture": {
        "tier": 2, "peso": "4%",
        "tickers": ["BIP", "DG.PA", "TCL.AX", "AWK", "AMT", "EQIX"]
    },
    # TIER 3 — Low Volatility Income (20%)
    "PIE13_Utility_Nucleare": {
        "tier": 3, "peso": "6%",
        "tickers": ["CEG", "ENEL.MI", "IBE.MC", "ETR", "SRG.MI", "TRN.MI", "D"]
    },
    "PIE14_Consumer_Staples": {
        "tier": 3, "peso": "5%",
        "tickers": ["PG", "KO", "PEP", "NESN.SW", "ULVR.L", "HINDUNILVR.NS", "COST"]
    },
    "PIE15_Gas_Industriali": {
        "tier": 3, "peso": "5%",
        "tickers": ["AI.PA", "LIN", "SIKA.SW", "SHW", "APD"]
    },
    "PIE16_Midstream_Pipeline": {
        "tier": 3, "peso": "4%",
        "tickers": ["WMB", "ENB", "KMI", "TRP", "SRG.MI"]
    },
    # TIER 4 — Momentum Growth (10%)
    "PIE17_AI_Tech": {
        "tier": 4, "peso": "6%",
        "tickers": ["NVDA", "GOOGL", "META", "AMZN", "AMD", "0700.HK"]
    },
    "PIE18_EM_Growth": {
        "tier": 4, "peso": "4%",
        "tickers": ["INFY", "HDB", "ITUB", "VALE", "RELIANCE.NS", "D05.SI", "IBN"]
    },
}

# Flatten tutti i ticker unici
ALL_TICKERS = list(set(
    t for pie in PORTFOLIO.values() for t in pie["tickers"]
))

TICKER_TO_PIE = {}
for pie_name, pie_data in PORTFOLIO.items():
    for t in pie_data["tickers"]:
        if t not in TICKER_TO_PIE:
            TICKER_TO_PIE[t] = []
        TICKER_TO_PIE[t].append(pie_name)

# Nomi leggibili per le query notizie
TICKER_NAMES = {
    "PG": "Procter Gamble", "JNJ": "Johnson Johnson", "KO": "Coca-Cola",
    "PEP": "PepsiCo", "ABT": "Abbott Laboratories", "MDT": "Medtronic",
    "WMT": "Walmart", "EMR": "Emerson Electric", "AI.PA": "Air Liquide",
    "NESN.SW": "Nestle", "OR.PA": "L Oreal", "SIKA.SW": "Sika",
    "WKL.AS": "Wolters Kluwer", "DSY.PA": "Dassault Systemes", "LIN": "Linde",
    "D05.SI": "DBS Group", "O39.SI": "OCBC Bank", "U11.SI": "UOB",
    "7203.T": "Toyota", "8766.T": "Tokio Marine", "CBA.AX": "Commonwealth Bank",
    "XOM": "ExxonMobil", "CVX": "Chevron", "WMB": "Williams Companies",
    "ENB": "Enbridge", "TTE": "TotalEnergies", "EOG": "EOG Resources",
    "CEG": "Constellation Energy", "HSBA.L": "HSBC", "CS.PA": "AXA",
    "ALV.DE": "Allianz", "AIG": "AIG", "UCG.MI": "UniCredit",
    "BNP.PA": "BNP Paribas", "MQG.AX": "Macquarie", "O": "Realty Income",
    "PLD": "Prologis", "AMT": "American Tower", "EQIX": "Equinix",
    "WPC": "WP Carey", "AWK": "American Water Works", "ASML": "ASML",
    "MSFT": "Microsoft", "TXN": "Texas Instruments", "AAPL": "Apple",
    "SAP": "SAP", "AVGO": "Broadcom", "MC.PA": "LVMH",
    "RMS.PA": "Hermes", "CFR.SW": "Richemont", "RACE": "Ferrari",
    "MONC.MI": "Moncler", "EL": "Estee Lauder", "LLY": "Eli Lilly",
    "NOVOB.CO": "Novo Nordisk", "AZN": "AstraZeneca", "TMO": "Thermo Fisher",
    "CSL.AX": "CSL", "UNH": "UnitedHealth", "LMT": "Lockheed Martin",
    "NOC": "Northrop Grumman", "RHM.DE": "Rheinmetall", "BAESY": "BAE Systems",
    "AIR.PA": "Airbus", "BWXT": "BWX Technologies", "GD": "General Dynamics",
    "TSM": "TSMC", "005930.KS": "Samsung", "000660.KS": "SK Hynix",
    "2454.TW": "MediaTek", "8035.T": "Tokyo Electron", "BIP": "Brookfield Infrastructure",
    "DG.PA": "Vinci", "TCL.AX": "Transurban", "ENEL.MI": "Enel",
    "IBE.MC": "Iberdrola", "ETR": "Entergy", "SRG.MI": "Snam",
    "TRN.MI": "Terna", "D": "Dominion Energy", "ULVR.L": "Unilever",
    "HINDUNILVR.NS": "Hindustan Unilever", "COST": "Costco", "SHW": "Sherwin-Williams",
    "APD": "Air Products", "KMI": "Kinder Morgan", "TRP": "TC Energy",
    "NVDA": "NVIDIA", "GOOGL": "Alphabet Google", "META": "Meta",
    "AMZN": "Amazon", "AMD": "AMD", "0700.HK": "Tencent",
    "INFY": "Infosys", "HDB": "HDFC Bank", "ITUB": "Itau Unibanco",
    "VALE": "Vale", "RELIANCE.NS": "Reliance Industries", "IBN": "ICICI Bank",
}

# ── DATABASE (anti-spam) ─────────────────────────────────────
def init_db():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS sent_alerts (
        hash TEXT PRIMARY KEY,
        ticker TEXT,
        alert_type TEXT,
        created_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS price_cache (
        ticker TEXT PRIMARY KEY,
        price REAL,
        prev_close REAL,
        avg_volume REAL,
        updated_at TEXT
    )""")
    conn.commit()
    conn.close()

def alert_already_sent(hash_key, hours=4):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    c.execute("SELECT 1 FROM sent_alerts WHERE hash=? AND created_at>?", (hash_key, cutoff))
    exists = c.fetchone() is not None
    conn.close()
    return exists

def mark_alert_sent(hash_key, ticker, alert_type):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO sent_alerts VALUES (?,?,?,?)",
              (hash_key, ticker, alert_type, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def cleanup_old_alerts():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    c.execute("DELETE FROM sent_alerts WHERE created_at<?", (cutoff,))
    conn.commit()
    conn.close()

# ── TELEGRAM ────────────────────────────────────────────────
def send_telegram(message, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram non configurato")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if not r.ok:
            log.error(f"Telegram error: {r.text}")
    except Exception as e:
        log.error(f"Telegram exception: {e}")

# ── PREZZI (Yahoo Finance) ───────────────────────────────────
def get_prices(tickers):
    results = {}
    # Batch download
    batch = [t for t in tickers if t not in ["0700.HK"]]  # HK separato
    try:
        data = yf.download(batch, period="2d", interval="1d",
                           group_by="ticker", auto_adjust=True, progress=False)
        for ticker in batch:
            try:
                if len(batch) == 1:
                    closes = data["Close"]
                else:
                    closes = data["Close"][ticker]
                closes = closes.dropna()
                if len(closes) >= 2:
                    results[ticker] = {
                        "price": float(closes.iloc[-1]),
                        "prev_close": float(closes.iloc[-2]),
                        "change_pct": float((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100)
                    }
            except Exception:
                pass
    except Exception as e:
        log.error(f"Yahoo Finance batch error: {e}")
    return results

def get_intraday_prices(tickers):
    """Prezzi intraday per movimento durante la sessione"""
    results = {}
    try:
        data = yf.download(tickers, period="1d", interval="5m",
                           group_by="ticker", auto_adjust=True, progress=False)
        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    closes = data["Close"]
                else:
                    closes = data["Close"][ticker]
                closes = closes.dropna()
                if len(closes) >= 2:
                    open_price = float(closes.iloc[0])
                    current    = float(closes.iloc[-1])
                    results[ticker] = {
                        "price": current,
                        "open": open_price,
                        "change_pct": (current - open_price) / open_price * 100
                    }
            except Exception:
                pass
    except Exception as e:
        log.error(f"Intraday error: {e}")
    return results

# ── NOTIZIE (NewsAPI) ────────────────────────────────────────
def get_news(query, hours=4, max_articles=5):
    if not NEWS_API_KEY:
        return []
    from_time = (datetime.utcnow() - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "from": from_time,
        "sortBy": "publishedAt",
        "language": "en",
        "pageSize": max_articles,
        "apiKey": NEWS_API_KEY
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.ok:
            articles = r.json().get("articles", [])
            return [{"title": a["title"], "source": a["source"]["name"],
                     "url": a["url"], "published": a["publishedAt"]} for a in articles]
    except Exception as e:
        log.error(f"NewsAPI error: {e}")
    return []

def get_portfolio_news(hours=2):
    """Cerca notizie per i titoli principali del portafoglio"""
    news_by_ticker = {}
    # Cerca per i titoli con nome noto
    priority_tickers = [
        "NVDA", "MSFT", "ASML", "GOOGL", "META", "AMZN", "AAPL",
        "XOM", "CVX", "LMT", "NOC", "JNJ", "LLY", "NOVOB.CO",
        "TSM", "005930.KS", "MC.PA", "RMS.PA", "LVMH", "AIR.PA"
    ]
    for ticker in priority_tickers:
        name = TICKER_NAMES.get(ticker, ticker)
        articles = get_news(f'"{name}" stock dividend earnings', hours=hours, max_articles=3)
        if articles:
            news_by_ticker[ticker] = articles
        time.sleep(0.3)  # rate limiting
    return news_by_ticker

# ── CLAUDE ANALYSIS ──────────────────────────────────────────
def analyze_with_claude(context):
    """Usa Claude per analizzare eventi e generare insight"""
    if not ANTHROPIC_API_KEY:
        return None
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    system = """Sei un analista finanziario senior specializzato in portafogli buy & hold dividend growth.
Il portafoglio monitora 18 PIE tematici con focus su: Dividend Aristocrats (Tier 1, 40%), Quality Compounders (Tier 2, 30%), Low Volatility Income (Tier 3, 20%) e Momentum Growth (Tier 4, 10%).
La filosofia e mantenere i titoli a lungo termine — non fare trading. Analizza solo se l evento cambia la tesi di investimento strutturale.
Rispondi in italiano, in modo conciso e diretto. Max 3 frasi per ogni elemento."""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            system=system,
            messages=[{"role": "user", "content": context}]
        )
        return msg.content[0].text
    except Exception as e:
        log.error(f"Claude API error: {e}")
        return None

# ── MONITOR PREZZI ───────────────────────────────────────────
def check_price_alerts():
    """Controlla movimenti anomali ogni minuto durante la sessione"""
    log.info("Controllo prezzi in corso...")
    now = datetime.utcnow()
    if not (MARKET_OPEN <= now.hour < MARKET_CLOSE):
        return

    # Controlla i ticker US (gli altri mercati hanno orari diversi)
    us_tickers = [t for t in ALL_TICKERS if "." not in t]
    prices = get_intraday_prices(us_tickers)

    critical_alerts = []
    for ticker, data in prices.items():
        change = data.get("change_pct", 0)
        abs_change = abs(change)

        if abs_change >= ALERT_MOVE_PCT:
            # Alert critico
            hash_key = hashlib.md5(
                f"{ticker}_{round(change)}_{now.strftime('%Y%m%d%H')}".encode()
            ).hexdigest()

            if not alert_already_sent(hash_key):
                pie_names = TICKER_TO_PIE.get(ticker, ["N/A"])
                critical_alerts.append({
                    "ticker": ticker,
                    "name": TICKER_NAMES.get(ticker, ticker),
                    "change": change,
                    "price": data["price"],
                    "pie": pie_names[0].replace("_", " ")
                })
                mark_alert_sent(hash_key, ticker, "price_alert")

    if critical_alerts:
        send_critical_price_alerts(critical_alerts)

def send_critical_price_alerts(alerts):
    """Invia alert prezzi critici su Telegram"""
    for a in alerts:
        direction = "📈" if a["change"] > 0 else "📉"
        sign = "+" if a["change"] > 0 else ""

        # Analisi Claude
        context = f"""Il titolo {a['name']} ({a['ticker']}) ha avuto un movimento di {sign}{a['change']:.1f}% oggi.
Fa parte del {a['pie']} nel portafoglio Factor Portfolio a 18 PIE.
Analizza brevemente se questo e rilevante per la tesi di investimento buy & hold dividend growth."""

        analysis = analyze_with_claude(context)

        msg = (
            f"🔴 <b>ALERT CRITICO</b> — {datetime.utcnow().strftime('%H:%M')} UTC\n"
            f"\n"
            f"{direction} <b>{a['name']}</b> ({a['ticker']})\n"
            f"<b>{'+' if a['change']>0 else ''}{a['change']:.2f}%</b> · ${a['price']:.2f}\n"
            f"\n"
            f"<i>{a['pie']}</i>\n"
        )
        if analysis:
            msg += f"\n🤖 <b>Analisi:</b> {analysis}"

        send_telegram(msg)
        time.sleep(1)

# ── MONITOR NOTIZIE ──────────────────────────────────────────
def check_news_alerts():
    """Controlla notizie ogni 30 minuti"""
    log.info("Controllo notizie in corso...")
    news = get_portfolio_news(hours=1)
    important_news = []

    for ticker, articles in news.items():
        for article in articles:
            title_lower = article["title"].lower()
            # Filtra notizie rilevanti
            keywords = ["dividend", "earnings", "revenue", "acquisition", "merger",
                        "ceo", "downgrade", "upgrade", "guidance", "outlook",
                        "cut", "raise", "beat", "miss", "quarter", "annual"]
            if any(kw in title_lower for kw in keywords):
                hash_key = hashlib.md5(article["title"].encode()).hexdigest()
                if not alert_already_sent(hash_key, hours=12):
                    important_news.append({
                        "ticker": ticker,
                        "name": TICKER_NAMES.get(ticker, ticker),
                        "article": article,
                        "pie": TICKER_TO_PIE.get(ticker, ["N/A"])[0].replace("_", " ")
                    })
                    mark_alert_sent(hash_key, ticker, "news_alert")

    if important_news:
        send_news_alerts(important_news[:5])  # max 5 per ciclo

def send_news_alerts(news_items):
    """Invia alert notizie su Telegram"""
    if len(news_items) == 1:
        item = news_items[0]
        context = f"""Notizia su {item['name']} ({item['ticker']}): "{item['article']['title']}"
Fonte: {item['article']['source']}
Analizza brevemente la rilevanza per la tesi di investimento."""
        analysis = analyze_with_claude(context)

        msg = (
            f"📰 <b>NOTIZIA RILEVANTE</b>\n"
            f"\n"
            f"<b>{item['name']}</b> ({item['ticker']})\n"
            f"{item['article']['title']}\n"
            f"<i>Fonte: {item['article']['source']}</i>\n"
            f"<i>{item['pie']}</i>\n"
        )
        if analysis:
            msg += f"\n🤖 {analysis}"
        send_telegram(msg)
    else:
        # Raggruppa in un messaggio
        msg = "📰 <b>NOTIZIE DAL PORTAFOGLIO</b>\n\n"
        for item in news_items:
            msg += f"• <b>{item['name']}</b>: {item['article']['title'][:80]}...\n"
        msg += f"\n<i>{len(news_items)} notizie — usa /dettagli per analisi</i>"
        send_telegram(msg)

# ── DIGEST ORARIO ────────────────────────────────────────────
def send_hourly_digest():
    """Digest orario con movimenti e notizie"""
    now = datetime.utcnow()
    if not (MARKET_OPEN <= now.hour < MARKET_CLOSE):
        return

    log.info("Invio digest orario...")
    us_tickers = [t for t in ALL_TICKERS if "." not in t]
    prices = get_intraday_prices(us_tickers)

    movers = [(t, d) for t, d in prices.items()
              if abs(d.get("change_pct", 0)) >= DIGEST_MOVE_PCT]
    movers.sort(key=lambda x: abs(x[1]["change_pct"]), reverse=True)

    if not movers:
        # Niente di significativo
        msg = (
            f"🟢 <b>DIGEST {now.strftime('%H:%M')} UTC</b>\n"
            f"Tutto tranquillo — nessun movimento significativo.\n"
            f"Portafoglio nella norma."
        )
        send_telegram(msg)
        return

    msg = f"🟡 <b>DIGEST {now.strftime('%H:%M')} UTC</b>\n\n"
    for ticker, data in movers[:8]:
        change = data["change_pct"]
        icon = "📈" if change > 0 else "📉"
        name = TICKER_NAMES.get(ticker, ticker)
        msg += f"{icon} <b>{name}</b> {'+' if change>0 else ''}{change:.1f}%\n"

    if len(movers) > 8:
        msg += f"\n<i>... e altri {len(movers)-8} movimenti</i>"

    send_telegram(msg)

# ── REPORT SERALE ────────────────────────────────────────────
def send_evening_report():
    """Report serale completo con analisi Claude"""
    log.info("Invio report serale...")
    us_tickers = [t for t in ALL_TICKERS if "." not in t][:30]  # limita per costi API
    prices = get_prices(us_tickers)

    gainers = [(t, d) for t, d in prices.items() if d.get("change_pct", 0) > 0]
    losers  = [(t, d) for t, d in prices.items() if d.get("change_pct", 0) < 0]
    gainers.sort(key=lambda x: x[1]["change_pct"], reverse=True)
    losers.sort(key=lambda x: x[1]["change_pct"])

    top3_up   = gainers[:3]
    top3_down = losers[:3]

    # Contesto per Claude
    context = f"""Report serale del portafoglio Factor Portfolio — {datetime.utcnow().strftime('%d/%m/%Y')}

Migliori performer oggi:
{chr(10).join([f"- {TICKER_NAMES.get(t,t)}: +{d['change_pct']:.1f}%" for t,d in top3_up])}

Peggiori performer oggi:
{chr(10).join([f"- {TICKER_NAMES.get(t,t)}: {d['change_pct']:.1f}%" for t,d in top3_down])}

Fornisci:
1. Una valutazione breve dell'andamento di oggi per il portafoglio buy & hold
2. Un consiglio operativo (mantenere tutto? monitorare qualcosa?)
3. Un outlook per domani"""

    analysis = analyze_with_claude(context)

    msg = (
        f"🟢 <b>REPORT SERALE — {datetime.utcnow().strftime('%d/%m/%Y')}</b>\n"
        f"\n"
        f"<b>Top performers oggi:</b>\n"
    )
    for t, d in top3_up:
        msg += f"📈 {TICKER_NAMES.get(t,t)} +{d['change_pct']:.1f}%\n"

    msg += f"\n<b>Peggiori oggi:</b>\n"
    for t, d in top3_down:
        msg += f"📉 {TICKER_NAMES.get(t,t)} {d['change_pct']:.1f}%\n"

    if analysis:
        msg += f"\n🤖 <b>Analisi:</b>\n{analysis}"

    msg += f"\n\n<i>Factor Portfolio · 18 PIE · Dividend Growth</i>"
    send_telegram(msg)

# ── REPORT SETTIMANALE ───────────────────────────────────────
def send_weekly_report():
    """Report settimanale ogni lunedi mattina"""
    now = datetime.utcnow()
    if now.weekday() != 0:  # 0 = lunedi
        return

    log.info("Invio report settimanale...")
    context = """Genera un breve report settimanale per un portafoglio Factor Portfolio dividend growth con 18 PIE.
Includi: 1) Cosa monitorare questa settimana nei mercati globali, 2) Earnings rilevanti in arrivo, 3) Consiglio generale per un investitore buy & hold."""

    analysis = analyze_with_claude(context)

    msg = (
        f"📊 <b>REPORT SETTIMANALE</b> — {now.strftime('%d/%m/%Y')}\n"
        f"\n"
    )
    if analysis:
        msg += analysis
    msg += f"\n\n<i>Buona settimana — Factor Portfolio · 18 PIE</i>"
    send_telegram(msg)

# ── SCHEDULER ───────────────────────────────────────────────
def run_scheduler():
    """Configura e avvia lo scheduler"""
    # Ogni minuto: check prezzi (filtro interno per orari borsa)
    schedule.every(1).minutes.do(check_price_alerts)

    # Ogni 30 minuti: check notizie
    schedule.every(30).minutes.do(check_news_alerts)

    # Digest orari durante la sessione
    for h in DIGEST_HOUR:
        schedule.every().day.at(f"{h:02d}:00").do(send_hourly_digest)

    # Report serale
    schedule.every().day.at(f"{REPORT_HOUR:02d}:30").do(send_evening_report)

    # Report settimanale lunedi 07:00 UTC
    schedule.every().monday.at("07:00").do(send_weekly_report)

    # Pulizia DB ogni notte
    schedule.every().day.at("02:00").do(cleanup_old_alerts)

    log.info("Scheduler avviato. Monitoraggio in corso...")
    send_telegram(
        "🚀 <b>Portfolio Agent avviato</b>\n"
        f"Monitoraggio attivo su {len(ALL_TICKERS)} ticker · 18 PIE\n"
        "Alert 🔴 critici · Digest 🟡 orari · Report 🟢 serali"
    )

    while True:
        schedule.run_pending()
        time.sleep(30)

# ── HEALTH CHECK (Railway richiede una porta HTTP aperta) ────
from http.server import HTTPServer, BaseHTTPRequestHandler

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Portfolio Agent running")
    def log_message(self, *args):
        pass  # silenzia i log HTTP

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()

# ── MAIN ────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Portfolio Agent — Factor Portfolio 18 PIE")
    log.info(f"Ticker monitorati: {len(ALL_TICKERS)}")
    init_db()
    # Health server in background (richiesto da Railway)
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    run_scheduler()
