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
from datetime import datetime, timezone, timedelta
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

ALERT_MOVE_PCT    = 3.0
DIGEST_MOVE_PCT   = 1.5
MARKET_OPEN       = 8
MARKET_CLOSE      = 22
DIGEST_HOUR       = [10, 12, 14, 16]
REPORT_HOUR       = 18

# Batch size ridotto per evitare connection pool overflow
BATCH_SIZE        = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── PORTAFOGLIO COMPLETO — 18 PIE ───────────────────────────
PORTFOLIO = {
    # TIER 1 — Dividend Growth Core (40%)
    # Regola duplicati: titolo in due PIE solo se tier diversi
    "PIE01_Aristocrats_USA": {
        "tier": 1, "peso": "8%",
        # PG/KO/JNJ/PEP restano anche in PIE14 (Tier3) — sovrappeso intenzionale
        "tickers": ["PG", "JNJ", "KO", "PEP", "ABT", "MDT", "WMT", "EMR"]
    },
    "PIE02_Aristocrats_EU": {
        "tier": 1, "peso": "7%",
        # AI.PA/LIN/SIKA restano anche in PIE15 (Tier3) — sovrappeso intenzionale
        # NESN rimossa da PIE14 — resta solo qui come Aristocrat EU
        "tickers": ["AI.PA", "NESN.SW", "OR.PA", "SIKA.SW", "WKL.AS", "DSY.PA", "LIN"]
    },
    "PIE03_Aristocrats_Asia": {
        "tier": 1, "peso": "7%",
        # OCBC(O39.SI) e UOB(U11.SI) non disponibili su T212 — DBS consolidata
        # Tokio Marine(8766.T) non disponibile su T212 — sostituita con Sony(6758.T)
        "tickers": ["D05.SI", "7203.T", "6758.T", "CBA.AX"]
    },
    "PIE04_Champions_Energia": {
        "tier": 1, "peso": "6%",
        # WMB/ENB restano anche in PIE16 (Tier3) — sovrappeso intenzionale
        # CEG resta anche in PIE13 (Tier3) — ruolo duale energia/utility
        "tickers": ["XOM", "CVX", "WMB", "ENB", "TTE", "EOG", "CEG"]
    },
    "PIE05_Champions_Finanza": {
        "tier": 1, "peso": "6%",
        "tickers": ["HSBA.L", "CS.PA", "ALV.DE", "AIG", "UCG.MI", "BNP.PA", "MQG.AX"]
    },
    "PIE06_REIT_Growth": {
        "tier": 1, "peso": "6%",
        # AMT/AWK/EQIX rimossi da PIE12 (stesso tier) — restano solo qui
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
        # JNJ resta anche in PIE01 (Tier1) — sovrappeso intenzionale
        "tickers": ["JNJ", "LLY", "NOVOB.CO", "AZN", "TMO", "ROG.SW", "UNH"]
    },
    "PIE10_Quality_Difesa": {
        "tier": 2, "peso": "5%",
        "tickers": ["LMT", "NOC", "RHM.DE", "BAESY", "AIR.PA", "BWXT", "GD"]
    },
    "PIE11_Quality_Chip": {
        "tier": 2, "peso": "4%",
        "tickers": ["TSM", "005930.KS", "000660.KS", "QCOM", "8035.T"]
    },
    "PIE12_Quality_Infrastrutture": {
        "tier": 2, "peso": "4%",
        # AMT/AWK/EQIX rimossi — stesso tier di PIE06, ridondanza senza valore
        # Sostituiti con titoli infrastrutture puri non presenti altrove
        "tickers": ["BIP", "DG.PA", "FER.MC", "GET.PA", "ATNI.MI"]
    },
    # TIER 3 — Low Volatility Income (20%)
    "PIE13_Utility_Nucleare": {
        "tier": 3, "peso": "6%",
        # CEG resta anche in PIE04 (Tier1) — ruolo duale giustificato
        # SRG.MI rimossa da PIE16 — utility italiana, non pipeline internazionale
        "tickers": ["CEG", "ENEL.MI", "IBE.MC", "ETR", "SRG.MI", "TRN.MI", "D"]
    },
    "PIE14_Consumer_Staples": {
        "tier": 3, "peso": "5%",
        # PG/KO/PEP restano — sovrappeso intenzionale da Tier1
        # NESN rimossa — resta solo in PIE02 come Aristocrat EU
        # Aggiunta ULVR per diversificazione UK
        "tickers": ["PG", "KO", "PEP", "ULVR.L", "COST"]
    },
    "PIE15_Gas_Industriali": {
        "tier": 3, "peso": "5%",
        # AI.PA/LIN/SIKA restano — sovrappeso intenzionale da Tier1
        "tickers": ["AI.PA", "LIN", "SIKA.SW", "SHW", "APD"]
    },
    "PIE16_Midstream_Pipeline": {
        "tier": 3, "peso": "4%",
        # WMB/ENB restano — sovrappeso intenzionale da Tier1
        # SRG.MI rimossa — utility, non pipeline. Aggiunta TRP
        "tickers": ["WMB", "ENB", "KMI", "TRP", "PPL"]
    },
    # TIER 4 — Momentum Growth (10%)
    "PIE17_AI_Tech": {
        "tier": 4, "peso": "6%",
        "tickers": ["NVDA", "GOOGL", "META", "AMZN", "AMD", "KWEB"]
    },
    "PIE18_EM_Growth": {
        "tier": 4, "peso": "4%",
        # D05.SI rimossa — Asia sviluppata, non EM. Aggiunta Tencent
        "tickers": ["INFY", "HDB", "ITUB", "VALE", "IBN", "KWEB", "RELIANCE.NS"]
    },
}

ALL_TICKERS = list(set(
    t for pie in PORTFOLIO.values() for t in pie["tickers"]
))

# Solo ticker USA/senza suffisso per il monitoraggio intraday
US_TICKERS = [t for t in ALL_TICKERS if "." not in t]

TICKER_TO_PIE = {}
for pie_name, pie_data in PORTFOLIO.items():
    for t in pie_data["tickers"]:
        if t not in TICKER_TO_PIE:
            TICKER_TO_PIE[t] = []
        TICKER_TO_PIE[t].append(pie_name)

TICKER_NAMES = {
    "PG": "Procter & Gamble", "JNJ": "Johnson & Johnson", "KO": "Coca-Cola",
    "PEP": "PepsiCo", "ABT": "Abbott", "MDT": "Medtronic",
    "WMT": "Walmart", "EMR": "Emerson Electric", "LIN": "Linde",
    "D05.SI": "DBS Group", "O39.SI": "OCBC", "U11.SI": "UOB",
    "7203.T": "Toyota", "8766.T": "Tokio Marine", "CBA.AX": "Commonwealth Bank",
    "XOM": "ExxonMobil", "CVX": "Chevron", "WMB": "Williams Companies",
    "ENB": "Enbridge", "TTE": "TotalEnergies", "EOG": "EOG Resources",
    "CEG": "Constellation Energy", "AIG": "AIG",
    "O": "Realty Income", "PLD": "Prologis", "AMT": "American Tower",
    "EQIX": "Equinix", "WPC": "WP Carey", "AWK": "American Water",
    "ASML": "ASML", "MSFT": "Microsoft", "TXN": "Texas Instruments",
    "AAPL": "Apple", "SAP": "SAP", "AVGO": "Broadcom",
    "RACE": "Ferrari", "EL": "Estee Lauder",
    "LLY": "Eli Lilly", "AZN": "AstraZeneca", "TMO": "Thermo Fisher",
    "UNH": "UnitedHealth", "LMT": "Lockheed Martin", "NOC": "Northrop Grumman",
    "BAESY": "BAE Systems", "BWXT": "BWX Technologies", "GD": "General Dynamics",
    "TSM": "TSMC", "BIP": "Brookfield Infrastructure",
    "ETR": "Entergy", "D": "Dominion Energy", "COST": "Costco",
    "SHW": "Sherwin-Williams", "APD": "Air Products", "KMI": "Kinder Morgan",
    "NVDA": "NVIDIA", "GOOGL": "Alphabet", "META": "Meta",
    "AMZN": "Amazon", "AMD": "AMD",
    "INFY": "Infosys", "HDB": "HDFC Bank", "ITUB": "Itau Unibanco",
    "VALE": "Vale", "IBN": "ICICI Bank",
    "TRP": "TC Energy", "NOC": "Northrop Grumman",
}

# ── DATABASE ─────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS sent_alerts (
        hash TEXT PRIMARY KEY,
        ticker TEXT,
        alert_type TEXT,
        created_at TEXT
    )""")
    conn.commit()
    conn.close()

def alert_already_sent(hash_key, hours=4):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    c.execute("SELECT 1 FROM sent_alerts WHERE hash=? AND created_at>?", (hash_key, cutoff))
    exists = c.fetchone() is not None
    conn.close()
    return exists

def mark_alert_sent(hash_key, ticker, alert_type):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO sent_alerts VALUES (?,?,?,?)",
              (hash_key, ticker, alert_type, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()

def cleanup_old_alerts():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    c.execute("DELETE FROM sent_alerts WHERE created_at<?", (cutoff,))
    conn.commit()
    conn.close()

# ── TELEGRAM ────────────────────────────────────────────────
def send_telegram(message, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram non configurato")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.ok:
            return True
        else:
            log.error(f"Telegram error: {r.text}")
            return False
    except Exception as e:
        log.error(f"Telegram exception: {e}")
        return False

# ── PREZZI — batch piccoli per evitare overflow ──────────────
def get_prices_batch(tickers):
    """Scarica prezzi in batch piccoli per evitare connection pool overflow"""
    results = {}
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        try:
            if len(batch) == 1:
                ticker_obj = yf.Ticker(batch[0])
                hist = ticker_obj.history(period="2d")
                if len(hist) >= 2:
                    results[batch[0]] = {
                        "price": float(hist["Close"].iloc[-1]),
                        "prev_close": float(hist["Close"].iloc[-2]),
                        "change_pct": float(
                            (hist["Close"].iloc[-1] - hist["Close"].iloc[-2])
                            / hist["Close"].iloc[-2] * 100
                        )
                    }
            else:
                data = yf.download(
                    batch, period="2d", interval="1d",
                    group_by="ticker", auto_adjust=True,
                    progress=False, threads=False
                )
                for ticker in batch:
                    try:
                        closes = data["Close"][ticker].dropna()
                        if len(closes) >= 2:
                            results[ticker] = {
                                "price": float(closes.iloc[-1]),
                                "prev_close": float(closes.iloc[-2]),
                                "change_pct": float(
                                    (closes.iloc[-1] - closes.iloc[-2])
                                    / closes.iloc[-2] * 100
                                )
                            }
                    except Exception:
                        pass
        except Exception as e:
            log.error(f"Batch error {batch}: {e}")
        time.sleep(1)  # pausa tra batch
    return results

def get_intraday_batch(tickers):
    """Prezzi intraday in batch piccoli"""
    results = {}
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        try:
            if len(batch) == 1:
                ticker_obj = yf.Ticker(batch[0])
                hist = ticker_obj.history(period="1d", interval="5m")
                if len(hist) >= 2:
                    open_p  = float(hist["Close"].iloc[0])
                    close_p = float(hist["Close"].iloc[-1])
                    results[batch[0]] = {
                        "price": close_p,
                        "open": open_p,
                        "change_pct": (close_p - open_p) / open_p * 100
                    }
            else:
                data = yf.download(
                    batch, period="1d", interval="5m",
                    group_by="ticker", auto_adjust=True,
                    progress=False, threads=False
                )
                for ticker in batch:
                    try:
                        closes = data["Close"][ticker].dropna()
                        if len(closes) >= 2:
                            open_p  = float(closes.iloc[0])
                            close_p = float(closes.iloc[-1])
                            results[ticker] = {
                                "price": close_p,
                                "open": open_p,
                                "change_pct": (close_p - open_p) / open_p * 100
                            }
                    except Exception:
                        pass
        except Exception as e:
            log.error(f"Intraday batch error {batch}: {e}")
        time.sleep(1)
    return results

# ── NOTIZIE ──────────────────────────────────────────────────
def get_news(query, hours=4, max_articles=3):
    if not NEWS_API_KEY:
        return []
    from_time = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
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
    priority = ["NVDA", "MSFT", "ASML", "GOOGL", "META", "AMZN",
                "XOM", "CVX", "LMT", "JNJ", "LLY", "TSM"]
    news_by_ticker = {}
    for ticker in priority:
        name = TICKER_NAMES.get(ticker, ticker)
        articles = get_news(f'"{name}" stock dividend', hours=hours, max_articles=2)
        if articles:
            news_by_ticker[ticker] = articles
        time.sleep(0.5)
    return news_by_ticker

# ── CLAUDE ───────────────────────────────────────────────────
def analyze_with_claude(context):
    if not ANTHROPIC_API_KEY:
        return None
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    system = """Sei un analista finanziario senior specializzato in portafogli buy & hold dividend growth.
Il portafoglio ha 18 PIE: Tier1 Dividend Aristocrats (40%), Tier2 Quality Compounders (30%), Tier3 Low Volatility (20%), Tier4 Momentum (10%).
La filosofia e mantenere a lungo termine. Analizza solo se l evento cambia la tesi strutturale.
Rispondi in italiano, massimo 2-3 frasi concise."""
    try:
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            system=system,
            messages=[{"role": "user", "content": context}]
        )
        return msg.content[0].text
    except Exception as e:
        log.error(f"Claude API error: {e}")
        return None

# ── MONITOR PREZZI ───────────────────────────────────────────
def check_price_alerts():
    now = datetime.now(timezone.utc)
    log.info("Controllo prezzi in corso...")
    if not (MARKET_OPEN <= now.hour < MARKET_CLOSE):
        log.info("Fuori orario di mercato, skip")
        return

    prices = get_intraday_batch(US_TICKERS)
    critical = []

    for ticker, data in prices.items():
        change = data.get("change_pct", 0)
        if abs(change) >= ALERT_MOVE_PCT:
            hash_key = hashlib.md5(
                f"{ticker}_{round(change)}_{now.strftime('%Y%m%d%H')}".encode()
            ).hexdigest()
            if not alert_already_sent(hash_key):
                critical.append({
                    "ticker": ticker,
                    "name": TICKER_NAMES.get(ticker, ticker),
                    "change": change,
                    "price": data["price"],
                    "pie": TICKER_TO_PIE.get(ticker, ["N/A"])[0].replace("_", " ")
                })
                mark_alert_sent(hash_key, ticker, "price_alert")

    for a in critical:
        direction = "📈" if a["change"] > 0 else "📉"
        sign = "+" if a["change"] > 0 else ""
        context = (f"Il titolo {a['name']} ({a['ticker']}) ha avuto un movimento di "
                   f"{sign}{a['change']:.1f}% oggi. Fa parte del {a['pie']}. "
                   f"E rilevante per la tesi buy & hold dividend growth?")
        analysis = analyze_with_claude(context)
        msg = (
            f"🔴 <b>ALERT</b> — {now.strftime('%H:%M')} UTC\n\n"
            f"{direction} <b>{a['name']}</b> ({a['ticker']})\n"
            f"<b>{sign}{a['change']:.2f}%</b> · {a['price']:.2f}\n"
            f"<i>{a['pie']}</i>\n"
        )
        if analysis:
            msg += f"\n🤖 {analysis}"
        send_telegram(msg)
        time.sleep(2)

# ── MONITOR NOTIZIE ──────────────────────────────────────────
def check_news_alerts():
    log.info("Controllo notizie in corso...")
    news = get_portfolio_news(hours=1)
    keywords = ["dividend", "earnings", "revenue", "acquisition",
                "ceo", "downgrade", "upgrade", "guidance", "cut", "raise"]
    important = []
    for ticker, articles in news.items():
        for article in articles:
            if any(kw in article["title"].lower() for kw in keywords):
                hash_key = hashlib.md5(article["title"].encode()).hexdigest()
                if not alert_already_sent(hash_key, hours=12):
                    important.append({
                        "ticker": ticker,
                        "name": TICKER_NAMES.get(ticker, ticker),
                        "article": article,
                        "pie": TICKER_TO_PIE.get(ticker, ["N/A"])[0].replace("_", " ")
                    })
                    mark_alert_sent(hash_key, ticker, "news_alert")

    for item in important[:3]:
        context = (f"Notizia su {item['name']}: \"{item['article']['title']}\". "
                   f"E rilevante per la tesi di investimento?")
        analysis = analyze_with_claude(context)
        msg = (
            f"📰 <b>NOTIZIA</b>\n\n"
            f"<b>{item['name']}</b> ({item['ticker']})\n"
            f"{item['article']['title']}\n"
            f"<i>{item['article']['source']} · {item['pie']}</i>\n"
        )
        if analysis:
            msg += f"\n🤖 {analysis}"
        send_telegram(msg)
        time.sleep(2)

# ── DIGEST ORARIO ────────────────────────────────────────────
def send_hourly_digest():
    now = datetime.now(timezone.utc)
    if not (MARKET_OPEN <= now.hour < MARKET_CLOSE):
        return
    log.info("Invio digest orario...")
    prices = get_intraday_batch(US_TICKERS)
    movers = [(t, d) for t, d in prices.items()
              if abs(d.get("change_pct", 0)) >= DIGEST_MOVE_PCT]
    movers.sort(key=lambda x: abs(x[1]["change_pct"]), reverse=True)

    if not movers:
        send_telegram(
            f"🟢 <b>DIGEST {now.strftime('%H:%M')} UTC</b>\n"
            f"Tutto tranquillo — nessun movimento significativo."
        )
        return

    msg = f"🟡 <b>DIGEST {now.strftime('%H:%M')} UTC</b>\n\n"
    for ticker, data in movers[:8]:
        change = data["change_pct"]
        icon = "📈" if change > 0 else "📉"
        name = TICKER_NAMES.get(ticker, ticker)
        msg += f"{icon} <b>{name}</b> {'+' if change>0 else ''}{change:.1f}%\n"
    if len(movers) > 8:
        msg += f"\n<i>+{len(movers)-8} altri movimenti</i>"
    send_telegram(msg)

# ── REPORT SERALE ────────────────────────────────────────────
def send_evening_report():
    log.info("Invio report serale...")
    now = datetime.now(timezone.utc)
    sample = US_TICKERS[:25]
    prices = get_prices_batch(sample)

    gainers = sorted([(t,d) for t,d in prices.items() if d.get("change_pct",0)>0],
                     key=lambda x: x[1]["change_pct"], reverse=True)
    losers  = sorted([(t,d) for t,d in prices.items() if d.get("change_pct",0)<0],
                     key=lambda x: x[1]["change_pct"])

    top_up  = ", ".join([f"{TICKER_NAMES.get(t,t)} +{d['change_pct']:.1f}%" for t,d in gainers[:3]])
    top_dn  = ", ".join([f"{TICKER_NAMES.get(t,t)} {d['change_pct']:.1f}%"  for t,d in losers[:3]])
    context = (
        f"Report serale {now.strftime('%d/%m/%Y')}. "
        f"Top 3 su: {top_up}. "
        f"Top 3 giu: {top_dn}. "
        f"Valuta la giornata per il portafoglio buy & hold dividend growth e dai un consiglio."
    )
    analysis = analyze_with_claude(context)

    msg = f"🟢 <b>REPORT SERALE — {now.strftime('%d/%m/%Y')}</b>\n\n"
    if gainers:
        msg += "<b>Migliori:</b>\n"
        for t, d in gainers[:3]:
            msg += f"📈 {TICKER_NAMES.get(t,t)} +{d['change_pct']:.1f}%\n"
    if losers:
        msg += "\n<b>Peggiori:</b>\n"
        for t, d in losers[:3]:
            msg += f"📉 {TICKER_NAMES.get(t,t)} {d['change_pct']:.1f}%\n"
    if analysis:
        msg += f"\n🤖 {analysis}"
    msg += f"\n\n<i>Factor Portfolio · 18 PIE</i>"
    send_telegram(msg)

# ── REPORT SETTIMANALE ───────────────────────────────────────
def send_weekly_report():
    now = datetime.now(timezone.utc)
    if now.weekday() != 0:
        return
    log.info("Invio report settimanale...")
    context = ("Genera un breve outlook settimanale per un portafoglio Factor Portfolio "
               "dividend growth con 18 PIE globali. Cosa monitorare questa settimana? "
               "Earnings rilevanti? Consiglio per investitore buy & hold.")
    analysis = analyze_with_claude(context)
    msg = f"📊 <b>OUTLOOK SETTIMANALE — {now.strftime('%d/%m/%Y')}</b>\n\n"
    if analysis:
        msg += analysis
    msg += "\n\n<i>Buona settimana · Factor Portfolio</i>"
    send_telegram(msg)

# ── SCHEDULER ───────────────────────────────────────────────
def run_scheduler():
    schedule.every(1).minutes.do(check_price_alerts)
    schedule.every(30).minutes.do(check_news_alerts)
    for h in DIGEST_HOUR:
        schedule.every().day.at(f"{h:02d}:00").do(send_hourly_digest)
    schedule.every().day.at(f"{REPORT_HOUR:02d}:30").do(send_evening_report)
    schedule.every().monday.at("07:00").do(send_weekly_report)
    schedule.every().day.at("02:00").do(cleanup_old_alerts)

    log.info("Scheduler avviato. Monitoraggio in corso...")
    ok = send_telegram(
        f"🚀 <b>Portfolio Agent avviato</b>\n"
        f"Monitoraggio attivo su {len(ALL_TICKERS)} ticker · 18 PIE\n"
        f"🔴 Alert critici · 🟡 Digest orari · 🟢 Report serali"
    )
    if ok:
        log.info("Messaggio Telegram di avvio inviato con successo")

    while True:
        schedule.run_pending()
        time.sleep(30)

# ── MAIN ────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Portfolio Agent — Factor Portfolio 18 PIE")
    log.info(f"Ticker monitorati: {len(ALL_TICKERS)}")
    log.info(f"Ticker US: {len(US_TICKERS)}")

    # DEBUG variabili d ambiente
    token = os.environ.get("TELEGRAM_TOKEN", "")
    chat  = os.environ.get("TELEGRAM_CHAT_ID", "")
    log.info(f"TELEGRAM_TOKEN lunghezza: {len(token)} | inizio: {repr(token[:15])}")
    log.info(f"TELEGRAM_CHAT_ID: {repr(chat)} | lunghezza: {len(chat)}")
    log.info(f"ANTHROPIC lunghezza: {len(os.environ.get('ANTHROPIC_API_KEY',''))}")
    log.info(f"NEWSAPI lunghezza: {len(os.environ.get('NEWS_API_KEY',''))}")

    init_db()
    run_scheduler()
