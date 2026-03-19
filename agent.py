"""
Portfolio Agent v2 — Factor Portfolio 18 PIE
Monitoraggio real-time + Ribilanciamento + Sostituzioni + Raccomandazioni operative
"""

import os
import time
import json
import logging
import hashlib
import sqlite3
from datetime import datetime, timezone, timedelta
import schedule
import requests
import yfinance as yf
import anthropic

# ── Configura yfinance con headers browser reali ─────────────
# Yahoo Finance blocca richieste automatizzate senza User-Agent
# Questi header simulano Chrome su Windows — massima compatibilità
_YF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

try:
    import yfinance.data as _yfdata
    _yfdata.YfData.HEADERS = _YF_HEADERS
except Exception:
    pass

# Applica anche via requests Session su yfinance
def _setup_yf_session():
    try:
        session = requests.Session()
        session.headers.update(_YF_HEADERS)
        yf.set_tz_cache_location("/tmp/yf_cache")
        return session
    except Exception:
        return None

_YF_SESSION = _setup_yf_session()

# ── CONFIG ───────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")
NEWS_API_KEY      = os.environ.get("NEWS_API_KEY", "")

ALERT_MOVE_PCT    = 3.0
DIGEST_MOVE_PCT   = 1.5
MARKET_OPEN       = 0   # Tokyo apre 00:00 UTC
MARKET_CLOSE      = 23  # Copertura completa tutti i mercati mondiali
DIGEST_HOUR       = [10, 12, 14, 16]
REPORT_HOUR       = 18
BATCH_SIZE        = 10
GITHUB_TOKEN      = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO       = os.environ.get("GITHUB_REPO", "portfolio-agent")
GITHUB_USERNAME   = os.environ.get("GITHUB_USERNAME", "")
PORTFOLIO_FILE    = "portfolio.json"

# Soglie ribilanciamento
REBALANCE_THRESHOLD = 5.0   # % di deviazione dal peso target che triggera raccomandazione
DRAWDOWN_ALERT      = 15.0  # % drawdown dal massimo che triggera watchlist sostituzione
DIVIDEND_CUT_DAYS   = 30    # giorni per verificare tagli dividendo

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# ── PORTAFOGLIO — 18 PIE con pesi target ────────────────────
PORTFOLIO = {
    "PIE01_Aristocrats_USA": {
        "tier": 1, "peso_target": 8.0,
        "tickers": ["PG", "JNJ", "KO", "PEP", "ABT", "MDT", "WMT", "EMR"]
    },
    "PIE02_Aristocrats_EU": {
        "tier": 1, "peso_target": 7.0,
        "tickers": ["AI.PA", "NESN.SW", "OR.PA", "SIKA.SW", "WKL.AS", "DSY.PA", "LIN"]
    },
    "PIE03_Aristocrats_Asia": {
        "tier": 1, "peso_target": 7.0,
        "tickers": ["D05.SI", "7203.T", "6758.T", "CBA.AX"]
    },
    "PIE04_Champions_Energia": {
        "tier": 1, "peso_target": 6.0,
        "tickers": ["XOM", "CVX", "WMB", "ENB", "TTE", "EOG", "CEG"]
    },
    "PIE05_Champions_Finanza": {
        "tier": 1, "peso_target": 6.0,
        "tickers": ["HSBA.L", "CS.PA", "ALV.DE", "AIG", "UCG.MI", "BNP.PA", "MQG.AX"]
    },
    "PIE06_REIT_Growth": {
        "tier": 1, "peso_target": 6.0,
        "tickers": ["O", "PLD", "AMT", "EQIX", "WPC", "AWK"]
    },
    "PIE07_Quality_Tech": {
        "tier": 2, "peso_target": 6.0,
        "tickers": ["ASML", "MSFT", "TXN", "AAPL", "SAP", "AVGO"]
    },
    "PIE08_Quality_Lusso": {
        "tier": 2, "peso_target": 6.0,
        "tickers": ["MC.PA", "RMS.PA", "CFR.SW", "RACE", "MONC.MI", "EL"]
    },
    "PIE09_Quality_Healthcare": {
        "tier": 2, "peso_target": 5.0,
        "tickers": ["JNJ", "LLY", "NOVOB.CO", "AZN", "TMO", "ROG.SW", "UNH"]
    },
    "PIE10_Quality_Difesa": {
        "tier": 2, "peso_target": 5.0,
        "tickers": ["LMT", "NOC", "RHM.DE", "BAESY", "AIR.PA", "BWXT", "GD"]
    },
    "PIE11_Quality_Chip": {
        "tier": 2, "peso_target": 4.0,
        "tickers": ["TSM", "005930.KS", "000660.KS", "QCOM", "8035.T"]
    },
    "PIE12_Quality_Infrastrutture": {
        "tier": 2, "peso_target": 4.0,
        "tickers": ["BIP", "DG.PA", "FER.MC", "GET.PA", "ATNI.MI"]
    },
    "PIE13_Utility_Nucleare": {
        "tier": 3, "peso_target": 6.0,
        "tickers": ["CEG", "ENEL.MI", "IBE.MC", "ETR", "SRG.MI", "TRN.MI", "D"]
    },
    "PIE14_Consumer_Staples": {
        "tier": 3, "peso_target": 5.0,
        "tickers": ["PG", "KO", "PEP", "ULVR.L", "COST"]
    },
    "PIE15_Gas_Industriali": {
        "tier": 3, "peso_target": 5.0,
        "tickers": ["AI.PA", "LIN", "SIKA.SW", "SHW", "APD"]
    },
    "PIE16_Midstream_Pipeline": {
        "tier": 3, "peso_target": 4.0,
        "tickers": ["WMB", "ENB", "KMI", "TRP", "PPL"]
    },
    "PIE17_AI_Tech": {
        "tier": 4, "peso_target": 6.0,
        "tickers": ["NVDA", "GOOGL", "META", "AMZN", "AMD", "KWEB"]
    },
    "PIE18_EM_Growth": {
        "tier": 4, "peso_target": 4.0,
        "tickers": ["INFY", "HDB", "ITUB", "VALE", "IBN", "KWEB", "RELIANCE.NS"]
    },
}

ALL_TICKERS = list(set(t for pie in PORTFOLIO.values() for t in pie["tickers"]))
US_TICKERS  = [t for t in ALL_TICKERS if "." not in t]

TICKER_TO_PIE = {}
for pie_name, pie_data in PORTFOLIO.items():
    for t in pie_data["tickers"]:
        TICKER_TO_PIE.setdefault(t, []).append(pie_name)

TICKER_NAMES = {
    "PG":"Procter & Gamble","JNJ":"Johnson & Johnson","KO":"Coca-Cola",
    "PEP":"PepsiCo","ABT":"Abbott","MDT":"Medtronic","WMT":"Walmart",
    "EMR":"Emerson Electric","LIN":"Linde","AI.PA":"Air Liquide",
    "NESN.SW":"Nestle","OR.PA":"L Oreal","SIKA.SW":"Sika","WKL.AS":"Wolters Kluwer",
    "DSY.PA":"Dassault Systemes","D05.SI":"DBS Group","7203.T":"Toyota",
    "6758.T":"Sony Group","CBA.AX":"Commonwealth Bank","XOM":"ExxonMobil",
    "CVX":"Chevron","WMB":"Williams Companies","ENB":"Enbridge",
    "TTE":"TotalEnergies","EOG":"EOG Resources","CEG":"Constellation Energy",
    "HSBA.L":"HSBC","CS.PA":"AXA","ALV.DE":"Allianz","AIG":"AIG",
    "UCG.MI":"UniCredit","BNP.PA":"BNP Paribas","MQG.AX":"Macquarie",
    "O":"Realty Income","PLD":"Prologis","AMT":"American Tower","EQIX":"Equinix",
    "WPC":"WP Carey","AWK":"American Water","ASML":"ASML","MSFT":"Microsoft",
    "TXN":"Texas Instruments","AAPL":"Apple","SAP":"SAP","AVGO":"Broadcom",
    "MC.PA":"LVMH","RMS.PA":"Hermes","CFR.SW":"Richemont","RACE":"Ferrari",
    "MONC.MI":"Moncler","EL":"Estee Lauder","LLY":"Eli Lilly",
    "NOVOB.CO":"Novo Nordisk","AZN":"AstraZeneca","TMO":"Thermo Fisher",
    "ROG.SW":"Roche","UNH":"UnitedHealth","LMT":"Lockheed Martin",
    "NOC":"Northrop Grumman","RHM.DE":"Rheinmetall","BAESY":"BAE Systems",
    "AIR.PA":"Airbus","BWXT":"BWX Technologies","GD":"General Dynamics",
    "TSM":"TSMC","005930.KS":"Samsung","000660.KS":"SK Hynix",
    "QCOM":"Qualcomm","8035.T":"Tokyo Electron","BIP":"Brookfield Infrastructure",
    "DG.PA":"Vinci","FER.MC":"Ferrovial","GET.PA":"Getlink","ATNI.MI":"Atlantia",
    "ENEL.MI":"Enel","IBE.MC":"Iberdrola","ETR":"Entergy","SRG.MI":"Snam",
    "TRN.MI":"Terna","D":"Dominion Energy","ULVR.L":"Unilever","COST":"Costco",
    "SHW":"Sherwin-Williams","APD":"Air Products","KMI":"Kinder Morgan",
    "TRP":"TC Energy","PPL":"PPL Corporation","NVDA":"NVIDIA","GOOGL":"Alphabet",
    "META":"Meta","AMZN":"Amazon","AMD":"AMD","KWEB":"China Internet ETF",
    "INFY":"Infosys","HDB":"HDFC Bank","ITUB":"Itau Unibanco","VALE":"Vale",
    "IBN":"ICICI Bank","RELIANCE.NS":"Reliance Industries",
}

# Soglie drawdown dinamiche per watchlist automatica — tutti i ticker
# Tier 1 e 2 (core): soglia piu alta perche sono posizioni principali
# Tier 3 (low vol): soglia intermedia — utility e staples non dovrebbero scendere molto
# Tier 4 (momentum): soglia piu bassa — piu volatili per natura
DRAWDOWN_THRESHOLDS = {
    1: -15.0,   # Dividend Aristocrats — alert a -15% (streak 25-68 anni, raramente scendono cosi)
    2: -20.0,   # Quality Compounders — alert a -20% (beta piu alto, oscillazioni normali fino -15%)
    3: -10.0,   # Low Volatility — alert a -10% (utility/staples/pipeline: devono essere stabili)
    4: -30.0,   # Momentum Growth — alert a -30% (NVDA/META/AMD: -25% e correzione normale)
}

# ── DATABASE ─────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS sent_alerts (
        hash TEXT PRIMARY KEY, ticker TEXT, alert_type TEXT, created_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS pending_recommendations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rec_type TEXT, ticker TEXT, pie TEXT,
        action TEXT, details TEXT,
        created_at TEXT, status TEXT DEFAULT 'pending')""")
    c.execute("""CREATE TABLE IF NOT EXISTS price_history (
        ticker TEXT, price REAL, date TEXT,
        PRIMARY KEY (ticker, date))""")
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

def save_recommendation(rec_type, ticker, pie, action, details):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("INSERT INTO pending_recommendations (rec_type,ticker,pie,action,details,created_at) VALUES (?,?,?,?,?,?)",
              (rec_type, ticker, pie, action, details, datetime.now(timezone.utc).isoformat()))
    rec_id = c.lastrowid
    conn.commit()
    conn.close()
    return rec_id

def update_recommendation_status(rec_id, status):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("UPDATE pending_recommendations SET status=? WHERE id=?", (status, rec_id))
    conn.commit()
    conn.close()

def save_price(ticker, price):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    c.execute("INSERT OR REPLACE INTO price_history VALUES (?,?,?)", (ticker, price, today))
    conn.commit()
    conn.close()

def get_max_price(ticker, days=90):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    c.execute("SELECT MAX(price) FROM price_history WHERE ticker=? AND date>?", (ticker, cutoff))
    result = c.fetchone()[0]
    conn.close()
    return result

def cleanup_old_data():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    cutoff_alerts = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cutoff_prices = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM sent_alerts WHERE created_at<?", (cutoff_alerts,))
    c.execute("DELETE FROM price_history WHERE date<?", (cutoff_prices,))
    # Pulisci raccomandazioni risolte piu vecchie di 7 giorni
    cutoff_recs = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    c.execute("DELETE FROM pending_recommendations WHERE status!='pending' AND created_at<?", (cutoff_recs,))
    conn.commit()
    conn.close()


# ── GITHUB — lettura e scrittura portfolio.json ──────────────
def load_portfolio_from_github():
    """Carica portfolio.json dal repository GitHub"""
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        log.info("GitHub non configurato — uso portfolio hardcoded")
        return None
    url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{PORTFOLIO_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.ok:
            import base64
            data = r.json()
            content_decoded = base64.b64decode(data["content"]).decode("utf-8")
            portfolio_data = json.loads(content_decoded)
            log.info(f"Portfolio caricato da GitHub: {len(portfolio_data['pies'])} PIE")
            return portfolio_data, data["sha"]
        else:
            log.error(f"GitHub read error: {r.status_code}")
    except Exception as e:
        log.error(f"GitHub load error: {e}")
    return None, None

def save_portfolio_to_github(portfolio_data, sha):
    """Salva portfolio.json aggiornato su GitHub"""
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        log.warning("GitHub non configurato — impossibile salvare")
        return False
    import base64
    from datetime import datetime, timezone
    portfolio_data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    content_str = json.dumps(portfolio_data, indent=2, ensure_ascii=False)
    content_b64 = base64.b64encode(content_str.encode("utf-8")).decode("utf-8")
    url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{PORTFOLIO_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    payload = {
        "message": f"Portfolio aggiornato automaticamente {portfolio_data['last_updated']}",
        "content": content_b64,
        "sha": sha
    }
    try:
        r = requests.put(url, headers=headers, json=payload, timeout=15)
        if r.ok:
            log.info("Portfolio salvato su GitHub con successo")
            return True
        else:
            log.error(f"GitHub write error: {r.status_code} {r.text}")
    except Exception as e:
        log.error(f"GitHub save error: {e}")
    return False

def update_portfolio_ticker(pie_name, old_ticker, new_ticker, new_name):
    """Sostituisce un ticker nel portfolio.json e aggiorna GitHub"""
    result = load_portfolio_from_github()
    if not result or not result[0]:
        log.warning("Impossibile aggiornare — GitHub non disponibile")
        return False
    portfolio_data, sha = result
    if pie_name not in portfolio_data["pies"]:
        log.error(f"PIE {pie_name} non trovato")
        return False
    tickers = portfolio_data["pies"][pie_name]["tickers"]
    if old_ticker in tickers:
        idx = tickers.index(old_ticker)
        tickers[idx] = new_ticker
        portfolio_data["pies"][pie_name]["tickers"] = tickers
        if new_name:
            portfolio_data["ticker_names"][new_ticker] = new_name
        if old_ticker in portfolio_data["ticker_names"]:
            del portfolio_data["ticker_names"][old_ticker]
        if save_portfolio_to_github(portfolio_data, sha):
            # Ricarica il portafoglio in memoria
            reload_portfolio(portfolio_data)
            return True
    return False

def add_ticker_to_pie(pie_name, ticker, name):
    """Aggiunge un nuovo ticker a un PIE esistente"""
    result = load_portfolio_from_github()
    if not result or not result[0]:
        return False
    portfolio_data, sha = result
    if pie_name not in portfolio_data["pies"]:
        return False
    if ticker not in portfolio_data["pies"][pie_name]["tickers"]:
        portfolio_data["pies"][pie_name]["tickers"].append(ticker)
        portfolio_data["ticker_names"][ticker] = name
        if save_portfolio_to_github(portfolio_data, sha):
            reload_portfolio(portfolio_data)
            return True
    return False

def remove_ticker_from_pie(pie_name, ticker):
    """Rimuove un ticker da un PIE"""
    result = load_portfolio_from_github()
    if not result or not result[0]:
        return False
    portfolio_data, sha = result
    if pie_name not in portfolio_data["pies"]:
        return False
    tickers = portfolio_data["pies"][pie_name]["tickers"]
    if ticker in tickers:
        tickers.remove(ticker)
        portfolio_data["pies"][pie_name]["tickers"] = tickers
        if save_portfolio_to_github(portfolio_data, sha):
            reload_portfolio(portfolio_data)
            return True
    return False

def reload_portfolio(portfolio_data):
    """Aggiorna le variabili globali con il nuovo portafoglio"""
    global PORTFOLIO, ALL_TICKERS, US_TICKERS, TICKER_TO_PIE, TICKER_NAMES, PIE_WEIGHTS
    PORTFOLIO = {}
    for pie_name, pie_data in portfolio_data["pies"].items():
        PORTFOLIO[pie_name] = {
            "tier": pie_data["tier"],
            "peso_target": pie_data["peso_target"],
            "tickers": pie_data["tickers"]
        }
    TICKER_NAMES.update(portfolio_data.get("ticker_names", {}))
    ALL_TICKERS = list(set(t for pie in PORTFOLIO.values() for t in pie["tickers"]))
    US_TICKERS  = [t for t in ALL_TICKERS if "." not in t]
    TICKER_TO_PIE = {}
    for pie_name, pie_data in PORTFOLIO.items():
        for t in pie_data["tickers"]:
            TICKER_TO_PIE.setdefault(t, []).append(pie_name)
    PIE_WEIGHTS = {pie: data["peso_target"] for pie, data in PORTFOLIO.items()}
    log.info(f"Portfolio ricaricato: {len(ALL_TICKERS)} ticker unici, {len(US_TICKERS)} US")


# ── TELEGRAM ─────────────────────────────────────────────────
def send_telegram(message, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram non configurato")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message,
                "parse_mode": parse_mode, "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.ok:
            return True
        log.error(f"Telegram error: {r.text}")
    except Exception as e:
        log.error(f"Telegram exception: {e}")
    return False

def send_telegram_with_buttons(message, buttons):
    """Invia messaggio con bottoni inline Approva/Rifiuta"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    keyboard = {"inline_keyboard": [[
        {"text": btn["text"], "callback_data": btn["data"]}
        for btn in row
    ] for row in buttons]}
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message,
                "parse_mode": "HTML", "reply_markup": keyboard,
                "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=10)
        return r.ok
    except Exception as e:
        log.error(f"Telegram buttons error: {e}")
    return False

def handle_callback(callback_data, message_id):
    """Gestisce la risposta ai bottoni Approva/Rifiuta"""
    parts = callback_data.split("_")
    action = parts[0]  # approve / reject
    rec_id = int(parts[1]) if len(parts) > 1 else None

    if not rec_id:
        return

    if action == "approve":
        update_recommendation_status(rec_id, "approved")
        # Recupera dettagli raccomandazione
        conn = sqlite3.connect("agent_state.db")
        c = conn.cursor()
        c.execute("SELECT rec_type,ticker,pie,action,details FROM pending_recommendations WHERE id=?", (rec_id,))
        row = c.fetchone()
        conn.close()
        if row:
            rec_type, ticker, pie, action_type, details = row
            send_telegram(
                f"✅ <b>RACCOMANDAZIONE APPROVATA</b>\n\n"
                f"<b>Azione:</b> {action_type}\n"
                f"<b>Titolo:</b> {TICKER_NAMES.get(ticker, ticker)} ({ticker})\n"
                f"<b>PIE:</b> {pie.replace('_',' ')}\n\n"
                f"<b>Istruzioni per T212:</b>\n{details}\n\n"
                f"<i>Esegui manualmente su Trading 212 quando sei pronto.</i>"
            )
    elif action == "reject":
        update_recommendation_status(rec_id, "rejected")
        send_telegram(f"❌ Raccomandazione #{rec_id} rifiutata. Nessuna azione necessaria.")

def check_callbacks():
    """Polling per i callback dei bottoni Telegram"""
    if not TELEGRAM_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    try:
        r = requests.get(url, params={"timeout": 1, "allowed_updates": ["callback_query"]}, timeout=5)
        if not r.ok:
            return
        updates = r.json().get("result", [])
        last_update_id = None
        for update in updates:
            last_update_id = update["update_id"]
            if "callback_query" in update:
                cq = update["callback_query"]
                handle_callback(cq["data"], cq["message"]["message_id"])
                # Risponde al callback per togliere il loading
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                    json={"callback_query_id": cq["id"]}, timeout=5
                )
        # Marca gli update come letti
        if last_update_id:
            requests.get(url, params={"offset": last_update_id + 1, "limit": 1}, timeout=5)
    except Exception as e:
        log.error(f"Callback check error: {e}")

# ── PREZZI ───────────────────────────────────────────────────
def get_prices_batch(tickers):
    """Scarica prezzi in UNA sola chiamata bulk — anti rate-limiting."""
    results = {}
    if not tickers:
        return results
    try:
        data = yf.download(
            tickers, period="3d", interval="1d",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=False,
            session=_YF_SESSION
        )
        if data.empty:
            return results
        for ticker in tickers:
            try:
                closes = data["Close"][ticker].dropna() if len(tickers) > 1 else data["Close"].dropna()
                if len(closes) >= 2:
                    p_curr = float(closes.iloc[-1])
                    p_prev = float(closes.iloc[-2])
                    if p_prev > 0:
                        results[ticker] = {
                            "price":      p_curr,
                            "prev_close": p_prev,
                            "change_pct": (p_curr - p_prev) / p_prev * 100
                        }
            except Exception:
                pass
    except Exception as e:
        log.error(f"get_prices_batch error: {e}")
        # Fallback in mini-batch da 20
        for i in range(0, len(tickers), 20):
            batch = tickers[i:i+20]
            try:
                data = yf.download(batch, period="3d", interval="1d",
                                   group_by="ticker", auto_adjust=True,
                                   progress=False, threads=False,
                                   session=_YF_SESSION)
                for ticker in batch:
                    try:
                        closes = data["Close"][ticker].dropna() if len(batch)>1 else data["Close"].dropna()
                        if len(closes) >= 2:
                            p_curr = float(closes.iloc[-1])
                            p_prev = float(closes.iloc[-2])
                            if p_prev > 0:
                                results[ticker] = {
                                    "price": p_curr, "prev_close": p_prev,
                                    "change_pct": (p_curr - p_prev) / p_prev * 100
                                }
                    except Exception:
                        pass
            except Exception as e2:
                log.error(f"Fallback error: {e2}")
            time.sleep(3)
    return results
def get_intraday_batch(tickers):
    """Scarica prezzi intraday in UNA sola chiamata bulk.
    Usa period=1d interval=1h per bilanciare granularita e rate limiting.
    Meno richieste = meno blocchi da Yahoo Finance."""
    results = {}
    if not tickers:
        return results
    try:
        # Una sola chiamata per tutti i ticker
        data = yf.download(
            tickers, period="1d", interval="1h",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=False,
            session=_YF_SESSION
        )
        if data.empty:
            # Fallback a dati giornalieri se intraday non disponibile
            return get_prices_batch(tickers)
        for ticker in tickers:
            try:
                closes = data["Close"][ticker].dropna() if len(tickers)>1 else data["Close"].dropna()
                if len(closes) >= 2:
                    op = float(closes.iloc[0])
                    cp = float(closes.iloc[-1])
                    if op > 0:
                        results[ticker] = {
                            "price":      cp,
                            "open":       op,
                            "change_pct": (cp - op) / op * 100
                        }
            except Exception:
                pass
    except Exception as e:
        log.error(f"get_intraday_batch error: {e}")
        # Fallback ai dati giornalieri
        return get_prices_batch(tickers)
    return results

# ── NOTIZIE ──────────────────────────────────────────────────
def get_news(query, hours=4, max_articles=3):
    if not NEWS_API_KEY:
        return []
    from_time = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        r = requests.get("https://newsapi.org/v2/everything", params={
            "q": query, "from": from_time, "sortBy": "publishedAt",
            "language": "en", "pageSize": max_articles, "apiKey": NEWS_API_KEY
        }, timeout=10)
        if r.ok:
            return [{"title": a["title"], "source": a["source"]["name"],
                     "url": a["url"]} for a in r.json().get("articles", [])]
    except Exception as e:
        log.error(f"NewsAPI error: {e}")
    return []

def get_portfolio_news(hours=8, tier_filter=None):
    """Cerca notizie per TUTTI i titoli di ogni PIE.
    2 query per PIE (4 ticker per query) = 35 query per scansione completa.
    tier_filter: se impostato, scansiona solo i PIE di quel tier (es. [1,2]).
    Budget: 35+35+23 = 93 richieste/giorno su 100 disponibili."""
    news_by_ticker = {}

    for pie_name, pie_data in PORTFOLIO.items():
        # Filtra per tier se richiesto
        if tier_filter and pie_data.get("tier") not in tier_filter:
            continue

        tickers = pie_data["tickers"]

        # Suddividi i ticker del PIE in gruppi da 4
        for i in range(0, len(tickers), 4):
            group   = tickers[i:i+4]
            names   = [TICKER_NAMES.get(t, "") for t in group if TICKER_NAMES.get(t, "")]
            if not names:
                continue

            # Query: "Air Liquide" OR "Nestle" OR "L Oreal" OR "Sika" stock
            query    = " OR ".join([f'"{n}"' for n in names]) + " stock"
            articles = get_news(query, hours=hours, max_articles=5)

            if articles:
                for article in articles:
                    title_lower = article["title"].lower()
                    matched     = False
                    # Associa al ticker corretto cercando il nome nel titolo
                    for ticker in group:
                        name = TICKER_NAMES.get(ticker, "").lower()
                        if name and any(w in title_lower for w in name.split()):
                            news_by_ticker.setdefault(ticker, []).append(article)
                            matched = True
                            break
                    if not matched:
                        news_by_ticker.setdefault(group[0], []).append(article)
            time.sleep(1)

    return news_by_ticker

# ── CLAUDE ───────────────────────────────────────────────────
def ask_claude(prompt, max_tokens=400):
    if not ANTHROPIC_API_KEY:
        return None
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    system = """Sei un analista finanziario senior specializzato in portafogli buy & hold dividend growth.
Il portafoglio ha 18 PIE: Tier1 Dividend Aristocrats (40%), Tier2 Quality Compounders (30%), Tier3 Low Volatility (20%), Tier4 Momentum (10%).
Filosofia: mantenere a lungo termine. Raccomanda azioni solo se cambiano la tesi strutturale.
Rispondi in italiano, conciso e diretto. Per raccomandazioni operative sii preciso sui numeri."""
    try:
        msg = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=max_tokens,
            system=system, messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text
    except Exception as e:
        log.error(f"Claude error: {e}")
        return None

# ── MODULO 1 — MONITOR PREZZI ────────────────────────────────
def check_price_alerts():
    now = datetime.now(timezone.utc)
    if not (MARKET_OPEN <= now.hour < MARKET_CLOSE):
        return
    log.info("Check prezzi tutti i ticker...")
    # Pausa casuale 2-5 secondi prima della richiesta — anti-fingerprinting
    time.sleep(2 + (now.second % 3))
    prices = get_intraday_batch(ALL_TICKERS)

    for ticker, data in prices.items():
        price = data.get("price", 0)
        if price > 0:
            save_price(ticker, price)
        change = data.get("change_pct", 0)
        if abs(change) >= ALERT_MOVE_PCT:
            hash_key = hashlib.md5(
                f"{ticker}_{round(change)}_{now.strftime('%Y%m%d%H')}".encode()
            ).hexdigest()
            if not alert_already_sent(hash_key):
                mark_alert_sent(hash_key, ticker, "price")
                send_price_alert(ticker, data, now)

def send_price_alert(ticker, data, now):
    change = data["change_pct"]
    icon   = "📈" if change > 0 else "📉"
    sign   = "+" if change > 0 else ""
    name   = TICKER_NAMES.get(ticker, ticker)
    pie    = TICKER_TO_PIE.get(ticker, ["N/A"])[0].replace("_", " ")

    analysis = ask_claude(
        f"Il titolo {name} ({ticker}) ha mosso {sign}{change:.1f}% oggi. "
        f"Fa parte del {pie}. La tesi buy & hold dividend growth e ancora valida? "
        f"Se suggerisci un azione operativa, sii preciso."
    )
    msg = (f"🔴 <b>ALERT</b> — {now.strftime('%H:%M')} UTC\n\n"
           f"{icon} <b>{name}</b> ({ticker})\n"
           f"<b>{sign}{change:.2f}%</b> · {data['price']:.2f}\n"
           f"<i>{pie}</i>\n")
    if analysis:
        msg += f"\n🤖 {analysis}"
    send_telegram(msg)

# ── MODULO 2 — MOTORE RIBILANCIAMENTO ────────────────────────
def run_rebalancing_check():
    """Controlla ogni lunedi se i pesi si sono spostati oltre la soglia"""
    log.info("Check ribilanciamento settimanale...")
    now = datetime.now(timezone.utc)

    # Ottieni prezzi per calcolare i pesi attuali
    sample_us = US_TICKERS[:30]
    prices    = get_prices_batch(sample_us)

    if not prices:
        return

    # Calcola performance per PIE (approssimazione con ticker US)
    pie_performance = {}
    for pie_name, pie_data in PORTFOLIO.items():
        us_tickers_in_pie = [t for t in pie_data["tickers"] if t in prices]
        if not us_tickers_in_pie:
            continue
        avg_change = sum(prices[t]["change_pct"] for t in us_tickers_in_pie) / len(us_tickers_in_pie)
        pie_performance[pie_name] = {
            "change": avg_change,
            "target": pie_data["peso_target"]
        }

    # Trova PIE che si sono spostati significativamente
    rebalance_needed = []
    for pie_name, perf in pie_performance.items():
        if abs(perf["change"]) >= REBALANCE_THRESHOLD:
            rebalance_needed.append({
                "pie": pie_name,
                "change": perf["change"],
                "target": perf["target"]
            })

    if rebalance_needed:
        send_rebalancing_recommendation(rebalance_needed, now)

def send_rebalancing_recommendation(pies, now):
    """Genera e invia raccomandazione di ribilanciamento con bottoni"""
    context = (
        f"Analisi ribilanciamento settimanale portafoglio Factor Portfolio 18 PIE.\n"
        f"PIE con deviazione significativa dal peso target:\n"
        + "\n".join([f"- {p['pie'].replace('_',' ')}: cambio {p['change']:+.1f}% vs target {p['target']}%" 
                     for p in pies])
        + "\n\nGenera una raccomandazione operativa precisa: quale PIE sovrappesare, "
          "quale ridurre, di quanto percentualmente. Sii specifico."
    )
    analysis = ask_claude(context, max_tokens=500)
    if not analysis:
        return

    # Salva la raccomandazione nel DB
    details = (
        "Su Trading 212:\n"
        "1. Vai nel PIE indicato\n"
        "2. Modifica le % di allocazione come suggerito\n"
        "3. Conferma il ribilanciamento automatico di T212"
    )
    rec_id = save_recommendation("rebalance", "PORTFOLIO", "TUTTI I PIE",
                                  "RIBILANCIA", details)

    msg = (
        f"⚖️ <b>RACCOMANDAZIONE RIBILANCIAMENTO</b>\n"
        f"{now.strftime('%d/%m/%Y')}\n\n"
        f"🤖 {analysis}\n\n"
        f"<i>Vuoi procedere con il ribilanciamento su Trading 212?</i>"
    )
    buttons = [[
        {"text": "✅ Approva — vedo le istruzioni", "data": f"approve_{rec_id}"},
        {"text": "❌ Rifiuta", "data": f"reject_{rec_id}"}
    ]]
    send_telegram_with_buttons(msg, buttons)

# ── MODULO 3 — WATCHLIST SOSTITUZIONE ────────────────────────

def send_substitution_recommendation(ticker, watch_data, drawdown, price):
    """Genera raccomandazione di sostituzione con bottoni"""
    name     = TICKER_NAMES.get(ticker, ticker)
    alt      = watch_data["alternativa"]
    alt_name = watch_data["alt_name"]
    pie      = TICKER_TO_PIE.get(ticker, ["N/A"])[0].replace("_", " ")

    context = (
        f"Il titolo {name} ({ticker}) ha un drawdown del {drawdown:.1f}% dai massimi degli ultimi 90 giorni. "
        f"Motivo monitoraggio: {watch_data['motivo']}. "
        f"Alternativa suggerita: {alt_name} ({alt}). "
        f"Fa parte del {pie}. "
        f"Raccomandi la sostituzione? Analizza la tesi e dai istruzioni operative precise se si."
    )
    analysis = ask_claude(context, max_tokens=400)
    if not analysis:
        return

    details = (
        f"Su Trading 212 — PIE: {pie}\n"
        f"1. Riduci {name} ({ticker}) al minimo o elimina\n"
        f"2. Aggiungi {alt_name} ({alt}) con il peso liberato\n"
        f"3. Ribilancia il PIE per mantenere il peso target"
    )
    rec_id = save_recommendation("substitution", ticker, pie,
                                  f"SOSTITUISCI {ticker} con {alt}", details)

    msg = (
        f"🔄 <b>SOSTITUZIONE CONSIGLIATA</b>\n\n"
        f"📉 <b>{name}</b> ({ticker})\n"
        f"Drawdown: <b>{drawdown:.1f}%</b> dai massimi 90gg\n"
        f"<i>{watch_data['motivo']}</i>\n\n"
        f"➡️ Alternativa: <b>{alt_name}</b> ({alt})\n\n"
        f"🤖 {analysis}\n\n"
        f"<i>Procedi con la sostituzione su Trading 212?</i>"
    )
    buttons = [[
        {"text": f"✅ Sì — sostituisci con {alt}", "data": f"approve_{rec_id}"},
        {"text": "❌ No — mantieni", "data": f"reject_{rec_id}"}
    ]]
    send_telegram_with_buttons(msg, buttons)

# ── MODULO 4 — RACCOMANDAZIONI OPERATIVE DA NOTIZIE ──────────
def check_news_and_recommend(tier_filter=None):
    """Analizza notizie e genera raccomandazioni operative se necessario.
    tier_filter=None -> scansione completa tutti i PIE (35 query)
    tier_filter=[1,2] -> solo tier 1 e 2 (23 query) per la scansione serale"""
    label = f"tier {tier_filter}" if tier_filter else "completo"
    log.info(f"Check notizie {label}...")
    news = get_portfolio_news(hours=8, tier_filter=tier_filter)
    keywords_critical = ["dividend cut", "dividend suspended", "bankruptcy",
                         "fraud", "ceo resign", "profit warning", "downgrade",
                         "miss", "guidance cut", "taglio dividendo"]

    critical_count = 0
    normal_msgs    = []

    for ticker, articles in news.items():
        for article in articles:
            title_lower = article["title"].lower()
            is_critical = any(kw in title_lower for kw in keywords_critical)
            hash_key    = hashlib.md5(article["title"].encode()).hexdigest()
            if alert_already_sent(hash_key, hours=12):
                continue
            mark_alert_sent(hash_key, ticker, "news")

            if is_critical:
                # Alert critici: invia subito uno per uno (max 5 per ciclo)
                if critical_count < 5:
                    send_critical_news_recommendation(ticker, article)
                    critical_count += 1
                    time.sleep(2)
            else:
                # Notizie normali: raggruppa in un digest per evitare spam
                name = TICKER_NAMES.get(ticker, ticker)
                pie  = TICKER_TO_PIE.get(ticker, ["N/A"])[0].replace("_", " ")
                normal_msgs.append(f"• <b>{name}</b>: {article['title'][:70]}...")

    # Invia digest notizie normali (max 10 per messaggio)
    if normal_msgs:
        for i in range(0, min(len(normal_msgs), 20), 10):
            batch = normal_msgs[i:i+10]
            send_telegram(
                "📰 <b>NOTIZIE PORTAFOGLIO</b>\n\n"
                + "\n".join(batch)
            )
            time.sleep(2)

def send_critical_news_recommendation(ticker, article):
    """Per notizie critiche genera raccomandazione operativa"""
    name = TICKER_NAMES.get(ticker, ticker)
    pie  = TICKER_TO_PIE.get(ticker, ["N/A"])[0].replace("_", " ")

    context = (
        f"NOTIZIA CRITICA su {name} ({ticker}) nel {pie}:\n"
        f"\"{article['title']}\"\n\n"
        f"Analizza l impatto sulla tesi buy & hold dividend growth. "
        f"Se raccomandi di vendere o ridurre la posizione, dai istruzioni operative precise "
        f"su quanto ridurre e come ribilanciare il PIE."
    )
    analysis = ask_claude(context, max_tokens=500)
    if not analysis:
        return

    details = (
        f"Su Trading 212 — PIE: {pie}\n"
        f"Controlla la posizione {name} ({ticker})\n"
        f"Segui le istruzioni dell analisi qui sopra\n"
        f"Ribilancia il PIE dopo qualsiasi modifica"
    )
    rec_id = save_recommendation("news_critical", ticker, pie,
                                  "VERIFICA E AGISCI", details)

    msg = (
        f"🚨 <b>NOTIZIA CRITICA — AZIONE RICHIESTA</b>\n\n"
        f"<b>{name}</b> ({ticker}) · <i>{pie}</i>\n\n"
        f"📰 {article['title']}\n\n"
        f"🤖 {analysis}\n\n"
        f"<i>Vuoi agire su questa posizione?</i>"
    )
    buttons = [[
        {"text": "✅ Sì — vedo le istruzioni", "data": f"approve_{rec_id}"},
        {"text": "❌ No — monitoro", "data": f"reject_{rec_id}"}
    ]]
    send_telegram_with_buttons(msg, buttons)


# ── MODULO 5 — ANALISI COMPLETA PORTAFOGLIO ─────────────────
# Monitora tutti i 99 ticker, calcola impatto % su PIE e portafoglio,
# propone sostituzioni prioritizzando titoli gia presenti nel portafoglio

PIE_WEIGHTS = {pie: data["peso_target"] for pie, data in PORTFOLIO.items()}

def get_ticker_portfolio_weight(ticker):
    pies = TICKER_TO_PIE.get(ticker, [])
    if not pies:
        return 0.0
    total = 0.0
    for pie_name in pies:
        pie_w = PIE_WEIGHTS.get(pie_name, 0)
        n     = len(PORTFOLIO[pie_name]["tickers"])
        total += pie_w / n if n > 0 else 0
    return round(total, 2)

def get_ticker_pie_weight(ticker, pie_name):
    n = len(PORTFOLIO.get(pie_name, {}).get("tickers", []))
    return round(100.0 / n, 1) if n > 0 else 0.0

def get_internal_alternatives(ticker, pie_name):
    return [t for t in PORTFOLIO.get(pie_name, {}).get("tickers", []) if t != ticker]

def run_full_portfolio_analysis():
    log.info("Analisi completa portafoglio in corso...")
    now = datetime.now(timezone.utc)
    prices = get_prices_batch(ALL_TICKERS)
    ticker_metrics = {}
    for ticker, pdata in prices.items():
        if not pdata:
            continue
        price    = pdata.get("price", 0)
        change   = pdata.get("change_pct", 0)
        max_p    = get_max_price(ticker, days=90)
        drawdown = (price - max_p) / max_p * 100 if max_p and max_p > 0 else 0.0
        port_w   = get_ticker_portfolio_weight(ticker)
        impact   = (change / 100) * port_w
        if price > 0:
            save_price(ticker, price)
        ticker_metrics[ticker] = {
            "price":   price, "change_1d": change,
            "draw90":  drawdown, "port_w": port_w,
            "impact":  impact, "name": TICKER_NAMES.get(ticker, ticker),
            "pies":    TICKER_TO_PIE.get(ticker, []),
        }
    critici    = [(t, m) for t, m in ticker_metrics.items() if m["draw90"] <= -20.0]
    attenzione = [(t, m) for t, m in ticker_metrics.items()
                  if -20.0 < m["draw90"] <= -12.0 or m["change_1d"] <= -4.0]
    critici.sort(key=lambda x: x[1]["impact"])
    attenzione.sort(key=lambda x: x[1]["impact"])
    candidati = critici + attenzione[:3]
    if not candidati:
        log.info("Nessun candidato critico trovato")
        send_telegram(
            "✅ <b>ANALISI COMPLETA PORTAFOGLIO</b>\n"
            + now.strftime("%d/%m/%Y") + "\n\n"
            + f"Analizzati {len(ticker_metrics)} ticker.\n"
            + "Nessun titolo critico. Portafoglio nella norma."
        )
        return
    send_telegram(
        "🔬 <b>ANALISI COMPLETA PORTAFOGLIO</b>\n"
        + now.strftime("%d/%m/%Y") + "\n\n"
        + f"Ticker analizzati: {len(ticker_metrics)}\n"
        + f"Candidati revisione: {len(candidati)}\n"
        + f"Critici (>20% drawdown): {len(critici)}"
    )
    time.sleep(2)
    for ticker, m in candidati[:5]:
        pie_name   = m["pies"][0] if m["pies"] else "N/A"
        pie_w      = get_ticker_pie_weight(ticker, pie_name)
        alts       = get_internal_alternatives(ticker, pie_name)
        alts_names = ", ".join([TICKER_NAMES.get(t, t) for t in alts[:3]])
        is_crit    = any(t == ticker for t, _ in critici)
        prompt = (
            "Analisi professionale: " + m["name"] + " (" + ticker + ")\n"
            + "Drawdown 90gg: " + f"{m['draw90']:.1f}%" + "\n"
            + "Movimento oggi: " + f"{m['change_1d']:+.1f}%" + "\n"
            + "Peso portafoglio: " + f"{m['port_w']:.2f}%" + "\n"
            + "Peso nel PIE: " + f"{pie_w:.1f}%" + "\n"
            + "Tier: " + str(PORTFOLIO.get(pie_name, {}).get("tier", "N/A")) + "\n"
            + "Gia nel portafoglio stesso PIE: " + (alts_names if alts_names else "nessuno") + "\n\n"
            + "REGOLE ANALISI:\n"
            + "1. Il drawdown e strutturale o temporaneo?\n"
            + "2. La tesi dividend growth e ancora valida?\n"
            + "3. PRIMA considera di aumentare peso titoli gia presenti nel PIE.\n"
            + "4. SOLO se necessario proponi titolo esterno NON in portafoglio.\n"
            + "5. Valutazione 1-5 stelle sulla posizione.\n"
            + "6. Azione: MANTIENI / RIDUCI-PESO / RIBILANCIA-INTERNO / SOSTITUISCI-ESTERNO"
        )
        analysis = ask_claude(prompt, max_tokens=500)
        if not analysis:
            continue
        al = analysis.lower()
        if "sostituisci-esterno" in al or ("sostituisci" in al and "esterno" in al):
            rec_type  = "SOSTITUZIONE ESTERNA"
            btn_label = "Vedi istruzioni sostituzione"
        elif "ribilancia-interno" in al or ("interno" in al and "ribilancia" in al):
            rec_type  = "RIBILANCIO INTERNO"
            btn_label = "Vedi istruzioni ribilancio"
        elif "riduci-peso" in al or "riduci" in al:
            rec_type  = "RIDUCI PESO"
            btn_label = "Vedi istruzioni riduzione"
        else:
            rec_type  = "MONITORA"
            btn_label = "Prendi nota"
        details = (
            "PIE: " + pie_name.replace("_", " ") + "\n"
            + "Titolo: " + m["name"] + " (" + ticker + ")\n"
            + "Peso PIE: " + f"{pie_w:.1f}%" + " | Peso portafoglio: " + f"{m['port_w']:.2f}%" + "\n"
            + ("Interni disponibili: " + alts_names + "\n" if alts_names else "")
            + "\nSu Trading 212: apri il PIE, segui le istruzioni, ribilancia."
        )
        rec_id = save_recommendation("full_analysis", ticker, pie_name.replace("_", " "), rec_type, details)
        icon = "🚨" if is_crit else "⚠️"
        msg = (
            icon + " <b>" + rec_type + "</b>\n\n"
            + "<b>" + m["name"] + "</b> (" + ticker + ")\n"
            + "PIE: <i>" + pie_name.replace("_", " ") + "</i>\n\n"
            + "📊 Drawdown 90gg: <b>" + f"{m['draw90']:.1f}%" + "</b>\n"
            + "📈 Oggi: <b>" + f"{m['change_1d']:+.1f}%" + "</b>\n"
            + "⚖️ Peso portafoglio: <b>" + f"{m['port_w']:.2f}%" + "</b>\n"
            + "💥 Impatto oggi: <b>" + f"{m['impact']:+.3f}%" + "</b>\n"
            + ("\n🔄 <b>Gia nel PIE:</b> " + alts_names + "\n" if alts_names else "")
            + "\n🤖 <b>Analisi:</b>\n" + analysis
            + "\n\n<i>Come vuoi procedere?</i>"
        )
        buttons = [[
            {"text": "✅ " + btn_label, "data": "approve_" + str(rec_id)},
            {"text": "❌ Mantieni", "data": "reject_" + str(rec_id)}
        ]]
        send_telegram_with_buttons(msg, buttons)
        time.sleep(3)


# ── DIGEST ORARIO ─────────────────────────────────────────────
def send_hourly_digest():
    now = datetime.now(timezone.utc)
    if not (MARKET_OPEN <= now.hour < MARKET_CLOSE):
        return
    log.info("Digest orario...")
    prices = get_intraday_batch(ALL_TICKERS)
    movers = sorted(
        [(t, d) for t, d in prices.items() if abs(d.get("change_pct", 0)) >= DIGEST_MOVE_PCT],
        key=lambda x: abs(x[1]["change_pct"]), reverse=True
    )
    if not movers:
        send_telegram(f"🟢 <b>DIGEST {now.strftime('%H:%M')} UTC</b>\nTutto tranquillo.")
        return
    msg = f"🟡 <b>DIGEST {now.strftime('%H:%M')} UTC</b>\n\n"
    for ticker, data in movers[:8]:
        change = data["change_pct"]
        msg += f"{'📈' if change>0 else '📉'} <b>{TICKER_NAMES.get(ticker,ticker)}</b> {'+' if change>0 else ''}{change:.1f}%\n"
    if len(movers) > 8:
        msg += f"\n<i>+{len(movers)-8} altri</i>"
    send_telegram(msg)

# ── REPORT SERALE ─────────────────────────────────────────────
def send_evening_report():
    log.info("Report serale...")
    now = datetime.now(timezone.utc)
    prices = get_prices_batch(ALL_TICKERS)
    gainers = sorted([(t,d) for t,d in prices.items() if d.get("change_pct",0)>0],
                     key=lambda x: x[1]["change_pct"], reverse=True)
    losers  = sorted([(t,d) for t,d in prices.items() if d.get("change_pct",0)<0],
                     key=lambda x: x[1]["change_pct"])

    top_up = ", ".join([f"{TICKER_NAMES.get(t,t)} +{d['change_pct']:.1f}%" for t,d in gainers[:3]])
    top_dn = ", ".join([f"{TICKER_NAMES.get(t,t)} {d['change_pct']:.1f}%" for t,d in losers[:3]])
    analysis = ask_claude(
        f"Report serale {now.strftime('%d/%m/%Y')}. "
        f"Migliori: {top_up}. Peggiori: {top_dn}. "
        f"Valuta la giornata per il portafoglio dividend growth e dai un consiglio operativo se necessario.",
        max_tokens=400
    )
    msg = f"🟢 <b>REPORT SERALE — {now.strftime('%d/%m/%Y')}</b>\n\n"
    if gainers:
        msg += "<b>Migliori:</b>\n"
        for t,d in gainers[:3]:
            msg += f"📈 {TICKER_NAMES.get(t,t)} +{d['change_pct']:.1f}%\n"
    if losers:
        msg += "\n<b>Peggiori:</b>\n"
        for t,d in losers[:3]:
            msg += f"📉 {TICKER_NAMES.get(t,t)} {d['change_pct']:.1f}%\n"
    if analysis:
        msg += f"\n🤖 {analysis}"
    msg += f"\n\n<i>Factor Portfolio · 18 PIE</i>"
    send_telegram(msg)
    if now.weekday() == 4:
        time.sleep(3)
        run_full_portfolio_analysis()
    # Ogni venerdi sera: analisi completa settimanale
    if now.weekday() == 4:  # venerdi
        time.sleep(3)
        run_full_portfolio_analysis()

# ── MODULO 4 — WATCHLIST SOSTITUZIONE DINAMICA ───────────────
def run_substitution_watchlist():
    """Controlla TUTTI i 99 ticker. Soglie per tier:
    T1 -15% | T2 -18% | T3 -12% | T4 -25%"""
    log.info("Check watchlist sostituzione tutti i ticker...")
    prices = get_prices_batch(ALL_TICKERS)
    alerts_sent = 0
    for ticker, pdata in prices.items():
        if not pdata or alerts_sent >= 5:
            break
        price     = pdata.get("price", 0)
        max_price = get_max_price(ticker, days=90)
        if price > 0:
            save_price(ticker, price)
        if not max_price or max_price <= 0:
            continue
        drawdown = (price - max_price) / max_price * 100
        pies     = TICKER_TO_PIE.get(ticker, [])
        tier     = PORTFOLIO.get(pies[0], {}).get("tier", 2) if pies else 2
        soglia   = DRAWDOWN_THRESHOLDS.get(tier, -18.0)
        if drawdown <= soglia:
            hash_key = hashlib.md5(
                f"watch_{ticker}_{round(drawdown/5)*5}".encode()
            ).hexdigest()
            if not alert_already_sent(hash_key, hours=72):
                mark_alert_sent(hash_key, ticker, "watchlist")
                send_watchlist_alert(ticker, drawdown, price, tier, pies)
                alerts_sent += 1
                time.sleep(2)

def send_watchlist_alert(ticker, drawdown, price, tier, pies):
    """Alert watchlist con analisi interna/esterna"""
    name      = TICKER_NAMES.get(ticker, ticker)
    pie_name  = pies[0] if pies else "N/A"
    pie_label = pie_name.replace("_", " ")
    port_w    = get_ticker_portfolio_weight(ticker)
    pie_w     = get_ticker_pie_weight(ticker, pie_name)
    alts      = get_internal_alternatives(ticker, pie_name)
    alts_names = ", ".join([TICKER_NAMES.get(t, t) for t in alts[:3]])
    tier_labels = {1:"Dividend Aristocrat",2:"Quality Compounder",
                   3:"Low Volatility",4:"Momentum Growth"}
    soglia = DRAWDOWN_THRESHOLDS.get(tier, -18.0)
    prompt = (
        "Watchlist alert: " + name + " (" + ticker + ")\n"
        + "Tier: " + tier_labels.get(tier, str(tier)) + "\n"
        + "Drawdown 90gg: " + f"{drawdown:.1f}%" + " (soglia tier: " + f"{soglia:.0f}%" + ")\n"
        + "Peso portafoglio: " + f"{port_w:.2f}%" + "\n"
        + "Peso nel PIE: " + f"{pie_w:.1f}%" + "\n"
        + "Gia nel PIE: " + (alts_names if alts_names else "nessuno") + "\n\n"
        + "ANALISI: 1) Drawdown strutturale o temporaneo? "
        + "2) Tesi dividend growth valida? "
        + "3) Prima considera ribilancio interno. "
        + "4) Solo se necessario: titolo esterno NON in portafoglio. "
        + "5) Azione: MANTIENI / RIDUCI-PESO / RIBILANCIA-INTERNO / SOSTITUISCI-ESTERNO"
    )
    analysis = ask_claude(prompt, max_tokens=400)
    if not analysis:
        return
    al = analysis.lower()
    if "sostituisci-esterno" in al:
        rec_type = "SOSTITUZIONE ESTERNA"
        btn_ok   = "Vedi istruzioni"
    elif "ribilancia-interno" in al:
        rec_type = "RIBILANCIO INTERNO"
        btn_ok   = "Vedi istruzioni"
    elif "riduci-peso" in al:
        rec_type = "RIDUCI PESO"
        btn_ok   = "Vedi istruzioni"
    else:
        rec_type = "MONITORA"
        btn_ok   = "Prendi nota"
    details = (
        "PIE: " + pie_label + "\n"
        + "Titolo: " + name + " (" + ticker + ")\n"
        + "Peso PIE: " + f"{pie_w:.1f}%" + " | Portafoglio: " + f"{port_w:.2f}%" + "\n"
        + ("Interni: " + alts_names + "\n" if alts_names else "")
        + "\nSu T212: apri il PIE, segui l analisi, ribilancia."
    )
    rec_id = save_recommendation("watchlist", ticker, pie_label, rec_type, details)
    msg = (
        "⚠️ <b>WATCHLIST — " + rec_type + "</b>\n\n"
        + "<b>" + name + "</b> (" + ticker + ") · Tier " + str(tier) + "\n"
        + "PIE: <i>" + pie_label + "</i>\n\n"
        + "📊 Drawdown 90gg: <b>" + f"{drawdown:.1f}%" + "</b> (soglia " + f"{soglia:.0f}%" + ")\n"
        + "⚖️ Peso portafoglio: <b>" + f"{port_w:.2f}%" + "</b>\n"
        + ("\n🔄 <b>Gia nel PIE:</b> " + alts_names + "\n" if alts_names else "")
        + "\n🤖 " + analysis
        + "\n\n<i>Come vuoi procedere?</i>"
    )
    buttons = [[
        {"text": "✅ " + btn_ok, "data": "approve_" + str(rec_id)},
        {"text": "❌ Mantieni", "data": "reject_" + str(rec_id)}
    ]]
    send_telegram_with_buttons(msg, buttons)

# ── REPORT SETTIMANALE ────────────────────────────────────────
def send_weekly_report():
    now = datetime.now(timezone.utc)
    if now.weekday() != 0:
        return
    log.info("Report settimanale + ribilanciamento...")
    # Report settimanale
    analysis = ask_claude(
        "Genera un outlook settimanale per Factor Portfolio dividend growth 18 PIE. "
        "Cosa monitorare? Earnings rilevanti? Rischi macro? Consiglio operativo.",
        max_tokens=400
    )
    msg = f"📊 <b>OUTLOOK SETTIMANALE — {now.strftime('%d/%m/%Y')}</b>\n\n"
    if analysis:
        msg += analysis
    msg += "\n\n<i>Buona settimana · Factor Portfolio</i>"
    send_telegram(msg)
    time.sleep(2)
    # Ribilanciamento settimanale
    run_rebalancing_check()
    time.sleep(5)
    # Analisi completa ogni domenica
    if now.weekday() == 6:
        run_full_portfolio_analysis()
    time.sleep(5)
    # Analisi completa tutti i ticker (domenica sera)
    if now.weekday() == 6:  # domenica
        run_full_portfolio_analysis()

# ── SCHEDULER ─────────────────────────────────────────────────
def run_scheduler():
    # Ogni minuto: prezzi + callback
    schedule.every(1).minutes.do(check_price_alerts)
    schedule.every(1).minutes.do(check_callbacks)
    # Notizie: schema ottimale 93 richieste/giorno (limite 100)
    # 08:00 CET — scansione completa 35 query (apertura mercati EU)
    # 16:00 CET — scansione completa 35 query (apertura NYSE)
    # 20:00 CET — scansione parziale 23 query tier 1+2 (chiusura NYSE)
    schedule.every().day.at("08:00").do(check_news_and_recommend)
    schedule.every().day.at("16:00").do(check_news_and_recommend)
    schedule.every().day.at("20:00").do(
        lambda: check_news_and_recommend(tier_filter=[1, 2])
    )
    # Digest orari
    for h in DIGEST_HOUR:
        schedule.every().day.at(f"{h:02d}:00").do(send_hourly_digest)
    # Report serale
    schedule.every().day.at(f"{REPORT_HOUR:02d}:30").do(send_evening_report)
    # Settimanale lunedi 07:00 UTC
    schedule.every().monday.at("07:00").do(send_weekly_report)
    # Watchlist sostituzione: tutti i giorni alle 06:00 UTC (prima apertura mercati)
    schedule.every().day.at("06:00").do(run_substitution_watchlist)
    # Pulizia notturna
    schedule.every().day.at("02:00").do(cleanup_old_data)

    log.info("Scheduler v2 avviato. Tutti i moduli attivi.")
    send_telegram(
        f"🚀 <b>Portfolio Agent v2 avviato</b>\n"
        f"<b>Moduli attivi:</b>\n"
        f"🔴 Alert prezzi real-time\n"
        f"📰 Notizie + raccomandazioni operative\n"
        f"⚖️ Ribilanciamento settimanale\n"
        f"🔄 Watchlist sostituzione titoli\n"
        f"✅ Bottoni Approva/Rifiuta su Telegram\n\n"
        f"<i>99 ticker · 18 PIE · Factor Portfolio</i>"
    )
    while True:
        schedule.run_pending()
        time.sleep(30)

# ── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Portfolio Agent v2 — Factor Portfolio 18 PIE")
    init_db()
    # Carica portfolio aggiornato da GitHub se disponibile
    result = load_portfolio_from_github()
    if result and result[0]:
        reload_portfolio(result[0])
        log.info("Portfolio caricato da GitHub")
    else:
        log.info("Portfolio caricato da configurazione locale")
    log.info(f"Ticker monitorati: {len(ALL_TICKERS)}")
    log.info(f"Ticker US: {len(US_TICKERS)}")
    log.info(f"Modulo watchlist: soglie dinamiche per tier su tutti i ticker")
    run_scheduler()
