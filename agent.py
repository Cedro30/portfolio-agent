"""
Portfolio Agent v3 — Factor Portfolio 18 PIE
Analisi professionale con Claude + Web Search
Nessuna API prezzi — copertura totale 98 titoli via ricerca web
"""

import os
import time
import json
import logging
import sqlite3
import schedule
import requests
from datetime import datetime, timezone, timedelta

# ── CONFIG ───────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")
GITHUB_TOKEN      = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO       = os.environ.get("GITHUB_REPO", "portfolio-agent")
GITHUB_USERNAME   = os.environ.get("GITHUB_USERNAME", "")
PORTFOLIO_FILE    = "portfolio.json"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# ── PORTAFOGLIO ──────────────────────────────────────────────
PORTFOLIO = {}
TICKER_NAMES = {}
ALL_TICKERS = []

def load_portfolio_from_github():
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        return None, None
    import base64
    url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{PORTFOLIO_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.ok:
            data = r.json()
            content = base64.b64decode(data["content"]).decode("utf-8")
            return json.loads(content), data["sha"]
    except Exception as e:
        log.error(f"GitHub load error: {e}")
    return None, None

def reload_portfolio(portfolio_data):
    global PORTFOLIO, TICKER_NAMES, ALL_TICKERS
    PORTFOLIO = {}
    for pie_name, pie_data in portfolio_data["pies"].items():
        PORTFOLIO[pie_name] = {
            "tier":         pie_data["tier"],
            "peso_target":  pie_data["peso_target"],
            "tickers":      pie_data["tickers"]
        }
    TICKER_NAMES.update(portfolio_data.get("ticker_names", {}))
    ALL_TICKERS = list(set(t for pie in PORTFOLIO.values() for t in pie["tickers"]))
    log.info(f"Portfolio ricaricato: {len(ALL_TICKERS)} ticker unici")

# ── DATABASE ─────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS sent_alerts (
        hash TEXT PRIMARY KEY, alert_type TEXT, created_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS recommendations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rec_type TEXT, details TEXT,
        created_at TEXT, status TEXT DEFAULT 'pending')""")
    conn.commit()
    conn.close()

def already_sent(hash_key, hours=12):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    c.execute("SELECT 1 FROM sent_alerts WHERE hash=? AND created_at>?", (hash_key, cutoff))
    exists = c.fetchone() is not None
    conn.close()
    return exists

def mark_sent(hash_key, alert_type):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO sent_alerts VALUES (?,?,?)",
              (hash_key, alert_type, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()

def save_recommendation(rec_type, details):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("INSERT INTO recommendations (rec_type,details,created_at) VALUES (?,?,?)",
              (rec_type, details, datetime.now(timezone.utc).isoformat()))
    rec_id = c.lastrowid
    conn.commit()
    conn.close()
    return rec_id

def update_recommendation(rec_id, status):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("UPDATE recommendations SET status=? WHERE id=?", (status, rec_id))
    conn.commit()
    conn.close()

def cleanup():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    c.execute("DELETE FROM sent_alerts WHERE created_at<?", (cutoff,))
    c.execute("DELETE FROM recommendations WHERE status!='pending' AND created_at<?", (cutoff,))
    conn.commit()
    conn.close()

# ── TELEGRAM ─────────────────────────────────────────────────
def send_telegram(message, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": message,
            "parse_mode": parse_mode, "disable_web_page_preview": True
        }, timeout=10)
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
    return False

def send_with_buttons(message, buttons):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    keyboard = {"inline_keyboard": [[
        {"text": b["text"], "callback_data": b["data"]} for b in row
    ] for row in buttons]}
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "HTML", "reply_markup": keyboard,
                  "disable_web_page_preview": True},
            timeout=10
        )
        return r.ok
    except Exception as e:
        log.error(f"Telegram buttons error: {e}")
    return False

def check_callbacks():
    if not TELEGRAM_TOKEN:
        return
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"timeout": 1, "allowed_updates": ["callback_query"]},
            timeout=5
        )
        if not r.ok:
            return
        updates = r.json().get("result", [])
        last_id = None
        for update in updates:
            last_id = update["update_id"]
            if "callback_query" in update:
                cq = update["callback_query"]
                handle_callback(cq["data"])
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                    json={"callback_query_id": cq["id"]}, timeout=5
                )
        if last_id:
            requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": last_id + 1, "limit": 1}, timeout=5
            )
    except Exception as e:
        log.error(f"Callback error: {e}")

def handle_callback(data):
    parts = data.split("_")
    action = parts[0]
    rec_id = int(parts[1]) if len(parts) > 1 else None
    if not rec_id:
        return
    if action == "approve":
        update_recommendation(rec_id, "approved")
        conn = sqlite3.connect("agent_state.db")
        c = conn.cursor()
        c.execute("SELECT details FROM recommendations WHERE id=?", (rec_id,))
        row = c.fetchone()
        conn.close()
        if row:
            send_telegram(
                "✅ <b>APPROVATO</b>\\n\\n"
                + row[0]
                + "\\n\\n<i>Esegui su Trading 212 quando sei pronto.</i>"
            )
    elif action == "reject":
        update_recommendation(rec_id, "rejected")
        send_telegram(f"❌ Raccomandazione #{rec_id} rifiutata. Nessuna azione.")

# ── CLAUDE CON WEB SEARCH ────────────────────────────────────
def claude_with_search(prompt, max_tokens=1500):
    """Chiama Claude con web search abilitato per analisi aggiornate."""
    if not ANTHROPIC_API_KEY:
        return None
    
    system = """Sei un analista finanziario senior di livello istituzionale.
Gestisci un Factor Portfolio con 18 PIE e 100 titoli globali su 4 tier:
- Tier 1 (40%): Dividend Aristocrats
- Tier 2 (30%): Quality Compounders
- Tier 3 (20%): Low Volatility Income
- Tier 4 (10%): Momentum Growth

COMPOSIZIONE COMPLETA PORTAFOGLIO - 18 PIE CON PESI REALI:
PIE01 Aristocrats USA (Tier 1, 8.0% portafoglio):
  Procter & Gamble: 16%
  Johnson & Johnson: 15%
  Coca-Cola: 14%
  PepsiCo: 12%
  Abbott: 11%
  Medtronic: 11%
  Walmart: 10%
  Emerson Electric: 11%
PIE02 Aristocrats EU (Tier 1, 7.0% portafoglio):
  Air Liquide: 18%
  Nestle: 15%
  L'Oreal: 14%
  Sika: 13%
  Wolters Kluwer: 12%
  Dassault Systemes: 12%
  Linde: 16%
PIE03 Aristocrats Asia (Tier 1, 7.0% portafoglio):
  DBS Group: 38%
  Toyota: 30%
  Sony Group: 20%
  Commonwealth Bank: 12%
PIE04 Champions Energia (Tier 1, 6.0% portafoglio):
  ExxonMobil: 20%
  Chevron: 18%
  Williams Companies: 16%
  Enbridge: 14%
  TotalEnergies: 12%
  EOG Resources: 10%
  Constellation Energy: 10%
PIE05 Champions Finanza (Tier 1, 6.0% portafoglio):
  HSBC: 18%
  AXA: 16%
  Allianz: 16%
  AIG: 14%
  UniCredit: 12%
  BNP Paribas: 12%
  Macquarie: 12%
PIE06 REIT Growth (Tier 1, 6.0% portafoglio):
  Realty Income: 22%
  Prologis: 18%
  American Tower: 18%
  Equinix: 14%
  WP Carey: 14%
  American Water: 14%
PIE07 Quality Tech (Tier 2, 6.0% portafoglio):
  ASML: 22%
  Microsoft: 20%
  Texas Instruments: 18%
  Apple: 16%
  SAP: 14%
  Broadcom: 10%
PIE08 Quality Lusso (Tier 2, 6.0% portafoglio):
  LVMH: 24%
  Hermes: 20%
  Richemont: 16%
  Ferrari: 16%
  Moncler: 12%
  Estee Lauder: 12%
PIE09 Quality Healthcare (Tier 2, 5.0% portafoglio):
  Johnson & Johnson: 18%
  Eli Lilly: 16%
  Novo Nordisk: 14%
  AstraZeneca: 14%
  Thermo Fisher: 12%
  Roche: 12%
  UnitedHealth: 14%
PIE10 Quality Difesa (Tier 2, 5.0% portafoglio):
  Lockheed Martin: 18%
  Northrop Grumman: 16%
  Rheinmetall: 16%
  BAE Systems: 14%
  Airbus: 14%
  BWX Technologies: 12%
  General Dynamics: 10%
PIE11 Quality Chip (Tier 2, 4.0% portafoglio):
  TSMC: 28%
  Samsung: 22%
  SK Hynix: 18%
  Qualcomm: 16%
  Tokyo Electron: 16%
PIE12 Quality Infrastrutture (Tier 2, 4.0% portafoglio):
  Brookfield Infrastructure: 26%
  Vinci: 22%
  Ferrovial: 20%
  Getlink: 17%
  National Grid: 15%
PIE13 Utility Nucleare (Tier 3, 6.0% portafoglio):
  Constellation Energy: 18%
  Enel: 16%
  Iberdrola: 15%
  Entergy: 13%
  Snam: 12%
  Terna: 12%
  Dominion Energy: 14%
PIE14 Consumer Staples (Tier 3, 5.0% portafoglio):
  Procter & Gamble: 20%
  Coca-Cola: 18%
  PepsiCo: 16%
  Unilever: 16%
  Costco: 16%
  Colgate-Palmolive: 14%
PIE15 Gas Industriali (Tier 3, 5.0% portafoglio):
  Air Liquide: 28%
  Linde: 24%
  Sika: 20%
  Sherwin-Williams: 14%
  Air Products: 14%
PIE16 Midstream Pipeline (Tier 3, 4.0% portafoglio):
  Williams Companies: 28%
  Enbridge: 24%
  Kinder Morgan: 20%
  TC Energy: 16%
  PPL Corporation: 12%
PIE17 AI Tech (Tier 4, 6.0% portafoglio):
  NVIDIA: 25%
  Alphabet: 22%
  Meta: 20%
  Amazon: 18%
  AMD: 10%
  China Internet ETF: 5%
PIE18 EM Growth (Tier 4, 4.0% portafoglio):
  Infosys: 20%
  HDFC Bank: 18%
  Itau Unibanco: 16%
  Vale: 14%
  Reliance Industries: 12%
  China Internet ETF: 12%
  ICICI Bank: 8%

REGOLE OBBLIGATORIE PER LE RACCOMANDAZIONI:
Quando suggerisci modifiche a un PIE elenca SEMPRE tutti i titoli con % precisa.
Non solo i titoli modificati - TUTTI, anche quelli invariati.
I pesi di ogni PIE devono sempre sommare esattamente a 100%.
Indica sempre il motivo della modifica in una frase.
Se suggerisci sostituzione indica: titolo aggiunto, titolo rimosso, nuovi pesi completi.
Mantieni la coerenza con la tesi dividend growth. No trading tattico.

Rispondi in italiano, tono professionale ma diretto. Usa emoji per chiarezza visiva."""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": max_tokens,
                "system": system,
                "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=60
        )
        if r.ok:
            data = r.json()
            # Estrai tutto il testo dalla risposta (inclusi risultati dopo web search)
            text_parts = [
                block["text"] for block in data.get("content", [])
                if block.get("type") == "text"
            ]
            return "\n".join(text_parts) if text_parts else None
        else:
            log.error(f"Claude API error: {r.status_code} {r.text[:200]}")
    except Exception as e:
        log.error(f"Claude error: {e}")
    return None

# ── BRIEFING MATTUTINO ───────────────────────────────────────
def send_morning_briefing():
    log.info("Briefing mattutino in corso...")
    now = datetime.now(timezone.utc)

    # Lista titoli per settore per la ricerca
    titoli_principali = [
        "NVIDIA", "Microsoft", "Apple", "ASML", "LVMH", "Hermes",
        "ExxonMobil", "Chevron", "Lockheed Martin", "Rheinmetall",
        "Novo Nordisk", "Eli Lilly", "Realty Income", "Air Liquide",
        "Toyota", "Samsung", "DBS Group", "Nestle"
    ]

    prompt = (
        f"Oggi è {now.strftime('%d/%m/%Y')}. "
        f"Fai una ricerca web sulle ultime notizie finanziarie di queste aziende del portafoglio: "
        f"{', '.join(titoli_principali)}. "
        f"Poi analizza le notizie trovate e produci un briefing professionale strutturato così:\\n"
        f"1. EVENTI CRITICI (se presenti): notizie che cambiano la tesi di investimento\\n"
        f"2. DA MONITORARE: sviluppi da tenere d'occhio\\n"
        f"3. POSITIVO: conferme della tesi\\n"
        f"4. CONSIGLIO OPERATIVO: azione concreta se necessaria (ribilancio PIE, cambio peso)\\n"
        f"Sii concreto e professionale. Se non ci sono notizie rilevanti dillo chiaramente."
    )

    analysis = claude_with_search(prompt, max_tokens=1500)
    if not analysis:
        return

    msg = (
        f"📋 <b>BRIEFING MATTUTINO — {now.strftime('%d/%m/%Y')}</b>\\n\\n"
        f"{analysis}\\n\\n"
        f"<i>Factor Portfolio · 18 PIE · Analisi Claude + Web Search</i>"
    )

    # Dividi se troppo lungo per Telegram (max 4096 char)
    if len(msg) > 4000:
        send_telegram(msg[:4000] + "...\\n<i>(continua)</i>")
        send_telegram("...<i>(segue)</i>\\n" + msg[4000:])
    else:
        send_telegram(msg)

# ── BRIEFING NYSE ─────────────────────────────────────────────
def send_nyse_briefing():
    log.info("Briefing apertura NYSE...")
    now = datetime.now(timezone.utc)

    prompt = (
        f"Oggi è {now.strftime('%d/%m/%Y')}, sono le 16:00 CET — apertura di Wall Street. "
        f"Cerca le notizie di oggi su questi titoli USA del portafoglio: "
        f"NVIDIA, Microsoft, Apple, Meta, Amazon, AMD, "
        f"ExxonMobil, Chevron, Lockheed Martin, Northrop Grumman, General Dynamics, "
        f"Johnson & Johnson, Eli Lilly, UnitedHealth, Thermo Fisher, "
        f"Realty Income, Prologis, American Tower, Equinix, "
        f"Procter & Gamble, Coca-Cola, Costco, Walmart. "
        f"Analizza il contesto macro di oggi (Fed, dati economici, geopolitica) "
        f"e il suo impatto specifico sui PIE del portafoglio. "
        f"Produci un briefing professionale con: contesto macro, titoli in evidenza, "
        f"impatto sui PIE, consiglio operativo se necessario."
    )

    analysis = claude_with_search(prompt, max_tokens=1200)
    if not analysis:
        return

    msg = (
        f"🗽 <b>BRIEFING NYSE — {now.strftime('%d/%m/%Y')}</b>\\n\\n"
        f"{analysis}\\n\\n"
        f"<i>Factor Portfolio · Analisi Claude + Web Search</i>"
    )
    if len(msg) > 4000:
        send_telegram(msg[:4000] + "...")
        send_telegram("..." + msg[4000:])
    else:
        send_telegram(msg)

# ── REPORT SERALE ─────────────────────────────────────────────
def send_evening_report():
    log.info("Report serale...")
    now = datetime.now(timezone.utc)

    # Il venerdi analisi piu approfondita
    is_friday = now.weekday() == 4

    prompt = (
        f"Oggi è {now.strftime('%d/%m/%Y')} — fine giornata. "
        f"Cerca le notizie piu rilevanti di oggi per il Factor Portfolio dividend growth. "
        f"Titoli da verificare: tutti i 18 PIE con focus su eventuali annunci dividendi, "
        f"earnings, guidance, M&A, cambi CEO, downgrade/upgrade rating. "
        f"Produci il report serale professionale con:\\n"
        f"1. SINTESI GIORNATA: 2-3 righe sull'andamento generale\\n"
        f"2. TITOLI IN EVIDENZA: max 5 notizie rilevanti con impatto sulla tesi\\n"
        f"3. CONSIGLIO OPERATIVO: se necessario, azione concreta con dettagli per T212\\n"
        f"{'4. ANALISI SETTIMANALE: bilancio della settimana e outlook prossima' if is_friday else ''}"
        f"\\nSii diretto e professionale. Rating da 1-5 stelle per la giornata."
    )

    analysis = claude_with_search(prompt, max_tokens=1500)
    if not analysis:
        return

    icon = "📊" if is_friday else "🟢"
    title = "REPORT SERALE + ANALISI SETTIMANALE" if is_friday else "REPORT SERALE"

    msg = (
        f"{icon} <b>{title} — {now.strftime('%d/%m/%Y')}</b>\\n\\n"
        f"{analysis}\\n\\n"
        f"<i>Factor Portfolio · 18 PIE · Analisi Claude + Web Search</i>"
    )

    # Se contiene raccomandazione operativa, aggiungi bottoni
    if any(kw in analysis.lower() for kw in ["consiglio operativo", "ribilancia", "sostituisci", "riduci"]):
        rec_id = save_recommendation("evening_report", analysis)
        buttons = [[
            {"text": "✅ Vedi istruzioni T212", "data": f"approve_{rec_id}"},
            {"text": "❌ Solo nota", "data": f"reject_{rec_id}"}
        ]]
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...")
            send_with_buttons("..." + msg[4000:], buttons)
        else:
            send_with_buttons(msg, buttons)
    else:
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...")
            send_telegram("..." + msg[4000:])
        else:
            send_telegram(msg)

# ── OUTLOOK SETTIMANALE ───────────────────────────────────────
def send_weekly_outlook():
    log.info("Outlook settimanale...")
    now = datetime.now(timezone.utc)

    prompt = (
        f"Oggi è lunedì {now.strftime('%d/%m/%Y')} — inizio settimana. "
        f"Cerca: earnings della settimana per i titoli del portafoglio, "
        f"dati macro attesi (Fed, CPI, PIL, disoccupazione), "
        f"rischi geopolitici rilevanti per il portafoglio (energia, difesa, tech). "
        f"Produci l'outlook settimanale professionale con:\\n"
        f"1. EARNINGS DA SEGUIRE questa settimana (solo titoli del portafoglio)\\n"
        f"2. DATI MACRO ATTESI e impatto sui PIE\\n"
        f"3. RISCHI E OPPORTUNITA della settimana\\n"
        f"4. CONSIGLIO STRATEGICO: cosa fare / non fare questa settimana\\n"
        f"5. RIBILANCIAMENTO: il portafoglio necessita aggiustamenti? Proponi se si.\\n"
        f"Tono da gestore istituzionale senior."
    )

    analysis = claude_with_search(prompt, max_tokens=1500)
    if not analysis:
        return

    msg = (
        f"📊 <b>OUTLOOK SETTIMANALE — {now.strftime('%d/%m/%Y')}</b>\\n\\n"
        f"{analysis}\\n\\n"
        f"<i>Buona settimana · Factor Portfolio · 18 PIE</i>"
    )

    if "ribilanc" in analysis.lower() or "consiglio" in analysis.lower():
        rec_id = save_recommendation("weekly_outlook", analysis)
        buttons = [[
            {"text": "✅ Vedi istruzioni", "data": f"approve_{rec_id}"},
            {"text": "❌ Solo nota", "data": f"reject_{rec_id}"}
        ]]
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...")
            send_with_buttons("..." + msg[4000:], buttons)
        else:
            send_with_buttons(msg, buttons)
    else:
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...")
            send_telegram("..." + msg[4000:])
        else:
            send_telegram(msg)

# ── ANALISI FONDAMENTALE DOMENICALE ───────────────────────────
def send_sunday_deep_analysis():
    log.info("Analisi fondamentale domenicale...")
    now = datetime.now(timezone.utc)

    prompt = (
        f"Oggi è domenica {now.strftime('%d/%m/%Y')}. "
        f"Fai una ricerca approfondita sullo stato fondamentale dei 18 PIE del portafoglio. "
        f"Cerca notizie recenti su: dividendi annunciati o tagliati, "
        f"risultati trimestrali, guidance aziendale, cambi strutturali nel settore. "
        f"Poi produci l'analisi fondamentale mensile del portafoglio:\\n"
        f"1. TIER 1 Dividend Aristocrats: le streak sono intatte? Novità sui dividendi?\\n"
        f"2. TIER 2 Quality Compounders: i moat sono ancora solidi?\\n"
        f"3. TIER 3 Low Volatility: utility e pipeline stabili?\\n"
        f"4. TIER 4 Momentum: le tesi growth reggono?\\n"
        f"5. TITOLI SOTTO OSSERVAZIONE: quali meritano attenzione nelle prossime settimane?\\n"
        f"6. RACCOMANDAZIONI PORTAFOGLIO: aggiustamenti suggeriti con dettagli operativi per T212.\\n"
        f"Analisi da gestore istituzionale senior. Valutazione complessiva portafoglio 1-5 stelle."
    )

    analysis = claude_with_search(prompt, max_tokens=2000)
    if not analysis:
        return

    msg = (
        f"🔬 <b>ANALISI FONDAMENTALE — {now.strftime('%d/%m/%Y')}</b>\\n\\n"
        f"{analysis}\\n\\n"
        f"<i>Factor Portfolio · 18 PIE · Analisi domenicale Claude + Web Search</i>"
    )

    rec_id = save_recommendation("sunday_analysis", analysis)
    buttons = [[
        {"text": "✅ Vedi istruzioni operativo", "data": f"approve_{rec_id}"},
        {"text": "❌ Solo lettura", "data": f"reject_{rec_id}"}
    ]]

    if len(msg) > 4000:
        send_telegram(msg[:4000] + "...")
        send_with_buttons("..." + msg[4000:], buttons)
    else:
        send_with_buttons(msg, buttons)


# ── ROTAZIONE PIE GIORNALIERA ────────────────────────────────
DAILY_ROTATION = {
    0: {
        "label": "PIE01 Aristocrats USA + PIE02 Aristocrats EU + PIE03 Aristocrats Asia",
        "titoli": ["Procter & Gamble","Johnson & Johnson","Coca-Cola","PepsiCo","Abbott","Medtronic","Walmart","Emerson Electric","Air Liquide","Nestle","L Oreal","Sika","Wolters Kluwer","Dassault Systemes","Linde","DBS Group","Toyota","Sony","Commonwealth Bank Australia"]
    },
    1: {
        "label": "PIE04 Champions Energia + PIE05 Champions Finanza + PIE06 REIT Growth",
        "titoli": ["ExxonMobil","Chevron","Williams Companies","Enbridge","TotalEnergies","EOG Resources","Constellation Energy","HSBC","AXA","Allianz","AIG","UniCredit","BNP Paribas","Macquarie","Realty Income","Prologis","American Tower","Equinix","WP Carey","American Water Works"]
    },
    2: {
        "label": "PIE07 Quality Tech + PIE08 Quality Lusso + PIE09 Quality Healthcare",
        "titoli": ["ASML","Microsoft","Texas Instruments","Apple","SAP","Broadcom","LVMH","Hermes","Richemont","Ferrari","Moncler","Estee Lauder","Johnson & Johnson","Eli Lilly","Novo Nordisk","AstraZeneca","Thermo Fisher","Roche","UnitedHealth"]
    },
    3: {
        "label": "PIE10 Quality Difesa + PIE11 Quality Chip + PIE12 Quality Infrastrutture",
        "titoli": ["Lockheed Martin","Northrop Grumman","Rheinmetall","BAE Systems","Airbus","BWX Technologies","General Dynamics","TSMC","Samsung Electronics","SK Hynix","Qualcomm","Tokyo Electron","Brookfield Infrastructure","Vinci","Ferrovial","Getlink"]
    },
    4: {
        "label": "PIE13 Utility Nucleare + PIE14 Consumer Staples + PIE15 Gas Industriali + PIE16 Midstream Pipeline",
        "titoli": ["Constellation Energy","Enel","Iberdrola","Entergy","Snam","Terna","Dominion Energy","Coca-Cola","PepsiCo","Unilever","Costco","Air Liquide","Linde","Sika","Sherwin-Williams","Air Products","Williams Companies","Enbridge","Kinder Morgan","TC Energy","PPL Corporation"]
    },
    5: None,
    6: {
        "label": "PIE17 AI Tech + PIE18 EM Growth + Revisione portafoglio completo",
        "titoli": ["NVIDIA","Alphabet","Meta","Amazon","AMD","KWEB China Internet ETF","Infosys","HDFC Bank","Itau Unibanco","Vale","ICICI Bank","Reliance Industries"]
    }
}

def send_daily_pie_analysis():
    """Analisi giornaliera dei PIE in rotazione settimanale."""
    now = datetime.now(timezone.utc)
    weekday = now.weekday()
    rotation = DAILY_ROTATION.get(weekday)

    if not rotation:
        log.info("Sabato — nessuna analisi PIE.")
        return

    label = rotation["label"]
    titoli = rotation["titoli"]
    log.info(f"Analisi PIE giornaliera: {label}")

    is_sunday = weekday == 6
    is_friday = weekday == 4

    if is_sunday:
        extra = (
            "Dopo aver analizzato i PIE17 e PIE18, produci una REVISIONE COMPLETA del portafoglio: "
            "valutazione complessiva 1-5 stelle, i 3 titoli piu solidi e i 3 piu a rischio, "
            "raccomandazione strategica generale per le prossime 2 settimane."
        )
    elif is_friday:
        extra = (
            "Essendo venerdi, aggiungi un bilancio settimanale per questi PIE: "
            "la settimana e stata positiva o negativa per il gruppo? "
            "Outlook per la prossima settimana."
        )
    else:
        extra = ""

    prompt = (
        f"Oggi e {now.strftime('%A %d/%m/%Y')}. "
        f"Analisi giornaliera del Factor Portfolio — {label}.\n\n"
        f"Cerca le ultime notizie su questi titoli: {', '.join(titoli)}.\n\n"
        f"Per ogni titolo che ha notizie rilevanti analizza:\n"
        f"• Cosa e successo\n"
        f"• Impatto sulla tesi dividend growth/quality\n"
        f"• Azione consigliata (mantieni / monitora / aggiusta peso quando investi)\n\n"
        f"Per i titoli senza notizie rilevanti scrivi solo: [Nessuna novita — tesi confermata]\n\n"
        f"Struttura finale:\n"
        f"1. SINTESI: 2 righe sul gruppo di PIE oggi\n"
        f"2. TITOLI IN EVIDENZA: solo quelli con notizie rilevanti\n"
        f"3. TUTTI GLI ALTRI: lista rapida con stato\n"
        f"4. CONSIGLIO OPERATIVO: azione concreta se necessaria per T212\n"
        f"{extra}"
    )

    analysis = claude_with_search(prompt, max_tokens=2000)
    if not analysis:
        log.error("Analisi PIE giornaliera fallita")
        return

    icon = "🔬" if is_sunday else ("📊" if is_friday else "📋")
    msg = (
        f"{icon} <b>ANALISI PIE — {label}</b>\n"
        f"<i>{now.strftime('%d/%m/%Y')}</i>\n\n"
        f"{analysis}\n\n"
        f"<i>Factor Portfolio · Claude + Web Search</i>"
    )

    if any(kw in analysis.lower() for kw in ["consiglio operativo", "aggiusta", "riduci", "aumenta", "sostituisci"]):
        rec_id = save_recommendation("daily_pie_analysis", analysis)
        buttons = [[
            {"text": "✅ Vedi istruzioni T212", "data": f"approve_{rec_id}"},
            {"text": "❌ Solo nota", "data": f"reject_{rec_id}"}
        ]]
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...")
            send_with_buttons("..." + msg[4000:], buttons)
        else:
            send_with_buttons(msg, buttons)
    else:
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...")
            send_telegram("..." + msg[4000:])
        else:
            send_telegram(msg)

# ── SCHEDULER ─────────────────────────────────────────────────
def run_scheduler():
    # Briefing mattutino — apertura mercati EU
    schedule.every().day.at("07:00").do(send_morning_briefing)
    # Briefing NYSE — apertura Wall Street
    schedule.every().day.at("14:30").do(send_nyse_briefing)
    # Report serale — chiusura mercati
    schedule.every().day.at("17:30").do(send_evening_report)
    # Outlook settimanale — lunedi mattina
    schedule.every().monday.at("06:30").do(send_weekly_outlook)
    # Analisi fondamentale — domenica
    schedule.every().sunday.at("09:00").do(send_sunday_deep_analysis)
    # Analisi PIE giornaliera in rotazione — 17:00 CET
    schedule.every().day.at("15:00").do(send_daily_pie_analysis)
    # Callback bottoni Telegram — ogni minuto
    schedule.every(1).minutes.do(check_callbacks)
    # Pulizia database — ogni notte
    schedule.every().day.at("02:00").do(cleanup)

    log.info("Scheduler v3 avviato.")
    send_telegram(
        "🚀 <b>Portfolio Agent v3 avviato</b>\\n\\n"
        "<b>Modalità:</b> Analisi Claude + Web Search\\n"
        "<b>Copertura:</b> 98 titoli · 18 PIE · tutti i mercati\\n\\n"
        "📋 Briefing mattutino: 09:00 CET\\n"
        "🗽 Briefing NYSE: 16:30 CET\\n"
        "🟢 Report serale: 19:30 CET\\n"
        "📊 Outlook lunedì: 08:30 CET\\n"
        "🔬 Analisi domenicale: 11:00 CET\\n\\n"
        "<i>Nessun limite di ticker · Analisi professionale · Bottoni Approva/Rifiuta</i>"
    )

    while True:
        schedule.run_pending()
        time.sleep(30)

# ── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Portfolio Agent v3 — Claude + Web Search")
    init_db()
    result = load_portfolio_from_github()
    if result and result[0]:
        reload_portfolio(result[0])
        log.info("Portfolio caricato da GitHub")
    log.info(f"Ticker totali: {len(ALL_TICKERS)}")
    run_scheduler()
