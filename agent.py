"""
Portfolio Agent v3 — Factor Portfolio 18 PIE
Analisi professionale con Claude + Web Search
Struttura: Morning Brief 08:00 | Evening Brief 20:00 (solo eventi critici) | Weekly Review domenica
"""

import os, time, json, logging, sqlite3, schedule, requests
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
    format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
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
            "tier": pie_data["tier"],
            "peso_target": pie_data["peso_target"],
            "tickers": pie_data["tickers"]
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
        rec_type TEXT, details TEXT, t212_instructions TEXT,
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

def save_recommendation(rec_type, details, t212_instructions=""):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("INSERT INTO recommendations (rec_type,details,t212_instructions,created_at) VALUES (?,?,?,?)",
              (rec_type, details, t212_instructions, datetime.now(timezone.utc).isoformat()))
    rec_id = c.lastrowid
    conn.commit()
    conn.close()
    return rec_id

def get_recommendation(rec_id):
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("SELECT details, t212_instructions FROM recommendations WHERE id=?", (rec_id,))
    row = c.fetchone()
    conn.close()
    return row

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
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }, timeout=10)
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
    return False

def send_with_buttons(message, rec_id, has_t212=True):
    """Invia messaggio con bottoni T212 e Solo nota.
    has_t212=True solo quando c'e una raccomandazione operativa concreta con pesi."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    buttons = []
    if has_t212:
        buttons.append({"text": "✅ Vedi istruzioni T212", "callback_data": f"t212_{rec_id}"})
    buttons.append({"text": "📝 Solo nota", "callback_data": f"note_{rec_id}"})
    keyboard = {"inline_keyboard": [buttons]}
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "reply_markup": keyboard,
                "disable_web_page_preview": True
            }, timeout=10)
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
            timeout=5)
        if not r.ok:
            return
        updates = r.json().get("result", [])
        last_id = None
        for update in updates:
            last_id = update["update_id"]
            if "callback_query" in update:
                cq = update["callback_query"]
                handle_callback(cq["data"], cq["id"])
        if last_id:
            requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": last_id + 1, "limit": 1}, timeout=5)
    except Exception as e:
        log.error(f"Callback error: {e}")

def handle_callback(data, callback_query_id):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id}, timeout=5)
    except:
        pass

    parts = data.split("_")
    action = parts[0]
    rec_id = int(parts[1]) if len(parts) > 1 else None
    if not rec_id:
        return

    if action == "t212":
        row = get_recommendation(rec_id)
        if row:
            details, t212_instructions = row
            update_recommendation(rec_id, "approved")
            
            # Aggiorna pesi su GitHub
            portfolio_data, sha = load_portfolio_from_github()
            if portfolio_data:
                ok, result = update_portfolio_weights_on_github(details, portfolio_data, sha)
                if ok:
                    reload_portfolio(portfolio_data)
                    update_msg = f"\n\n✅ <b>Pesi aggiornati automaticamente</b> in {result} — le prossime analisi useranno i nuovi pesi."
                else:
                    update_msg = f"\n\n⚠️ Aggiornamento automatico non riuscito ({result}) — aggiorna manualmente portfolio.json."
            else:
                update_msg = ""
            
            # Invia istruzioni T212
            if t212_instructions:
                send_telegram(
                    f"📱 <b>ISTRUZIONI TRADING 212</b>\n\n"
                    f"{t212_instructions}"
                    f"{update_msg}\n\n"
                    f"<i>Esegui quando sei pronto.</i>"
                )
            else:
                instructions = generate_t212_instructions(details)
                if instructions:
                    send_telegram(
                        f"📱 <b>ISTRUZIONI TRADING 212</b>\n\n"
                        f"{instructions}"
                        f"{update_msg}\n\n"
                        f"<i>Esegui quando sei pronto.</i>"
                    )
                else:
                    send_telegram(f"📱 Raccomandazione approvata.{update_msg}")
    elif action == "note":
        update_recommendation(rec_id, "noted")
        send_telegram("📝 <i>Annotato. Nessuna azione richiesta. I pesi rimangono invariati.</i>")

def generate_t212_instructions(analysis):
    """Genera istruzioni operative specifiche per T212 basate sull'analisi."""
    prompt = (
        f"Basandoti su questa analisi del portafoglio:\n\n{analysis}\n\n"
        f"Genera istruzioni operative SPECIFICHE e PRATICHE per eseguire le modifiche su Trading 212.\n"
        f"Formato richiesto:\n"
        f"Per ogni PIE da modificare:\n"
        f"1. Nome del PIE su T212\n"
        f"2. Azione: apri il PIE → clicca Modifica → cambia il peso di [TITOLO] da X% a Y%\n"
        f"3. Lista completa tutti i titoli del PIE con i nuovi pesi\n"
        f"4. Clicca Ribilancia per applicare\n\n"
        f"Sii specifico sui numeri. Se non ci sono modifiche operative concrete, scrivi: "
        f"'Nessuna azione richiesta — mantieni i pesi attuali.'"
    )
    return claude_with_search(prompt, max_tokens=800)

# ── CLAUDE CON WEB SEARCH ────────────────────────────────────
SYSTEM_PROMPT = """Sei un analista finanziario senior di livello istituzionale.
Gestisci un Factor Portfolio con 18 PIE e 100 titoli globali su 4 tier:
- Tier 1 (40%): Dividend Aristocrats
- Tier 2 (30%): Quality Compounders
- Tier 3 (20%): Low Volatility Income
- Tier 4 (10%): Momentum Growth

COMPOSIZIONE COMPLETA PORTAFOGLIO - 18 PIE CON PESI REALI:
PIE01 Aristocrats USA (Tier 1, 8.0% portafoglio):
  Procter & Gamble: 16%, Johnson & Johnson: 15%, Coca-Cola: 14%, PepsiCo: 12%
  Abbott: 11%, Medtronic: 11%, Walmart: 10%, Emerson Electric: 11%
PIE02 Aristocrats EU (Tier 1, 7.0% portafoglio):
  Air Liquide: 18%, Nestle: 15%, L'Oreal: 14%, Sika: 13%
  Wolters Kluwer: 12%, Dassault Systemes: 12%, Linde: 16%
PIE03 Aristocrats Asia (Tier 1, 7.0% portafoglio):
  DBS Group: 38%, Toyota: 30%, Sony Group: 20%, Commonwealth Bank: 12%
PIE04 Champions Energia (Tier 1, 6.0% portafoglio):
  ExxonMobil: 20%, Chevron: 18%, Williams Companies: 16%, Enbridge: 14%
  TotalEnergies: 12%, EOG Resources: 10%, Constellation Energy: 10%
PIE05 Champions Finanza (Tier 1, 6.0% portafoglio):
  HSBC: 18%, AXA: 16%, Allianz: 16%, AIG: 14%
  UniCredit: 12%, BNP Paribas: 12%, Macquarie: 12%
PIE06 REIT Growth (Tier 1, 6.0% portafoglio):
  Realty Income: 22%, Prologis: 18%, American Tower: 18%
  Equinix: 14%, WP Carey: 14%, American Water: 14%
PIE07 Quality Tech (Tier 2, 6.0% portafoglio):
  ASML: 22%, Microsoft: 20%, Texas Instruments: 18%
  Apple: 16%, SAP: 14%, Broadcom: 10%
PIE08 Quality Lusso (Tier 2, 6.0% portafoglio):
  LVMH: 24%, Hermes: 20%, Richemont: 16%
  Ferrari: 16%, Moncler: 12%, Estee Lauder: 12%
PIE09 Quality Healthcare (Tier 2, 5.0% portafoglio):
  Johnson & Johnson: 18%, Eli Lilly: 16%, Novo Nordisk: 14%
  AstraZeneca: 14%, Thermo Fisher: 12%, Roche: 12%, UnitedHealth: 14%
PIE10 Quality Difesa (Tier 2, 5.0% portafoglio):
  Lockheed Martin: 18%, Northrop Grumman: 16%, Rheinmetall: 16%
  BAE Systems: 14%, Airbus: 14%, BWX Technologies: 12%, General Dynamics: 10%
PIE11 Quality Chip (Tier 2, 4.0% portafoglio):
  TSMC: 28%, Samsung: 22%, SK Hynix: 18%, Qualcomm: 16%, Tokyo Electron: 16%
PIE12 Quality Infrastrutture (Tier 2, 4.0% portafoglio):
  Brookfield Infrastructure: 26%, Vinci: 22%, Ferrovial: 20%
  Getlink: 17%, National Grid: 15%
PIE13 Utility Nucleare (Tier 3, 6.0% portafoglio):
  Constellation Energy: 18%, Enel: 16%, Iberdrola: 15%
  Entergy: 13%, Snam: 12%, Terna: 12%, Dominion Energy: 14%
PIE14 Consumer Staples (Tier 3, 5.0% portafoglio):
  Procter & Gamble: 20%, Coca-Cola: 18%, PepsiCo: 16%
  Unilever: 16%, Costco: 16%, Colgate-Palmolive: 14%
PIE15 Gas Industriali (Tier 3, 5.0% portafoglio):
  Air Liquide: 28%, Linde: 24%, Sika: 20%
  Sherwin-Williams: 14%, Air Products: 14%
PIE16 Midstream Pipeline (Tier 3, 4.0% portafoglio):
  Williams Companies: 28%, Enbridge: 24%, Kinder Morgan: 20%
  TC Energy: 16%, PPL Corporation: 12%
PIE17 AI Tech (Tier 4, 6.0% portafoglio):
  NVIDIA: 25%, Alphabet: 22%, Meta: 20%
  Amazon: 18%, AMD: 10%, China Internet ETF: 5%
PIE18 EM Growth (Tier 4, 4.0% portafoglio):
  Infosys: 20%, HDFC Bank: 18%, Itau Unibanco: 16%
  Vale: 14%, Reliance Industries: 12%, China Internet ETF: 12%, ICICI Bank: 8%

REGOLE OBBLIGATORIE PER LE RACCOMANDAZIONI:
Quando suggerisci modifiche a un PIE elenca SEMPRE tutti i titoli con % precisa.
Non solo i titoli modificati - TUTTI, anche quelli invariati.
I pesi di ogni PIE devono sempre sommare esattamente a 100%.
Indica sempre il motivo della modifica in una frase.
Se suggerisci sostituzione: titolo aggiunto, titolo rimosso, nuovi pesi completi.
Mantieni la coerenza con la tesi dividend growth. No trading tattico.

Rispondi in italiano, tono professionale ma diretto. Usa emoji per chiarezza visiva."""

def claude_with_search(prompt, max_tokens=1500):
    if not ANTHROPIC_API_KEY:
        return None
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
                "system": SYSTEM_PROMPT,
                "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=90
        )
        if r.ok:
            data = r.json()
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

def has_operational_recommendation(text):
    """Controlla se il testo contiene una raccomandazione operativa concreta con pesi."""
    keywords = [
        "riduci ", "aumenta ", "sostituisci ", "sposta ", "modifica il peso",
        "dal ", "% al ", "nuovo peso", "ribilancia",
        "era ", "nuova allocazione", "riduzione peso", "nuovo peso",
        "% (era", "consiglio operativo", "azione consigliata"
    ]
    # Conta quante keyword sono presenti
    text_lower = text.lower()
    count = sum(1 for kw in keywords if kw in text_lower)
    # Controlla pattern di cambio peso: "18% (era 20%)" o "dal 20% al 18%"
    import re
    peso_changes = re.findall(r"\d+%.{1,10}era.{1,5}\d+%", text_lower)
    dal_al = re.findall(r"dal \d+% al \d+%", text_lower)
    return count >= 2 or len(peso_changes) >= 1 or len(dal_al) >= 1

# ── ROTAZIONE PIE ────────────────────────────────────────────
# Tier 2 e 4 ogni giorno, Tier 1 lun/mer/ven, Tier 3 mar/gio
DAILY_PIE_ROTATION = {
    0: {  # Lunedi
        "label": "PIE01+02+03 (Tier 1 Aristocrats) + PIE07+08+10+11+12+17+18 (Tier 2&4)",
        "titoli_t1": ["Procter & Gamble","Johnson & Johnson","Coca-Cola","PepsiCo","Abbott","Medtronic","Walmart","Emerson Electric","Air Liquide","Nestle","L'Oreal","Sika","Wolters Kluwer","Dassault Systemes","Linde","DBS Group","Toyota","Sony Group","Commonwealth Bank"],
        "titoli_t24": ["ASML","Microsoft","Texas Instruments","Apple","SAP","Broadcom","LVMH","Hermes","Richemont","Ferrari","Moncler","Estee Lauder","Lockheed Martin","Northrop Grumman","Rheinmetall","BAE Systems","Airbus","BWX Technologies","General Dynamics","TSMC","Samsung","SK Hynix","Qualcomm","Tokyo Electron","Brookfield Infrastructure","Vinci","Ferrovial","Getlink","National Grid","NVIDIA","Alphabet","Meta","Amazon","AMD","Infosys","HDFC Bank","Itau Unibanco","Vale","ICICI Bank","Reliance Industries"],
    },
    1: {  # Martedi
        "label": "PIE04+05+06 (Tier 3 Pipeline/Finanza) + PIE07+08+10+11+12+17+18 (Tier 2&4)",
        "titoli_t3": ["ExxonMobil","Chevron","Williams Companies","Enbridge","TotalEnergies","EOG Resources","Constellation Energy","HSBC","AXA","Allianz","AIG","UniCredit","BNP Paribas","Macquarie","Realty Income","Prologis","American Tower","Equinix","WP Carey","American Water","Constellation Energy","Enel","Iberdrola","Entergy","Snam","Terna","Dominion Energy","Coca-Cola","PepsiCo","Unilever","Costco","Colgate-Palmolive","Williams Companies","Enbridge","Kinder Morgan","TC Energy","PPL Corporation"],
        "titoli_t24": ["ASML","Microsoft","Texas Instruments","Apple","SAP","Broadcom","LVMH","Hermes","Richemont","Ferrari","Moncler","Estee Lauder","Lockheed Martin","Northrop Grumman","Rheinmetall","BAE Systems","Airbus","BWX Technologies","General Dynamics","TSMC","Samsung","SK Hynix","Qualcomm","Tokyo Electron","Brookfield Infrastructure","Vinci","Ferrovial","Getlink","National Grid","NVIDIA","Alphabet","Meta","Amazon","AMD","Infosys","HDFC Bank","Itau Unibanco","Vale","ICICI Bank","Reliance Industries"],
    },
    2: {  # Mercoledi
        "label": "PIE01+02+03 (Tier 1 Aristocrats) + PIE07+08+09+10+11+12+17+18 (Tier 2&4)",
        "titoli_t1": ["Procter & Gamble","Johnson & Johnson","Coca-Cola","PepsiCo","Abbott","Medtronic","Walmart","Emerson Electric","Air Liquide","Nestle","L'Oreal","Sika","Wolters Kluwer","Dassault Systemes","Linde","DBS Group","Toyota","Sony Group","Commonwealth Bank"],
        "titoli_t24": ["ASML","Microsoft","Texas Instruments","Apple","SAP","Broadcom","LVMH","Hermes","Richemont","Ferrari","Moncler","Estee Lauder","Johnson & Johnson","Eli Lilly","Novo Nordisk","AstraZeneca","Thermo Fisher","Roche","UnitedHealth","Lockheed Martin","Northrop Grumman","Rheinmetall","BAE Systems","Airbus","BWX Technologies","General Dynamics","TSMC","Samsung","SK Hynix","Qualcomm","Tokyo Electron","Brookfield Infrastructure","Vinci","Ferrovial","Getlink","National Grid","NVIDIA","Alphabet","Meta","Amazon","AMD","Infosys","HDFC Bank","Itau Unibanco","Vale","ICICI Bank","Reliance Industries"],
    },
    3: {  # Giovedi
        "label": "PIE13+14+15+16 (Tier 3 Utility/Staples/Gas/Pipeline) + PIE07+08+10+11+12+17+18 (Tier 2&4)",
        "titoli_t3": ["Constellation Energy","Enel","Iberdrola","Entergy","Snam","Terna","Dominion Energy","Procter & Gamble","Coca-Cola","PepsiCo","Unilever","Costco","Colgate-Palmolive","Air Liquide","Linde","Sika","Sherwin-Williams","Air Products","Williams Companies","Enbridge","Kinder Morgan","TC Energy","PPL Corporation"],
        "titoli_t24": ["ASML","Microsoft","Texas Instruments","Apple","SAP","Broadcom","LVMH","Hermes","Richemont","Ferrari","Moncler","Estee Lauder","Lockheed Martin","Northrop Grumman","Rheinmetall","BAE Systems","Airbus","BWX Technologies","General Dynamics","TSMC","Samsung","SK Hynix","Qualcomm","Tokyo Electron","Brookfield Infrastructure","Vinci","Ferrovial","Getlink","National Grid","NVIDIA","Alphabet","Meta","Amazon","AMD","Infosys","HDFC Bank","Itau Unibanco","Vale","ICICI Bank","Reliance Industries"],
    },
    4: {  # Venerdi
        "label": "PIE04+05+06 (Tier 1 Energia/Finanza/REIT) + PIE07+08+09+10+11+12+17+18 (Tier 2&4)",
        "titoli_t1": ["ExxonMobil","Chevron","Williams Companies","Enbridge","TotalEnergies","EOG Resources","Constellation Energy","HSBC","AXA","Allianz","AIG","UniCredit","BNP Paribas","Macquarie","Realty Income","Prologis","American Tower","Equinix","WP Carey","American Water"],
        "titoli_t24": ["ASML","Microsoft","Texas Instruments","Apple","SAP","Broadcom","LVMH","Hermes","Richemont","Ferrari","Moncler","Estee Lauder","Johnson & Johnson","Eli Lilly","Novo Nordisk","AstraZeneca","Thermo Fisher","Roche","UnitedHealth","Lockheed Martin","Northrop Grumman","Rheinmetall","BAE Systems","Airbus","BWX Technologies","General Dynamics","TSMC","Samsung","SK Hynix","Qualcomm","Tokyo Electron","Brookfield Infrastructure","Vinci","Ferrovial","Getlink","National Grid","NVIDIA","Alphabet","Meta","Amazon","AMD","Infosys","HDFC Bank","Itau Unibanco","Vale","ICICI Bank","Reliance Industries"],
    },
    5: None,  # Sabato — riposo
    6: {  # Domenica — Weekly Review completa
        "label": "Tutti i 18 PIE — Weekly Review completa",
    }
}

# ── MORNING BRIEF ─────────────────────────────────────────────
def send_morning_brief():
    now = datetime.now(timezone.utc)
    weekday = now.weekday()
    rotation = DAILY_PIE_ROTATION.get(weekday)

    if not rotation:
        log.info("Sabato — nessun Morning Brief.")
        return

    log.info(f"Morning Brief: {rotation['label']}")
    is_friday = weekday == 4
    is_monday = weekday == 0

    # Raccogli tutti i titoli del giorno
    tutti_titoli = []
    for key in ["titoli_t1", "titoli_t3", "titoli_t24"]:
        tutti_titoli.extend(rotation.get(key, []))
    # Rimuovi duplicati mantenendo ordine
    seen = set()
    titoli_unici = [t for t in tutti_titoli if not (t in seen or seen.add(t))]

    extra = ""
    if is_monday:
        extra = "Essendo lunedi, includi una breve anticipazione della settimana: earnings attesi e dati macro rilevanti."
    elif is_friday:
        extra = "Essendo venerdi, aggiungi un bilancio settimanale: la settimana e stata positiva o negativa per il portafoglio?"

    prompt = (
        f"Oggi e {now.strftime('%A %d/%m/%Y')}. Sono le 08:00 CET — apertura mercati europei.\n\n"
        f"Cerca le ultime notizie finanziarie su questi titoli del portafoglio:\n"
        f"{', '.join(titoli_unici)}\n\n"
        f"Produci il Morning Brief professionale con questa struttura:\n\n"
        f"1. SINTESI APERTURA (2 righe): contesto macro del giorno\n"
        f"2. EVENTI CRITICI (se presenti): notizie che cambiano la tesi di investimento\n"
        f"   Per ogni evento: cosa e successo | impatto sulla tesi | azione consigliata\n"
        f"3. DA MONITORARE: sviluppi da tenere d'occhio nelle prossime ore\n"
        f"4. CONFERMATI: titoli con notizie positive che rafforzano la tesi\n"
        f"5. TITOLI SENZA NOVITA: lista rapida con [tesi confermata]\n"
        f"6. CONSIGLIO OPERATIVO: se necessario, modifica pesi con % precise su TUTTI i titoli del PIE\n"
        f"{extra}\n\n"
        f"Rating giornata: X/5 stelle con motivazione in una frase."
    )

    analysis = claude_with_search(prompt, max_tokens=2000)
    if not analysis:
        log.error("Morning Brief fallito")
        return

    day_names = {0:"Lunedi",1:"Martedi",2:"Mercoledi",3:"Giovedi",4:"Venerdi",5:"Sabato",6:"Domenica"}
    icon = "📊" if is_friday else ("📋" if is_monday else "☀️")

    msg = (
        f"{icon} <b>MORNING BRIEF — {day_names[weekday]} {now.strftime('%d/%m/%Y')}</b>\n"
        f"<i>{rotation['label']}</i>\n\n"
        f"{analysis}\n\n"
        f"<i>Factor Portfolio · 18 PIE · Claude + Web Search</i>"
    )

    if has_operational_recommendation(analysis):
        # Genera istruzioni T212 specifiche
        t212 = generate_t212_instructions(analysis)
        rec_id = save_recommendation("morning_brief", analysis, t212 or "")
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...\n<i>(continua)</i>")
            send_with_buttons("...<i>(segue)</i>\n" + msg[4000:], rec_id, has_t212=True)
        else:
            send_with_buttons(msg, rec_id, has_t212=True)
    else:
        rec_id = save_recommendation("morning_brief", analysis, "")
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...\n<i>(continua)</i>")
            send_with_buttons("...<i>(segue)</i>\n" + msg[4000:], rec_id, has_t212=False)
        else:
            send_with_buttons(msg, rec_id, has_t212=False)

# ── EVENING BRIEF (solo eventi critici) ──────────────────────
def send_evening_brief():
    now = datetime.now(timezone.utc)
    weekday = now.weekday()
    if weekday == 6:  # Domenica nessun evening
        return

    log.info("Evening Brief — scansione eventi critici su tutti i 100 titoli...")

    tutti = [
        "Procter & Gamble","Johnson & Johnson","Coca-Cola","PepsiCo","Abbott","Medtronic","Walmart","Emerson Electric",
        "Air Liquide","Nestle","L'Oreal","Sika","Wolters Kluwer","Dassault Systemes","Linde",
        "DBS Group","Toyota","Sony Group","Commonwealth Bank",
        "ExxonMobil","Chevron","Williams Companies","Enbridge","TotalEnergies","EOG Resources","Constellation Energy",
        "HSBC","AXA","Allianz","AIG","UniCredit","BNP Paribas","Macquarie",
        "Realty Income","Prologis","American Tower","Equinix","WP Carey","American Water",
        "ASML","Microsoft","Texas Instruments","Apple","SAP","Broadcom",
        "LVMH","Hermes","Richemont","Ferrari","Moncler","Estee Lauder",
        "Johnson & Johnson","Eli Lilly","Novo Nordisk","AstraZeneca","Thermo Fisher","Roche","UnitedHealth",
        "Lockheed Martin","Northrop Grumman","Rheinmetall","BAE Systems","Airbus","BWX Technologies","General Dynamics",
        "TSMC","Samsung","SK Hynix","Qualcomm","Tokyo Electron",
        "Brookfield Infrastructure","Vinci","Ferrovial","Getlink","National Grid",
        "Enel","Iberdrola","Entergy","Snam","Terna","Dominion Energy",
        "Unilever","Costco","Colgate-Palmolive","Sherwin-Williams","Air Products","Kinder Morgan","TC Energy","PPL Corporation",
        "NVIDIA","Alphabet","Meta","Amazon","AMD",
        "Infosys","HDFC Bank","Itau Unibanco","Vale","ICICI Bank","Reliance Industries"
    ]

    prompt = (
        f"Oggi e {now.strftime('%d/%m/%Y')}. Sono le 20:00 CET — chiusura mercati.\n\n"
        f"Fai una scansione rapida degli ULTIMI EVENTI CRITICI accaduti oggi su questi 100 titoli del portafoglio:\n"
        f"{', '.join(tutti)}\n\n"
        f"REGOLA FONDAMENTALE: invia una risposta SOLO se hai trovato almeno un evento critico.\n"
        f"Un evento critico e: earnings a sorpresa, taglio dividendo, crollo >5%, acquisizione, "
        f"decisione FDA, contratto rilevante, guidance rivista, cambio CEO, indagine regolatoria.\n\n"
        f"Se NON hai trovato eventi critici oggi, rispondi ESATTAMENTE con: 'NESSUN_EVENTO_CRITICO'\n\n"
        f"Se hai trovato eventi critici, produci:\n"
        f"1. EVENTO: [nome titolo] — cosa e successo\n"
        f"2. IMPATTO sul PIE e sulla tesi\n"
        f"3. AZIONE: mantieni / monitora / modifica peso (con % precise su TUTTI i titoli del PIE)\n"
        f"Rating urgenza: BASSO / MEDIO / ALTO"
    )

    analysis = claude_with_search(prompt, max_tokens=1500)
    if not analysis:
        log.error("Evening Brief fallito")
        return

    # Se nessun evento critico, silenzio professionale
    if "NESSUN_EVENTO_CRITICO" in analysis:
        log.info("Evening Brief: nessun evento critico — silenzio professionale")
        return

    log.info("Evening Brief: eventi critici trovati — invio notifica")

    msg = (
        f"🔴 <b>EVENING BRIEF — {now.strftime('%d/%m/%Y')}</b>\n"
        f"<i>Evento critico rilevato sul portafoglio</i>\n\n"
        f"{analysis}\n\n"
        f"<i>Factor Portfolio · 18 PIE · Claude + Web Search</i>"
    )

    if has_operational_recommendation(analysis):
        t212 = generate_t212_instructions(analysis)
        rec_id = save_recommendation("evening_brief", analysis, t212 or "")
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...")
            send_with_buttons("..." + msg[4000:], rec_id, has_t212=True)
        else:
            send_with_buttons(msg, rec_id, has_t212=True)
    else:
        rec_id = save_recommendation("evening_brief", analysis, "")
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...")
            send_with_buttons("..." + msg[4000:], rec_id, has_t212=False)
        else:
            send_with_buttons(msg, rec_id, has_t212=False)

# ── WEEKLY REVIEW (domenica) ──────────────────────────────────
def send_weekly_review():
    now = datetime.now(timezone.utc)
    log.info("Weekly Review domenicale — analisi completa 18 PIE...")

    prompt = (
        f"Oggi e domenica {now.strftime('%d/%m/%Y')} — Weekly Review del Factor Portfolio.\n\n"
        f"Cerca le notizie della settimana su TUTTI i 100 titoli del portafoglio e produci "
        f"la revisione settimanale completa:\n\n"
        f"1. VALUTAZIONE SETTIMANA: rating 1-5 stelle con sintesi in 3 righe\n\n"
        f"2. ANALISI PER TIER:\n"
        f"   TIER 1 Dividend Aristocrats: streak intatte? Novita sui dividendi?\n"
        f"   TIER 2 Quality Compounders: moat ancora solidi? Cambi competitivi?\n"
        f"   TIER 3 Low Volatility: utility e pipeline stabili?\n"
        f"   TIER 4 Momentum: tesi growth reggono?\n\n"
        f"3. TOP 3 NOTIZIE DELLA SETTIMANA: le piu rilevanti per il portafoglio\n\n"
        f"4. TITOLI SOTTO OSSERVAZIONE: max 3 titoli che meritano attenzione\n\n"
        f"5. EARNINGS PROSSIMA SETTIMANA: solo titoli del portafoglio\n\n"
        f"6. RACCOMANDAZIONI OPERATIVE: se necessario, modifiche pesi con % precise "
        f"su TUTTI i titoli del PIE coinvolto. Se nessuna modifica, scrivi esplicitamente "
        f"'Nessuna modifica necessaria — portafoglio solido.'\n\n"
        f"7. CONSIGLIO STRATEGICO: una frase per la settimana che inizia."
    )

    analysis = claude_with_search(prompt, max_tokens=2500)
    if not analysis:
        log.error("Weekly Review fallita")
        return

    msg = (
        f"🔬 <b>WEEKLY REVIEW — {now.strftime('%d/%m/%Y')}</b>\n"
        f"<i>Analisi completa 18 PIE · 100 titoli</i>\n\n"
        f"{analysis}\n\n"
        f"<i>Factor Portfolio · Claude + Web Search · Buona settimana</i>"
    )

    if has_operational_recommendation(analysis):
        t212 = generate_t212_instructions(analysis)
        rec_id = save_recommendation("weekly_review", analysis, t212 or "")
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...\n<i>(continua)</i>")
            send_with_buttons("...<i>(segue)</i>\n" + msg[4000:], rec_id, has_t212=True)
        else:
            send_with_buttons(msg, rec_id, has_t212=True)
    else:
        rec_id = save_recommendation("weekly_review", analysis, "")
        if len(msg) > 4000:
            send_telegram(msg[:4000] + "...\n<i>(continua)</i>")
            send_with_buttons("...<i>(segue)</i>\n" + msg[4000:], rec_id, has_t212=False)
        else:
            send_with_buttons(msg, rec_id, has_t212=False)


# ── AGGIORNAMENTO PESI SU GITHUB ─────────────────────────────
def extract_new_weights(analysis_text):
    """Estrae i nuovi pesi dal testo di analisi di Claude.
    Cerca pattern come: 'ExxonMobil: 18% (era 20%)' o 'NVIDIA: 25%'"""
    import re
    weights = {}
    
    # Pattern: "Nome Titolo: XX% (era YY%)" o "Nome Titolo: XX%"
    patterns = [
        r"([A-Za-z &']+):\s*\*{0,2}(\d{1,3})%\*{0,2}\s*[(]era",
        r"-\s*([A-Za-z &']+):\s*\*{0,2}(\d{1,3})%\*{0,2}",
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, analysis_text)
        for name, pct in matches:
            name = name.strip().strip('*').strip()
            if len(name) > 2:
                weights[name] = int(pct)
    
    return weights

def find_pie_for_weights(weights, portfolio_data):
    """Trova quale PIE corrisponde ai titoli con nuovi pesi."""
    ticker_names = portfolio_data.get("ticker_names", {})
    # Inverti: nome -> ticker
    name_to_ticker = {v.lower(): k for k, v in ticker_names.items()}
    
    # Per ogni PIE, controlla quanti titoli matchano
    best_pie = None
    best_count = 0
    
    for pie_name, pie_data in portfolio_data["pies"].items():
        count = 0
        for weight_name in weights.keys():
            for ticker in pie_data["tickers"]:
                ticker_name = ticker_names.get(ticker, ticker).lower()
                if weight_name.lower() in ticker_name or ticker_name in weight_name.lower():
                    count += 1
                    break
        if count > best_count:
            best_count = count
            best_pie = pie_name
    
    return best_pie if best_count >= 2 else None

def update_portfolio_weights_on_github(analysis_text, portfolio_data, sha):
    """Estrae i nuovi pesi dall'analisi e aggiorna portfolio.json su GitHub."""
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        return False, "GitHub non configurato"
    
    weights = extract_new_weights(analysis_text)
    if not weights:
        return False, "Nessun peso trovato nel testo"
    
    log.info(f"Pesi estratti: {weights}")
    
    # Trova il PIE corrispondente
    pie_name = find_pie_for_weights(weights, portfolio_data)
    if not pie_name:
        return False, "PIE non identificato"
    
    log.info(f"PIE identificato: {pie_name}")
    
    # Verifica che i pesi sommino a 100
    total = sum(weights.values())
    if abs(total - 100) > 5:
        return False, f"I pesi sommano a {total}% — verifica prima di salvare"
    
    # Salva i pesi come note nel portfolio (non modifichiamo la struttura dei ticker)
    if "pie_weights" not in portfolio_data:
        portfolio_data["pie_weights"] = {}
    
    ticker_names = portfolio_data.get("ticker_names", {})
    name_to_ticker = {v.lower(): k for k, v in ticker_names.items()}
    
    pie_weights = {}
    for weight_name, pct in weights.items():
        # Trova il ticker corrispondente
        for ticker in portfolio_data["pies"][pie_name]["tickers"]:
            ticker_name = ticker_names.get(ticker, ticker).lower()
            if weight_name.lower() in ticker_name or ticker_name in weight_name.lower():
                pie_weights[ticker] = pct
                break
    
    if pie_weights:
        portfolio_data["pie_weights"][pie_name] = pie_weights
        portfolio_data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        portfolio_data["last_change"] = f"{pie_name}: {weights}"
    
    # Aggiorna su GitHub
    import json as json_module, base64
    content_str = json_module.dumps(portfolio_data, indent=2, ensure_ascii=False)
    content_b64 = base64.b64encode(content_str.encode()).decode()
    
    url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{PORTFOLIO_FILE}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    
    payload = {
        "message": f"Aggiornamento pesi {pie_name} — approvato via Telegram",
        "content": content_b64,
        "sha": sha
    }
    
    try:
        r = requests.put(url, headers=headers, json=payload, timeout=15)
        if r.ok:
            log.info(f"portfolio.json aggiornato su GitHub: {pie_name}")
            return True, pie_name
        else:
            log.error(f"GitHub update error: {r.status_code} {r.text[:200]}")
            return False, f"Errore GitHub: {r.status_code}"
    except Exception as e:
        log.error(f"GitHub update exception: {e}")
        return False, str(e)

# ── SCHEDULER ─────────────────────────────────────────────────
def run_scheduler():
    schedule.every().day.at("07:00").do(send_morning_brief)     # 09:00 CET
    schedule.every().day.at("18:00").do(send_evening_brief)     # 20:00 CET
    schedule.every().sunday.at("08:00").do(send_weekly_review)  # 10:00 CET domenica
    schedule.every(1).minutes.do(check_callbacks)
    schedule.every().day.at("02:00").do(cleanup)

    log.info("Scheduler v3 avviato.")
    send_telegram(
        "🚀 <b>Portfolio Agent v3 avviato</b>\n\n"
        "<b>Struttura professionale:</b>\n"
        "☀️ 09:00 CET — Morning Brief (Tier 2&4 ogni giorno + Tier 1/3 in rotazione)\n"
        "🔴 20:00 CET — Evening Brief (solo eventi critici su tutti i 100 titoli)\n"
        "🔬 Domenica 10:00 — Weekly Review completa 18 PIE\n\n"
        "<b>Bottoni:</b>\n"
        "✅ Vedi istruzioni T212 → istruzioni operative specifiche\n"
        "📝 Solo nota → annota senza azione\n\n"
        "<i>Copertura totale · 100 titoli · Claude + Web Search</i>"
    )

    while True:
        schedule.run_pending()
        time.sleep(30)

# ── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Portfolio Agent v3 — Morning/Evening Brief + Weekly Review")
    init_db()
    result, sha = load_portfolio_from_github()
    if result:
        reload_portfolio(result)
        log.info("Portfolio caricato da GitHub")
    log.info(f"Ticker totali: {len(ALL_TICKERS)}")
    run_scheduler()
