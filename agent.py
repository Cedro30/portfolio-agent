"""
Portfolio Agent v3 — Factor Portfolio 18 PIE
Filosofia: gestione istituzionale buy & hold dividend growth
Una review settimanale profonda ogni lunedi mattina.
Agisce solo quando cambia la tesi strutturale, non quando il prezzo fluttua.
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

PORTFOLIO     = {}
TICKER_NAMES  = {}
ALL_TICKERS   = []
PORTFOLIO_SHA = None

# ── PORTAFOGLIO ──────────────────────────────────────────────
def load_portfolio_from_github():
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        return None, None
    import base64
    url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{PORTFOLIO_FILE}"
    try:
        r = requests.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}, timeout=10)
        if r.ok:
            data = r.json()
            content = base64.b64decode(data["content"]).decode("utf-8")
            return json.loads(content), data["sha"]
    except Exception as e:
        log.error(f"GitHub load error: {e}")
    return None, None

def reload_portfolio(portfolio_data, sha=None):
    global PORTFOLIO, TICKER_NAMES, ALL_TICKERS, PORTFOLIO_SHA
    PORTFOLIO = {}
    for pie_name, pie_data in portfolio_data["pies"].items():
        PORTFOLIO[pie_name] = {
            "tier": pie_data["tier"],
            "peso_target": pie_data["peso_target"],
            "tickers": pie_data["tickers"]
        }
    TICKER_NAMES.update(portfolio_data.get("ticker_names", {}))
    ALL_TICKERS = list(set(t for pie in PORTFOLIO.values() for t in pie["tickers"]))
    if sha:
        PORTFOLIO_SHA = sha
    log.info(f"Portfolio: {len(ALL_TICKERS)} ticker unici, {len(PORTFOLIO)} PIE")

# ── DATABASE ─────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("agent_state.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS recommendations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rec_type TEXT, details TEXT, t212_instructions TEXT,
        created_at TEXT, status TEXT DEFAULT 'pending')""")
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

# ── TELEGRAM ─────────────────────────────────────────────────
def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10)
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
    return False

def send_with_buttons(message, rec_id, has_t212=True):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    buttons = []
    if has_t212:
        buttons.append({"text": "✅ Vedi istruzioni T212", "callback_data": f"t212_{rec_id}"})
    buttons.append({"text": "📝 Solo nota", "callback_data": f"note_{rec_id}"})
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "HTML", "disable_web_page_preview": True,
                  "reply_markup": {"inline_keyboard": [buttons]}},
            timeout=10)
        return r.ok
    except Exception as e:
        log.error(f"Telegram buttons error: {e}")
    return False

def send_long(message, rec_id=None, has_t212=False):
    """Invia messaggio lungo spezzandolo se necessario."""
    chunks = []
    while len(message) > 3800:
        split = message[:3800].rfind("\n")
        if split < 0:
            split = 3800
        chunks.append(message[:split])
        message = message[split:]
    chunks.append(message)

    for i, chunk in enumerate(chunks):
        is_last = (i == len(chunks) - 1)
        if is_last and rec_id is not None:
            send_with_buttons(chunk, rec_id, has_t212)
        else:
            send_telegram(chunk)
        time.sleep(0.5)

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
            update_msg = ""
            portfolio_data, sha = load_portfolio_from_github()
            if portfolio_data:
                ok, result = update_portfolio_weights_on_github(details, portfolio_data, sha)
                if ok:
                    reload_portfolio(portfolio_data, sha)
                    update_msg = f"\n\n✅ <b>Pesi aggiornati</b> in {result} — le prossime analisi useranno i nuovi pesi."
                else:
                    update_msg = f"\n\n⚠️ Aggiornamento automatico non riuscito ({result})."

            # Genera istruzioni T212 se non presenti
            instructions = t212_instructions or generate_t212_instructions(details)
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
        send_telegram("📝 <i>Annotato. Nessuna azione. I pesi rimangono invariati.</i>")

# ── AGGIORNAMENTO PESI SU GITHUB ─────────────────────────────
def extract_new_weights(text):
    import re
    weights = {}
    patterns = [
        r"([A-Za-z &']+):\s*\*{0,2}(\d{1,3})%\*{0,2}\s*[(]era",
        r"-\s*([A-Za-z &']+):\s*\*{0,2}(\d{1,3})%\*{0,2}",
    ]
    for pattern in patterns:
        for name, pct in re.findall(pattern, text):
            name = name.strip().strip("*").strip()
            if len(name) > 2:
                weights[name] = int(pct)
    return weights

def find_pie_for_weights(weights, portfolio_data):
    ticker_names = portfolio_data.get("ticker_names", {})
    best_pie, best_count = None, 0
    for pie_name, pie_data in portfolio_data["pies"].items():
        count = sum(
            1 for wn in weights
            for t in pie_data["tickers"]
            if wn.lower() in ticker_names.get(t, t).lower()
            or ticker_names.get(t, t).lower() in wn.lower()
        )
        if count > best_count:
            best_count, best_pie = count, pie_name
    return best_pie if best_count >= 2 else None

def update_portfolio_weights_on_github(analysis_text, portfolio_data, sha):
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        return False, "GitHub non configurato"
    weights = extract_new_weights(analysis_text)
    if not weights:
        return False, "Nessun peso trovato"
    pie_name = find_pie_for_weights(weights, portfolio_data)
    if not pie_name:
        return False, "PIE non identificato"
    if abs(sum(weights.values()) - 100) > 5:
        return False, f"Pesi sommano a {sum(weights.values())}%"

    ticker_names = portfolio_data.get("ticker_names", {})
    if "pie_weights" not in portfolio_data:
        portfolio_data["pie_weights"] = {}
    pie_weights = {}
    for wn, pct in weights.items():
        for t in portfolio_data["pies"][pie_name]["tickers"]:
            if wn.lower() in ticker_names.get(t, t).lower():
                pie_weights[t] = pct
                break
    if pie_weights:
        portfolio_data["pie_weights"][pie_name] = pie_weights
        portfolio_data["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        portfolio_data["last_change"] = f"{pie_name} aggiornato"

    import base64
    content_str = json.dumps(portfolio_data, indent=2, ensure_ascii=False)
    content_b64 = base64.b64encode(content_str.encode()).decode()
    try:
        r = requests.put(
            f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{PORTFOLIO_FILE}",
            headers={"Authorization": f"token {GITHUB_TOKEN}"},
            json={"message": f"Aggiornamento pesi {pie_name}",
                  "content": content_b64, "sha": sha},
            timeout=15)
        if r.ok:
            return True, pie_name
        return False, f"GitHub error {r.status_code}"
    except Exception as e:
        return False, str(e)

# ── CLAUDE CON WEB SEARCH ────────────────────────────────────
SYSTEM_PROMPT = """Sei un analista finanziario senior con 20 anni di esperienza nella gestione istituzionale.

FILOSOFIA FONDAMENTALE:
Gestisci questo portafoglio come un fondo pensione o un endowment universitario.
Non reagisci mai alle fluttuazioni di prezzo. Agisci solo quando cambia la TESI STRUTTURALE.
Una tesi cambia quando: dividendo tagliato permanentemente, frode contabile, perdita del moat competitivo,
cambio radicale del settore, acquisizione che distrugge valore, indagine regolatoria seria.
Una tesi NON cambia per: volatilità di mercato, notizie geopolitiche temporanee, fluttuazioni trimestrali.

PORTAFOGLIO — 18 PIE, 100 TITOLI, 4 TIER:

PIE01 Aristocrats USA (Tier 1, 8%): Procter & Gamble 16%, Johnson & Johnson 15%, Coca-Cola 14%, PepsiCo 12%, Abbott 11%, Medtronic 11%, Walmart 10%, Emerson Electric 11%
PIE02 Aristocrats EU (Tier 1, 7%): Air Liquide 18%, Nestle 15%, L'Oreal 14%, Sika 13%, Wolters Kluwer 12%, Dassault Systemes 12%, Linde 16%
PIE03 Aristocrats Asia (Tier 1, 7%): DBS Group 38%, Toyota 30%, Sony Group 20%, Commonwealth Bank 12%
PIE04 Champions Energia (Tier 1, 6%): ExxonMobil 18%, Chevron 16%, Williams Companies 18%, Enbridge 16%, TotalEnergies 8%, EOG Resources 12%, Constellation Energy 12%
PIE05 Champions Finanza (Tier 1, 6%): HSBC 18%, AXA 16%, Allianz 16%, AIG 14%, UniCredit 12%, BNP Paribas 12%, Macquarie 12%
PIE06 REIT Growth (Tier 1, 6%): Realty Income 22%, Prologis 18%, American Tower 18%, Equinix 14%, WP Carey 14%, American Water 14%
PIE07 Quality Tech (Tier 2, 6%): ASML 22%, Microsoft 20%, Texas Instruments 18%, Apple 16%, SAP 14%, Broadcom 10%
PIE08 Quality Lusso (Tier 2, 6%): LVMH 24%, Hermes 20%, Richemont 16%, Ferrari 16%, Moncler 12%, Estee Lauder 12%
PIE09 Quality Healthcare (Tier 2, 5%): Johnson & Johnson 18%, Eli Lilly 16%, Novo Nordisk 14%, AstraZeneca 14%, Thermo Fisher 12%, Roche 12%, UnitedHealth 14%
PIE10 Quality Difesa (Tier 2, 5%): Lockheed Martin 18%, Northrop Grumman 16%, Rheinmetall 16%, BAE Systems 14%, Airbus 14%, BWX Technologies 12%, General Dynamics 10%
PIE11 Quality Chip (Tier 2, 4%): TSMC 28%, Samsung 22%, SK Hynix 18%, Qualcomm 16%, Tokyo Electron 16%
PIE12 Quality Infrastrutture (Tier 2, 4%): Brookfield Infrastructure 26%, Vinci 22%, Ferrovial 20%, Getlink 17%, National Grid 15%
PIE13 Utility Nucleare (Tier 3, 6%): Constellation Energy 18%, Enel 16%, Iberdrola 15%, Entergy 13%, Snam 12%, Terna 12%, Dominion Energy 14%
PIE14 Consumer Staples (Tier 3, 5%): Procter & Gamble 20%, Coca-Cola 18%, PepsiCo 16%, Unilever 16%, Costco 16%, Colgate-Palmolive 14%
PIE15 Gas Industriali (Tier 3, 5%): Air Liquide 28%, Linde 24%, Sika 20%, Sherwin-Williams 14%, Air Products 14%
PIE16 Midstream Pipeline (Tier 3, 4%): Williams Companies 28%, Enbridge 24%, Kinder Morgan 20%, TC Energy 16%, PPL Corporation 12%
PIE17 AI Tech (Tier 4, 6%): NVIDIA 25%, Alphabet 22%, Meta 20%, Amazon 18%, AMD 10%, China Internet ETF 5%
PIE18 EM Growth (Tier 4, 4%): Infosys 20%, HDFC Bank 18%, Itau Unibanco 16%, Vale 14%, Reliance Industries 12%, China Internet ETF 12%, ICICI Bank 8%

REGOLE RACCOMANDAZIONI OPERATIVE:
Quando modifichi un PIE elenca SEMPRE tutti i titoli con % precise che sommano a 100%.
Motiva ogni modifica in una frase. No trading tattico. Solo cambi strutturali."""

def claude_with_search(prompt, max_tokens=2000):
    if not ANTHROPIC_API_KEY:
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514",
                  "max_tokens": max_tokens,
                  "system": SYSTEM_PROMPT,
                  "tools": [{"type": "web_search_20250305", "name": "web_search"}],
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=120)
        if r.ok:
            parts = [b["text"] for b in r.json().get("content", []) if b.get("type") == "text"]
            return "\n".join(parts) if parts else None
        log.error(f"Claude error: {r.status_code}")
    except Exception as e:
        log.error(f"Claude exception: {e}")
    return None

def generate_t212_instructions(analysis):
    prompt = (
        f"Basandoti su questa analisi:\n\n{analysis}\n\n"
        f"Genera istruzioni PRATICHE e SPECIFICHE per Trading 212:\n"
        f"Per ogni PIE da modificare:\n"
        f"1. Apri T212 → sezione Pie → nome del PIE\n"
        f"2. Clicca Modifica pesi\n"
        f"3. Lista tutti i titoli con i nuovi pesi precisi\n"
        f"4. Clicca Ribilancia\n\n"
        f"Se non ci sono modifiche scrivi: 'Nessuna azione — mantieni i pesi attuali.'"
    )
    return claude_with_search(prompt, max_tokens=600)

def has_operational_recommendation(text):
    import re
    keywords = ["riduci ","aumenta ","sostituisci ","nuova allocazione",
                "riduzione peso","% (era","consiglio operativo","modifica il peso"]
    text_lower = text.lower()
    count = sum(1 for kw in keywords if kw in text_lower)
    peso_changes = re.findall(r"\d+%.{1,10}era.{1,5}\d+%", text_lower)
    dal_al = re.findall(r"dal \d+% al \d+%", text_lower)
    return count >= 2 or len(peso_changes) >= 1 or len(dal_al) >= 1

# ── WEEKLY REVIEW — lunedi 09:00 CET ─────────────────────────
def send_weekly_review():
    now = datetime.now(timezone.utc)
    log.info("Weekly Review — analisi istituzionale settimanale...")

    tutti_titoli = [
        "Procter & Gamble","Johnson & Johnson","Coca-Cola","PepsiCo","Abbott","Medtronic","Walmart","Emerson Electric",
        "Air Liquide","Nestle","L'Oreal","Sika","Wolters Kluwer","Dassault Systemes","Linde",
        "DBS Group","Toyota","Sony Group","Commonwealth Bank",
        "ExxonMobil","Chevron","Williams Companies","Enbridge","TotalEnergies","EOG Resources","Constellation Energy",
        "HSBC","AXA","Allianz","AIG","UniCredit","BNP Paribas","Macquarie",
        "Realty Income","Prologis","American Tower","Equinix","WP Carey","American Water",
        "ASML","Microsoft","Texas Instruments","Apple","SAP","Broadcom",
        "LVMH","Hermes","Richemont","Ferrari","Moncler","Estee Lauder",
        "Eli Lilly","Novo Nordisk","AstraZeneca","Thermo Fisher","Roche","UnitedHealth",
        "Lockheed Martin","Northrop Grumman","Rheinmetall","BAE Systems","Airbus","BWX Technologies","General Dynamics",
        "TSMC","Samsung","SK Hynix","Qualcomm","Tokyo Electron",
        "Brookfield Infrastructure","Vinci","Ferrovial","Getlink","National Grid",
        "Enel","Iberdrola","Entergy","Snam","Terna","Dominion Energy",
        "Unilever","Costco","Colgate-Palmolive","Sherwin-Williams","Air Products","Kinder Morgan","TC Energy","PPL Corporation",
        "NVIDIA","Alphabet","Meta","Amazon","AMD",
        "Infosys","HDFC Bank","Itau Unibanco","Vale","ICICI Bank","Reliance Industries"
    ]

    prompt = (
        f"Oggi e lunedi {now.strftime('%d/%m/%Y')} — Weekly Review istituzionale del Factor Portfolio.\n\n"
        f"Cerca le notizie della settimana scorsa su questi 100 titoli:\n"
        f"{', '.join(tutti_titoli)}\n\n"
        f"Analizza con la mentalita di un gestore istituzionale senior:\n"
        f"NON commentare le fluttuazioni di prezzo.\n"
        f"Concentrati SOLO su eventi che cambiano la TESI STRUTTURALE:\n"
        f"tagli dividendo, perdita moat, frodi, cambi settoriali permanenti, M&A rilevanti.\n\n"
        f"STRUTTURA OBBLIGATORIA:\n\n"
        f"1. VALUTAZIONE SETTIMANA (1-5 stelle)\n"
        f"   Una frase sul contesto macro rilevante per il portafoglio.\n\n"
        f"2. EVENTI CHE CAMBIANO LA TESI (se presenti)\n"
        f"   Per ogni evento: Titolo | PIE | Cosa e cambiato | Impatto sulla tesi | Azione\n"
        f"   Se nessun evento strutturale: scrivi 'Nessun evento strutturale — tesi intatte.'\n\n"
        f"3. TITOLI SOTTO OSSERVAZIONE\n"
        f"   Max 3 titoli da monitorare nelle prossime settimane con motivazione.\n\n"
        f"4. EARNINGS SETTIMANA CORRENTE\n"
        f"   Solo titoli del portafoglio con earnings attesi questa settimana.\n\n"
        f"5. STATO PIE PER TIER\n"
        f"   Tier 1 Aristocrats: streak dividendi intatte? Novita rilevanti?\n"
        f"   Tier 2 Quality: moat competitivi invariati?\n"
        f"   Tier 3 Low Volatility: stabilita confermata?\n"
        f"   Tier 4 Momentum: tesi growth reggono?\n\n"
        f"6. RACCOMANDAZIONI OPERATIVE\n"
        f"   SOLO se un evento strutturale giustifica una modifica.\n"
        f"   Elenca TUTTI i titoli del PIE con % precise che sommano a 100%.\n"
        f"   Se nessuna modifica necessaria: 'Portafoglio solido — nessuna azione.'\n\n"
        f"7. FRASE DELLA SETTIMANA\n"
        f"   Un insight strategico per la settimana che inizia."
    )

    analysis = claude_with_search(prompt, max_tokens=2500)
    if not analysis:
        log.error("Weekly Review fallita")
        return

    msg = (
        f"📊 <b>WEEKLY REVIEW — {now.strftime('%d/%m/%Y')}</b>\n"
        f"<i>Analisi istituzionale · 18 PIE · 100 titoli</i>\n\n"
        f"{analysis}\n\n"
        f"<i>Factor Portfolio · Buy & Hold Dividend Growth · Claude + Web Search</i>"
    )

    has_t212 = has_operational_recommendation(analysis)
    if has_t212:
        t212 = generate_t212_instructions(analysis)
        rec_id = save_recommendation("weekly_review", analysis, t212 or "")
    else:
        rec_id = save_recommendation("weekly_review", analysis, "")

    send_long(msg, rec_id, has_t212)

# ── SCHEDULER ─────────────────────────────────────────────────
def run_scheduler():
    # Weekly Review ogni lunedi alle 07:00 UTC = 09:00 CET
    schedule.every().monday.at("07:00").do(send_weekly_review)
    # Callback bottoni ogni minuto
    schedule.every(1).minutes.do(check_callbacks)

    log.info("Scheduler v3 avviato — Weekly Review ogni lunedi 09:00 CET")
    send_telegram(
        "🚀 <b>Portfolio Agent v3 avviato</b>\n\n"
        "<b>Filosofia:</b> gestione istituzionale buy & hold\n"
        "<b>Frequenza:</b> Weekly Review ogni lunedi 09:00 CET\n\n"
        "📊 Analisi completa 18 PIE · 100 titoli\n"
        "🎯 Solo eventi strutturali — no rumore di mercato\n"
        "✅ Vedi istruzioni T212 → solo con raccomandazioni concrete\n"
        "📝 Solo nota → annota senza azione\n\n"
        "<i>Prossima review: lunedi mattina</i>"
    )

    while True:
        schedule.run_pending()
        time.sleep(30)

# ── MAIN ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Portfolio Agent v3 — Gestione Istituzionale Buy & Hold")
    init_db()
    data, sha = load_portfolio_from_github()
    if data:
        reload_portfolio(data, sha)
        log.info("Portfolio caricato da GitHub")
    run_scheduler()
