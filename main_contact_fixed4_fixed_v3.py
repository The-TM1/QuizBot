import os, json, time, random, logging, sqlite3, asyncio, traceback, re
from threading import Thread
from math import ceil
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    PollAnswerHandler, ContextTypes, filters
)
from dotenv import load_dotenv

# ------------ Config / ENV ------------
load_dotenv("secrets.env")
OWNER_ID = 5902126578
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "db.sqlite3")
ADMIN_IDS_ENV = os.getenv("ADMIN_ID", "")  # optional comma list

PAGE_SIZE = 8
LB_PAGE = 20
DEFAULT_OPEN_PERIOD = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("quizbot")

# ------------ Database ------------
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")

def _cols(table): return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
def _add_col(table, col, decl):
    if col not in _cols(table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl};")
        conn.commit()

def db_init():
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);")
    c.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            chat_id INTEGER,
            last_seen INTEGER,
            is_banned INTEGER DEFAULT 0
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS quizzes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            options_json TEXT NOT NULL,
            correct INTEGER NOT NULL,
            explanation TEXT,
            subject TEXT,
            chapter TEXT,
            created_at INTEGER NOT NULL,
            added_by INTEGER,
            ai_generated INTEGER DEFAULT 0
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            total INTEGER NOT NULL,
            open_period INTEGER NOT NULL,
            started_at INTEGER NOT NULL,
            finished_at INTEGER,
            state TEXT NOT NULL,
            current_index INTEGER DEFAULT 0
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS session_items(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            quiz_id INTEGER NOT NULL,
            poll_id TEXT,
            message_id INTEGER,
            sent_at INTEGER,
            chosen INTEGER,
            is_correct INTEGER,
            closed_at INTEGER,
            idx INTEGER
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS active_polls(
            poll_id TEXT PRIMARY KEY,
            session_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS admin_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER NOT NULL,
            quiz_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
    """)
    conn.commit()
    # migrations (idempotent)
    _add_col("quizzes", "explanation", "TEXT")
    _add_col("quizzes", "subject", "TEXT")
    _add_col("quizzes", "chapter", "TEXT")
    _add_col("quizzes", "ai_generated", "INTEGER DEFAULT 0")
    _add_col("users", "is_banned", "INTEGER DEFAULT 0")
    _add_col("sessions", "current_index", "INTEGER DEFAULT 0")
    _add_col("sessions", "finished_at", "INTEGER")

# ------------ Helpers ------------

pending_contact = {}

async def busy(chat, action=ChatAction.TYPING, secs=0.25):
    await asyncio.sleep(secs)

def sget(key, default=None):
    r = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r[0] if r else default

def sset(key, value):
    conn.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    ); conn.commit()

def admin_ids_from_settings():
    if sget("admin_ids"):
        return {int(x) for x in sget("admin_ids").split(",") if x.strip().isdigit()}
    if ADMIN_IDS_ENV:
        return {int(x) for x in ADMIN_IDS_ENV.split(",") if x.strip().isdigit()}
    return set()

def is_owner(uid): return int(uid) == int(OWNER_ID)
def is_admin(uid): return is_owner(uid) or uid in admin_ids_from_settings()
def add_admin(uid):
    ids = admin_ids_from_settings(); ids.add(int(uid))
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))
def remove_admin(uid):
    ids = admin_ids_from_settings()
    if int(uid) in ids:
        ids.remove(int(uid))
        sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

def is_user_banned(uid):
    r = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(r and int(r["is_banned"]) == 1)

def _uname_row(u):
    if not u: return "unknown"
    if u.get("username") if isinstance(u, dict) else u["username"]:
        return f"@{u['username']}"
    n = " ".join(filter(None, [u.get("first_name") if isinstance(u, dict) else u["first_name"],
                               u.get("last_name") if isinstance(u, dict) else u["last_name"]]))
    return n or f"id:{u.get('user_id') if isinstance(u, dict) else u['user_id']}"

async def notify_owner_unauthorized(bot, offender_id, action, details=""):
    if is_owner(offender_id): return
    try:
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (offender_id,)).fetchone()
        await bot.send_message(
            OWNER_ID,
            "🚨 *Unauthorized attempt*\n"
            f"User: {_uname_row(u)} (id:{offender_id})\n"
            f"Action: `{action}`\n"
            f"Details: {details or '-'}",
            parse_mode="Markdown"
        )
    except Exception:
        pass

def upsert_user(update: Update):
    u = update.effective_user
    c = update.effective_chat
    if not u or not c: return False
    existed = conn.execute("SELECT 1 FROM users WHERE user_id=?", (u.id,)).fetchone() is not None
    conn.execute(
        "INSERT INTO users(user_id,username,first_name,last_name,chat_id,last_seen) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, "
        "first_name=excluded.first_name, last_name=excluded.last_name, "
        "chat_id=excluded.chat_id, last_seen=excluded.last_seen",
        (u.id, u.username, u.first_name, u.last_name, c.id, int(time.time()))
    ); conn.commit()
    return not existed

def sanitize_for_poll(q, options, expl):
    def trunc(s, n): 
        s = (s or "").strip()
        return s if len(s) <= n else s[:n-1] + "…"
    q = trunc(q, 292)
    options = [trunc(o, 100) for o in (options or []) if (o or "").strip()]
    # dedupe + cap 10
    seen, clean = set(), []
    for o in options:
        if o in seen: continue
        seen.add(o); clean.append(o)
    options = clean[:10]
    if len(options) < 2: raise ValueError("Need at least 2 options.")
    expl = trunc(expl, 200) if expl else None
    return q, options, expl

# ------------ Parsing helpers for commands ------------
def _quoted_parts(s: str):
    return re.findall(r'"([^"]+)"', s)

def _subject_exists(name, ai=False):
    sql = "SELECT 1 FROM quizzes WHERE subject=?"
    if ai: sql += " AND ai_generated=1"
    else: sql += " AND COALESCE(ai_generated,0)=0"
    return conn.execute(sql + " LIMIT 1", (name,)).fetchone() is not None

def _chapter_exists(subj, chap, ai=False):
    sql = "SELECT 1 FROM quizzes WHERE subject=? AND chapter=?"
    if ai: sql += " AND ai_generated=1"
    else: sql += " AND COALESCE(ai_generated,0)=0"
    return conn.execute(sql + " LIMIT 1", (subj, chap)).fetchone() is not None

def parse_subject_chapter(raw: str, ai=False):
    raw = raw.strip()
    # 1) quoted "Subject" "Chapter"
    qp = _quoted_parts(raw)
    if len(qp) >= 2:
        return qp[0].strip(), qp[1].strip()
    # 2) with '|' or '->'
    if "|" in raw:
        a, b = raw.split("|", 1); return a.strip(), b.strip()
    if "->" in raw:
        a, b = raw.split("->", 1); return a.strip(), b.strip()
    # 3) token sweep with DB validation
    toks = raw.split()
    for i in range(1, len(toks)):
        subj = " ".join(toks[:i]).strip()
        chap = " ".join(toks[i:]).strip()
        if _subject_exists(subj, ai) and _chapter_exists(subj, chap, ai):
            return subj, chap
    # fallback: guess split in the middle
    mid = len(toks)//2
    return " ".join(toks[:mid]).strip(), " ".join(toks[mid:]).strip()

def parse_old_new(raw: str):
    raw = raw.strip()
    qp = _quoted_parts(raw)
    if len(qp) >= 2: return qp[0].strip(), qp[1].strip()
    if "|" in raw:
        a, b = raw.split("|", 1); return a.strip(), b.strip()
    if "->" in raw:
        a, b = raw.split("->", 1); return a.strip(), b.strip()
    toks = raw.split()
    mid = len(toks)//2
    return " ".join(toks[:mid]).strip(), " ".join(toks[mid:]).strip()

def parse_subject_old_new_chap(raw: str, ai=False):
    # expects 3 items: subject, old_chap, new_chap
    qp = _quoted_parts(raw)
    if len(qp) >= 3: return qp[0].strip(), qp[1].strip(), qp[2].strip()
    if "|" in raw:
        a, rest = raw.split("|", 1)
        b, c = parse_old_new(rest)
        return a.strip(), b.strip(), c.strip()
    if "->" in raw:
        # subject | old -> new
        parts = raw.split("->")
        left = parts[0]
        new = parts[1]
        if "|" in left:
            subj, old = [x.strip() for x in left.split("|", 1)]
            return subj, old, new.strip()
    # token sweep: try to find subject then split remaining by middle
    toks = raw.split()
    for i in range(1, len(toks)-1):
        subj = " ".join(toks[:i]).strip()
        rest = " ".join(toks[i:]).strip()
        if _subject_exists(subj, ai):
            old, new = parse_old_new(rest)
            return subj, old, new
    # fallback
    if len(toks) >= 3:
        return toks[0], toks[1], " ".join(toks[2:])
    return "", "", ""

# ------------ Data Views ------------
def list_subjects_with_counts(ai_only=False):
    if ai_only:
        cur = conn.execute("SELECT subject s, COUNT(DISTINCT chapter) chs, COUNT(*) qs "
                           "FROM quizzes WHERE ai_generated=1 GROUP BY s ORDER BY qs DESC, s")
    else:
        cur = conn.execute("SELECT subject s, COUNT(DISTINCT chapter) chs, COUNT(*) qs "
                           "FROM quizzes WHERE COALESCE(ai_generated,0)=0 GROUP BY s ORDER BY qs DESC, s")
    return [(r["s"], r["chs"], r["qs"]) for r in cur.fetchall()]

def list_chapters_with_counts(subject, ai_only=False):
    if ai_only:
        cur = conn.execute("SELECT chapter c, COUNT(*) qs FROM quizzes "
                           "WHERE subject=? AND ai_generated=1 GROUP BY c ORDER BY qs DESC, c", (subject,))
    else:
        cur = conn.execute("SELECT chapter c, COUNT(*) qs FROM quizzes "
                           "WHERE subject=? AND COALESCE(ai_generated,0)=0 GROUP BY c ORDER BY qs DESC, c", (subject,))
    return [(r["c"], r["qs"]) for r in cur.fetchall()]

def has_ai_quizzes():
    return conn.execute("SELECT 1 FROM quizzes WHERE ai_generated=1 LIMIT 1").fetchone() is not None

# ------------ Menus ------------
def main_menu(uid: int):
    rows = [
        [InlineKeyboardButton("▶️ Start quiz", callback_data="u:start")],
        [InlineKeyboardButton("📊 My stats", callback_data="u:stats"),
         InlineKeyboardButton("📨 Contact admin", callback_data="u:contact")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="u:help")]
    ]
    if has_ai_quizzes():
        rows.insert(1, [InlineKeyboardButton("🤖 AI Gen Quiz", callback_data="uai:start")])
    if is_admin(uid):
        rows.insert(1, [InlineKeyboardButton("🛠 Admin panel", callback_data="a:panel")])
    if is_owner(uid):
        rows[1].insert(0, InlineKeyboardButton("🏆 Leaderboard", callback_data="u:lb"))
    return InlineKeyboardMarkup(rows)

def admin_menu(uid: int):
    if not is_owner(uid):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add quiz", callback_data="a:add")],
            [InlineKeyboardButton("⛔️ Delete last", callback_data="a:dellast")],
            [InlineKeyboardButton("⬅️ Back", callback_data="a:back")]
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add quiz", callback_data="a:add")],
        [InlineKeyboardButton("📥 Import JSON", callback_data="a:import"),
         InlineKeyboardButton("📤 Export JSON", callback_data="a:export_menu")],
        [InlineKeyboardButton("⛔️ Delete last", callback_data="a:dellast"),
         InlineKeyboardButton("#️⃣ Count", callback_data="a:count")],
        [InlineKeyboardButton("📣 Broadcast", callback_data="a:broadcast"),
         InlineKeyboardButton("🔎 Search Quiz id", callback_data="a:search_id")],
        [InlineKeyboardButton("👑 Admins", callback_data="a:admins"),
         InlineKeyboardButton("👥 Users", callback_data="a:users")],
        [InlineKeyboardButton("🗂 Export users DB", callback_data="a:export_users"),
         InlineKeyboardButton("📥 Import users DB", callback_data="a:import_users")],
        [InlineKeyboardButton("🤖 Add AI gen Quiz", callback_data="a:ai_import")],
        [InlineKeyboardButton("⬅️ Back", callback_data="a:back")]
    ])

# ------------ Basic Commands ------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_new = upsert_user(update)
    uid = update.effective_user.id
    if not admin_ids_from_settings():
        add_admin(uid)  # first runner becomes admin; owner can prune later
    if is_user_banned(uid):
        await update.effective_chat.send_message("You are banned from using this bot.")
        return
    hi = update.effective_user.first_name or "there"
    await update.effective_chat.send_message(
        f"Hey {hi}, welcome to our *Madhyamik Helper Quiz Bot! 🎓*",
        parse_mode="Markdown",
        reply_markup=main_menu(uid)
    )
    if is_new:
        total = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        try:
            await context.bot.send_message(
                OWNER_ID,
                "✅New user joined\n"
                f"Username: {_uname_row(u)}\n"
                f"Userid: {uid}\n\n\n"
                f"Total users: {total}"
            )
        except Exception:
            pass

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if is_user_banned(update.effective_user.id):
        await update.message.reply_text("You are banned from using this bot."); return
    await update.message.reply_text(
        "Start → Subject → Chapter → Timer (or Without Timer) → I am ready!\n"
        "Use /stop to cancel anytime."
    )

# ------------ User flow: Subjects & Chapters (Human + AI) ------------
async def user_subjects(update: Update, page: int = 0):
    uid = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    subs = list_subjects_with_counts(ai_only=False)
    if not subs:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
        await edit_or_reply(update, "No subjects added yet.", kb)
        return
    pages = max(1, ceil(len(subs) / PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    slice_ = subs[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    rows = [[InlineKeyboardButton(f"📚 {s} (chapters: {chs} | quizzes: {qs})", callback_data=f"u:subj:{s}")]
            for (s, chs, qs) in slice_]
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:subjp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages - 1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:subjp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    await edit_or_reply(update, "Home › Subjects\n\nChoose a subject:", InlineKeyboardMarkup(rows))

async def user_chapters(update: Update, subject: str, page: int = 0):
    # store chosen subject for later steps
    if isinstance(update, Update):
        ctx_user_data = {}  # not used; kept for compatibility

    # put into callback context via button handler; we still set it here for safety
    try:
        # If this was called from btn(), context.user_data is already set.
        pass
    except Exception:
        pass
    chs = list_chapters_with_counts(subject, ai_only=False)
    if not chs:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:startback")]])
        await edit_or_reply(update, f"No chapters found in *{subject}*.", kb, parse_mode="Markdown")
        return
    pages = max(1, ceil(len(chs) / PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    slice_ = chs[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"u:chap:{c}")]
            for (c, qs) in slice_]
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:chpp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages - 1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:chpp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:startback")])
    await edit_or_reply(update, f"Home › Subjects › *{subject}*\n\nChoose a chapter:",
                        InlineKeyboardMarkup(rows), parse_mode="Markdown")

async def user_subjects_ai(update: Update, page: int = 0):
    subs = list_subjects_with_counts(ai_only=True)
    if not subs:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
        await edit_or_reply(update, "No AI-generated subjects available.", kb)
        return
    pages = max(1, ceil(len(subs) / PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    slice_ = subs[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    rows = [[InlineKeyboardButton(f"🤖 {s} (chapters: {chs} | quizzes: {qs})", callback_data=f"uai:subj:{s}")]
            for (s, chs, qs) in slice_]
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"uai:subjp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages - 1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"uai:subjp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    await edit_or_reply(update, "AI Gen › Subjects\n\nChoose a subject:", InlineKeyboardMarkup(rows))

async def user_chapters_ai(update: Update, subject: str, page: int = 0):
    chs = list_chapters_with_counts(subject, ai_only=True)
    if not chs:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="uai:startback")]])
        await edit_or_reply(update, f"No chapters found in *{subject}*.", kb, parse_mode="Markdown")
        return
    pages = max(1, ceil(len(chs) / PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    slice_ = chs[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"uai:chap:{c}")]
            for (c, qs) in slice_]
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"uai:chpp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages - 1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"uai:chpp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="uai:startback")])
    await edit_or_reply(update, f"AI Gen › Subjects › *{subject}*\n\nChoose a chapter:",
                        InlineKeyboardMarkup(rows), parse_mode="Markdown")

# ------------ UI helpers ------------
async def edit_or_reply(obj, text, markup=None, **kwargs):
    if hasattr(obj, "callback_query") and obj.callback_query:
        await obj.callback_query.message.edit_text(text, reply_markup=markup, **kwargs)
    elif isinstance(obj, Update):
        await obj.effective_chat.send_message(text, reply_markup=markup, **kwargs)

# ------------ Timer & Pre-quiz ------------
async def timer_menu(update_or_query):
    chat = update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat
    await busy(chat)
    times = [15, 30, 45, 60]
    rows = [[InlineKeyboardButton(f"{t}s", callback_data=f"u:timer:{t}") for t in times]]
    rows.append([InlineKeyboardButton("Without Timer", callback_data="u:timer:0")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:chapback")])
    await edit_or_reply(update_or_query, "Home › Subjects › Chapter › Timer\n\nChoose time per question:",
                        InlineKeyboardMarkup(rows))

async def timer_menu_ai(update_or_query):
    chat = update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat
    await busy(chat)
    times = [15, 30, 45, 60]
    rows = [[InlineKeyboardButton(f"{t}s", callback_data=f"uai:timer:{t}") for t in times]]
    rows.append([InlineKeyboardButton("Without Timer", callback_data="uai:timer:0")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="uai:chapback")])
    await edit_or_reply(update_or_query, "AI Gen › Subjects › Chapter › Timer\n\nChoose time per question:",
                        InlineKeyboardMarkup(rows))

async def pre_quiz_screen(q, context: ContextTypes.DEFAULT_TYPE):
    subj = context.user_data.get("subject")
    chap = context.user_data.get("chapter")
    if "open_period" not in context.user_data:
        context.user_data["open_period"] = DEFAULT_OPEN_PERIOD
    op = int(context.user_data.get("open_period", DEFAULT_OPEN_PERIOD))
    timer_text = "Without Timer" if op == 0 else f"{op}s"
    txt = (f"Home › Subjects › {subj} › {chap} › Timer\n\n"
           f"Get ready!\n\nSubject: {subj}\nChapter: {chap}\nTimer: {timer_text}\n\n"
           "Press the button when ready. Send /stop to cancel.")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("I am ready!", callback_data="u:ready")],
                               [InlineKeyboardButton("⬅️ Back", callback_data="u:timerback")]])
    await q.message.edit_text(txt, reply_markup=kb)

async def pre_quiz_screen_ai(q, context: ContextTypes.DEFAULT_TYPE):
    subj = context.user_data.get("ai_subject")
    chap = context.user_data.get("ai_chapter")
    if "ai_open_period" not in context.user_data:
        context.user_data["ai_open_period"] = DEFAULT_OPEN_PERIOD
    op = int(context.user_data.get("ai_open_period", DEFAULT_OPEN_PERIOD))
    timer_text = "Without Timer" if op == 0 else f"{op}s"
    txt = (f"AI Gen › {subj} › {chap} › Timer\n\n"
           f"Get ready!\n\nSubject: {subj}\nChapter: {chap}\nTimer: {timer_text}\n\n"
           "Press the button when ready. Send /stop to cancel.")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("I am ready!", callback_data="uai:ready")],
                               [InlineKeyboardButton("⬅️ Back", callback_data="uai:timerback")]])
    await q.message.edit_text(txt, reply_markup=kb)

async def begin_quiz_session(q, context: ContextTypes.DEFAULT_TYPE):
    try:
        subj = context.user_data.get("subject"); chap = context.user_data.get("chapter")
        if not subj or not chap:
            await q.message.edit_text("Please choose Subject and Chapter first.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:start")]]))
            return
        uid = q.from_user.id
        if is_user_banned(uid):
            await q.message.edit_text("You are banned from using this bot.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
            return
        chat_id = q.message.chat.id
        op = int(context.user_data.get("open_period", DEFAULT_OPEN_PERIOD))

        # ✅ validate quizzes up-front
        ids = _collect_valid_quiz_ids(subj, chap, ai=False)
        if not ids:
            await q.message.edit_text("No valid quizzes found for this selection.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
            return
        random.shuffle(ids)

        # save for retry
        context.user_data["last_subject"] = subj
        context.user_data["last_chapter"] = chap
        context.user_data["last_open_period"] = op

        conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
        conn.execute("INSERT INTO sessions(user_id,chat_id,total,open_period,started_at,state) VALUES(?,?,?,?,?,?)",
                     (uid, chat_id, len(ids), op, int(time.time()), "running"))
        sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for i, qid in enumerate(ids):
            conn.execute("INSERT INTO session_items(session_id,quiz_id,poll_id,message_id,idx) VALUES(?,?,?,?,?)",
                         (sid, qid, "", 0, i))
        conn.commit()

        await q.message.edit_text("Quiz started! 🎯\nSend /stop to cancel.")
        await send_next_quiz(context.bot, sid)

    except Exception as e:
        log.error("begin_quiz_session error: %s\n%s", e, traceback.format_exc())
        try:
            await q.message.reply_text(
                "Couldn't start quiz due to an error. Please check your items and try again."
            )
        except Exception:
            pass

# ------------ Start sessions ------------
# --- validate & collect quiz ids before starting (handles bad data safely) ---
def _collect_valid_quiz_ids(subject: str, chapter: str, ai: bool):
    sql = "SELECT id, question, options_json, correct, explanation FROM quizzes WHERE subject=? AND chapter=? AND "
    sql += "ai_generated=1" if ai else "COALESCE(ai_generated,0)=0"
    rows = conn.execute(sql, (subject, chapter)).fetchall()
    valid = []
    for r in rows:
        try:
            opts = json.loads(r["options_json"])
            # reuse the same sanitizer the poll uses (lengths, min options, etc.)
            _q, _opts, _expl = sanitize_for_poll(r["question"], opts, r["explanation"])
            # ensure correct index still points inside the (possibly de-duplicated) options
            if 0 <= int(r["correct"]) < len(_opts):
                valid.append(int(r["id"]))
        except Exception:
            # skip bad quiz silently
            continue
    return valid

async def begin_quiz_session(q, context: ContextTypes.DEFAULT_TYPE):
    try:
        subj = context.user_data.get("subject")
        chap = context.user_data.get("chapter")
        if not subj or not chap:
            await q.message.edit_text(
                "Please choose Subject and Chapter first.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:start")]])
            )
            return
        uid = q.from_user.id
        if is_user_banned(uid):
            await q.message.edit_text(
                "You are banned from using this bot.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
            )
            return
        chat_id = q.message.chat.id
        op = int(context.user_data.get("open_period", DEFAULT_OPEN_PERIOD))

        # ✅ validate quizzes up-front (human-only)
        ids = _collect_valid_quiz_ids(subj, chap, ai=False)
        if not ids:
            await q.message.edit_text(
                "No valid quizzes found for this selection.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
            )
            return
        random.shuffle(ids)

        # save for retry
        context.user_data["last_subject"] = subj
        context.user_data["last_chapter"] = chap
        context.user_data["last_open_period"] = op

        conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
        conn.execute(
            "INSERT INTO sessions(user_id,chat_id,total,open_period,started_at,state) VALUES(?,?,?,?,?,?)",
            (uid, chat_id, len(ids), op, int(time.time()), "running")
        )
        sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for i, qid in enumerate(ids):
            conn.execute(
                "INSERT INTO session_items(session_id,quiz_id,poll_id,message_id,idx) VALUES(?,?,?,?,?)",
                (sid, qid, "", 0, i)
            )
        conn.commit()

        await q.message.edit_text("Quiz started! 🎯\nSend /stop to cancel.")
        await send_next_quiz(context.bot, sid)

    except Exception as e:
        log.error("begin_quiz_session error: %s\n%s", e, traceback.format_exc())
        try:
            await q.message.reply_text(
                "Couldn't start quiz due to an error. Please check your items and try again."
            )
        except Exception:
            pass

async def begin_quiz_session_ai(q, context: ContextTypes.DEFAULT_TYPE):
    try:
        subj = context.user_data.get("ai_subject")
        chap = context.user_data.get("ai_chapter")
        if not subj or not chap:
            await q.message.edit_text(
                "Please choose Subject and Chapter first.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="uai:start")]])
            )
            return
        uid = q.from_user.id
        if is_user_banned(uid):
            await q.message.edit_text(
                "You are banned from using this bot.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
            )
            return
        chat_id = q.message.chat.id
        op = int(context.user_data.get("ai_open_period", DEFAULT_OPEN_PERIOD))

        # ✅ validate quizzes up-front (AI-only)
        ids = _collect_valid_quiz_ids(subj, chap, ai=True)
        if not ids:
            await q.message.edit_text(
                "No valid AI quizzes found for this selection.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
            )
            return
        random.shuffle(ids)

        conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
        conn.execute(
            "INSERT INTO sessions(user_id,chat_id,total,open_period,started_at,state) VALUES(?,?,?,?,?,?)",
            (uid, chat_id, len(ids), op, int(time.time()), "running")
        )
        sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for i, qid in enumerate(ids):
            conn.execute(
                "INSERT INTO session_items(session_id,quiz_id,poll_id,message_id,idx) VALUES(?,?,?,?,?)",
                (sid, qid, "", 0, i)
            )
        conn.commit()

        await q.message.edit_text("AI Quiz started! 🤖🎯")
        await send_next_quiz(context.bot, sid)

    except Exception as e:
        log.error("begin_quiz_session_ai error: %s\n%s", e, traceback.format_exc())
        try:
            await q.message.reply_text(
                "Couldn't start AI quiz due to an error. Please check your items and try again."
            )
        except Exception:
            pass

# --- progress on answer; timer handled by fallback ---
async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ans = update.poll_answer
    pid = ans.poll_id
    chosen = ans.option_ids[0] if ans.option_ids else None
    row = conn.execute("SELECT * FROM active_polls WHERE poll_id=?", (pid,)).fetchone()
    if not row:
        return
    item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND poll_id=?", (row["session_id"], pid)).fetchone()
    if not item:
        return
    quiz = conn.execute("SELECT correct FROM quizzes WHERE id=?", (item["quiz_id"],)).fetchone()
    ok = 1 if chosen is not None and int(chosen) == int(quiz["correct"]) else 0
    conn.execute("UPDATE session_items SET chosen=?, is_correct=?, closed_at=? WHERE id=?",
                 (chosen, ok, int(time.time()), item["id"]))
    conn.execute("DELETE FROM active_polls WHERE poll_id=?", (pid,))
    srow = conn.execute("SELECT * FROM sessions WHERE id=?", (row["session_id"],)).fetchone()
    if not srow:
        conn.commit()
        return
    if srow["open_period"] > 0 or chosen is not None:
        conn.execute("UPDATE sessions SET current_index=current_index+1 WHERE id=?", (row["session_id"],))
        conn.commit()
        await send_next_quiz(context.bot, row["session_id"])
    else:
        conn.commit()

async def timeout_fallback(bot, session_id: int, poll_id: str, wait_secs: int):
    try:
        await asyncio.sleep(max(1, wait_secs))
        row = conn.execute("SELECT * FROM active_polls WHERE poll_id=?", (poll_id,)).fetchone()
        if not row:
            return
        item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND poll_id=?", (session_id, poll_id)).fetchone()
        if item:
            conn.execute("UPDATE session_items SET chosen=?, is_correct=?, closed_at=? WHERE id=?",
                         (-1, 0, int(time.time()), item["id"]))
        conn.execute("DELETE FROM active_polls WHERE poll_id=?", (poll_id,))
        conn.execute("UPDATE sessions SET current_index=current_index+1 WHERE id=?", (session_id,))
        conn.commit()
        await send_next_quiz(bot, session_id)
    except Exception as e:
        log.error("timeout_fallback error: %s\n%s", e, traceback.format_exc())

async def send_next_quiz(bot, session_id: int):
    try:
        srow = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
        if not srow or srow["state"] != "running":
            return
        idx = srow["current_index"]
        items = conn.execute("SELECT * FROM session_items WHERE session_id=? ORDER BY idx", (session_id,)).fetchall()

        if idx >= len(items):
            tot = len(items)
            correct = sum(1 for it in items if it["is_correct"] == 1)
            wrong = sum(1 for it in items if (it["chosen"] is not None and it["chosen"] >= 0 and it["is_correct"] == 0))
            missed = tot - correct - wrong
            elapsed = int(time.time()) - srow["started_at"]
            mins, secs = divmod(elapsed, 60)
            msg = (f"🏁 The quiz has finished!\n"
                   f"You answered *{tot}* questions:\n\n"
                   f"✅ Correct – *{correct}*\n"
                   f"❌ Wrong – *{wrong}*\n"
                   f"⌛️ Missed – *{missed}*\n"
                   f"⏱ Time – {mins} min {secs} sec")
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Try again", callback_data="u:retry"),
                                        InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
            await bot.send_message(srow["chat_id"], msg, parse_mode="Markdown", reply_markup=kb)
            conn.execute("UPDATE sessions SET state='finished', finished_at=? WHERE id=?", (int(time.time()), session_id))
            conn.commit()
            return

        it = items[idx]
        qrow = conn.execute("SELECT * FROM quizzes WHERE id=?", (it["quiz_id"],)).fetchone()
        q_text, q_opts, q_expl = sanitize_for_poll(qrow["question"], json.loads(qrow["options_json"]), qrow["explanation"])

        total = len(items)
        display_q = f"[{idx+1}/{total}] {q_text}"

        payload = dict(
            question=display_q,
            options=q_opts,
            type="quiz",
            correct_option_id=min(int(qrow["correct"]), max(0, len(q_opts)-1)),
            explanation=q_expl,
            is_anonymous=False
        )
        if srow["open_period"] > 0:
            payload["open_period"] = srow["open_period"]

        msg = await bot.send_poll(srow["chat_id"], **payload)

        conn.execute("UPDATE session_items SET poll_id=?, message_id=?, sent_at=? WHERE id=?",
                     (msg.poll.id, msg.message_id, int(time.time()), it["id"]))
        conn.execute("INSERT OR REPLACE INTO active_polls(poll_id,session_id,user_id) VALUES(?,?,?)",
                     (msg.poll.id, session_id, srow["user_id"]))
        conn.commit()

        # ✂️ No extra message; only /stop is used to cancel.

        if srow["open_period"] > 0:
            asyncio.create_task(timeout_fallback(bot, session_id, msg.poll.id, srow["open_period"] + 2))

    except Exception as e:
        err = f"send_next_quiz error: {e}"
        log.error(err + "\n" + traceback.format_exc())
        try:
            for aid in admin_ids_from_settings():
                await bot.send_message(aid, f"[Admin alert] {err}")
            if srow:
                await bot.send_message(srow["chat_id"], "Hmm, I couldn’t send the quiz. Please try again.")
        except Exception:
            pass

# ------------ Stats & Leaderboard (owner only) ------------
async def show_stats(q):
    uid = q.from_user.id
    r = conn.execute(
        "SELECT COUNT(si.id) tot, COALESCE(SUM(si.is_correct),0) ok, "
        "COALESCE(SUM(CASE WHEN si.chosen>=0 AND si.is_correct=0 THEN 1 ELSE 0 END),0) wrong "
        "FROM sessions s LEFT JOIN session_items si ON si.session_id=s.id "
        "WHERE s.user_id=?",
        (uid,)
    ).fetchone()
    tot = r["tot"] or 0
    ok = r["ok"] or 0
    wrong = r["wrong"] or 0
    missed = max(0, tot - (ok + wrong))
    txt = (
        "📊 *Your Stats*\n\n"
        f"✅ Correct — *{ok}*\n"
        f"❌ Wrong — *{wrong}*\n"
        f"⌛️ Missed — *{missed}*"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
    await q.message.edit_text(txt, parse_mode="Markdown", reply_markup=kb)

def leaderboard_page_rows(offset: int, limit: int):
    sql = """
    SELECT s.user_id AS uid, COALESCE(SUM(CASE WHEN si.is_correct=1 THEN 1 ELSE 0 END),0) AS ok,
           COUNT(si.id) tot
    FROM sessions s LEFT JOIN session_items si ON si.session_id = s.id
    GROUP BY s.user_id
    ORDER BY ok DESC, uid ASC
    LIMIT ? OFFSET ?"""
    return conn.execute(sql, (limit, offset)).fetchall()

def leaderboard_count():
    r = conn.execute("SELECT COUNT(DISTINCT user_id) c FROM sessions").fetchone()
    return r["c"] or 0

async def leaderboard(q, page=0):
    if not is_owner(q.from_user.id):
        await q.message.edit_text("Owner only.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
        return
    total_users = leaderboard_count()
    if total_users == 0:
        await q.message.edit_text("No data yet.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
        return
    pages = max(1, ceil(total_users / LB_PAGE))
    page = max(0, min(page, pages - 1))
    rows = leaderboard_page_rows(page * LB_PAGE, LB_PAGE)

    lines = [f"🏆 Leaderboard (page {page+1}/{pages}) — all users"]
    rank = page * LB_PAGE + 1
    for r in rows:
        uid = r["uid"]; score = r["ok"]; tot = r["tot"]
        urow = conn.execute("SELECT username, first_name, last_name FROM users WHERE user_id=?", (uid,)).fetchone()
        uname = ("@" + (urow["username"] or "")) if (urow and urow["username"]) else (
            " ".join(filter(None, [urow["first_name"] if urow else None, urow["last_name"] if urow else None])) or f"id:{uid}"
        )
        lines.append(f"{rank}. {uname} (id:{uid}) — {score}/{tot} correct")
        rank += 1

    nav = []
    if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:lbp:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if page < pages - 1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:lbp:{page+1}"))
    rows_kb = [nav] if nav else []
    rows_kb.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    await q.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(rows_kb))

# ------------ /delquiz with confirmation ------------
async def delquiz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Usage: /delquiz <quiz_id>")
        return
    try:
        qid = int(context.args[0])
    except:
        await update.message.reply_text("quiz_id must be a number.")
        return
    row = conn.execute("SELECT * FROM quizzes WHERE id=?", (qid,)).fetchone()
    if not row:
        await update.message.reply_text("Quiz not found.")
        return
    if not (is_owner(uid) or (is_admin(uid) and int(row["added_by"] or 0) == int(uid))):
        await notify_owner_unauthorized(context.bot, uid, "/delquiz", f"qid:{qid}")
        await update.message.reply_text("Only owner can delete arbitrary quizzes. Admins may delete only their own.")
        return
    # preview & confirm
    txt = f"Quiz #{row['id']} — {row['subject']} / {row['chapter']}\n\n{row['question']}\n\nOptions:\n"
    for i, o in enumerate(json.loads(row["options_json"])):
        mark = "✅" if i == row["correct"] else "▫️"
        txt += f"{mark} {o}\n"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm delete", callback_data=f"a:delquiz:{row['id']}")],
        [InlineKeyboardButton("⬅️ Cancel", callback_data="a:panel")]
    ])
    await update.message.reply_text(txt, reply_markup=kb)

# ------------ Export helpers ------------
def _export_items(where_sql: str = "", params: tuple = (), filename: str = "quizzes.json"):
    from io import BytesIO
    q = "SELECT * FROM quizzes"
    if where_sql:
        q += " WHERE " + where_sql
    q += " ORDER BY id"
    cur = conn.execute(q, params)
    items = []
    for r in cur.fetchall():
        d = dict(r)
        d["options"] = json.loads(r["options_json"])
        d.pop("options_json", None)
        items.append(d)
    data = json.dumps(items, ensure_ascii=False, indent=2).encode("utf-8")
    bio = BytesIO(data); bio.name = filename
    return bio

def _export_users_blob(filename="users.json"):
    from io import BytesIO
    cur = conn.execute("SELECT user_id, username, first_name, last_name, chat_id, last_seen, is_banned FROM users ORDER BY user_id")
    users = [dict(r) for r in cur.fetchall()]
    data = json.dumps(users, ensure_ascii=False, indent=2).encode("utf-8")
    bio = BytesIO(data); bio.name = filename
    return bio

# ------------ Users panel (owner) ------------
async def users_panel(q, page=0):
    uid = q.from_user.id
    if not is_owner(uid):
        await notify_owner_unauthorized(q.bot, uid, "open_users_panel")
        await q.message.edit_text("Owner only.", reply_markup=admin_menu(uid))
        return
    rowsdb = conn.execute("SELECT user_id, username, first_name, last_name, is_banned FROM users ORDER BY last_seen DESC").fetchall()
    pages = max(1, ceil(len(rowsdb)/PAGE_SIZE))
    page = max(0, min(page, pages-1))
    slice_ = rowsdb[page*PAGE_SIZE:(page+1)*PAGE_SIZE]
    rows = []
    for r in slice_:
        name = f"@{r['username']}" if r["username"] else " ".join(
            filter(None, [r["first_name"], r["last_name"]])) or f"id:{r['user_id']}"
        tag = "🚫" if r["is_banned"] else "✅"
        rows.append([InlineKeyboardButton(f"{tag} {name} (id:{r['user_id']})", callback_data=f"a:users:view:{r['user_id']}")])
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"a:users:p:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"a:users:p:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:panel")])
    await q.message.edit_text("Admin panel › Users\n\nSelect a user to manage:",
                              reply_markup=InlineKeyboardMarkup(rows))

async def user_detail_panel(q, tgt: int):
    row = conn.execute("SELECT * FROM users WHERE user_id=?", (tgt,)).fetchone()
    if not row:
        await q.message.edit_text("User not found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:users")]]))
        return
    name = _uname_row(row)
    banned = bool(row["is_banned"])
    rows = [
        [InlineKeyboardButton("✅ Unban" if banned else "🚫 Ban", callback_data=f"a:users:toggle:{tgt}")],
        [InlineKeyboardButton("✉️ Message user", callback_data=f"a:users:msg:{tgt}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="a:users")]
    ]
    await q.message.edit_text(f"Users › {name}\n\nUser id: {tgt}\nStatus: {'BANNED' if banned else 'Active'}",
                              reply_markup=InlineKeyboardMarkup(rows))

# ------------ Admin callbacks (includes Export fix & Search ID) ------------
async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer(cache_time=1)
    uid = q.from_user.id
    act = q.data.split(":", 1)[1]  # remove "a:"

    OWNER_ONLY = {"export_menu", "export_all", "export_subj", "export_subj_confirm",
                  "export_chap", "export_chap_confirm", "count", "broadcast",
                  "admins", "users", "export_users", "import_users", "ai_import",
                  "search_id"}
    if act.split(":")[0] in OWNER_ONLY and not is_owner(uid):
        await notify_owner_unauthorized(context.bot, uid, f"admin_cb:{act}")
        await q.message.reply_text("Owner only.")
        return

    # basics
    if act == "panel":
        await q.message.edit_text("Admin panel:", reply_markup=admin_menu(uid)); return
    if act == "back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid)); return

    # ---- add quiz (+ import inside chapter) ----
    if act == "add":
        subs = list_subjects_with_counts(ai_only=False)
        rows = [[InlineKeyboardButton(f"📚 {s} (chapters: {chs} | quizzes: {qs})", callback_data=f"a:add_subj:{s}")]
                for s, chs, qs in subs]
        rows.insert(0, [InlineKeyboardButton("➕ Add new Subject", callback_data="a:newsubj")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:panel")])
        await q.message.edit_text("Admin › Add quiz\n\nChoose a Subject (or add new):",
                                  reply_markup=InlineKeyboardMarkup(rows))
        return

    if act == "newsubj":
        context.user_data["mode"] = "NEW_SUBJECT"
        await q.message.edit_text("Admin › Add quiz\n\nSend the *Subject* name:",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]]),
                                  parse_mode="Markdown")
        return

    if act.startswith("add_subj:"):
        subject = act.split(":", 1)[1]
        context.user_data["add_subject"] = subject
        chs = list_chapters_with_counts(subject, ai_only=False)
        rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"a:add_chap:{c}")] for c, qs in chs]
        rows.insert(0, [InlineKeyboardButton("➕ Add new Chapter", callback_data="a:newchap")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:add")])
        await q.message.edit_text(f"Admin › Add quiz › {subject}\n\nChoose a Chapter (or add new):",
                                  reply_markup=InlineKeyboardMarkup(rows))
        return

    if act == "newchap":
        if not context.user_data.get("add_subject"):
            await q.message.edit_text("Pick a subject first.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]]))
            return
        context.user_data["mode"] = "NEW_CHAPTER"
        await q.message.edit_text("Admin › Add quiz\n\nSend the *Chapter* name:",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"a:add_subj:{context.user_data['add_subject']}")]]),
                                  parse_mode="Markdown")
        return

    if act.startswith("add_chap:"):
        chapter = act.split(":", 1)[1]
        context.user_data["add_chapter"] = chapter
        context.user_data["mode"] = "ADDING"
        sub = context.user_data.get("add_subject")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Import JSON into this chapter", callback_data="a:add_import_here")],
            [InlineKeyboardButton("⬅️ Back", callback_data=f"a:add_subj:{sub}")]
        ])
        await q.message.edit_text(
            f"Admin › Add quiz › {sub} › {chapter}\n\nNow send *Quiz-type* polls to add, or import a JSON.",
            reply_markup=kb, parse_mode="Markdown"
        )
        return

    if act == "add_import_here":
        context.user_data["mode"] = "IMPORT_CHAPTER"
        sub = context.user_data.get("add_subject"); chap = context.user_data.get("add_chapter")
        await q.message.edit_text(
            f"Admin › Add quiz › {sub} › {chap}\n\nSend a .json file to import into this chapter.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"a:add_chap:{chap}")]])
        )
        return

    # ---- delete last ----
    if act == "dellast":
        await q.message.edit_text("Admin › Delete last\n\nDelete the *last quiz you added*?\nThis cannot be undone.",
                                  reply_markup=InlineKeyboardMarkup([
                                      [InlineKeyboardButton("✅ Confirm", callback_data="a:dellast_yes"),
                                       InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]
                                  ]), parse_mode="Markdown")
        return
    if act == "dellast_yes":
        last = conn.execute("SELECT quiz_id FROM admin_log WHERE admin_id=? ORDER BY id DESC LIMIT 1", (uid,)).fetchone()
        if not last:
            await q.message.edit_text("No recent addition found for you.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        else:
            conn.execute("DELETE FROM quizzes WHERE id=?", (last["quiz_id"],))
            conn.execute("DELETE FROM admin_log WHERE admin_id=? AND quiz_id=?", (uid, last["quiz_id"]))
            conn.commit()
            await q.message.edit_text("Deleted your last added quiz.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        return

    # ---- export menus/actions (FIXED chapter parser) ----
    if act == "export_menu":
        subs = conn.execute("SELECT DISTINCT subject FROM quizzes WHERE subject IS NOT NULL").fetchall()
        rows = [[InlineKeyboardButton("📤 Export all", callback_data="a:export_all")]]
        for r in subs:
            s = r["subject"]
            rows.append([InlineKeyboardButton(f"📚 {s}", callback_data=f"a:export_subj:{s}")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:panel")])
        await q.message.edit_text("Export › Choose option:", reply_markup=InlineKeyboardMarkup(rows))
        return
    if act == "export_all":
        bio = _export_items(filename="quizzes_all.json")
        await q.message.reply_document(bio, caption="Exported all quizzes."); return
    if act.startswith("export_subj:"):
        subj = act.split(":", 1)[1]
        chs = conn.execute("SELECT DISTINCT chapter FROM quizzes WHERE subject=?", (subj,)).fetchall()
        rows = [[InlineKeyboardButton(f"📖 {r['chapter']}", callback_data=f"a:export_chap:{subj}:{r['chapter']}")] for r in chs]
        rows.append([InlineKeyboardButton(f"📤 Export whole subject", callback_data=f"a:export_subj_confirm:{subj}")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:export_menu")])
        await q.message.edit_text(f"Export › {subj}", reply_markup=InlineKeyboardMarkup(rows)); return
    if act.startswith("export_subj_confirm:"):
        subj = act.split(":", 1)[1]
        bio = _export_items("subject=?", (subj,), filename=f"quizzes_{subj}.json")
        await q.message.reply_document(bio, caption=f"Exported subject: {subj}"); return
    if act.startswith("export_chap:"):
        # FIX: split into exactly 3 parts: ["export_chap", subj, chap]
        _, subj, chap = act.split(":", 2)
        rows = [
            [InlineKeyboardButton("📤 Export this chapter", callback_data=f"a:export_chap_confirm:{subj}:{chap}")],
            [InlineKeyboardButton("⬅️ Back", callback_data=f"a:export_subj:{subj}")]
        ]
        await q.message.edit_text(f"Export › {subj} › {chap}", reply_markup=InlineKeyboardMarkup(rows)); return
    if act.startswith("export_chap_confirm:"):
        _, subj, chap = act.split(":", 2)
        bio = _export_items("subject=? AND chapter=?", (subj, chap), filename=f"quizzes_{subj}_{chap}.json")
        await q.message.reply_document(bio, caption=f"Exported: {subj} › {chap}"); return

    # ---- users & messaging ----
    if act == "users": await users_panel(q, page=0); return
    if act.startswith("users:p:"):
        pg = int(act.split(":")[2]); await users_panel(q, page=pg); return
    if act.startswith("users:view:"):
        tgt = int(act.split(":")[2]); await user_detail_panel(q, tgt); return
    if act.startswith("users:toggle:"):
        tgt = int(act.split(":")[2])
        if tgt == OWNER_ID:
            await q.message.edit_text("Cannot ban the owner.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:users")]]))
        else:
            cur = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (tgt,)).fetchone()
            conn.execute("UPDATE users SET is_banned=? WHERE user_id=?", (0 if cur and cur["is_banned"] else 1, tgt))
            conn.commit()
            await user_detail_panel(q, tgt)
        return
    if act.startswith("users:msg:"):
        tgt = int(act.split(":")[2])
        context.user_data["mode"] = "MSG_USER"
        context.user_data["msg_user_id"] = tgt
        await q.message.edit_text(f"Type the message to send only to user id:{tgt}.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"a:users:view:{tgt}")]]))
        return

    # ---- admins (owner only) ----
    if act == "admins":
        ids = sorted(list(admin_ids_from_settings()))
        if OWNER_ID in ids: ids.remove(OWNER_ID)
        slice_ = ids[:PAGE_SIZE]
        rows = [[InlineKeyboardButton(f"👤 {i}", callback_data=f"a:admins:view:{i}")] for i in slice_]
        rows.append([InlineKeyboardButton("➕ Add admin", callback_data="a:admins:add")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:panel")])
        await q.message.edit_text("Admin panel › Admins", reply_markup=InlineKeyboardMarkup(rows))
        return
    if act == "admins:add":
        context.user_data["mode"] = "ADMINS_ADD_PROMPT"
        await q.message.edit_text("Admins › Add\n\nSend a *user id* or *@username* to add as admin.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:admins")]]),
                                  parse_mode="Markdown")
        return
    if act.startswith("admins:view:"):
        tgt = int(act.split(":")[2])
        rows = [
            [InlineKeyboardButton("🗑 Remove admin", callback_data=f"a:admins:rm:{tgt}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="a:admins")]
        ]
        await q.message.edit_text(f"Admins › id:{tgt}", reply_markup=InlineKeyboardMarkup(rows))
        return
    if act.startswith("admins:rm:"):
        tgt = int(act.split(":")[2])
        if tgt == OWNER_ID:
            await q.message.edit_text("Cannot remove owner.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:admins")]]))
        else:
            remove_admin(tgt)
            await q.message.edit_text("Removed.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:admins")]]))
        return

    # ---- counters / broadcast / users DB / AI import / search id ----
    if act == "count":
        r = conn.execute("SELECT COUNT(*) c FROM quizzes").fetchone()
        await q.message.edit_text(f"Total quizzes: {r['c']}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        return
    if act == "broadcast":
        context.user_data["mode"] = "BROADCAST_ENTER"
        await q.message.edit_text("Send the message to broadcast to all users.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        return
    if act == "export_users":
        bio = _export_users_blob()
        await q.message.reply_document(bio, caption="Exported users database."); return
    if act == "import_users":
        context.user_data["mode"] = "IMPORT_USERS"
        await q.message.edit_text("Send a *users JSON* exported earlier to import users DB.",
                                  parse_mode="Markdown",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        return
    if act == "ai_import":
        context.user_data["mode"] = "AI_IMPORT"
        await q.message.edit_text("Send the *AI-generated quizzes JSON* (with subject & chapter fields).",
                                  parse_mode="Markdown",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        return
    if act == "search_id":
        context.user_data["mode"] = "SEARCH_ID"
        await q.message.edit_text("Send the *quiz* (forward the Quiz-type poll) or paste the *exact question text*.",
                                  parse_mode="Markdown",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        return

# ------------ Command helpers: edit/del subject/chapter ------------
async def _owner_required(update):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Owner only.")
        return False
    return True

async def editsub(update: Update, context: ContextTypes.DEFAULT_TYPE, ai=False):
    if not await _owner_required(update): return
    raw = " ".join(context.args) if context.args else ""
    old, new = parse_old_new(raw)
    if not old or not new:
        await update.message.reply_text('Usage: /editsub "Old Subject" "New Subject"  (or use | or ->)')
        return
    sql = "UPDATE quizzes SET subject=? WHERE subject=? AND "
    sql += "ai_generated=1" if ai else "COALESCE(ai_generated,0)=0"
    conn.execute(sql, (new, old)); conn.commit()
    await update.message.reply_text(f"Subject renamed: {old} → {new} ({'AI' if ai else 'Human'}).")

async def editchap(update: Update, context: ContextTypes.DEFAULT_TYPE, ai=False):
    if not await _owner_required(update): return
    raw = " ".join(context.args) if context.args else ""
    subj, old, new = parse_subject_old_new_chap(raw, ai=ai)
    if not subj or not old or not new:
        await update.message.reply_text('Usage: /editchap "Subject" "Old Chapter" "New Chapter"  (or Subject | Old -> New)')
        return
    sql = "UPDATE quizzes SET chapter=? WHERE subject=? AND chapter=? AND "
    sql += "ai_generated=1" if ai else "COALESCE(ai_generated,0)=0"
    conn.execute(sql, (new, subj, old)); conn.commit()
    await update.message.reply_text(f"Chapter renamed in {subj}: {old} → {new} ({'AI' if ai else 'Human'}).")

async def delsub(update: Update, context: ContextTypes.DEFAULT_TYPE, ai=False):
    if not await _owner_required(update): return
    raw = " ".join(context.args) if context.args else ""
    subj = _quoted_parts(raw)[0] if _quoted_parts(raw) else raw.strip()
    if not subj:
        await update.message.reply_text('Usage: /delsub "Subject"')
        return
    sql = "DELETE FROM quizzes WHERE subject=? AND "
    sql += "ai_generated=1" if ai else "COALESCE(ai_generated,0)=0"
    cur = conn.execute(sql, (subj,)); cnt = cur.rowcount
    conn.commit()
    await update.message.reply_text(f"Deleted subject '{subj}' ({cnt} quizzes) ({'AI' if ai else 'Human'}).")

async def delchap(update: Update, context: ContextTypes.DEFAULT_TYPE, ai=False):
    if not await _owner_required(update): return
    raw = " ".join(context.args) if context.args else ""
    subj, chap = parse_subject_chapter(raw, ai=ai)
    if not subj or not chap:
        await update.message.reply_text('Usage: /delchap "Subject" "Chapter"  (or Subject | Chapter)')
        return
    sql = "DELETE FROM quizzes WHERE subject=? AND chapter=? AND "
    sql += "ai_generated=1" if ai else "COALESCE(ai_generated,0)=0"
    cur = conn.execute(sql, (subj, chap)); cnt = cur.rowcount
    conn.commit()
    await update.message.reply_text(f"Deleted {subj} › {chap} ({cnt} quizzes) ({'AI' if ai else 'Human'}).")

# ------------ Text / Poll handler (modes incl. SEARCH_ID) ------------
async def text_or_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    uid = update.effective_user.id
    mode = context.user_data.get("mode")

    if is_user_banned(uid):
        if update.message:
            await update.message.reply_text("You are banned from using this bot.")
        return

    # SEARCH QUIZ ID
    if mode == "SEARCH_ID" and update.message:
        if not is_owner(uid):
            await notify_owner_unauthorized(context.bot, uid, "SEARCH_ID"); 
            context.user_data["mode"] = None
            return
        # if poll provided, match by exact structure
        if update.message.poll:
            p = update.message.poll
            qtxt = p.question
            opts = [o.text for o in p.options]
            cand = conn.execute("SELECT id, question, options_json, correct, subject, chapter FROM quizzes WHERE question=?", (qtxt,)).fetchall()
            matches = []
            for r in cand:
                try:
                    ropts = json.loads(r["options_json"])
                except Exception:
                    ropts = []
                if ropts == opts and int(r["correct"]) == int(p.correct_option_id):
                    matches.append(r)
            if not matches:
                await update.message.reply_text("No exact match found by poll. Try sending the plain question text.")
            else:
                lines = ["Found:"]
                for r in matches[:10]:
                    lines.append(f"• id:{r['id']} — {r['subject']} › {r['chapter']}")
                await update.message.reply_text("\n".join(lines))
        elif update.message.text:
            qtxt = update.message.text.strip()
            rows = conn.execute("SELECT id, subject, chapter FROM quizzes WHERE question LIKE ? ORDER BY id LIMIT 10", (f"%{qtxt}%",)).fetchall()
            if not rows:
                await update.message.reply_text("No match.")
            else:
                lines = ["Matches:"]
                for r in rows:
                    lines.append(f"• id:{r['id']} — {r['subject']} › {r['chapter']}")
                await update.message.reply_text("\n".join(lines))
        context.user_data["mode"] = None
        return

    # owner sends message to specific user
    if mode == "MSG_USER" and update.message and update.message.text:
        if not is_owner(uid):
            await notify_owner_unauthorized(context.bot, uid, "MSG_USER", update.message.text)
            return
        tgt = int(context.user_data.get("msg_user_id", 0))
        if not tgt:
            await update.message.reply_text("No target user selected.")
        else:
            row = conn.execute("SELECT chat_id FROM users WHERE user_id=?", (tgt,)).fetchone()
            chat_id = row["chat_id"] if row and row["chat_id"] else tgt
            try:
                await context.bot.send_message(chat_id, f"📩 *Message from the owner:*\n\n{update.message.text}", parse_mode="Markdown")
                await update.message.reply_text("✅ Sent.")
            except Exception as e:
                await update.message.reply_text(f"Failed to send: {e}")
        context.user_data["mode"] = None
        return

    # admins add poll to selected subject/chapter
    if mode == "ADDING" and update.message and update.message.poll:
        if not is_admin(uid):
            await notify_owner_unauthorized(context.bot, uid, "ADDING_POLL"); return
        poll = update.message.poll
        if poll.type != "quiz":
            await update.message.reply_text("Please send a *quiz-type* poll.", parse_mode="Markdown"); return
        sub = context.user_data.get("add_subject"); chap = context.user_data.get("add_chapter")
        if not sub or not chap:
            await update.message.reply_text("Please pick subject & chapter again from Admin › Add quiz."); return
        question = poll.question
        options = [o.text for o in poll.options]
        correct = poll.correct_option_id
        try:
            qtext, opts, expl = sanitize_for_poll(question, options, poll.explanation)
            conn.execute(
                "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by,ai_generated) "
                "VALUES(?,?,?,?,?,?,?,?,0)",
                (qtext, json.dumps(opts, ensure_ascii=False), int(correct), expl, sub, chap, int(time.time()), int(uid))
            )
            qid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute("INSERT INTO admin_log(admin_id,quiz_id,created_at) VALUES(?,?,?)",
                         (uid, qid, int(time.time())))
            conn.commit()
            await update.message.reply_text(f"✅ Added to {sub} › {chap} (id:{qid}).")
        except Exception as e:
            await update.message.reply_text(f"Add error: {e}")
        return

    # global import (owner menu "Import JSON")
    if mode == "IMPORT" and update.message and update.message.document:
        if not is_owner(uid):
            await notify_owner_unauthorized(context.bot, uid, "IMPORT", update.message.document.file_name)
            await update.message.reply_text("Owner only."); return
        try:
            tgfile = await update.message.document.get_file()
            text = bytes(await tgfile.download_as_bytearray()).decode("utf-8-sig").strip()
            data = json.loads(text)
            count = 0
            for it in data:
                conn.execute(
                    "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by,ai_generated) "
                    "VALUES(?,?,?,?,?,?,?,?,0)",
                    (it["question"], json.dumps(it["options"], ensure_ascii=False), int(it["correct"]),
                     it.get("explanation"), it.get("subject"), it.get("chapter"), int(time.time()), int(uid))
                ); count += 1
            conn.commit()
            await update.message.reply_text(f"Imported {count} quizzes.")
        except Exception as e:
            await update.message.reply_text("Import error: " + str(e))
        finally:
            context.user_data["mode"] = None
        return

    # import into selected chapter
    if mode == "IMPORT_CHAPTER" and update.message and update.message.document:
        if not is_admin(uid):
            await notify_owner_unauthorized(context.bot, uid, "IMPORT_CHAPTER", update.message.document.file_name)
            await update.message.reply_text("Admins only."); return
        sub = context.user_data.get("add_subject"); chap = context.user_data.get("add_chapter")
        try:
            tgfile = await update.message.document.get_file()
            text = bytes(await tgfile.download_as_bytearray()).decode("utf-8-sig").strip()
            data = json.loads(text)
            count = 0
            for it in data:
                question = it["question"]; options = it["options"]; correct = int(it["correct"])
                explanation = it.get("explanation")
                conn.execute(
                    "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by,ai_generated) "
                    "VALUES(?,?,?,?,?,?,?,?,0)",
                    (question, json.dumps(options, ensure_ascii=False), correct, explanation,
                     sub, chap, int(time.time()), int(uid))
                ); count += 1
            conn.commit()
            await update.message.reply_text(f"Imported {count} items into {sub} › {chap}.")
        except Exception as e:
            await update.message.reply_text("Import error: " + str(e))
        finally:
            context.user_data["mode"] = None
        return

    # import users DB
    if mode == "IMPORT_USERS" and update.message and update.message.document:
        if not is_owner(uid):
            await notify_owner_unauthorized(context.bot, uid, "IMPORT_USERS", update.message.document.file_name)
            await update.message.reply_text("Owner only."); return
        try:
            tgfile = await update.message.document.get_file()
            data = bytes(await tgfile.download_as_bytearray()).decode("utf-8-sig")
            users = json.loads(data)
            imported = 0
            for u in users:
                conn.execute(
                    "INSERT INTO users(user_id,username,first_name,last_name,chat_id,last_seen,is_banned) "
                    "VALUES(?,?,?,?,?,?,?) "
                    "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, "
                    "last_name=excluded.last_name, chat_id=excluded.chat_id, last_seen=excluded.last_seen, is_banned=excluded.is_banned",
                    (u.get("user_id"), u.get("username"), u.get("first_name"), u.get("last_name"),
                     u.get("chat_id"), u.get("last_seen"), int(u.get("is_banned", 0)))
                ); imported += 1
            conn.commit()
            await update.message.reply_text(f"Imported users: {imported}")
        except Exception as e:
            await update.message.reply_text("Import users error: " + str(e))
        finally:
            context.user_data["mode"] = None
        return

    # AI import
    if mode == "AI_IMPORT" and update.message and update.message.document:
        if not is_owner(uid):
            await notify_owner_unauthorized(context.bot, uid, "AI_IMPORT", update.message.document.file_name)
            await update.message.reply_text("Owner only."); return
        try:
            tgfile = await update.message.document.get_file()
            text = bytes(await tgfile.download_as_bytearray()).decode("utf-8-sig").strip()
            data = json.loads(text)
            count = 0
            for it in data:
                conn.execute(
                    "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by,ai_generated) "
                    "VALUES(?,?,?,?,?,?,?,?,1)",
                    (it["question"], json.dumps(it["options"], ensure_ascii=False), int(it["correct"]),
                     it.get("explanation"), it.get("subject"), it.get("chapter"),
                     int(time.time()), int(uid))
                ); count += 1
            conn.commit()
            await update.message.reply_text(f"Imported AI quizzes: {count}")
        except Exception as e:
            await update.message.reply_text("AI import error: " + str(e))
        finally:
            context.user_data["mode"] = None
        return

    # broadcast
    # broadcast (two-step: preview then confirm)
    if mode == "BROADCAST_ENTER" and update.message and update.message.text:
        if not is_owner(uid):
            await notify_owner_unauthorized(context.bot, uid, "BROADCAST_ENTER")
            context.user_data["mode"] = None
            return
        draft = update.message.text
        context.user_data["broadcast_draft"] = draft
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm broadcast", callback_data="a:bcast_confirm")],
            [InlineKeyboardButton("⬅️ Cancel", callback_data="a:bcast_cancel")]
        ])
        await update.message.reply_text(f"*Broadcast preview:*\n\n{draft}", parse_mode="Markdown", reply_markup=kb)
        context.user_data["mode"] = None
        return

        # contact admin → owner only
    logging.info(f"[contact-debug] text_or_poll called; uid={uid}; msg={(update.message.text if update.message else None)}")
    if pending_contact.get(uid) and update.message:
        u = update.effective_user
        header = f"📨 Message to owner from {_uname_row({'username': u.username, 'first_name': u.first_name, 'last_name': u.last_name, 'user_id': u.id})} (id:{u.id}):"
        try:
            await context.bot.send_message(OWNER_ID, header)
            await context.bot.copy_message(
                chat_id=OWNER_ID,
                from_chat_id=update.message.chat.id,
                message_id=update.message.message_id
            )
        except Exception as e:
            logging.exception("[contact-debug] failed to forward message to OWNER_ID")
            try:
                await update.message.reply_text("❌ Failed to send your message to the owner. Please try again later.", reply_markup=main_menu(u.id))
            except Exception:
                pass
            pending_contact.pop(uid, None)
            return
        pending_contact.pop(uid, None)
        await update.message.reply_text(
            "✅ Your message has been sent to the owner.",
            reply_markup=main_menu(u.id)
        )
        return

    # admins add subject/chapter by text# admins add subject/chapter by text

    if mode == "NEW_SUBJECT" and update.message and update.message.text:
        context.user_data["add_subject"] = update.message.text.strip()
        context.user_data["mode"] = None
        subject = context.user_data["add_subject"]
        # build chapters list with 'Add new Chapter' and 'Back' buttons as in callback flow
        chs = list_chapters_with_counts(subject, ai_only=False)
        rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"a:add_chap:{c}")] for c, qs in chs]
        rows.insert(0, [InlineKeyboardButton("➕ Add new Chapter", callback_data="a:newchap")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:add")])
        await update.message.reply_text(f"Admin › Add quiz › {subject}\n\nChoose a Chapter (or add new):",
                                        reply_markup=InlineKeyboardMarkup(rows))
        return
    if mode == "NEW_CHAPTER" and update.message and update.message.text:
        context.user_data["add_chapter"] = update.message.text.strip()
        context.user_data["mode"] = "ADDING"
        await update.message.reply_text(f"Chapter set to *{context.user_data['add_chapter']}*.\nNow send Quiz polls to add.",
                                        parse_mode="Markdown")
        return

    # owner add admin (by id or @username)
    if mode == "ADMINS_ADD_PROMPT" and update.message and update.message.text:
        if not is_owner(uid):
            await notify_owner_unauthorized(context.bot, uid, "ADMINS_ADD_PROMPT"); return
        t = update.message.text.strip()
        target_id = None
        if t.startswith("@"):
            r = conn.execute("SELECT user_id FROM users WHERE username=?", (t[1:],)).fetchone()
            if r: target_id = int(r["user_id"])
        else:
            try: target_id = int(t)
            except: pass
        if not target_id:
            await update.message.reply_text("Could not find that user. Make sure they used /start at least once.")
        else:
            add_admin(target_id)
            await update.message.reply_text(f"✅ Added admin: {target_id}")
        context.user_data["mode"] = None
        return

# ------------ Buttons dispatcher ------------
async def btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    q = update.callback_query
    await q.answer(cache_time=1)
    uid = q.from_user.id
    # --- handle broadcast confirm/cancel quickly (works even if admin_cb dispatch had issues) ---
    if q.data == "a:bcast_confirm":
        # allow only admins
        if not is_admin(q.from_user.id):
            await notify_owner_unauthorized(context.bot, q.from_user.id, "bcast_confirm")
            await q.message.reply_text("Only admin can use this."); return
        await q.answer()
        draft = context.user_data.get("broadcast_draft")
        if not draft:
            await q.message.edit_text("No broadcast draft to send.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
            return
        rows = conn.execute("SELECT chat_id FROM users").fetchall()
        ok = 0
        for r in rows:
            try:
                if r["chat_id"]:
                    await context.bot.send_message(r["chat_id"], draft)
                    ok += 1
            except Exception:
                pass
        context.user_data["broadcast_draft"] = None
        await q.message.edit_text(f"✅ Broadcasted to {ok} users.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        return

    if q.data == "a:bcast_cancel":
        # allow only admins
        if not is_admin(q.from_user.id):
            await notify_owner_unauthorized(context.bot, q.from_user.id, "bcast_cancel")
            await q.message.reply_text("Only admin can use this."); return
        await q.answer()
        context.user_data["broadcast_draft"] = None
        await q.message.edit_text("❌ Broadcast cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        return

    data = q.data
    uid = q.from_user.id

    if is_user_banned(uid):
        await q.message.edit_text("You are banned from using this bot.")
        return

    # user menu
    if data == "u:help":
        await q.message.edit_text("Start → Subject → Chapter → Timer (or Without Timer) → I am ready!",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])); return
    if data == "u:stats": await show_stats(q); return
    if data == "u:lb":
        if not is_owner(uid): await q.message.reply_text("Owner only."); return
        await leaderboard(q, page=0); return
    if data.startswith("u:lbp:"):
        await leaderboard(q, page=int(data.split(":")[2])); return
    if data == "u:contact":
        pending_contact[uid] = True
        await q.message.edit_text("Type your message for the owner:",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])); return

    # start quiz (human)
    if data == "u:start": await user_subjects(update); return
    if data.startswith("u:subjp:"): await user_subjects(update, int(data.split(":")[2])); return
    if data.startswith("u:subj:"):
        context.user_data["subject"] = data.split(":", 2)[2]; await user_chapters(update, context.user_data["subject"], 0); return
    if data == "u:startback": await user_subjects(update); return
    if data.startswith("u:chpp:"):
        await user_chapters(update, context.user_data.get("subject"), int(data.split(":")[2])); return
    if data.startswith("u:chap:"):
        context.user_data["chapter"] = data.split(":", 2)[2]; await timer_menu(update); return
    if data == "u:chapback": await user_chapters(update, context.user_data.get("subject"), 0); return
    if data.startswith("u:timer:"):
        context.user_data["open_period"] = int(data.split(":")[2]); await pre_quiz_screen(q, context); return
    if data == "u:timerback": await timer_menu(update); return
    if data == "u:ready": await begin_quiz_session(q, context); return
    if data == "u:retry":
        context.user_data["subject"] = context.user_data.get("last_subject")
        context.user_data["chapter"] = context.user_data.get("last_chapter")
        context.user_data["open_period"] = context.user_data.get("last_open_period", DEFAULT_OPEN_PERIOD)
        await begin_quiz_session(q, context); return
    if data == "u:back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid)); return

    # AI menu
    if data == "uai:start": await user_subjects_ai(update); return
    if data.startswith("uai:subjp:"): await user_subjects_ai(update, int(data.split(":")[2])); return
    if data.startswith("uai:subj:"):
        context.user_data["ai_subject"] = data.split(":", 2)[2]; await user_chapters_ai(update, context.user_data["ai_subject"], 0); return
    if data == "uai:startback": await user_subjects_ai(update); return
    if data.startswith("uai:chpp:"):
        await user_chapters_ai(update, context.user_data.get("ai_subject"), int(data.split(":")[2])); return
    if data.startswith("uai:chap:"):
        context.user_data["ai_chapter"] = data.split(":", 2)[2]; await timer_menu_ai(update); return
    if data == "uai:chapback": await user_chapters_ai(update, context.user_data.get("ai_subject"), 0); return
    if data.startswith("uai:timer:"):
        context.user_data["ai_open_period"] = int(data.split(":")[2]); await pre_quiz_screen_ai(q, context); return
    if data == "uai:timerback": await timer_menu_ai(update); return
    if data == "uai:ready": await begin_quiz_session_ai(q, context); return

    # admin dispatch
    if data.startswith("a:"):
        if not is_admin(uid):
            await notify_owner_unauthorized(context.bot, uid, f"callback:{data}")
            await q.message.reply_text("Only admin can use this.")
            return
        # top-level import (owner)
        if data == "a:import":
            if not is_owner(uid):
                await notify_owner_unauthorized(context.bot, uid, "a:import"); 
                await q.message.reply_text("Owner only."); return
            context.user_data["mode"] = "IMPORT"
            await q.message.edit_text("Send the JSON file to import quizzes.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
            return
        await admin_cb(update, context)
        return

# ------------ Owner-only command wrappers ------------
async def editsub_cmd(update, context): await editsub(update, context, ai=False)
async def editsub_ai_cmd(update, context): await editsub(update, context, ai=True)
async def editchap_cmd(update, context): await editchap(update, context, ai=False)
async def editchap_ai_cmd(update, context): await editchap(update, context, ai=True)
async def delsub_cmd(update, context): await delsub(update, context, ai=False)
async def delsub_ai_cmd(update, context): await delsub(update, context, ai=True)
async def delchap_cmd(update, context): await delchap(update, context, ai=False)
async def delchap_ai_cmd(update, context): await delchap(update, context, ai=True)

# ------------ Commands ------------
async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
    conn.commit()
    await update.message.reply_text("Quiz stopped.", reply_markup=main_menu(uid))


# ------------ Keepalive (optional) ------------
app = Flask(__name__)
@app.get("/")
def home(): return "OK"
def run_keepalive(): app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

# ------------ Main bootstrap ------------
if __name__ == "__main__":
    db_init()
    Thread(target=run_keepalive, daemon=True).start()
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    
async def done_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Finish quiz adding (admin uses /done while in ADDING mode)."""
    uid = update.effective_user.id
    # If user was adding quizzes, stop the mode; otherwise just acknowledge.
    if context.user_data.get("mode") == "ADDING":
        context.user_data["mode"] = None
        await update.message.reply_text("Finished adding quizzes.", reply_markup=main_menu(uid))
    else:
        await update.message.reply_text("Not currently adding quizzes.")


app_.add_handler(CommandHandler("start", start))
    app_.add_handler(CommandHandler("help", help_cmd))
    app_.add_handler(CommandHandler("stop", stop_cmd))
    app_.add_handler(CommandHandler("done", done_cmd))
    app_.add_handler(CommandHandler("delquiz", delquiz_cmd))
    # restored owner tools (human)
    app_.add_handler(CommandHandler("editsub", editsub_cmd))
    app_.add_handler(CommandHandler("editchap", editchap_cmd))
    app_.add_handler(CommandHandler("delsub", delsub_cmd))
    app_.add_handler(CommandHandler("delchap", delchap_cmd))
    # AI variants
    app_.add_handler(CommandHandler("editsub_ai", editsub_ai_cmd))
    app_.add_handler(CommandHandler("editchap_ai", editchap_ai_cmd))
    app_.add_handler(CommandHandler("delsub_ai", delsub_ai_cmd))
    app_.add_handler(CommandHandler("delchap_ai", delchap_ai_cmd))

    app_.add_handler(CallbackQueryHandler(btn))
    app_.add_handler(PollAnswerHandler(poll_answer))
    # One handler is enough; we branch on mode (text, poll, document)
    app_.add_handler(MessageHandler(filters.ALL, text_or_poll))
    app_.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_or_poll))
    app_.run_polling()