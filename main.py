import os, json, time, random, logging, sqlite3, asyncio, re, traceback
from threading import Thread
from math import ceil
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    PollAnswerHandler, ContextTypes, filters
)
from dotenv import load_dotenv

# Load local .env file
load_dotenv("secrets.env")

# ---------- Config ----------
OWNER_ID = 5902126578  # Owner controls everything
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS_ENV = os.getenv("ADMIN_ID", "")
DB_PATH = os.getenv("DB_PATH", "db.sqlite3")

PAGE_SIZE = 8
LB_PAGE = 20
DEFAULT_OPEN_PERIOD = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("quizbot")

# ---------- DB ----------
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")

def table_cols(table: str) -> set:
    cur = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in cur}

def add_col_if_missing(table: str, col: str, decl: str):
    cols = table_cols(table)
    if col not in cols:
        log.info("DB migrate: adding %s.%s", table, col)
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl};")
        conn.commit()

def db_init():
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);")
    cur.execute("""
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
    cur.execute("""
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
    cur.execute("""
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
    cur.execute("""
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_polls(
            poll_id TEXT PRIMARY KEY,
            session_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER NOT NULL,
            quiz_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
    """)
    conn.commit()
    # migrations
    add_col_if_missing("quizzes", "subject", "TEXT")
    add_col_if_missing("quizzes", "chapter", "TEXT")
    add_col_if_missing("quizzes", "explanation", "TEXT")
    add_col_if_missing("quizzes", "ai_generated", "INTEGER DEFAULT 0")
    add_col_if_missing("users", "is_banned", "INTEGER DEFAULT 0")
    add_col_if_missing("sessions", "current_index", "INTEGER DEFAULT 0")
    add_col_if_missing("sessions", "finished_at", "INTEGER")

# ---------- Helpers ----------
async def busy(chat, action=ChatAction.TYPING, secs=0.35):
    await asyncio.sleep(secs)

def sget(key, default=None):
    r = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r[0] if r else default

def sset(key, value):
    conn.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    conn.commit()

def admin_ids_from_settings():
    saved = sget("admin_ids")
    if saved:
        return {int(x) for x in saved.split(",") if x.strip().isdigit()}
    if ADMIN_IDS_ENV:
        return {int(x) for x in ADMIN_IDS_ENV.split(",") if x.strip().isdigit()}
    return set()

def is_owner(uid: int) -> bool:
    return int(uid) == int(OWNER_ID)

def is_admin(uid: int) -> bool:
    return is_owner(uid) or uid in admin_ids_from_settings()

def add_admin(uid: int):
    ids = admin_ids_from_settings()
    ids.add(int(uid))
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

def remove_admin(uid: int):
    ids = admin_ids_from_settings()
    if int(uid) in ids:
        ids.remove(int(uid))
        sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

def is_user_banned(uid: int) -> bool:
    r = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(r and int(r["is_banned"]) == 1)

def set_ban(uid: int, flag: bool):
    conn.execute("UPDATE users SET is_banned=? WHERE user_id=?", (1 if flag else 0, uid))
    conn.commit()

def _uname_row(urow):
    if not urow:
        return "unknown"
    if urow["username"]:
        return f"@{urow['username']}"
    name = " ".join(filter(None, [urow["first_name"], urow["last_name"]]))
    return name or f"id:{urow['user_id']}"

async def notify_owner_unauthorized(bot, offender_id: int, action: str, details: str = ""):
    if is_owner(offender_id):
        return
    try:
        urow = conn.execute("SELECT * FROM users WHERE user_id=?", (offender_id,)).fetchone()
        uname = _uname_row(urow)
        msg = (
            "🚨 *Unauthorized admin attempt*\n"
            f"User: {uname} (id:{offender_id})\n"
            f"Action: `{action}`\n"
            f"Details:\n{details or '-'}"
        )
        await bot.send_message(OWNER_ID, msg, parse_mode="Markdown")
    except Exception:
        pass

def upsert_user(update: Update):
    u = update.effective_user
    c = update.effective_chat
    if not u or not c:
        return False
    existed = conn.execute("SELECT 1 FROM users WHERE user_id=?", (u.id,)).fetchone() is not None
    conn.execute(
        "INSERT INTO users(user_id,username,first_name,last_name,chat_id,last_seen) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "username=excluded.username, first_name=excluded.first_name, "
        "last_name=excluded.last_name, chat_id=excluded.chat_id, last_seen=excluded.last_seen",
        (u.id, u.username, u.first_name, u.last_name, c.id, int(time.time()))
    )
    conn.commit()
    return not existed  # True if new

def sanitize_for_poll(question: str, options: list, explanation: str):
    def _truncate(s: str, n: int) -> str:
        s = s or ""
        if len(s) <= n: return s
        return s[: max(0, n-1)] + "…"
    question = _truncate((question or "").strip(), 292)
    options = [o or "" for o in options]
    explanation = explanation or None
    options = [_truncate(o, 100) for o in options if o.strip()]
    opts = []
    seen = set()
    for o in options:
        if o in seen: continue
        seen.add(o); opts.append(o)
    options = opts[:10]
    if len(options) < 2:
        raise ValueError("Not enough valid options after cleaning (need ≥2).")
    if explanation:
        explanation = _truncate(explanation, 200)
    return question, options, explanation

# ---------- Subject/Chapter helpers ----------
def list_subjects_with_counts(ai_only=False):
    if ai_only:
        cur = conn.execute(
            "SELECT subject s, COUNT(DISTINCT chapter) chs, COUNT(*) qs "
            "FROM quizzes WHERE ai_generated=1 GROUP BY s ORDER BY qs DESC, s"
        )
    else:
        cur = conn.execute(
            "SELECT subject s, COUNT(DISTINCT chapter) chs, COUNT(*) qs "
            "FROM quizzes WHERE COALESCE(ai_generated,0)=0 GROUP BY s ORDER BY qs DESC, s"
        )
    return [(r["s"], r["chs"], r["qs"]) for r in cur.fetchall()]

def list_chapters_with_counts(subject: str, ai_only=False):
    if ai_only:
        cur = conn.execute(
            "SELECT chapter c, COUNT(*) qs FROM quizzes "
            "WHERE subject=? AND ai_generated=1 GROUP BY c ORDER BY qs DESC, c", (subject,)
        )
    else:
        cur = conn.execute(
            "SELECT chapter c, COUNT(*) qs FROM quizzes "
            "WHERE subject=? AND COALESCE(ai_generated,0)=0 GROUP BY c ORDER BY qs DESC, c", (subject,)
        )
    return [(r["c"], r["qs"]) for r in cur.fetchall()]

def _has_ai_quizzes() -> bool:
    return conn.execute("SELECT 1 FROM quizzes WHERE ai_generated=1 LIMIT 1").fetchone() is not None

# ---------- Menus ----------
def main_menu(uid: int):
    rows = [
        [InlineKeyboardButton("▶️ Start quiz", callback_data="u:start")],
        [InlineKeyboardButton("📊 My stats", callback_data="u:stats"),
         InlineKeyboardButton("📨 Contact admin", callback_data="u:contact")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="u:help")]
    ]
    if _has_ai_quizzes():
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
        [InlineKeyboardButton("📣 Broadcast", callback_data="a:broadcast")],
        [InlineKeyboardButton("👑 Admins", callback_data="a:admins"),
         InlineKeyboardButton("👥 Users", callback_data="a:users")],
        [InlineKeyboardButton("🗂 Export users DB", callback_data="a:export_users"),
         InlineKeyboardButton("📥 Import users DB", callback_data="a:import_users")],
        [InlineKeyboardButton("🤖 Add AI gen Quiz", callback_data="a:ai_import")],
        [InlineKeyboardButton("⬅️ Back", callback_data="a:back")]
    ])

# ---------- Commands ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_new = upsert_user(update)
    uid = update.effective_user.id
    if not admin_ids_from_settings():
        add_admin(uid)  # first runner becomes admin; owner can prune later
        log.info("Auto-assigned admin to %s", uid)
    if is_user_banned(uid):
        await update.effective_chat.send_message("You are banned from using this bot.")
        return
    first = update.effective_user.first_name or "there"
    await update.effective_chat.send_message(
        f"Hey {first}, welcome to our *Madhyamik Helper Quiz Bot*! 🎓",
        parse_mode="Markdown",
        reply_markup=main_menu(uid)
    )
    if is_new:
        total = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        uname = _uname_row(conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone())
        try:
            await context.bot.send_message(
                OWNER_ID,
                "✅New user joined\n"
                f"Username: {uname}\n"
                f"Userid: {uid}\n\n\n"
                f"Total users: {total}"
            )
        except Exception:
            pass

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if is_user_banned(update.effective_user.id):
        await update.message.reply_text("You are banned from using this bot.")
        return
    await update.message.reply_text(
        "Start → Subject → Chapter → Timer (or Without Timer) → I am ready!\n"
        "Use /stop any time to cancel. Admins can add quizzes; owner controls everything."
    )

# ---------- UI helpers ----------
async def edit_or_reply(obj, text, markup=None, **kwargs):
    if hasattr(obj, "callback_query") and obj.callback_query:
        await obj.callback_query.message.edit_text(text, reply_markup=markup, **kwargs)
    elif isinstance(obj, Update):
        await obj.effective_chat.send_message(text, reply_markup=markup, **kwargs)

# ---------- Flows: Human quizzes ----------
async def user_subjects(update_or_query, page=0):
    chat = update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat
    await busy(chat)
    subs = list_subjects_with_counts(ai_only=False)
    if not subs:
        await edit_or_reply(update_or_query, "Home › Subjects\n\nNo subjects yet. Ask admin to add quizzes.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
        return
    pages = max(1, ceil(len(subs) / PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    slice_ = subs[page*PAGE_SIZE:(page+1)*PAGE_SIZE]
    rows = [[InlineKeyboardButton(f"📚 {s} (chapters: {chs} | quizzes: {qs})", callback_data=f"u:subj:{s}")]
            for s, chs, qs in slice_]
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:subjp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:subjp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    await edit_or_reply(update_or_query, "Home › Subjects\n\nChoose a subject:", InlineKeyboardMarkup(rows))

async def user_chapters(update_or_query, subject: str, page=0):
    chat = update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat
    await busy(chat)
    chs = list_chapters_with_counts(subject, ai_only=False)
    if not chs:
        rows = [[InlineKeyboardButton("⬅️ Back", callback_data="u:startback")]]
        await edit_or_reply(update_or_query, f"Home › Subjects › {subject}\n\nNo chapters found.", InlineKeyboardMarkup(rows))
        return
    CH_PAGE = PAGE_SIZE
    pages = max(1, ceil(len(chs) / CH_PAGE))
    page = max(0, min(page, pages-1))
    slice_ = chs[page*CH_PAGE:(page+1)*CH_PAGE]
    rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"u:chap:{c}")]
            for c, qs in slice_]
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:chpp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:chpp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:startback")])
    await edit_or_reply(update_or_query, f"Home › Subjects › {subject}\n\nChoose a chapter:",
                        InlineKeyboardMarkup(rows))

# ---------- Flows: AI quizzes ----------
async def user_subjects_ai(update_or_query, page=0):
    chat = update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat
    await busy(chat)
    subs = list_subjects_with_counts(ai_only=True)
    if not subs:
        await edit_or_reply(update_or_query, "AI Gen › Subjects\n\nNo AI quizzes available.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
        return
    pages = max(1, ceil(len(subs) / PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    slice_ = subs[page*PAGE_SIZE:(page+1)*PAGE_SIZE]
    rows = [[InlineKeyboardButton(f"🤖 {s} (chapters: {chs} | quizzes: {qs})", callback_data=f"uai:subj:{s}")]
            for s, chs, qs in slice_]
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"uai:subjp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"uai:subjp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    await edit_or_reply(update_or_query, "AI Gen › Subjects\n\nChoose a subject:", InlineKeyboardMarkup(rows))

async def user_chapters_ai(update_or_query, subject: str, page=0):
    chat = update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat
    await busy(chat)
    chs = list_chapters_with_counts(subject, ai_only=True)
    if not chs:
        rows = [[InlineKeyboardButton("⬅️ Back", callback_data="uai:startback")]]
        await edit_or_reply(update_or_query, f"AI Gen › Subjects › {subject}\n\nNo chapters found.", InlineKeyboardMarkup(rows))
        return
    CH_PAGE = PAGE_SIZE
    pages = max(1, ceil(len(chs) / CH_PAGE))
    page = max(0, min(page, pages-1))
    slice_ = chs[page*CH_PAGE:(page+1)*CH_PAGE]
    rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"uai:chap:{c}")]
            for c, qs in slice_]
    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"uai:chpp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"uai:chpp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="uai:startback")])
    await edit_or_reply(update_or_query, f"AI Gen › Subjects › {subject}\n\nChoose a chapter:",
                        InlineKeyboardMarkup(rows))

# ---------- Timer & Pre-quiz ----------
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

# ---------- Start sessions ----------
async def begin_quiz_session(q, context: ContextTypes.DEFAULT_TYPE):
    try:
        subj = context.user_data.get("subject")
        chap = context.user_data.get("chapter")
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

        context.user_data["last_subject"] = subj
        context.user_data["last_chapter"] = chap
        context.user_data["last_open_period"] = op

        rows = conn.execute(
            "SELECT id FROM quizzes WHERE subject=? AND chapter=? AND COALESCE(ai_generated,0)=0",
            (subj, chap)
        ).fetchall()
        ids = [r[0] for r in rows]
        if not ids:
            await q.message.edit_text("No quizzes found for this selection.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
            return
        random.shuffle(ids)

        conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
        conn.execute("INSERT INTO sessions(user_id,chat_id,total,open_period,started_at,state) VALUES(?,?,?,?,?,?)",
                     (uid, chat_id, len(ids), op, int(time.time()), "running"))
        sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        for i, qid in enumerate(ids):
            conn.execute("INSERT INTO session_items(session_id,quiz_id,poll_id,message_id,idx) VALUES(?,?,?,?,?)",
                         (sid, qid, "", 0, i))
        conn.commit()

        await q.message.edit_text("Quiz started! 🎯")
        await send_next_quiz(context.bot, sid)

    except Exception as e:
        log.error("begin_quiz_session error: %s\n%s", e, traceback.format_exc())
        try:
            for aid in admin_ids_from_settings():
                await context.bot.send_message(aid, f"[Admin alert] begin_quiz_session error: {e}")
            await q.message.reply_text("Couldn't start quiz due to an error. Please try again.")
        except Exception:
            pass

async def begin_quiz_session_ai(q, context: ContextTypes.DEFAULT_TYPE):
    try:
        subj = context.user_data.get("ai_subject")
        chap = context.user_data.get("ai_chapter")
        if not subj or not chap:
            await q.message.edit_text("Please choose Subject and Chapter first.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="uai:start")]]))
            return
        uid = q.from_user.id
        if is_user_banned(uid):
            await q.message.edit_text("You are banned from using this bot.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
            return
        chat_id = q.message.chat.id
        op = int(context.user_data.get("ai_open_period", DEFAULT_OPEN_PERIOD))

        rows = conn.execute(
            "SELECT id FROM quizzes WHERE subject=? AND chapter=? AND ai_generated=1",
            (subj, chap)
        ).fetchall()
        ids = [r[0] for r in rows]
        if not ids:
            await q.message.edit_text("No AI quizzes found for this selection.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
            return
        random.shuffle(ids)

        conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
        conn.execute("INSERT INTO sessions(user_id,chat_id,total,open_period,started_at,state) VALUES(?,?,?,?,?,?)",
                     (uid, chat_id, len(ids), op, int(time.time()), "running"))
        sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for i, qid in enumerate(ids):
            conn.execute("INSERT INTO session_items(session_id,quiz_id,poll_id,message_id,idx) VALUES(?,?,?,?,?)",
                         (sid, qid, "", 0, i))
        conn.commit()

        await q.message.edit_text("AI Quiz started! 🤖🎯")
        await send_next_quiz(context.bot, sid)
    except Exception as e:
        log.error("begin_quiz_session_ai error: %s\n%s", e, traceback.format_exc())

# --- advance on answer; timer handled by fallback ---
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

        # ✂️ No extra "Controls" message — only /stop works now.

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



# ---------- Stats ----------
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

# ---------- Leaderboard (owner only) ----------
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
        uid = r["uid"]
        score = r["ok"]
        tot = r["tot"]
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

# ---------- /delquiz with confirmation ----------
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

# ---------- Export helpers ----------
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

# ---------- Export menus ----------
async def export_menu(q):
    subs = conn.execute("SELECT DISTINCT subject FROM quizzes WHERE subject IS NOT NULL").fetchall()
    rows = [[InlineKeyboardButton("📤 Export all", callback_data="a:export_all")]]
    for r in subs:
        s = r["subject"]
        rows.append([InlineKeyboardButton(f"📚 {s}", callback_data=f"a:export_subj:{s}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:panel")])
    await q.message.edit_text("Export › Choose option:", reply_markup=InlineKeyboardMarkup(rows))

async def export_subject_menu(q, subject):
    chs = conn.execute("SELECT DISTINCT chapter FROM quizzes WHERE subject=?", (subject,)).fetchall()
    rows = []
    for r in chs:
        c = r["chapter"]
        rows.append([InlineKeyboardButton(f"📖 {c}", callback_data=f"a:export_chap:{subject}:{c}")])
    rows.append([InlineKeyboardButton(f"📤 Export whole subject", callback_data=f"a:export_subj_confirm:{subject}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:export_menu")])
    await q.message.edit_text(f"Export › {subject}", reply_markup=InlineKeyboardMarkup(rows))

# ---------- Users panel (owner) with message option ----------
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

# ---------- Admin callback router (OWNER paths only in this tail) ----------
async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer(cache_time=1)
    uid = q.from_user.id
    act = q.data.split(":", 1)[1]

    OWNER_ONLY = {"export_menu", "export_all", "export_subj", "export_subj_confirm",
                  "export_chap", "export_chap_confirm", "users"}
    if act.split(":")[0] in OWNER_ONLY and not is_owner(uid):
        await notify_owner_unauthorized(context.bot, uid, f"admin_cb:{act}")
        await q.message.reply_text("Owner only.")
        return

    if act == "export_menu":
        await export_menu(q)
    elif act == "export_all":
        bio = _export_items(filename="quizzes_all.json")
        await q.message.reply_document(bio, caption="Exported all quizzes.")
    elif act.startswith("export_subj:"):
        subj = act.split(":",2)[1]
        await export_subject_menu(q, subj)
    elif act.startswith("export_subj_confirm:"):
        subj = act.split(":",2)[1]
        bio = _export_items("subject=?", (subj,), filename=f"quizzes_{subj}.json")
        await q.message.reply_document(bio, caption=f"Exported subject: {subj}")
    elif act.startswith("export_chap:"):
        _, _, subj, chap = act.split(":",3)
        rows = [
            [InlineKeyboardButton("📤 Export this chapter", callback_data=f"a:export_chap_confirm:{subj}:{chap}")],
            [InlineKeyboardButton("⬅️ Back", callback_data=f"a:export_subj:{subj}")]
        ]
        await q.message.edit_text(f"Export › {subj} › {chap}", reply_markup=InlineKeyboardMarkup(rows))
    elif act.startswith("export_chap_confirm:"):
        _, _, subj, chap = act.split(":",3)
        bio = _export_items("subject=? AND chapter=?", (subj, chap), filename=f"quizzes_{subj}_{chap}.json")
        await q.message.reply_document(bio, caption=f"Exported: {subj} › {chap}")
    elif act == "users":
        await users_panel(q, page=0)
    elif act.startswith("users:p:"):
        pg = int(act.split(":")[2]); await users_panel(q, page=pg)
    elif act.startswith("users:view:"):
        tgt = int(act.split(":")[2]); await user_detail_panel(q, tgt)
    elif act.startswith("users:toggle:"):
        tgt = int(act.split(":")[2])
        cur = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (tgt,)).fetchone()
        conn.execute("UPDATE users SET is_banned=? WHERE user_id=?", (0 if cur and cur["is_banned"] else 1, tgt))
        conn.commit()
        await user_detail_panel(q, tgt)
    elif act.startswith("users:msg:"):
        tgt = int(act.split(":")[2])
        context.user_data["mode"] = "MSG_USER"
        context.user_data["msg_user_id"] = tgt
        await q.message.edit_text(f"Type the message to send only to user id:{tgt}.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"a:users:view:{tgt}")]]))
    elif act.startswith("delquiz:"):
        qid = int(act.split(":")[1])
        row = conn.execute("SELECT added_by FROM quizzes WHERE id=?", (qid,)).fetchone()
        if not row:
            await q.message.edit_text("Quiz already removed.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
            return
        if not (is_owner(uid) or (is_admin(uid) and int(row["added_by"] or 0) == int(uid))):
            await q.message.reply_text("Not permitted.")
            return
        conn.execute("DELETE FROM quizzes WHERE id=?", (qid,))
        conn.commit()
        await q.message.edit_text(f"Deleted quiz #{qid}.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))

# ---------- Text/Poll handler (extra modes used here) ----------
async def text_or_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    uid = update.effective_user.id
    mode = context.user_data.get("mode")

    if is_user_banned(uid):
        if update.message:
            await update.message.reply_text("You are banned from using this bot.")
        return

    # owner sends message to a user
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

    # contact admin → owner only
    if mode == "CONTACT_ADMIN" and update.message:
        u = update.effective_user
        header = f"📨 Message to owner from @{u.username or u.first_name} (id:{u.id}):"
        try:
            await context.bot.send_message(OWNER_ID, header)
            await context.bot.copy_message(chat_id=OWNER_ID,
                                           from_chat_id=update.message.chat.id,
                                           message_id=update.message.message_id)
        except Exception:
            pass
        context.user_data["mode"] = None
        await update.message.reply_text("✅ Your message has been sent to the owner.",
                                        reply_markup=main_menu(u.id))
        return

# ---------- Button dispatcher (user entrypoints already exist earlier) ----------
async def btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    q = update.callback_query
    await q.answer(cache_time=1)
    data = q.data
    uid = q.from_user.id

    if is_user_banned(uid):
        await q.message.edit_text("You are banned from using this bot.")
        return

    if data == "u:stats":
        await show_stats(q); return
    if data == "u:lb":
        if not is_owner(uid): await q.message.reply_text("Owner only."); return
        await leaderboard(q, page=0); return
    if data.startswith("u:lbp:"):
        pg = int(data.split(":")[2]); await leaderboard(q, page=pg); return
    if data == "u:contact":
        context.user_data["mode"] = "CONTACT_ADMIN"
        await q.message.edit_text("Type your message for the owner:",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
        return

    # admin-only routes (owner sections handled inside admin_cb)
    if data.startswith("a:"):
        if not is_admin(uid):
            await notify_owner_unauthorized(context.bot, uid, f"callback:{data}")
            await q.message.reply_text("Only admin can use this.")
            return
        await admin_cb(update, context)
        return

# ---------- /stop ----------
async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
    conn.commit()
    await update.message.reply_text("Quiz stopped.", reply_markup=main_menu(uid))

# ---------- Keepalive ----------
app = Flask(__name__)
@app.get("/")
def home(): return "OK"
def run_keepalive(): app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

# ---------- Main ----------
if __name__ == "__main__":
    db_init()
    Thread(target=run_keepalive, daemon=True).start()
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    app_.add_handler(CommandHandler("start", start))
    app_.add_handler(CommandHandler("help", help_cmd))
    app_.add_handler(CommandHandler("stop", stop_cmd))
    app_.add_handler(CommandHandler("delquiz", delquiz_cmd))
    app_.add_handler(CallbackQueryHandler(btn))
    app_.add_handler(PollAnswerHandler(poll_answer))
    app_.add_handler(MessageHandler(filters.ALL, text_or_poll))
    app_.run_polling()
