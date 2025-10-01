# --- main.py (Part 1/6) ---
import os, json, time, random, logging, sqlite3, asyncio, re, traceback, hashlib
from threading import Thread
from math import ceil

from dotenv import load_dotenv
load_dotenv("secrets.env")

from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    PollAnswerHandler, ContextTypes, filters
)

# ========== CONFIG ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS_ENV = os.getenv("ADMIN_ID", "")  # comma-separated
OWNER_ID = int(os.getenv("OWNER_ID", "5902126578"))  # hard-protected owner
DB_PATH = os.getenv("DB_PATH", "db.sqlite3")

PAGE_SIZE = 10               # general pagination
LB_PAGE = 20                 # leaderboard size
USERS_PAGE = 25              # users list page
DEFAULT_OPEN_PERIOD = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("quizbot")

# ---- DB ----
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")

def db_init():
    cur = conn.cursor()

    # core tables
    cur.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_at INTEGER DEFAULT (strftime('%s','now')),
            last_seen INTEGER DEFAULT (strftime('%s','now'))
        );
    """)
    cur.execute("""CREATE TABLE IF NOT EXISTS admins(user_id INTEGER PRIMARY KEY);""")

    # 🔧 missing table that caused your crash
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bans(
            user_id INTEGER PRIMARY KEY,
            reason TEXT,
            banned_at INTEGER DEFAULT (strftime('%s','now'))
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
            added_by INTEGER
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
            idx INTEGER NOT NULL,
            poll_id TEXT,
            message_id INTEGER,
            chosen INTEGER,
            is_correct INTEGER,
            sent_at INTEGER,
            closed_at INTEGER
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_polls(
            poll_id TEXT PRIMARY KEY,
            session_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL
        );
    """)

    # ensure owner is always an admin
    cur.execute("INSERT OR IGNORE INTO admins(user_id) VALUES(?)", (OWNER_ID,))
    conn.commit()

def is_banned(uid: int) -> bool:
    """Return True if user is banned; safe even if schema was incomplete."""
    try:
        return conn.execute("SELECT 1 FROM bans WHERE user_id=?", (uid,)).fetchone() is not None
    except sqlite3.OperationalError:
        # if table didn’t exist yet, rebuild schema and treat as not banned
        db_init()
        return False

def ban_user_id(uid: int, reason: str = None):
    conn.execute("INSERT OR REPLACE INTO bans(user_id, reason, banned_at) VALUES(?,?,strftime('%s','now'))",
                 (uid, reason))
    conn.commit()

def unban_user_id(uid: int):
    conn.execute("DELETE FROM bans WHERE user_id=?", (uid,))
    conn.commit()

    # migrations
    add_col_if_missing("quizzes", "subject", "TEXT")
    add_col_if_missing("quizzes", "chapter", "TEXT")
    add_col_if_missing("quizzes", "explanation", "TEXT")
    add_col_if_missing("sessions", "chat_id", "INTEGER NOT NULL DEFAULT 0")
    add_col_if_missing("sessions", "current_index", "INTEGER DEFAULT 0")
    add_col_if_missing("sessions", "finished_at", "INTEGER")
    add_col_if_missing("session_items", "idx", "INTEGER")
    add_col_if_missing("session_items", "sent_at", "INTEGER")
    add_col_if_missing("session_items", "poll_id", "TEXT")
    add_col_if_missing("session_items", "message_id", "INTEGER")
    add_col_if_missing("session_items", "chosen", "INTEGER")
    add_col_if_missing("session_items", "is_correct", "INTEGER")
    add_col_if_missing("session_items", "closed_at", "INTEGER")

# ========== SETTINGS / ADMINS ==========
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
    ids = set()
    if saved:
        ids |= {int(x) for x in saved.split(",") if x.strip().isdigit()}
    if ADMIN_IDS_ENV:
        ids |= {int(x) for x in ADMIN_IDS_ENV.split(",") if x.strip().isdigit()}
    ids.add(OWNER_ID)  # ensure owner always admin
    return ids

def is_admin(uid: int) -> bool:
    return uid in admin_ids_from_settings()

def add_admin(uid: int):
    ids = admin_ids_from_settings()
    ids.add(uid)
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

def remove_admin(uid: int):
    if uid == OWNER_ID:
        return  # cannot remove owner
    ids = admin_ids_from_settings()
    if uid in ids:
        ids.remove(uid)
        sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

# ========== BANS ==========
def is_banned(uid: int) -> bool:
    return conn.execute("SELECT 1 FROM banned WHERE user_id=?", (uid,)).fetchone() is not None

def ban_user(uid: int, by_uid: int, reason: str = None):
    if uid == OWNER_ID:
        return
    conn.execute(
        "INSERT OR REPLACE INTO banned(user_id,reason,banned_at,banned_by) VALUES(?,?,?,?)",
        (uid, reason or "", int(time.time()), by_uid)
    )
    conn.commit()

def unban_user(uid: int):
    conn.execute("DELETE FROM banned WHERE user_id=?", (uid,))
    conn.commit()

# ========== HELPERS ==========
async def busy(chat, action=ChatAction.TYPING, secs=0.15):
    # light typing effect
    await asyncio.sleep(secs)

def format_uname_row(row):
    if not row: return None
    if row["username"]:
        return f"@{row['username']}"
    name = " ".join(filter(None, [row["first_name"], row["last_name"]]))
    return name or None

def send_admin_alert_sync(text: str):
    from telegram import Bot
    for aid in admin_ids_from_settings():
        try:
            Bot(BOT_TOKEN).send_message(aid, text)
        except Exception:
            pass

async def send_admin_alert(text: str, bot=None):
    try:
        if bot is None:
            send_admin_alert_sync(text)
            return
        for aid in admin_ids_from_settings():
            try:
                await bot.send_message(aid, text)
            except Exception:
                pass
    except Exception:
        pass

def upsert_user(update: Update):
    """Insert or update user info without chat_id column."""
    u = update.effective_user
    try:
        conn.execute(
            """
            INSERT INTO users(user_id, username, first_name, last_name, joined_at, last_seen)
            VALUES(?, ?, ?, ?, strftime('%s','now'), strftime('%s','now'))
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                last_seen=strftime('%s','now')
            """,
            (u.id, u.username, u.first_name, u.last_name),
        )
        conn.commit()
    except sqlite3.OperationalError:
        # recreate tables if schema mismatch
        db_init()
    is_new = not existed
    if is_new and admin_ids_from_settings():
        total = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        uname = f"@{u.username}" if u.username else (u.first_name or "—")
        text = f"✅ New user joined\nUsername: {uname}\nUserid: {u.id}\n\nTotal users: {total}"
        send_admin_alert_sync(text)
    return is_new

def list_subjects_with_counts():
    cur = conn.execute(
        "SELECT COALESCE(subject,'Uncategorized') s, COUNT(DISTINCT chapter) chs, COUNT(*) qs "
        "FROM quizzes GROUP BY s ORDER BY qs DESC, s"
    )
    return [(r["s"], r["chs"], r["qs"]) for r in cur.fetchall()]

def list_chapters_with_counts(subject: str):
    cur = conn.execute(
        "SELECT COALESCE(chapter,'General') c, COUNT(*) qs FROM quizzes WHERE subject=? GROUP BY c ORDER BY qs DESC, c",
        (subject,)
    )
    return [(r["c"], r["qs"]) for r in cur.fetchall()]

def short_id(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]

async def edit_or_reply(obj, text, markup=None, **kwargs):
    if hasattr(obj, "callback_query") and obj.callback_query:
        await obj.callback_query.message.edit_text(text, reply_markup=markup, **kwargs)
    elif isinstance(obj, Update):
        await obj.effective_chat.send_message(text, reply_markup=markup, **kwargs)

def _truncate(s: str, n: int) -> str:
    s = s or ""
    if len(s) <= n: return s
    return s[: max(0, n-1)] + "…"

def sanitize_for_poll(question: str, options: list, explanation: str):
    question = question or ""
    options = [(o or "") for o in options]
    explanation = explanation or None
    options = [_truncate(o, 100) for o in options]
    seen, opts = set(), []
    for o in options:
        if not o.strip(): continue
        if o in seen: continue
        seen.add(o); opts.append(o)
    options = opts[:10]
    if len(options) < 2:
        raise ValueError("Not enough valid options (need ≥2).")
    if explanation:
        explanation = _truncate(explanation, 200)
    question = _truncate(question.strip(), 292)
    return question, options, explanation

# ========== UI / MENUS ==========
def main_menu(uid: int):
    rows = [
        [InlineKeyboardButton("▶️ Start quiz", callback_data="u:start")],
        [InlineKeyboardButton("📊 My stats", callback_data="u:stats"),
         InlineKeyboardButton("📨 Contact admin", callback_data="u:contact")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="u:help")]
    ]
    if is_admin(uid):
        rows.insert(1, [
            InlineKeyboardButton("🏆 Leaderboard", callback_data="u:lb"),
            InlineKeyboardButton("🛠 Admin panel", callback_data="a:panel")
        ])
    return InlineKeyboardMarkup(rows)

def admin_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add quiz", callback_data="a:add")],
        [InlineKeyboardButton("📥 Import JSON", callback_data="a:import"),
         InlineKeyboardButton("📤 Export JSON", callback_data="a:export")],
        [InlineKeyboardButton("#️⃣ Count", callback_data="a:count"),
         InlineKeyboardButton("👥 Users", callback_data="a:users")],
        [InlineKeyboardButton("⛔️ Delete last", callback_data="a:dellast")],
        [InlineKeyboardButton("📣 Broadcast", callback_data="a:broadcast")],
        [InlineKeyboardButton("👑 Admins", callback_data="a:admins")],
        [InlineKeyboardButton("⬅️ Back", callback_data="a:back")]
    ])

def admins_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📃 View admins", callback_data="a:admins_list")],
        [InlineKeyboardButton("➕ Add admin (enter ID)", callback_data="a:addadmin")],
        [InlineKeyboardButton("🗑 Remove admin (enter ID)", callback_data="a:rmadmin")],
        [InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]
    ])

# ========== COMMANDS ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_banned(uid) and not is_admin(uid):
        await update.effective_chat.send_message("🚫 You are banned from using this bot.")
        return
    upsert_user(update)
    if not admin_ids_from_settings():
        add_admin(uid)
        log.info("Auto-assigned admin to %s", uid)
    first = update.effective_user.first_name or "there"
    text = (
        f"👋 Hey *{first}*, welcome to *Madhyamik Helper Quiz Bot*!\n\n"
        "• Pick a *Subject* → *Chapter* → *Timer* → tap *I am ready!* 🎯\n"
        "• Use /stop anytime to end a quiz."
    )
    await update.effective_chat.send_message(text, parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu(uid))

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_banned(uid) and not is_admin(uid):
        await update.effective_chat.send_message("🚫 You are banned from using this bot.")
        return
    upsert_user(update)
    await update.message.reply_text(
        "📘 *How to use*\n"
        "1) Start → Subject → Chapter → Timer (or Without Timer)\n"
        "2) Tap *I am ready!*\n"
        "3) Answer each question; next appears automatically.\n\n"
        "Admins can manage quizzes, users, broadcast & admins.",
        parse_mode=ParseMode.MARKDOWN
    )

# --- main.py (Part 2/6) ---

# ========== ADMIN FEATURES ==========
async def show_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await send_admin_alert(
            f"🚨 Unauthorized admin-list attempt!\n"
            f"User: @{update.effective_user.username}\n"
            f"ID: {update.effective_user.id}",
            context.bot
        )
        return
    ids = admin_ids_from_settings()
    rows = []
    for i in ids:
        tag = "(Owner)" if i == OWNER_ID else ""
        rows.append(f"• `{i}` {tag}")
    await update.effective_chat.send_message("👑 *Admins:*\n" + "\n".join(rows), parse_mode=ParseMode.MARKDOWN)

async def add_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        await send_admin_alert(
            f"🚨 Unauthorized add-admin attempt!\n"
            f"User: @{update.effective_user.username}\nID: {uid}",
            context.bot
        )
        return
    if not context.args:
        await update.message.reply_text("Usage: /addadmin <user_id>")
        return
    try:
        new_id = int(context.args[0])
    except:
        await update.message.reply_text("⚠️ Invalid user ID.")
        return
    if new_id == OWNER_ID:
        await update.message.reply_text("⚠️ You cannot modify the owner.")
        return
    add_admin(new_id)
    await update.message.reply_text(f"✅ User `{new_id}` promoted to admin.", parse_mode=ParseMode.MARKDOWN)

async def remove_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        await send_admin_alert(
            f"🚨 Unauthorized remove-admin attempt!\n"
            f"User: @{update.effective_user.username}\nID: {uid}",
            context.bot
        )
        return
    if not context.args:
        await update.message.reply_text("Usage: /rmadmin <user_id>")
        return
    try:
        rid = int(context.args[0])
    except:
        await update.message.reply_text("⚠️ Invalid user ID.")
        return
    if rid == OWNER_ID:
        await update.message.reply_text("⚠️ You cannot remove the owner.")
        return
    remove_admin(rid)
    await update.message.reply_text(f"🗑 User `{rid}` removed from admin list.", parse_mode=ParseMode.MARKDOWN)

async def list_users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await send_admin_alert(
            f"🚨 Unauthorized users-list attempt!\n"
            f"User: @{update.effective_user.username}\nID: {update.effective_user.id}",
            context.bot
        )
        return
    cur = conn.execute("SELECT user_id, username FROM users ORDER BY last_seen DESC LIMIT 50;")
    rows = []
    for r in cur.fetchall():
        uname = f"@{r['username']}" if r["username"] else ""
        rows.append(f"• {uname} ({r['user_id']})")
    total = conn.execute("SELECT COUNT(*) c FROM users;").fetchone()["c"]
    await update.message.reply_text(
        "👥 *Recent Users:*\n" + "\n".join(rows) + f"\n\nTotal: {total}",
        parse_mode=ParseMode.MARKDOWN
    )

async def ban_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await send_admin_alert(
            f"🚨 Unauthorized ban attempt!\n"
            f"User: @{update.effective_user.username}\nID: {update.effective_user.id}",
            context.bot
        )
        return
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id>")
        return
    try:
        bid = int(context.args[0])
    except:
        await update.message.reply_text("⚠️ Invalid user ID.")
        return
    if bid == OWNER_ID:
        await update.message.reply_text("⚠️ You cannot ban the owner.")
        return
    ban_user(bid, update.effective_user.id)
    await update.message.reply_text(f"⛔️ User `{bid}` banned.", parse_mode=ParseMode.MARKDOWN)

async def unban_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("⚠️ Invalid user ID.")
        return
    unban_user(uid)
    await update.message.reply_text(f"✅ User `{uid}` unbanned.", parse_mode=ParseMode.MARKDOWN)

# --- main.py (Part 3/6) ---

# ========== USER TRACKING ==========
def track_user(user):
    uid = user.id
    uname = user.username
    conn.execute(
        "INSERT OR IGNORE INTO users(user_id, username, joined, last_seen) VALUES(?,?,?,?)",
        (uid, uname, int(time.time()), int(time.time()))
    )
    conn.execute(
        "UPDATE users SET username=?, last_seen=? WHERE user_id=?",
        (uname, int(time.time()), uid)
    )
    conn.commit()

    # Notify admins if new user
    cur = conn.execute("SELECT COUNT(*) c FROM users;")
    total = cur.fetchone()["c"]
    if total == 1 or conn.total_changes > 0:
        for admin in admin_ids_from_settings():
            context_bot = None
            try:
                context_bot = app.bot
            except:
                pass
            if context_bot:
                context_bot.send_message(
                    admin,
                    f"✅ *New user joined*\n"
                    f"Username: @{uname}\n"
                    f"Userid: {uid}\n\n"
                    f"Total users: {total}",
                    parse_mode=ParseMode.MARKDOWN
                )

def is_banned(uid: int) -> bool:
    cur = conn.execute("SELECT 1 FROM bans WHERE user_id=?", (uid,))
    return cur.fetchone() is not None

def ban_user(uid: int, by: int):
    conn.execute("INSERT OR REPLACE INTO bans(user_id, banned_by) VALUES(?,?)", (uid, by))
    conn.commit()

def unban_user(uid: int):
    conn.execute("DELETE FROM bans WHERE user_id=?", (uid,))
    conn.commit()

# ========== EXPORT/IMPORT ==========
async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    data = {}
    for row in conn.execute("SELECT * FROM subjects;"):
        subj_id = row["id"]
        data[subj_id] = {"name": row["name"], "chapters": {}}
        for c in conn.execute("SELECT * FROM chapters WHERE subject_id=?", (subj_id,)):
            chap_id = c["id"]
            data[subj_id]["chapters"][chap_id] = {"name": c["name"], "quizzes": []}
            for q in conn.execute("SELECT * FROM quizzes WHERE chapter_id=?", (chap_id,)):
                data[subj_id]["chapters"][chap_id]["quizzes"].append({
                    "question": q["question"],
                    "options": json.loads(q["options"]),
                    "correct": q["correct"],
                    "explanation": q["explanation"]
                })
    fname = f"quiz_export_{int(time.time())}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    await update.message.reply_document(document=open(fname, "rb"), filename=fname)

async def import_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not update.message.document:
        await update.message.reply_text("⚠️ Please attach a JSON file.")
        return
    file = await update.message.document.get_file()
    path = f"/tmp/{update.message.document.file_name}"
    await file.download_to_drive(path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for subj_id, subj in data.items():
        conn.execute("INSERT OR IGNORE INTO subjects(id, name) VALUES(?,?)", (subj_id, subj["name"]))
        for chap_id, chap in subj["chapters"].items():
            conn.execute("INSERT OR IGNORE INTO chapters(id, subject_id, name) VALUES(?,?,?)",
                         (chap_id, subj_id, chap["name"]))
            for q in chap["quizzes"]:
                conn.execute(
                    "INSERT INTO quizzes(chapter_id, question, options, correct, explanation) VALUES(?,?,?,?,?)",
                    (chap_id, q["question"], json.dumps(q["options"]), q["correct"], q.get("explanation"))
                )
    conn.commit()
    await update.message.reply_text("✅ Import complete.")

# --- main.py (Part 4/6) ---

# ========== SUBJECTS / CHAPTERS UI ==========
def subjects_markup(page: int = 0):
    subs = list_subjects_with_counts()
    if not subs:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
    PAGE = PAGE_SIZE
    pages = max(1, (len(subs)+PAGE-1)//PAGE)
    page = max(0, min(page, pages-1))
    start = page*PAGE
    slice_ = subs[start:start+PAGE]
    rows = []
    for name, chs, qs in slice_:
        rows.append([InlineKeyboardButton(f"{name}  ({chs} chapters • {qs} q.)", callback_data=f"u:sub:{name}")])
    nav = []
    if pages>1:
        if page>0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:subp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page<pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:subp:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    return InlineKeyboardMarkup(rows)

async def show_subjects(update_or_query, page=0):
    await edit_or_reply(update_or_query, "📚 *Choose a subject:*", subjects_markup(page), parse_mode=ParseMode.MARKDOWN)

def chapters_markup(subject: str, page: int = 0):
    chs = list_chapters_with_counts(subject)
    if not chs:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Subjects", callback_data="u:cat")],
            [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]
        ])
    PAGE = PAGE_SIZE
    pages = max(1, (len(chs)+PAGE-1)//PAGE)
    page = max(0, min(page, pages-1))
    start = page*PAGE
    slice_ = chs[start:start+PAGE]
    rows = []
    for name, qs in slice_:
        rows.append([InlineKeyboardButton(f"{name}  ({qs} q.)", callback_data=f"u:chap:{subject}:{name}")])
    nav = []
    if pages>1:
        if page>0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:chapp:{subject}:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page<pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:chapp:{subject}:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Subjects", callback_data="u:cat"),
                 InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    return InlineKeyboardMarkup(rows)

async def show_chapters(update_or_query, subject: str, page=0):
    await edit_or_reply(update_or_query, f"📖 *{subject}* — choose a chapter:", chapters_markup(subject, page), parse_mode=ParseMode.MARKDOWN)

def timer_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ 15s", callback_data="u:t:15"),
         InlineKeyboardButton("⏱ 30s", callback_data="u:t:30"),
         InlineKeyboardButton("⏱ 45s", callback_data="u:t:45")],
        [InlineKeyboardButton("🚫 Without Timer", callback_data="u:t:0")],
        [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]
    ])

async def timer_select(update_or_query):
    await edit_or_reply(update_or_query, "⏲ *Select a timer per question:*", timer_markup(), parse_mode=ParseMode.MARKDOWN)

async def pre_quiz_screen(q, context: ContextTypes.DEFAULT_TYPE):
    sd = context.user_data
    subj = sd.get("subject")
    chap = sd.get("chapter")
    t = sd.get("timer", 0)
    txt = (
        "🏁 *Get ready!*\n\n"
        f"*Subject:* {subj}\n"
        f"*Chapter:* {chap}\n"
        f"*Timer:* {'Without Timer' if int(t)==0 else str(t)+'s'}\n\n"
        "Press the button when ready. Send /stop to cancel."
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I am ready!", callback_data="u:ready")],
        [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]
    ])
    await q.message.edit_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)

# ========== QUIZ ENGINE ==========
def pick_questions(subj: str, chap: str):
    cur = conn.execute(
        "SELECT id FROM quizzes WHERE subject=? AND chapter=? ORDER BY RANDOM()",
        (subj, chap)
    )
    return [r["id"] for r in cur.fetchall()]

async def begin_quiz_session(q, context: ContextTypes.DEFAULT_TYPE):
    uid = q.from_user.id
    chat_id = q.message.chat_id
    sd = context.user_data
    subj = sd.get("subject"); chap = sd.get("chapter")
    t = int(sd.get("timer", 0))
    ids = pick_questions(subj, chap)
    if not ids:
        await q.message.edit_text("⚠️ No questions available for this selection.", reply_markup=main_menu(uid))
        return

    conn.execute(
        "INSERT INTO sessions(user_id,chat_id,total,open_period,started_at,state,current_index) VALUES(?,?,?,?,?,?,?)",
        (uid, chat_id, len(ids), t if t>0 else DEFAULT_OPEN_PERIOD, int(time.time()), "running", 0)
    )
    conn.commit()
    sid = conn.execute("SELECT last_insert_rowid() id").fetchone()["id"]

    for idx, qid in enumerate(ids):
        conn.execute(
            "INSERT INTO session_items(session_id,quiz_id,idx) VALUES(?,?,?)",
            (sid, qid, idx)
        )
    conn.commit()

    await q.message.edit_text("🎯 *Quiz started!* Good luck!\n(Use /stop to end.)", parse_mode=ParseMode.MARKDOWN)
    await send_next_quiz(context.bot, sid, uid, schedule_timer=(t>0))

async def send_next_quiz(bot, session_id: int, uid: int, schedule_timer: bool):
    # get next item
    srow = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    if not srow or srow["state"] != "running":
        return
    idx = int(srow["current_index"])
    total = int(srow["total"])
    if idx >= total:
        await finalize_session(bot, srow)
        return

    item = conn.execute(
        "SELECT * FROM session_items WHERE session_id=? AND idx=?",
        (session_id, idx)
    ).fetchone()
    if not item:
        # skip forward
        conn.execute("UPDATE sessions SET current_index=current_index+1 WHERE id=?", (session_id,))
        conn.commit()
        await send_next_quiz(bot, session_id, uid, schedule_timer)
        return

    qrow = conn.execute("SELECT * FROM quizzes WHERE id=?", (item["quiz_id"],)).fetchone()
    options = json.loads(qrow["options_json"])
    question, options, explanation = sanitize_for_poll(qrow["question"], options, qrow["explanation"])

    msg = await bot.send_poll(
        chat_id=srow["chat_id"],
        question=question,
        options=options,
        type="quiz",
        correct_option_id=int(qrow["correct"]),
        explanation=explanation,
        is_anonymous=False,
        open_period=srow["open_period"] if schedule_timer else None
    )
    conn.execute(
        "UPDATE session_items SET poll_id=?, message_id=?, sent_at=? WHERE id=?",
        (msg.poll.id, msg.message_id, int(time.time()), item["id"])
    )
    conn.execute(
        "INSERT OR REPLACE INTO active_polls(poll_id,session_id,user_id) VALUES(?,?,?)",
        (msg.poll.id, session_id, uid)
    )
    conn.commit()

    # schedule timer end
    if schedule_timer and srow["open_period"]:
        try:
            app.job_queue.run_once(
                lambda *_: asyncio.create_task(handle_timer_expiry(bot, msg.poll.id)),
                when=srow["open_period"]+1,
                name=f"timer_{msg.poll.id}"
            )
        except Exception as e:
            await send_admin_alert(f"[Admin alert] scheduling error: {e}", bot)

async def handle_timer_expiry(bot, poll_id: str):
    row = conn.execute("SELECT session_id,user_id FROM active_polls WHERE poll_id=?", (poll_id,)).fetchone()
    if not row:  # already answered
        return
    # mark as missed
    item = conn.execute(
        "SELECT si.* FROM session_items si WHERE si.session_id=? AND si.poll_id=?",
        (row["session_id"], poll_id)
    ).fetchone()
    if not item:
        return
    if item["chosen"] is None:
        conn.execute("UPDATE session_items SET chosen=-1, is_correct=0, closed_at=? WHERE id=?",
                     (int(time.time()), item["id"]))
        conn.execute("DELETE FROM active_polls WHERE poll_id=?", (poll_id,))
        conn.execute("UPDATE sessions SET current_index=current_index+1 WHERE id=?", (row["session_id"],))
        conn.commit()
        await send_next_quiz(bot, row["session_id"], row["user_id"], schedule_timer=True)

async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ans = update.poll_answer
    pid = ans.poll_id
    chosen = ans.option_ids[0] if ans.option_ids else None
    arow = conn.execute("SELECT session_id,user_id FROM active_polls WHERE poll_id=?", (pid,)).fetchone()
    if not arow:
        return
    item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND poll_id=?", (arow["session_id"], pid)).fetchone()
    if not item:
        return
    quiz = conn.execute("SELECT correct FROM quizzes WHERE id=?", (item["quiz_id"],)).fetchone()
    ok = 1 if (chosen is not None and int(chosen) == int(quiz["correct"])) else 0
    conn.execute("UPDATE session_items SET chosen=?, is_correct=?, closed_at=? WHERE id=?",
                 (chosen if chosen is not None else -1, ok, int(time.time()), item["id"]))
    conn.execute("DELETE FROM active_polls WHERE poll_id=?", (pid,))
    conn.execute("UPDATE sessions SET current_index=current_index+1 WHERE id=?", (arow["session_id"],))
    conn.commit()

    # without timer -> send immediately; with timer -> also send immediately (per requirement)
    await send_next_quiz(context.bot, arow["session_id"], arow["user_id"], schedule_timer=(context.user_data.get("timer",0)>0))

async def finalize_session(bot, srow):
    # compute stats
    rows = conn.execute(
        "SELECT COUNT(*) t, SUM(CASE WHEN chosen>=0 THEN 1 ELSE 0 END) answered, "
        "SUM(CASE WHEN is_correct=1 THEN 1 ELSE 0 END) ok "
        "FROM session_items WHERE session_id=?", (srow["id"],)
    ).fetchone()
    t = rows["t"] or 0
    answered = rows["answered"] or 0
    ok = rows["ok"] or 0
    missed = max(0, t - answered)
    wrong = max(0, answered - ok)
    dur = max(1, int(time.time()) - int(srow["started_at"]))
    mm = dur // 60
    ss = dur % 60

    conn.execute("UPDATE sessions SET state='finished', finished_at=? WHERE id=?", (int(time.time()), srow["id"]))
    conn.commit()

    # message
    text = (
        "🏁 *The quiz has finished!*\n"
        f"You answered *{answered}*/*{t}* questions:\n\n"
        f"✅ Correct – *{ok}*    ❌ Wrong – *{wrong}*    ⌛️ Missed – *{missed}*\n"
        f"🕒 Time - {mm} min {ss} sec"
    )
    # buttons: Try again (same subject/chapter/timer)
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁 Try again", callback_data="u:ready")],
        [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]
    ])
    await bot.send_message(srow["chat_id"], text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)

# ========== STATS / LEADERBOARD ==========
async def show_stats(q):
    uid = q.from_user.id
    r = conn.execute(
        "SELECT COUNT(si.id) tot, SUM(si.is_correct) ok "
        "FROM sessions s LEFT JOIN session_items si ON si.session_id=s.id "
        "WHERE s.user_id=?", (uid,)
    ).fetchone()
    tot = r["tot"] or 0
    ok = r["ok"] or 0
    wrong = max(0, tot - ok)
    await q.message.edit_text(
        f"📊 *Your overall stats*\nCorrect: *{ok}*\nWrong: *{wrong}*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_menu(uid)
    )

async def leaderboard(q):
    if not is_admin(q.from_user.id):
        await q.message.edit_text("🏆 Leaderboard is admin-only.", reply_markup=main_menu(q.from_user.id))
        return
    rows = conn.execute(
        "SELECT s.user_id, COALESCE(SUM(si.is_correct),0) ok "
        "FROM sessions s LEFT JOIN session_items si ON si.session_id=s.id "
        "GROUP BY s.user_id ORDER BY ok DESC"
    ).fetchall()
    if not rows:
        await q.message.edit_text("No data yet.", reply_markup=main_menu(q.from_user.id))
        return
    lines = ["🏆 *Leaderboard (all time):*"]
    pos = 1
    for r in rows:
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (r["user_id"],)).fetchone()
        name = format_uname_row(u) or f"user {r['user_id']}"
        lines.append(f"{pos}. {name} — *{r['ok']}* correct")
        pos += 1
    await q.message.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu(q.from_user.id))

# --- main.py (Part 5/6) ---

# ========== ADMIN FEATURES ==========
async def view_admins(q):
    if not is_admin(q.from_user.id):
        await q.message.edit_text("⛔ Only admins can access this.", reply_markup=main_menu(q.from_user.id))
        return
    rows = conn.execute("SELECT * FROM admins").fetchall()
    if not rows:
        await q.message.edit_text("No admins set.", reply_markup=main_menu(q.from_user.id))
        return
    lines = ["👮 *Admin list:*"]
    for r in rows:
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (r["user_id"],)).fetchone()
        name = format_uname_row(u) or f"user {r['user_id']}"
        lines.append(f"- {name} (`{r['user_id']}`)")
    await q.message.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=admin_panel())

async def add_admin(q):
    if not is_admin(q.from_user.id):
        await q.message.edit_text("⛔ Unauthorized.", reply_markup=main_menu(q.from_user.id))
        await send_admin_alert(f"⚠️ Unauthorized admin attempt\nUser: {q.from_user.username} ({q.from_user.id})", None)
        return
    await q.message.edit_text("Send the *user id* to add as admin.", parse_mode=ParseMode.MARKDOWN)
    context_user = q.from_user.id
    context_waiting[context_user] = "add_admin"

async def remove_admin(q):
    if not is_admin(q.from_user.id):
        await q.message.edit_text("⛔ Unauthorized.", reply_markup=main_menu(q.from_user.id))
        await send_admin_alert(f"⚠️ Unauthorized admin attempt\nUser: {q.from_user.username} ({q.from_user.id})", None)
        return
    await q.message.edit_text("Send the *user id* to remove from admins.", parse_mode=ParseMode.MARKDOWN)
    context_user = q.from_user.id
    context_waiting[context_user] = "remove_admin"

async def total_users(q):
    if not is_admin(q.from_user.id):
        await q.message.edit_text("⛔ Unauthorized.", reply_markup=main_menu(q.from_user.id))
        return
    rows = conn.execute("SELECT * FROM users").fetchall()
    count = len(rows)
    lines = [f"👥 *Total users:* {count}"]
    for r in rows:
        uname = format_uname_row(r) or "unknown"
        lines.append(f"- {uname} (`{r['user_id']}`)")
    await q.message.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=admin_panel())

async def ban_user(q):
    if not is_admin(q.from_user.id):
        await q.message.edit_text("⛔ Unauthorized.", reply_markup=main_menu(q.from_user.id))
        return
    await q.message.edit_text("Send the *user id* to ban.", parse_mode=ParseMode.MARKDOWN)
    context_waiting[q.from_user.id] = "ban_user"

context_waiting = {}

async def process_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id
    if uid not in context_waiting: return
    action = context_waiting.pop(uid)
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ Invalid user id.")
        return
    target_id = int(text)
    if action == "add_admin":
        conn.execute("INSERT OR IGNORE INTO admins(user_id) VALUES(?)", (target_id,))
        conn.commit()
        await update.message.reply_text(f"✅ {target_id} added as admin.", reply_markup=admin_panel())
    elif action == "remove_admin":
        if target_id == 5902126578:
            await update.message.reply_text("❌ Owner cannot be removed.")
            return
        conn.execute("DELETE FROM admins WHERE user_id=?", (target_id,))
        conn.commit()
        await update.message.reply_text(f"🗑 {target_id} removed from admins.", reply_markup=admin_panel())
    elif action == "ban_user":
        conn.execute("DELETE FROM users WHERE user_id=?", (target_id,))
        conn.commit()
        await update.message.reply_text(f"🚫 {target_id} banned.", reply_markup=admin_panel())

# ========== USER JOIN NOTIFICATION ==========
async def on_new_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    conn.execute("INSERT OR IGNORE INTO users(user_id,username,first_name) VALUES(?,?,?)",
                 (user.id, user.username, user.first_name))
    conn.commit()
    count = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
    txt = (
        "✅ New user joined\n"
        f"Username: @{user.username}\n"
        f"Userid: {user.id}\n\n"
        f"Total users: {count}"
    )
    for row in conn.execute("SELECT user_id FROM admins"):
        await context.bot.send_message(row["user_id"], txt)

      # --- main.py (Part 6/6) ---

# ========== CONTACT ADMIN (inline) ==========
async def contact_admin_start(q, context):
    if is_banned(q.from_user.id) and not is_admin(q.from_user.id):
        await q.message.edit_text("🚫 You are banned from using this bot.")
        return
    context.user_data["mode"] = "CONTACTING"
    await q.message.edit_text(
        "✍️ Please type your message for the admin.\nSend /cancel to abort.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
    )

async def handle_contact_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("mode") == "CONTACTING":
        context.user_data["mode"] = None
        u = update.effective_user
        uname = f"@{u.username}" if u.username else (u.first_name or "")
        msg = f"📩 Message from user\nUsername: {uname}\nUserID: {u.id}\n\n{update.message.text}"
        await send_admin_alert(msg, context.bot)
        await update.message.reply_text("✅ Your message has been sent to the admin.", reply_markup=main_menu(u.id))

# ========== ADMIN PANEL CALLBACKS ==========
def _count_by_subject_chapter_lines():
    total = conn.execute("SELECT COUNT(*) c FROM quizzes").fetchone()["c"]
    cats = conn.execute(
        "SELECT subject,chapter,COUNT(*) n FROM quizzes GROUP BY subject,chapter ORDER BY subject,chapter"
    ).fetchall()
    lines = [f"📦 Total quizzes: *{total}*"]
    for r in cats:
        lines.append(f"• *{r['subject']}* › {r['chapter']}: {r['n']}")
    return "\n".join(lines)

async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    if not is_admin(uid):
        await send_admin_alert(
            f"⚠️ Unauthorized admin action\nUsername: @{q.from_user.username}\nUserID: {uid}\nAction: {q.data}",
            context.bot
        )
        await q.message.reply_text("Only admin can use this.")
        return

    action = q.data.split(":", 1)[1]
    if action == "panel":
        await q.message.edit_text("🛠 *Admin panel*", parse_mode=ParseMode.MARKDOWN, reply_markup=admin_menu())

    elif action == "back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid))

    elif action == "export":
        path = "quizzes_export.json"
        cur = conn.execute("SELECT * FROM quizzes ORDER BY id")
        items = []
        for r in cur.fetchall():
            d = dict(r)
            d["options"] = json.loads(r["options_json"])
            d.pop("options_json", None)
            items.append(d)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        await q.message.reply_document(InputFile(path, filename="quizzes_export.json"), caption="Backup exported.")

    elif action == "count":
        lines = _count_by_subject_chapter_lines()
        await q.message.edit_text(lines, parse_mode=ParseMode.MARKDOWN, reply_markup=admin_menu())

    elif action == "users":
        total = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        rows = conn.execute(
            "SELECT user_id,username,first_name,last_name FROM users ORDER BY last_seen DESC LIMIT 50"
        ).fetchall()
        lines = [f"👥 *Users* (showing up to 50 recent)\nTotal: *{total}*"]
        for r in rows:
            uname = f"@{r['username']}" if r["username"] else (r["first_name"] or "")
            lines.append(f"- {uname} (`{r['user_id']}`)")
        lines.append("\nUse `/users`, `/ban <id>`, `/unban <id>` for full control.")
        await q.message.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=admin_menu())

    elif action == "broadcast":
        context.user_data["mode"] = "BCAST_WAIT"
        await q.message.edit_text(
            "📣 Send the message to *broadcast* to all users.\n/cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]])
        )

    elif action == "admins":
        ids = sorted(admin_ids_from_settings())
        lines = ["👑 *Admins:*"]
        for aid in ids:
            tag = " (Owner)" if aid == OWNER_ID else ""
            lines.append(f"- `{aid}`{tag}")
        lines.append("\nUse:\n/addadmin <id>\n/rmadmin <id>\n(Owner cannot be removed)")
        await q.message.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=admins_menu())

    elif action == "dellast":
        context.user_data["mode"] = "CONFIRM_DELLAST"
        await q.message.edit_text(
            "⚠️ Delete the *last* added quiz?\nThis cannot be undone.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm", callback_data="a:dellast_yes"),
                 InlineKeyboardButton("❌ Cancel", callback_data="a:panel")]
            ])
        )

    elif action == "dellast_yes":
        # delete last quiz id
        row = conn.execute("SELECT id FROM quizzes ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            await q.message.edit_text("No quizzes to delete.", reply_markup=admin_menu())
            return
        conn.execute("DELETE FROM quizzes WHERE id=?", (row["id"],))
        conn.commit()
        await q.message.edit_text(f"🗑 Deleted last quiz (id {row['id']}).", reply_markup=admin_menu())

# ========== BUTTON HANDLER ==========
async def btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    uid = q.from_user.id

    if is_banned(uid) and not is_admin(uid):
        await q.message.edit_text("🚫 You are banned from using this bot.")
        return

    try:
        if data == "u:help":
            await q.message.edit_text(
                "Tap Start quiz. Choose Subject → Chapter → Timer then Start.",
                reply_markup=main_menu(uid)
            )

        elif data == "u:stats":
            await show_stats(q)

        elif data == "u:contact":
            await contact_admin_start(q, context)

        elif data == "u:start":
            await show_subjects(update)

        elif data.startswith("u:subp:"):
            page = int(data.split(":")[2])
            await show_subjects(update, page)

        elif data.startswith("u:sub:"):
            subject = data.split(":", 2)[2]
            context.user_data["subject"] = subject
            await show_chapters(update, subject, 0)

        elif data.startswith("u:chapp:"):
            # u:chapp:SUBJECT:PAGE
            _, _, rest = data.split(":", 2)
            subj, page_s = rest.rsplit(":", 1)
            await show_chapters(update, subj, int(page_s))

        elif data.startswith("u:chap:"):
            # u:chap:SUBJECT:CHAPTER
            _, _, subj, chap = data.split(":", 3)
            context.user_data["subject"] = subj
            context.user_data["chapter"] = chap
            await timer_select(update)

        elif data.startswith("u:t:"):
            # timer selected
            t = int(data.split(":")[2])
            context.user_data["timer"] = t
            await pre_quiz_screen(q, context)

        elif data == "u:ready":
            await begin_quiz_session(q, context)

        elif data == "u:back":
            await q.message.edit_text("Menu:", reply_markup=main_menu(uid))

        elif data == "u:stop_now":
            conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
            conn.commit()
            await q.message.edit_text("⏹️ Quiz stopped.", reply_markup=main_menu(uid))

        elif data.startswith("a:"):
            await admin_cb(update, context)

        else:
            await q.message.edit_text("Unknown action.", reply_markup=main_menu(uid))

    except Exception as e:
        log.error("btn error: %s\n%s", e, traceback.format_exc())
        try:
            await q.message.reply_text("⚠️ An error occurred.")
        except Exception:
            pass

# ========== TEXT HANDLER (modes) ==========
async def text_or_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    uid = update.effective_user.id
    if is_banned(uid) and not is_admin(uid):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return

    mode = context.user_data.get("mode")
    if mode == "BCAST_WAIT" and is_admin(uid):
        # collect and confirm
        context.user_data["mode"] = "BCAST_CONFIRM"
        context.user_data["bcast_payload"] = update.message.to_dict()
        await update.message.reply_text(
            "Admin Message:\n\n" + (update.message.text or "_media_"),
            parse_mode=ParseMode.MARKDOWN,
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm broadcast", callback_data="a:bcast_go"),
             InlineKeyboardButton("❌ Cancel", callback_data="a:panel")]
        ])
        await update.message.reply_text("Send this to *all users*?", parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return

    if mode == "CONTACTING":
        await handle_contact_message(update, context)
        return

    # default: ignore plain text (or add other modes you need)
    return

# broadcast go handler via btn
async def broadcast_go(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    if not is_admin(uid):
        await q.message.edit_text("⛔ Unauthorized.", reply_markup=main_menu(uid))
        return
    if context.user_data.get("mode") != "BCAST_CONFIRM":
        await q.message.edit_text("Nothing to broadcast.", reply_markup=admin_menu())
        return
    payload = context.user_data.get("bcast_payload")
    context.user_data["mode"] = None
    sent = 0
    failed = 0
    cur = conn.execute("SELECT user_id FROM users")
    for r in cur.fetchall():
        try:
            if "text" in payload and payload["text"]:
                await context.bot.send_message(r["user_id"], "Admin Message:\n\n" + payload["text"])
            elif "photo" in payload:
                await context.bot.send_photo(r["user_id"], payload["photo"][-1]["file_id"], caption="Admin Message")
            elif "document" in payload:
                await context.bot.send_document(r["user_id"], payload["document"]["file_id"], caption="Admin Message")
            else:
                continue
            sent += 1
        except Exception:
            failed += 1
            continue
    await q.message.edit_text(f"📣 Broadcast done. Sent: {sent}, Failed: {failed}", reply_markup=admin_menu())

# Hook broadcast_go into admin_cb path
# We'll intercept a:bcast_go in btn
# Already handled in btn → admin_cb; add handler in btn:
# done via btn() calling admin_cb; admin_cb handles most, but for bcast_go we process here via btn routing:

# ========== STOP COMMAND ==========
async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
    conn.commit()
    await update.message.reply_text("⏹️ Quiz stopped.", reply_markup=main_menu(uid))

# ========== FLASK KEEPALIVE (optional) ==========
app = Flask(__name__)

@app.get("/")
def home():
    return "OK"

def run_keepalive():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

# ========== MAIN ==========
if __name__ == "__main__":
    db_init()
    Thread(target=run_keepalive, daemon=True).start()

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("stop", stop_cmd))
    application.add_handler(CommandHandler("admins", show_admins))
    application.add_handler(CommandHandler("addadmin", add_admin_cmd))
    application.add_handler(CommandHandler("rmadmin", remove_admin_cmd))
    application.add_handler(CommandHandler("users", list_users_cmd))
    application.add_handler(CommandHandler("ban", ban_user_cmd))
    application.add_handler(CommandHandler("unban", unban_user_cmd))

    # Buttons & Poll answers
    application.add_handler(CallbackQueryHandler(lambda u,c: broadcast_go(u,c) if u.callback_query.data=="a:bcast_go" else btn(u,c)))
    application.add_handler(PollAnswerHandler(poll_answer))

    # Messages / modes
    application.add_handler(MessageHandler(filters.ALL, text_or_poll))

    application.run_polling()
