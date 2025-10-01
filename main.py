import os, json, time, random, logging, sqlite3, asyncio, re, traceback
from threading import Thread
from math import ceil
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    PollAnswerHandler, ContextTypes, filters
)
from dotenv import load_dotenv

# Load local .env file
load_dotenv("secrets.env")

# ---------- Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS_ENV = os.getenv("ADMIN_ID", "")
DB_PATH = os.getenv("DB_PATH", "db.sqlite3")
OWNER_ID = int(os.getenv("OWNER_ID", "5902126578"))  # hard owner

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
            last_seen INTEGER
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
    # bans table for owner moderation
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bans(
            user_id INTEGER PRIMARY KEY,
            reason TEXT,
            banned_at INTEGER
        );
    """)
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

# ---------- Helpers ----------
async def busy(chat, action=ChatAction.TYPING, secs=0.15):
    # tiny debounce; no visible typing
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
    ids = set()
    if saved:
        ids |= {int(x) for x in saved.split(",") if x.strip().isdigit()}
    if ADMIN_IDS_ENV:
        ids |= {int(x) for x in ADMIN_IDS_ENV.split(",") if x.strip().isdigit()}
    ids.add(OWNER_ID)  # owner always admin
    return ids

def is_owner(uid: int) -> bool:
    return int(uid) == int(OWNER_ID)

def is_admin(uid: int) -> bool:
    return uid in admin_ids_from_settings()

def add_admin(uid: int):
    if int(uid) == int(OWNER_ID):
        return
    ids = admin_ids_from_settings()
    ids.add(uid)
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

def remove_admin(uid: int):
    if int(uid) == int(OWNER_ID):
        return
    ids = admin_ids_from_settings()
    if uid in ids:
        ids.remove(uid)
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

async def send_owner_alert(text: str, bot=None):
    try:
        if bot:
            await bot.send_message(OWNER_ID, text)
        else:
            from telegram import Bot
            Bot(BOT_TOKEN).send_message(OWNER_ID, text)
    except Exception:
        pass

def upsert_user(update: Update) -> bool:
    u = update.effective_user
    c = update.effective_chat
    if not u or not c: return False
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
    return not existed

def is_banned(uid: int) -> bool:
    return conn.execute("SELECT 1 FROM bans WHERE user_id=?", (uid,)).fetchone() is not None

def ban_user_any(identifier: str, by_owner: int):
    # identifier: user_id string or @username
    uid = None
    if identifier.startswith("@"):
        r = conn.execute("SELECT user_id FROM users WHERE username=?", (identifier[1:],)).fetchone()
        if r: uid = int(r["user_id"])
    else:
        if identifier.isdigit():
            uid = int(identifier)
    if uid and uid != OWNER_ID:
        conn.execute("INSERT OR REPLACE INTO bans(user_id,reason,banned_at) VALUES(?,?,?)",
                     (uid, "owner-ban", int(time.time())))
        conn.commit()
        return uid
    return None

def unban_user_any(identifier: str):
    uid = None
    if identifier.startswith("@"):
        r = conn.execute("SELECT user_id FROM users WHERE username=?", (identifier[1:],)).fetchone()
        if r: uid = int(r["user_id"])
    else:
        if identifier.isdigit(): uid = int(identifier)
    if uid:
        conn.execute("DELETE FROM bans WHERE user_id=?", (uid,))
        conn.commit()
        return uid
    return None

def list_subjects_with_counts():
    cur = conn.execute(
        "SELECT COALESCE(subject,'Uncategorized') s, COUNT(DISTINCT chapter) chs, COUNT(*) qs "
        "FROM quizzes GROUP BY s ORDER BY qs DESC, s"
    )
    return [(r["s"], r["chs"], r["qs"]) for r in cur.fetchall()]

def list_chapters_with_counts(subject: str, limit=None, offset=0):
    sql = (
        "SELECT COALESCE(chapter,'General') c, COUNT(*) qs "
        "FROM quizzes WHERE subject=? GROUP BY c ORDER BY qs DESC, c"
    )
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        cur = conn.execute(sql, (subject, limit, offset))
    else:
        cur = conn.execute(sql, (subject,))
    return [(r["c"], r["qs"]) for r in cur.fetchall()]

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
        raise ValueError("Not enough valid options after cleaning (need ≥2).")
    if explanation:
        explanation = _truncate(explanation, 200)
    question = _truncate(question.strip(), 292)
    return question, options, explanation

# ---------- Menus ----------
def admin_menu(uid: int):
    rows = [[InlineKeyboardButton("➕ Add quiz", callback_data="a:add")]]
    # owner-only controls
    if is_owner(uid):
        rows.append([InlineKeyboardButton("📥 Import JSON", callback_data="a:import"),
                     InlineKeyboardButton("📤 Export JSON", callback_data="a:export")])
        rows.append([InlineKeyboardButton("#️⃣ Count", callback_data="a:count"),
                     InlineKeyboardButton("👥 Users", callback_data="a:users")])
        rows.append([InlineKeyboardButton("👑 Admins", callback_data="a:admins")])
        rows.append([InlineKeyboardButton("📣 Broadcast", callback_data="a:broadcast")])
    # admins (including owner) can delete *their* last
    rows.append([InlineKeyboardButton("⛔️ Delete my last quiz", callback_data="a:dellast")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:back")])
    return InlineKeyboardMarkup(rows)

def main_menu(uid: int):
    rows = [
        [InlineKeyboardButton("▶️ Start quiz", callback_data="u:start")],
        [InlineKeyboardButton("📊 My stats", callback_data="u:stats"),
         InlineKeyboardButton("📨 Contact admin", callback_data="u:contact")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="u:help")]
    ]
    if is_admin(uid):
        rows.insert(1, [InlineKeyboardButton("🏆 Leaderboard", callback_data="u:lb"),
                        InlineKeyboardButton("🛠 Admin panel", callback_data="a:panel")])
    return InlineKeyboardMarkup(rows)

# ---------- Commands ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    is_new = upsert_user(update)
    if is_new:
        # notify owner
        count = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        u = update.effective_user
        uname = f"@{u.username}" if u.username else u.first_name or ""
        txt = f"✅New user joined\nUsername: {uname}\nUserid: {u.id}\n\nTotal users: {count}"
        await send_owner_alert(txt, context.bot)
    await update.effective_chat.send_message(
        f"Hey {update.effective_user.first_name}, welcome to Madhyamik Helper Quiz Bot!",
        reply_markup=main_menu(uid)
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "This bot runs quizzes.\n\nUsers: Choose subject > chapter > timer > start quiz.\n"
        "Admins: Can add quizzes.\nOwner: Full control."
    )

# ---------- Admin & User Actions ----------
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
        f"📊 Your stats:\nTotal answered: {tot}\n✅ Correct: {ok}\n❌ Wrong: {wrong}",
        reply_markup=main_menu(uid)
    )

async def leaderboard(q):
    if not is_admin(q.from_user.id):
        await q.message.edit_text("Leaderboard is for admins only.", reply_markup=main_menu(q.from_user.id))
        return
    rows = conn.execute(
        "SELECT s.user_id, COALESCE(SUM(si.is_correct),0) ok "
        "FROM sessions s LEFT JOIN session_items si ON si.session_id=s.id "
        "GROUP BY s.user_id ORDER BY ok DESC"
    ).fetchall()
    if not rows:
        await q.message.edit_text("No data yet.", reply_markup=main_menu(q.from_user.id))
        return
    lines = ["🏆 Leaderboard:"]
    pos = 1
    for r in rows:
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (r["user_id"],)).fetchone()
        uname = f"@{u['username']}" if u and u["username"] else f"user {r['user_id']}"
        lines.append(f"{pos}. {uname} — {r['ok']} correct")
        pos += 1
    await q.message.edit_text("\n".join(lines), reply_markup=main_menu(q.from_user.id))

# ---------- Admin Panel ----------
async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    if not is_admin(uid):
        u = q.from_user
        uname = f"@{u.username}" if u.username else u.first_name
        await send_owner_alert(f"⚠️ Unauthorized admin attempt\nUser: {uname} ({uid})\nAction: {q.data}", context.bot)
        await q.message.reply_text("Only admins can use this.")
        return

    action = q.data.split(":", 1)[1]
    if action == "panel":
        await q.message.edit_text("Admin panel:", reply_markup=admin_menu(uid))
    elif action == "back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid))
    elif action == "export":
        # proper filename
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
        total = conn.execute("SELECT COUNT(*) FROM quizzes").fetchone()[0]
        subs = conn.execute("SELECT subject, chapter, COUNT(*) n FROM quizzes GROUP BY subject, chapter").fetchall()
        lines = [f"Total quizzes: {total}"]
        for r in subs:
            lines.append(f"- {r['subject']} › {r['chapter']}: {r['n']}")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu(uid))
    elif action == "admins":
        if not is_owner(uid):
            await q.message.edit_text("Only owner can manage admins.", reply_markup=admin_menu(uid))
            return
        rows = admin_ids_from_settings()
        lines = ["👑 Admins:"]
        for aid in sorted(rows):
            tag = " (Owner)" if aid == OWNER_ID else ""
            lines.append(f"- {aid}{tag}")
        lines.append("\nUse /addadmin <id> or /rmadmin <id>")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu(uid))
    elif action == "users":
        if not is_owner(uid):
            await q.message.edit_text("Only owner can see users.", reply_markup=admin_menu(uid))
            return
        rows = conn.execute("SELECT * FROM users").fetchall()
        count = len(rows)
        lines = [f"👥 Total users: {count}"]
        for r in rows:
            uname = f"@{r['username']}" if r["username"] else r["first_name"] or ""
            lines.append(f"- {uname} ({r['user_id']})")
        lines.append("\nOwner can /ban <id|@username> or /unban <id|@username>")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu(uid))

  # ---------- Subject / Chapter UI ----------
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
        rows.append([InlineKeyboardButton(f"{name} ({chs} ch • {qs} q)", callback_data=f"u:sub:{name}")])
    nav = []
    if pages > 1:
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:subp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:subp:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    return InlineKeyboardMarkup(rows)

async def show_subjects(update_or_query, page=0):
    await edit_or_reply(update_or_query, "📚 Choose a subject:", subjects_markup(page))

def chapters_markup(subject: str, page: int = 0):
    chs = list_chapters_with_counts(subject, limit=PAGE_SIZE, offset=page*PAGE_SIZE)
    if not chs:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Subjects", callback_data="u:cat")]])
    total = conn.execute("SELECT COUNT(DISTINCT chapter) c FROM quizzes WHERE subject=?", (subject,)).fetchone()["c"]
    pages = max(1, (total+PAGE_SIZE-1)//PAGE_SIZE)
    page = max(0, min(page, pages-1))
    rows = []
    for name, qs in chs:
        rows.append([InlineKeyboardButton(f"{name} ({qs} q)", callback_data=f"u:chap:{subject}:{name}")])
    nav = []
    if pages > 1:
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:chapp:{subject}:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:chapp:{subject}:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Subjects", callback_data="u:cat")])
    return InlineKeyboardMarkup(rows)

async def show_chapters(update_or_query, subject: str, page=0):
    await edit_or_reply(update_or_query, f"📖 {subject} — choose a chapter:", chapters_markup(subject, page))

def timer_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ 15s", callback_data="u:t:15"),
         InlineKeyboardButton("⏱ 30s", callback_data="u:t:30"),
         InlineKeyboardButton("⏱ 45s", callback_data="u:t:45")],
        [InlineKeyboardButton("🚫 Without Timer", callback_data="u:t:0")],
        [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]
    ])

async def timer_select(update_or_query):
    await edit_or_reply(update_or_query, "⏲ Select a timer:", timer_markup())

async def pre_quiz_screen(q, context: ContextTypes.DEFAULT_TYPE):
    sd = context.user_data
    subj = sd.get("subject"); chap = sd.get("chapter")
    t = sd.get("timer", 0)
    txt = (
        "🏁 Get ready!\n\n"
        f"Subject: {subj}\n"
        f"Chapter: {chap}\n"
        f"Timer: {'Without Timer' if int(t)==0 else str(t)+'s'}\n\n"
        "Press 'I am ready!' when ready. Send /stop to cancel."
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I am ready!", callback_data="u:ready")],
        [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]
    ])
    await q.message.edit_text(txt, reply_markup=markup)

# ---------- Quiz Engine ----------
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
        await q.message.edit_text("⚠️ No questions in this chapter.", reply_markup=main_menu(uid))
        return
    conn.execute(
        "INSERT INTO sessions(user_id,chat_id,total,open_period,started_at,state,current_index) VALUES(?,?,?,?,?,?,?)",
        (uid, chat_id, len(ids), t if t>0 else DEFAULT_OPEN_PERIOD, int(time.time()), "running", 0)
    )
    conn.commit()
    sid = conn.execute("SELECT last_insert_rowid() id").fetchone()["id"]
    for idx, qid in enumerate(ids):
        conn.execute("INSERT INTO session_items(session_id,quiz_id,idx) VALUES(?,?,?)", (sid, qid, idx))
    conn.commit()
    await q.message.edit_text("🎯 Quiz started!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏹ Stop quiz", callback_data="u:stop_now")]]))
    await send_next_quiz(context.bot, sid, uid, schedule_timer=(t>0))

async def send_next_quiz(bot, session_id: int, uid: int, schedule_timer: bool):
    srow = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    if not srow or srow["state"] != "running": return
    idx = int(srow["current_index"]); total = int(srow["total"])
    if idx >= total:
        await finalize_session(bot, srow)
        return
    item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND idx=?", (session_id, idx)).fetchone()
    if not item:
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
    conn.execute("INSERT OR REPLACE INTO active_polls(poll_id,session_id,user_id) VALUES(?,?,?)", (msg.poll.id, session_id, uid))
    conn.commit()
    if schedule_timer and srow["open_period"]:
        try:
            app.job_queue.run_once(lambda *_: asyncio.create_task(handle_timer_expiry(bot, msg.poll.id)),
                                   when=srow["open_period"]+1,
                                   name=f"timer_{msg.poll.id}")
        except Exception as e:
            await send_owner_alert(f"[Owner alert] scheduling error: {e}", bot)

async def handle_timer_expiry(bot, poll_id: str):
    row = conn.execute("SELECT session_id,user_id FROM active_polls WHERE poll_id=?", (poll_id,)).fetchone()
    if not row: return
    item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND poll_id=?", (row['session_id'], poll_id)).fetchone()
    if not item: return
    if item["chosen"] is None:
        conn.execute("UPDATE session_items SET chosen=-1, is_correct=0, closed_at=? WHERE id=?", (int(time.time()), item["id"]))
        conn.execute("DELETE FROM active_polls WHERE poll_id=?", (poll_id,))
        conn.execute("UPDATE sessions SET current_index=current_index+1 WHERE id=?", (row["session_id"],))
        conn.commit()
        await send_next_quiz(bot, row["session_id"], row["user_id"], schedule_timer=True)

async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ans = update.poll_answer
    pid = ans.poll_id
    chosen = ans.option_ids[0] if ans.option_ids else None
    arow = conn.execute("SELECT session_id,user_id FROM active_polls WHERE poll_id=?", (pid,)).fetchone()
    if not arow: return
    item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND poll_id=?", (arow["session_id"], pid)).fetchone()
    if not item: return
    quiz = conn.execute("SELECT correct FROM quizzes WHERE id=?", (item["quiz_id"],)).fetchone()
    ok = 1 if (chosen is not None and int(chosen) == int(quiz["correct"])) else 0
    conn.execute("UPDATE session_items SET chosen=?, is_correct=?, closed_at=? WHERE id=?",
                 (chosen if chosen is not None else -1, ok, int(time.time()), item["id"]))
    conn.execute("DELETE FROM active_polls WHERE poll_id=?", (pid,))
    conn.execute("UPDATE sessions SET current_index=current_index+1 WHERE id=?", (arow["session_id"],))
    conn.commit()
    await send_next_quiz(context.bot, arow["session_id"], arow["user_id"], schedule_timer=(context.user_data.get("timer",0)>0))

async def finalize_session(bot, srow):
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
    mm, ss = divmod(dur, 60)
    conn.execute("UPDATE sessions SET state='finished', finished_at=? WHERE id=?", (int(time.time()), srow["id"]))
    conn.commit()
    text = (f"🏁 The quiz has finished!\nYou answered {answered}/{t} questions:\n\n"
            f"✅ Correct – {ok}\n❌ Wrong – {wrong}\n⌛️ Missed – {missed}\n🕒 Time - {mm} min {ss} sec")
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Try again", callback_data="u:ready")],
                                   [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
    await bot.send_message(srow["chat_id"], text, reply_markup=markup)

# ---------- Contact Admin ----------
async def contact_admin_start(q, context):
    context.user_data["mode"] = "CONTACTING"
    await q.message.edit_text(
        "✍️ Type the message you want to send to the owner. Send /cancel to abort.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
    )

async def handle_contact_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("mode") == "CONTACTING":
        context.user_data["mode"] = None
        u = update.effective_user
        uname = f"@{u.username}" if u.username else (u.first_name or "")
        msg = f"📩 Message from user\nUsername: {uname}\nUserID: {u.id}\n\n{update.message.text}"
        await send_owner_alert(msg, context.bot)
        await update.message.reply_text("✅ Your message has been sent to the owner.", reply_markup=main_menu(u.id))

# ---------- Admin Add Flow (admins can only add; owner has extra panels) ----------
def admin_add_menu(uid: int):
    # existing subjects + "new subject"
    subs = [s for s, _, _ in list_subjects_with_counts()]
    rows = []
    for s in subs[:PAGE_SIZE]:
        rows.append([InlineKeyboardButton(f"📚 {s}", callback_data=f"a:add_subj:{s}")])
    rows.append([InlineKeyboardButton("➕ New subject", callback_data="a:add_new_subj")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:panel")])
    return InlineKeyboardMarkup(rows)

def admin_chapters_menu(subject: str):
    chs = list_chapters_with_counts(subject)[:PAGE_SIZE]
    rows = []
    for c, qs in chs:
        rows.append([InlineKeyboardButton(f"📖 {c} ({qs} q)", callback_data=f"a:add_chap:{subject}:{c}")])
    rows.append([InlineKeyboardButton("➕ New chapter", callback_data=f"a:add_new_chap:{subject}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:add")])
    return InlineKeyboardMarkup(rows)

async def save_quiz_from_poll(message, uid, subject, chapter):
    poll = message.poll
    if poll.type != "quiz":
        await message.reply_text("Please send *Quiz-type* polls (one correct answer).")
        return
    question = poll.question
    options = [o.text for o in poll.options]
    correct = int(poll.correct_option_id)
    explanation = poll.explanation if poll.explanation else None
    conn.execute(
        "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (question, json.dumps(options, ensure_ascii=False), correct, explanation, subject, chapter, int(time.time()), int(uid))
    )
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM quizzes WHERE subject=? AND chapter=?", (subject, chapter)).fetchone()[0]
    await message.reply_text(f"✅ Saved. Total in *{subject} › {chapter}*: {total}", parse_mode=ParseMode.MARKDOWN)

# ---------- Buttons ----------
async def btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    uid = q.from_user.id

    # Basic user actions
    if data == "u:help":
        await q.message.edit_text(
            "Tap Start quiz. Choose Subject → Chapter → Timer then Start.",
            reply_markup=main_menu(uid)
        )
        return
    if data == "u:stats":
        await show_stats(q); return
    if data == "u:contact":
        await contact_admin_start(q, context); return
    if data == "u:back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid)); return
    if data == "u:start":
        await show_subjects(update); return
    if data.startswith("u:subp:"):
        page = int(data.split(":")[2]); await show_subjects(update, page); return
    if data.startswith("u:sub:"):
        subject = data.split(":", 2)[2]
        context.user_data["subject"] = subject
        await show_chapters(update, subject, 0); return
    if data.startswith("u:chapp:"):
        _, _, rest = data.split(":", 2)
        subj, page_s = rest.rsplit(":", 1)
        await show_chapters(update, subj, int(page_s)); return
    if data.startswith("u:chap:"):
        _, _, subj, chap = data.split(":", 3)
        context.user_data["subject"] = subj
        context.user_data["chapter"] = chap
        await timer_select(update); return
    if data.startswith("u:t:"):
        t = int(data.split(":")[2])
        context.user_data["timer"] = t
        await pre_quiz_screen(q, context); return
    if data == "u:ready":
        await begin_quiz_session(q, context); return
    if data == "u:stop_now":
        conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
        conn.commit()
        await q.message.edit_text("⏹️ Quiz stopped.", reply_markup=main_menu(uid))
        return

    # Admin panel guard
    if data.startswith("a:") and not is_admin(uid):
        u = q.from_user
        uname = f"@{u.username}" if u.username else u.first_name or ""
        await send_owner_alert(f"⚠️ Unauthorized admin feature:\nUser: {uname} ({uid})\nAction: {data}", context.bot)
        await q.message.reply_text("Only admins can use this.")
        return

    # Admin panel routes
    if data == "a:panel":
        await q.message.edit_text("Admin panel:", reply_markup=admin_menu(uid)); return
    if data == "a:back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid)); return

    # Owner-only sections via panel buttons
    if data == "a:admins":
        if not is_owner(uid):
            await q.message.edit_text("Only owner can manage admins.", reply_markup=admin_menu(uid)); return
        # show current admins & hints (add/remove via commands)
        ids = sorted(admin_ids_from_settings())
        lines = ["👑 Admins:"]
        for aid in ids:
            tag = " (Owner)" if aid == OWNER_ID else ""
            lines.append(f"- {aid}{tag}")
        lines.append("\nOwner can:\n/addadmin <id>\n/rmadmin <id>")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu(uid)); return

    if data == "a:users":
        if not is_owner(uid):
            await q.message.edit_text("Only owner can see users.", reply_markup=admin_menu(uid)); return
        rows = conn.execute("SELECT * FROM users ORDER BY last_seen DESC").fetchall()
        lines = [f"👥 Total users: {len(rows)}"]
        for r in rows[:200]:  # cap to avoid huge messages
            uname = f"@{r['username']}" if r["username"] else r["first_name"] or ""
            lines.append(f"- {uname} ({r['user_id']})")
        lines.append("\nOwner can:\n/ban <id|@username>\n/unban <id|@username>")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu(uid)); return

    # Export/Count handled already in Part 2

    # Add flow (admins & owner)
    if data == "a:add":
        await q.message.edit_text("Choose a subject for adding quizzes:", reply_markup=admin_add_menu(uid))
        return
    if data.startswith("a:add_subj:"):
        subject = data.split(":", 2)[2]
        context.user_data["add_subject"] = subject
        await q.message.edit_text(f"Subject: *{subject}*\nNow choose a chapter:", parse_mode=ParseMode.MARKDOWN,
                                  reply_markup=admin_chapters_menu(subject))
        return
    if data == "a:add_new_subj":
        context.user_data["mode"] = "ADD_SUBJECT"
        await q.message.edit_text("Send the *subject name*.", parse_mode=ParseMode.MARKDOWN,
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]]))
        return
    if data.startswith("a:add_chap:"):
        _, _, subj, chap = data.split(":", 3)
        context.user_data["add_subject"] = subj
        context.user_data["add_chapter"] = chap
        context.user_data["mode"] = "ADDING"
        await q.message.edit_text(
            f"Subject: *{subj}*\nChapter: *{chap}*\n\nNow send *Quiz-type polls* to add.\nSend /done when finished.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]])
        )
        return
    if data.startswith("a:add_new_chap:"):
        subject = data.split(":", 2)[2]
        context.user_data["add_subject"] = subject
        context.user_data["mode"] = "ADD_CHAPTER"
        await q.message.edit_text(
            f"Subject: *{subject}*\nSend the *chapter name*.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]])
        )
        return

    # Delete last (only the quiz this admin added)
    if data == "a:dellast":
        # confirm
        context.user_data["mode"] = "CONFIRM_DELLAST"
        await q.message.edit_text(
            "⚠️ Delete *your* last added quiz?\nThis cannot be undone.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm", callback_data="a:dellast_yes"),
                 InlineKeyboardButton("❌ Cancel", callback_data="a:panel")]
            ])
        )
        return
    if data == "a:dellast_yes":
        row = conn.execute("SELECT id FROM quizzes WHERE added_by=? ORDER BY id DESC LIMIT 1", (uid,)).fetchone()
        if not row:
            await q.message.edit_text("No quizzes of yours to delete.", reply_markup=admin_menu(uid)); return
        conn.execute("DELETE FROM quizzes WHERE id=?", (row["id"],)); conn.commit()
        await q.message.edit_text(f"🗑 Deleted your last quiz (id {row['id']}).", reply_markup=admin_menu(uid)); return

# ---------- Text & Poll Handler (modes) ----------
async def text_or_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Handle modes for admin add/import/contact
    if update.message is None:
        return

    uid = update.effective_user.id
    text = update.message.text or ""

    # Unauthorized edits alert -> owner
    if context.user_data.get("mode") in {"ADD_SUBJECT", "ADD_CHAPTER", "ADDING", "IMPORT"} and not is_admin(uid):
        u = update.effective_user
        uname = f"@{u.username}" if u.username else u.first_name or ""
        await send_owner_alert(
            f"⚠️ Unauthorized edit attempt\nUser: {uname} ({uid})\nMode: {context.user_data.get('mode')}\nContent: {text[:200]}",
            context.bot
        )
        await update.message.reply_text("Only admins can edit content.")
        context.user_data["mode"] = None
        return

    mode = context.user_data.get("mode")

    # Owner-only import
    if mode == "IMPORT":
        if not is_owner(uid):
            await update.message.reply_text("Only owner can import.")
            context.user_data["mode"] = None
            return
        if update.message.document and update.message.document.file_name.lower().endswith(".json"):
            tgfile = await update.message.document.get_file()
            path = "import.json"
            await tgfile.download_to_drive(custom_path=path)
            try:
                data = json.load(open(path, "r", encoding="utf-8"))
                count = 0
                for it in data:
                    question = it["question"]
                    options = it["options"]
                    correct = int(it["correct"])
                    explanation = it.get("explanation")
                    subject = it.get("subject")
                    chapter = it.get("chapter")
                    conn.execute(
                        "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by) "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (question, json.dumps(options, ensure_ascii=False), correct, explanation, subject, chapter, int(time.time()), int(uid))
                    )
                    count += 1
                conn.commit()
                await update.message.reply_text(f"Imported {count} items.", reply_markup=admin_menu(uid))
            except Exception as e:
                await update.message.reply_text(f"Import error: {e}")
            finally:
                context.user_data["mode"] = None
            return
        else:
            await update.message.reply_text("Please send a .json file.")
            return

    # Add Subject name
    if mode == "ADD_SUBJECT" and text:
        context.user_data["add_subject"] = text.strip()
        context.user_data["mode"] = None
        await update.message.reply_text(f"Subject set to: *{context.user_data['add_subject']}*", parse_mode=ParseMode.MARKDOWN,
                                        reply_markup=admin_chapters_menu(context.user_data["add_subject"]))
        return

    # Add Chapter name
    if mode == "ADD_CHAPTER" and text:
        context.user_data["add_chapter"] = text.strip()
        context.user_data["mode"] = "ADDING"
        await update.message.reply_text(
            f"Subject: *{context.user_data['add_subject']}*\nChapter: *{context.user_data['add_chapter']}*\n\n"
            f"Now send Quiz-type polls. /done when finished.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Adding quizzes by sending polls
    if mode == "ADDING" and update.message.poll:
        await save_quiz_from_poll(update.message, uid, context.user_data.get("add_subject"), context.user_data.get("add_chapter"))
        return

    if text.strip().lower() == "/done":
        if mode == "ADDING":
            context.user_data["mode"] = None
            await update.message.reply_text("Finished.", reply_markup=admin_menu(uid))
        return

    # Contacting owner (handled in separate function)
    if mode == "CONTACTING":
        await handle_contact_message(update, context)
        return

    # Shortcuts
    if text.strip().lower() == "/menu":
        await update.message.reply_text("Menu:", reply_markup=main_menu(uid))
        return

# ---------- Owner & Admin Commands ----------
async def addadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can add admins.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /addadmin <user_id>")
        return
    try:
        new_id = int(context.args[0])
    except:
        await update.message.reply_text("Invalid ID.")
        return
    add_admin(new_id)
    await update.message.reply_text(f"✅ {new_id} added as admin.")

async def rmadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can remove admins.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /rmadmin <user_id>")
        return
    try:
        rid = int(context.args[0])
    except:
        await update.message.reply_text("Invalid ID.")
        return
    if rid == OWNER_ID:
        await update.message.reply_text("Owner cannot be removed.")
        return
    remove_admin(rid)
    await update.message.reply_text(f"🗑 {rid} removed from admins.")

async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can ban.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id|@username>")
        return
    who = context.args[0].strip()
    uid = ban_user_any(who, update.effective_user.id)
    if uid:
        await update.message.reply_text(f"⛔️ Banned {uid}.")
    else:
        await update.message.reply_text("User not found.")

async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can unban.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id|@username>")
        return
    who = context.args[0].strip()
    uid = unban_user_any(who)
    if uid:
        await update.message.reply_text(f"✅ Unbanned {uid}.")
    else:
        await update.message.reply_text("User not found.")

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
    conn.commit()
    await update.message.reply_text("⏹️ Quiz stopped.", reply_markup=main_menu(uid))

# ---------- Flask keepalive ----------
app_flask = Flask(__name__)

@app_flask.get("/")
def home():
    return "OK"

def run_keepalive():
    app_flask.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

# ---------- Main ----------
if __name__ == "__main__":
    db_init()
    Thread(target=run_keepalive, daemon=True).start()

    application = ApplicationBuilder().token(BOT_TOKEN).build()
    # make a global alias used by timer scheduling
    global app
    app = application

    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("stop", stop_cmd))
    application.add_handler(CommandHandler("addadmin", addadmin_cmd))
    application.add_handler(CommandHandler("rmadmin", rmadmin_cmd))
    application.add_handler(CommandHandler("ban", ban_cmd))
    application.add_handler(CommandHandler("unban", unban_cmd))

    # Buttons & Poll answers
    application.add_handler(CallbackQueryHandler(btn))
    application.add_handler(PollAnswerHandler(poll_answer))

    # Messages / modes
    application.add_handler(MessageHandler(filters.ALL, text_or_poll))

    application.run_polling()
