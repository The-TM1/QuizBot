import os, json, time, random, logging, sqlite3, asyncio, traceback
from threading import Thread
from math import ceil
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    PollAnswerHandler, ContextTypes, filters
)
from dotenv import load_dotenv

# ---- Load env ----
load_dotenv("secrets.env")

# ---- Config ----
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS_ENV = os.getenv("ADMIN_ID", "")
DB_PATH = os.getenv("DB_PATH", "db.sqlite3")
OWNER_ID = int(os.getenv("OWNER_ID", "5902126578"))  # hard owner id

PAGE_SIZE = 8
DEFAULT_OPEN_PERIOD = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("quizbot")

# ---- DB ----
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")

def table_cols(table: str) -> set:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

def add_col_if_missing(table: str, col: str, decl: str):
    if col not in table_cols(table):
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
        CREATE TABLE IF NOT EXISTS bans(
            user_id INTEGER PRIMARY KEY,
            reason TEXT,
            banned_at INTEGER
        );
    """)
    conn.commit()
    # safe migrations
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

# ---- Settings & Admins ----
def sget(key, default=None):
    r = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r[0] if r else default

def sset(key, value):
    conn.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value)
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
    return int(uid) == OWNER_ID

def is_admin(uid: int) -> bool:
    return int(uid) in admin_ids_from_settings()

def add_admin(uid: int):
    if int(uid) == OWNER_ID: return
    ids = admin_ids_from_settings(); ids.add(int(uid))
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

def remove_admin(uid: int):
    if int(uid) == OWNER_ID: return
    ids = admin_ids_from_settings(); ids.discard(int(uid))
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

async def send_owner_alert(text: str, bot=None):
    try:
        if bot: await bot.send_message(OWNER_ID, text)
        else:
            from telegram import Bot
            Bot(BOT_TOKEN).send_message(OWNER_ID, text)
    except Exception:
        pass

# ---- Users / Bans helpers ----
def upsert_user(update: Update) -> bool:
    u = update.effective_user; c = update.effective_chat
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

def ban_user_any(identifier: str):
    uid = None
    if identifier.startswith("@"):
        r = conn.execute("SELECT user_id FROM users WHERE username=?", (identifier[1:],)).fetchone()
        if r: uid = int(r["user_id"])
    elif identifier.isdigit():
        uid = int(identifier)
    if uid and uid != OWNER_ID:
        conn.execute(
            "INSERT OR REPLACE INTO bans(user_id,reason,banned_at) VALUES(?,?,?)",
            (uid, "owner-ban", int(time.time()))
        ); conn.commit()
        return uid
    return None

def unban_user_any(identifier: str):
    uid = None
    if identifier.startswith("@"):
        r = conn.execute("SELECT user_id FROM users WHERE username=?", (identifier[1:],)).fetchone()
        if r: uid = int(r["user_id"])
    elif identifier.isdigit():
        uid = int(identifier)
    if uid:
        conn.execute("DELETE FROM bans WHERE user_id=?", (uid,)); conn.commit()
        return uid
    return None

# ---- Lists / Counts ----
def list_subjects_with_counts():
    cur = conn.execute(
        "SELECT COALESCE(subject,'Uncategorized') s, COUNT(DISTINCT chapter) chs, COUNT(*) qs "
        "FROM quizzes GROUP BY s ORDER BY qs DESC, s"
    )
    return [(r["s"], r["chs"], r["qs"]) for r in cur.fetchall()]

def list_chapters_with_counts(subject: str, limit=None, offset=0):
    sql = ("SELECT COALESCE(chapter,'General') c, COUNT(*) qs "
           "FROM quizzes WHERE subject=? GROUP BY c ORDER BY qs DESC, c")
    cur = conn.execute(sql if limit is None else sql + " LIMIT ? OFFSET ?",
                       (subject,) if limit is None else (subject, limit, offset))
    return [(r["c"], r["qs"]) for r in cur.fetchall()]

def _truncate(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n-1] + "…"

def sanitize_for_poll(question: str, options: list, explanation: str):
    question = _truncate((question or "").strip(), 292)
    options = [_truncate(o or "", 100) for o in options if (o or "").strip()]
    # de-dup
    seen = set(); opts = []
    for o in options:
        if o in seen: continue
        seen.add(o); opts.append(o)
    options = opts[:10]
    if len(options) < 2: raise ValueError("Not enough valid options.")
    explanation = _truncate(explanation or "", 200) if explanation else None
    return question, options, explanation

# ---- Menus (UI preserved) ----
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

def admin_menu(uid: int):
    rows = [[InlineKeyboardButton("➕ Add quiz", callback_data="a:add")]]
    if is_owner(uid):
        rows.append([InlineKeyboardButton("📥 Import JSON", callback_data="a:import"),
                     InlineKeyboardButton("📤 Export JSON", callback_data="a:export")])
        rows.append([InlineKeyboardButton("#️⃣ Count", callback_data="a:count"),
                     InlineKeyboardButton("👥 Users", callback_data="a:users")])
        rows.append([InlineKeyboardButton("🚫 Banned", callback_data="a:banned")])
        rows.append([InlineKeyboardButton("👑 Admins", callback_data="a:admins")])
        rows.append([InlineKeyboardButton("📣 Broadcast", callback_data="a:broadcast")])
    rows.append([InlineKeyboardButton("⛔️ Delete my last quiz", callback_data="a:dellast")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:back")])
    return InlineKeyboardMarkup(rows)

# ---- Commands ----
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_new = upsert_user(update)
    uid = update.effective_user.id

    # Notify owner once per user
    seen_key = f"user_seen:{uid}"
    if sget(seen_key) != "1":
        sset(seen_key, "1")
        count = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        u = update.effective_user
        uname = f"@{u.username}" if u.username else (u.first_name or "")
        txt = f"✅New user joined\nUsername: {uname}\nUserid: {u.id}\n\nTotal users: {count}"
        try:
            await context.bot.send_message(OWNER_ID, txt)
        except Exception:
            pass

    first = update.effective_user.first_name or "there"
    await update.effective_chat.send_message(
        f"Hey {first}, welcome to our Madhyamik Helper Quiz Bot.",
        reply_markup=main_menu(uid)
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Users: Start → choose Subject → Chapter → Timer → I am ready.\n"
        "Admins: Can add quizzes.\nOwner: Full control."
    )

# ---- Stats / Leaderboard ----
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
        f"📊 Your overall stats\nTotal answered: {tot}\n✅ Correct: {ok}\n❌ Wrong: {wrong}",
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
    lines = ["🏆 Leaderboard (all time):"]
    pos = 1
    for r in rows:
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (r["user_id"],)).fetchone()
        uname = f"@{u['username']}" if u and u["username"] else f"user {r['user_id']}"
        lines.append(f"{pos}. {uname} — {r['ok']} correct")
        pos += 1
    await q.message.edit_text("\n".join(lines), reply_markup=main_menu(q.from_user.id))

# ---- Subject / Chapter (UI preserved; adds pagination for big lists) ----
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
    total = conn.execute("SELECT COUNT(DISTINCT chapter) c FROM quizzes WHERE subject=?", (subject,)).fetchone()["c"]
    pages = max(1, (total+PAGE_SIZE-1)//PAGE_SIZE)
    page = max(0, min(page, pages-1))
    chs = list_chapters_with_counts(subject, limit=PAGE_SIZE, offset=page*PAGE_SIZE)
    if not chs:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Subjects", callback_data="u:cat")]])
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

# ---- Quiz Engine ----
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
    await q.message.edit_text(
        "🎯 Quiz started!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏹ Stop quiz", callback_data="u:stop_now")]])
    )
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
            f"✅ Correct – {ok}    ❌ Wrong – {wrong}    ⌛️ Missed – {missed}\n"
            f"🕒 Time - {mm} min {ss} sec")
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Try again", callback_data="u:ready")],
                                   [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
    await bot.send_message(srow["chat_id"], text, reply_markup=markup)

# ---- Contact Admin ----
async def contact_admin_start(q, context):
    context.user_data["mode"] = "CONTACTING"
    await q.message.edit_text(
        "✍️ Type the message you want to send to the owner.\nSend /cancel to abort.",
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

# ---- Admin add menus (UI unchanged style) ----
def admin_add_menu(uid: int):
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
        await message.reply_text("Please send *Quiz-type* polls (one correct answer).", parse_mode=ParseMode.MARKDOWN)
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

# ---- Admin Panel Callback (owner/admin rules enforced) ----
async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

    if not is_admin(uid):
        u = q.from_user
        uname = f"@{u.username}" if u.username else u.first_name or ""
        await send_owner_alert(f"⚠️ Unauthorized admin attempt\nUser: {uname} ({uid})\nAction: {q.data}", context.bot)
        await q.message.reply_text("Only admins can use this.")
        return

    action = q.data.split(":", 1)[1]

    if action == "panel":
        await q.message.edit_text("Admin panel:", reply_markup=admin_menu(uid))

    elif action == "back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid))

    elif action == "export":
        # proper filename on download
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
        subs = conn.execute("SELECT subject, chapter, COUNT(*) n FROM quizzes GROUP BY subject, chapter ORDER BY subject, chapter").fetchall()
        lines = [f"Total quizzes: {total}"]
        for r in subs:
            lines.append(f"- {r['subject']} › {r['chapter']}: {r['n']}")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu(uid))

    elif action == "users":
        if not is_owner(uid):
            await q.message.edit_text("Only owner can see users.", reply_markup=admin_menu(uid)); return
        rows = conn.execute("SELECT user_id, username, first_name FROM users ORDER BY last_seen DESC").fetchall()
        count = len(rows)
        lines = [f"👥 Total users: {count}"]
        for r in rows[:200]:
            uname = f"@{r['username']}" if r["username"] else r["first_name"] or ""
            lines.append(f"- {uname} ({r['user_id']})")
        lines.append("\nOwner can use:\n/ban <id|@username>\n/unban <id|@username>")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu(uid))

    elif action == "banned":
        if not is_owner(uid):
            await q.message.edit_text("Only owner can view banned users.", reply_markup=admin_menu(uid)); return
        rows = conn.execute("SELECT b.user_id, u.username, u.first_name FROM bans b LEFT JOIN users u ON u.user_id=b.user_id ORDER BY b.banned_at DESC").fetchall()
        if not rows:
            await q.message.edit_text("🚫 No banned users.", reply_markup=admin_menu(uid)); return
        lines = ["🚫 *Banned users:*"]
        for r in rows[:200]:
            uname = f"@{r['username']}" if r["username"] else (r["first_name"] or "")
            lines.append(f"- {uname} ({r['user_id']})")
        lines.append("\nOwner can /unban <id|@username>")
        await q.message.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=admin_menu(uid))

    elif action == "admins":
        if not is_owner(uid):
            await q.message.edit_text("Only owner can manage admins.", reply_markup=admin_menu(uid)); return
        ids = sorted(admin_ids_from_settings())
        lines = ["👑 Admins:"]
        for aid in ids:
            tag = " (Owner)" if aid == OWNER_ID else ""
            lines.append(f"- {aid}{tag}")
        lines.append("\nOwner can:\n/addadmin <id>\n/rmadmin <id>")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu(uid))

    elif action == "add":
        await q.message.edit_text("Choose a subject for adding quizzes:", reply_markup=admin_add_menu(uid))

    elif action.startswith("add_subj:"):
        subject = action.split(":", 1)[1]
        context.user_data["add_subject"] = subject
        await q.message.edit_text(f"Subject: *{subject}*\nNow choose a chapter:", parse_mode=ParseMode.MARKDOWN,
                                  reply_markup=admin_chapters_menu(subject))

    elif action == "add_new_subj":
        context.user_data["mode"] = "ADD_SUBJECT"
        await q.message.edit_text("Send the *subject name*.", parse_mode=ParseMode.MARKDOWN,
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]]))

    elif action.startswith("add_chap:"):
        _, subj, chap = action.split(":", 2)
        context.user_data["add_subject"] = subj
        context.user_data["add_chapter"] = chap
        context.user_data["mode"] = "ADDING"
        await q.message.edit_text(
            f"Subject: *{subj}*\nChapter: *{chap}*\n\nNow send *Quiz-type polls* to add.\nSend /done when finished.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]])
        )

    elif action.startswith("add_new_chap:"):
        subject = action.split(":", 1)[1]
        context.user_data["add_subject"] = subject
        context.user_data["mode"] = "ADD_CHAPTER"
        await q.message.edit_text(
            f"Subject: *{subject}*\nSend the *chapter name*.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]])
        )

    elif action == "dellast":
        # confirm delete own last quiz
        context.user_data["mode"] = "CONFIRM_DELLAST"
        await q.message.edit_text(
            "⚠️ Delete *your* last added quiz?\nThis cannot be undone.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm", callback_data="a:dellast_yes"),
                 InlineKeyboardButton("❌ Cancel", callback_data="a:panel")]
            ])
        )

    elif action == "dellast_yes":
        row = conn.execute("SELECT id FROM quizzes WHERE added_by=? ORDER BY id DESC LIMIT 1", (uid,)).fetchone()
        if not row:
            await q.message.edit_text("No quizzes of yours to delete.", reply_markup=admin_menu(uid)); return
        conn.execute("DELETE FROM quizzes WHERE id=?", (row["id"],)); conn.commit()
        await q.message.edit_text(f"🗑 Deleted your last quiz (id {row['id']}).", reply_markup=admin_menu(uid))

    elif action == "import":
        if not is_owner(uid):
            await q.message.edit_text("Only owner can import.", reply_markup=admin_menu(uid)); return
        context.user_data["mode"] = "IMPORT"
        await q.message.edit_text("Send a *.json* file to import quizzes.", reply_markup=admin_menu(uid))

  # ---- Text/Poll intake for admin flows & import ----
async def text_or_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    mode = context.user_data.get("mode")

    # ===== Contact owner =====
    if mode == "CONTACTING" and update.message and update.message.text:
        await handle_contact_message(update, context)
        return

    # ===== Add new Subject name =====
    if mode == "ADD_SUBJECT" and update.message and update.message.text:
        if not is_admin(uid):
            await update.message.reply_text("Only admins can add.", reply_markup=main_menu(uid))
            return
        subject = update.message.text.strip()
        if not subject:
            await update.message.reply_text("Send a valid subject name.")
            return
        context.user_data["add_subject"] = subject
        context.user_data["mode"] = "ADD_CHAPTER"
        await update.message.reply_text(
            f"Subject set to *{subject}*.\nNow send the *chapter name*.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]])
        )
        return

    # ===== Add new Chapter name =====
    if mode == "ADD_CHAPTER" and update.message and update.message.text:
        if not is_admin(uid):
            await update.message.reply_text("Only admins can add.", reply_markup=main_menu(uid))
            return
        chapter = update.message.text.strip()
        if not chapter:
            await update.message.reply_text("Send a valid chapter name.")
            return
        context.user_data["add_chapter"] = chapter
        context.user_data["mode"] = "ADDING"
        subject = context.user_data.get("add_subject", "Uncategorized")
        await update.message.reply_text(
            f"Subject: *{subject}*\nChapter: *{chapter}*\n\nNow send *Quiz-type polls*.\nSend /done when finished.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]])
        )
        return

    # ===== Save quizzes by sending polls =====
    if mode == "ADDING" and update.message and update.message.poll:
        if not is_admin(uid):
            await update.message.reply_text("Only admins can add.", reply_markup=main_menu(uid))
            return
        subject = context.user_data.get("add_subject", "Uncategorized")
        chapter = context.user_data.get("add_chapter", "General")
        await save_quiz_from_poll(update.message, uid, subject, chapter)
        return

    # ===== Import from JSON file =====
    if mode == "IMPORT" and update.message and update.message.document:
        if not is_owner(uid):
            await update.message.reply_text("Only owner can import.", reply_markup=admin_menu(uid))
            return
        doc = update.message.document
        if not doc.file_name.lower().endswith(".json"):
            await update.message.reply_text("Please send a *.json* file.")
            return
        tgfile = await doc.get_file()
        tmp = "import.json"
        await tgfile.download_to_drive(custom_path=tmp)
        try:
            data = json.load(open(tmp, "r", encoding="utf-8"))
            count = 0
            for it in data:
                question = it["question"]
                options = it["options"]
                correct = int(it["correct"])
                explanation = it.get("explanation")
                subject = it.get("subject")
                chapter = it.get("chapter")
                added_by = it.get("added_by", uid)
                conn.execute(
                    "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (question, json.dumps(options, ensure_ascii=False), correct, explanation, subject, chapter, int(time.time()), int(added_by))
                )
                count += 1
            conn.commit()
            await update.message.reply_text(f"Imported {count} items.", reply_markup=admin_menu(uid))
        except Exception as e:
            await update.message.reply_text(f"Import error: {e}")
        finally:
            context.user_data["mode"] = None
        return

    # ===== Shortcuts =====
    if update.message and update.message.text:
        t = update.message.text.strip()
        if t == "/done":
            context.user_data["mode"] = None
            await update.message.reply_text("Finished.", reply_markup=admin_menu(uid) if is_admin(uid) else main_menu(uid))
            return
        if t == "/cancel":
            context.user_data["mode"] = None
            await update.message.reply_text("Cancelled.", reply_markup=main_menu(uid))
            return

# ---- Start / Help ----
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_banned(uid) and not is_admin(uid):
        await update.effective_chat.send_message("🚫 You are banned.")
        return
    is_new = upsert_user(update)
    if is_new:
        # notify owner
        u = update.effective_user
        uname = f"@{u.username}" if u.username else (u.first_name or "")
        total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        await send_owner_alert(f"✅ New user joined\nUsername: {uname}\nUserid: {uid}\n\nTotal users: {total}", context.bot)
    # welcome
    first = (update.effective_user.first_name or "there")
    await update.effective_chat.send_message(
        f"Hey {first}, welcome to our *Madhyamik Helper Quiz Bot*!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_menu(uid)
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Tap *Start Quiz* → choose Subject → Chapter → Timer → *I am ready!*",
                                    parse_mode=ParseMode.MARKDOWN)

# ---- User Buttons ----
async def show_subjects(q, for_quiz=False):
    subs = list_subjects_with_counts()
    if not subs:
        await q.message.edit_text("No subjects yet. Ask admin to add.", reply_markup=main_menu(q.from_user.id))
        return
    rows = []
    for s, ch, qc in subs[:PAGE_SIZE]:
        rows.append([InlineKeyboardButton(f"📚 {s} ({ch} ch)", callback_data=("u:picksub:" + s))])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    await q.message.edit_text("Choose a subject:", reply_markup=InlineKeyboardMarkup(rows))

async def show_chapters(q, subject):
    chs = list_chapters_with_counts(subject)
    if not chs:
        await q.message.edit_text("No chapters yet.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:start")]]))
        return
    rows = []
    for c, n in chs[:PAGE_SIZE]:
        rows.append([InlineKeyboardButton(f"📖 {c} ({n} q)", callback_data=f"u:pickchap:{subject}:{c}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:start")])
    await q.message.edit_text(f"Subject: *{subject}*\nChoose a chapter:", parse_mode=ParseMode.MARKDOWN,
                              reply_markup=InlineKeyboardMarkup(rows))

def timer_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ 15s", callback_data="u:settimer:15"),
         InlineKeyboardButton("⏱ 30s", callback_data="u:settimer:30"),
         InlineKeyboardButton("⏱ 45s", callback_data="u:settimer:45")],
        [InlineKeyboardButton("🕓 Without Timer", callback_data="u:settimer:0")],
        [InlineKeyboardButton("⬅️ Back", callback_data="u:start")]
    ])

async def pre_quiz_screen(q, context):
    s = context.user_data.get("subject")
    c = context.user_data.get("chapter")
    t = context.user_data.get("timer", 30)
    ttxt = "Without Timer" if not t else f"{t}s"
    txt = f"Get ready!\n\nSubject: {s}\nChapter: {c}\nTimer: {ttxt}\n\nPress the button when ready. Send /stop to cancel."
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("I am ready!", callback_data="u:ready")],
        [InlineKeyboardButton("⬅️ Back", callback_data="u:start")]
    ])
    await q.message.edit_text(txt, reply_markup=markup)

# ---- Sessions & Quiz sending ----
def pick_quiz_ids(subject, chapter):
    rows = conn.execute(
        "SELECT id FROM quizzes WHERE subject=? AND chapter=? ORDER BY RANDOM()", (subject, chapter)
    ).fetchall()
    return [r[0] for r in rows]

async def begin_quiz_session(q, context):
    uid = q.from_user.id
    subject = context.user_data.get("subject")
    chapter = context.user_data.get("chapter")
    timer = int(context.user_data.get("timer", DEFAULT_OPEN_PERIOD))
    ids = pick_quiz_ids(subject, chapter)
    if not ids:
        await q.message.edit_text("No questions available.", reply_markup=main_menu(uid)); return
    # create session
    conn.execute(
        "INSERT INTO sessions(user_id,total,open_period,started_at,state,subject,chapter) VALUES(?,?,?,?,?,?,?)",
        (uid, len(ids), timer if timer > 0 else DEFAULT_OPEN_PERIOD, int(time.time()), "running", subject, chapter)
    ); conn.commit()
    sid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    context.user_data["session_id"] = sid
    context.user_data["queue_ids"] = ids
    context.user_data["timer_mode"] = (timer > 0)
    await q.message.edit_text("Quiz started! Good luck.",
                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
    await send_next_quiz(q.message.chat_id, context)

async def send_next_quiz(chat_id, context):
    sid = context.user_data.get("session_id")
    ids = context.user_data.get("queue_ids", [])
    if not sid or not ids:
        # finish
        await finish_session(chat_id, sid, context); return
    qid = ids.pop(0)
    row = conn.execute("SELECT * FROM quizzes WHERE id=?", (qid,)).fetchone()
    options = json.loads(row["options_json"])
    timer = context.user_data.get("timer", 0)
    msg = await context.bot.send_poll(
        chat_id=chat_id,
        question=row["question"],
        options=options,
        type="quiz",
        correct_option_id=int(row["correct"]),
        explanation=row["explanation"],
        is_anonymous=False,
        open_period=(timer if timer else None)
    )
    # register
    conn.execute(
        "INSERT INTO session_items(session_id,quiz_id,poll_id,message_id) VALUES(?,?,?,?)",
        (sid, qid, msg.poll.id, msg.message_id)
    )
    conn.execute(
        "INSERT OR REPLACE INTO active_polls(poll_id,session_id,user_id) VALUES(?,?,?)",
        (msg.poll.id, sid, chat_id)
    )
    conn.commit()
    # schedule timeout follow-up if timer mode
    if timer:
        # add one second buffer after open_period
        when = time.time() + timer + 1
        context.job_queue.run_once(lambda *_: asyncio.create_task(on_poll_timeout(chat_id, msg.poll.id, context)),
                                   when=when)

async def on_poll_timeout(chat_id, poll_id, context):
    # if user didn't answer, proceed
    # close item if not yet marked
    row = conn.execute("SELECT session_id,id,chosen FROM session_items WHERE poll_id=?", (poll_id,)).fetchone()
    if not row:
        return
    if row["chosen"] is None:
        conn.execute("UPDATE session_items SET closed_at=? WHERE id=?", (int(time.time()), row["id"]))
        conn.commit()
    await send_next_quiz(chat_id, context)

async def finish_session(chat_id, sid, context):
    if not sid:
        await context.bot.send_message(chat_id, "All done!", reply_markup=main_menu(chat_id)); return
    r = conn.execute(
        "SELECT COUNT(id) tot, SUM(CASE WHEN is_correct=1 THEN 1 ELSE 0 END) ok, "
        "SUM(CASE WHEN chosen IS NULL THEN 1 ELSE 0 END) miss "
        "FROM session_items WHERE session_id=?", (sid,)
    ).fetchone()
    tot = r["tot"] or 0
    ok = r["ok"] or 0
    miss = r["miss"] or 0
    wrong = max(0, tot - ok - miss)
    # mark finished
    conn.execute("UPDATE sessions SET finished_at=?, state=? WHERE id=?", (int(time.time()), "finished", sid))
    conn.commit()
    # message
    mins, secs = divmod(max(1, int(time.time() - conn.execute("SELECT started_at FROM sessions WHERE id=?", (sid,)).fetchone()[0])), 60)
    txt = (f"🏁 The quiz has finished!\n"
           f"You answered *{tot}* questions:\n\n"
           f"✅ Correct – *{ok}*\n❌ Wrong – *{wrong}*\n⌛️ Missed – *{miss}*\n"
           f"Time - {mins} min {secs} second")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁 Try again", callback_data="u:retry")],
        [InlineKeyboardButton("⬅️ Back", callback_data="u:back")]
    ])
    await context.bot.send_message(chat_id, txt, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    context.user_data.pop("session_id", None)
    context.user_data.pop("queue_ids", None)

# ---- Poll answers ----
async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ans = update.poll_answer
    pid = ans.poll_id
    chosen = ans.option_ids[0] if ans.option_ids else None
    row = conn.execute("SELECT session_id,user_id FROM active_polls WHERE poll_id=?", (pid,)).fetchone()
    if not row:
        return
    item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND poll_id=?", (row["session_id"], pid)).fetchone()
    if not item:
        return
    quiz = conn.execute("SELECT correct FROM quizzes WHERE id=?", (item["quiz_id"],)).fetchone()
    ok = 1 if (chosen is not None and int(chosen) == int(quiz["correct"])) else 0
    conn.execute("UPDATE session_items SET chosen=?, is_correct=?, closed_at=? WHERE id=?", (chosen, ok, int(time.time()), item["id"]))
    conn.commit()
    # if no timer mode -> send next immediately
    if not context.user_data.get("timer", 0):
        await send_next_quiz(row["user_id"], context)

# ---- Button router ----
async def btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

    if q.data == "u:help":
        await q.message.edit_text("Tap Start quiz → Subject → Chapter → Timer → I am ready!", reply_markup=main_menu(uid))
    elif q.data == "u:back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid))
    elif q.data == "u:contact":
        await contact_admin_start(q, context)
    elif q.data == "u:start":
        await show_subjects(q)
    elif q.data.startswith("u:picksub:"):
        subject = q.data.split(":", 2)[2]
        context.user_data["subject"] = subject
        await show_chapters(q, subject)
    elif q.data.startswith("u:pickchap:"):
        _, _, subject, chapter = q.data.split(":", 3)
        context.user_data["subject"] = subject
        context.user_data["chapter"] = chapter
        await q.message.edit_text(f"Subject: *{subject}*\nChapter: *{chapter}*\nChoose timer:",
                                  parse_mode=ParseMode.MARKDOWN, reply_markup=timer_menu_kb())
    elif q.data.startswith("u:settimer:"):
        t = int(q.data.split(":")[2])
        context.user_data["timer"] = t
        await pre_quiz_screen(q, context)
    elif q.data == "u:ready":
        try:
            await begin_quiz_session(q, context)
        except Exception as e:
            await send_owner_alert(f"[Admin alert] begin_quiz_session error: {e}", context.bot)
            await q.message.reply_text("Couldn't start quiz due to an error. Please try again.")
    elif q.data == "u:retry":
        # reuse last chosen subject/chapter/timer
        try:
            await begin_quiz_session(q, context)
        except Exception as e:
            await send_owner_alert(f"[Admin alert] retry error: {e}", context.bot)
            await q.message.reply_text("Couldn't start quiz due to an error. Please try again.")
    elif q.data.startswith("a:"):
        await admin_cb(update, context)

# ---- Commands for owner moderation ----
async def addadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not context.args: 
        await update.message.reply_text("Usage: /addadmin <user_id>"); return
    try:
        aid = int(context.args[0])
    except: 
        await update.message.reply_text("Provide numeric user_id."); return
    ids = set(admin_ids_from_settings()); ids.add(aid)
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))
    await update.message.reply_text(f"Added admin: {aid}")

async def rmadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not context.args: 
        await update.message.reply_text("Usage: /rmadmin <user_id>"); return
    try:
        aid = int(context.args[0])
    except: 
        await update.message.reply_text("Provide numeric user_id."); return
    if aid == OWNER_ID:
        await update.message.reply_text("Owner cannot be removed."); return
    ids = set(admin_ids_from_settings()); 
    ids.discard(aid)
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))
    await update.message.reply_text(f"Removed admin: {aid}")

async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Usage: /ban <id|@username>"); return
    target = context.args[0]
    uid = None
    if target.startswith("@"):
        r = conn.execute("SELECT user_id FROM users WHERE username=?", (target[1:],)).fetchone()
        if r: uid = r["user_id"]
    else:
        try: uid = int(target)
        except: pass
    if not uid:
        await update.message.reply_text("User not found."); return
    conn.execute("INSERT OR IGNORE INTO bans(user_id,banned_at) VALUES(?,?)", (uid, int(time.time()))); conn.commit()
    await update.message.reply_text(f"User {uid} banned.")

async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Usage: /unban <id|@username>"); return
    target = context.args[0]
    uid = None
    if target.startswith("@"):
        r = conn.execute("SELECT user_id FROM users WHERE username=?", (target[1:],)).fetchone()
        if r: uid = r["user_id"]
    else:
        try: uid = int(target)
        except: pass
    if not uid:
        await update.message.reply_text("User not found."); return
    conn.execute("DELETE FROM bans WHERE user_id=?", (uid,)); conn.commit()
    await update.message.reply_text(f"User {uid} unbanned.")

# ---- Keepalive (Replit-like) ----
app = Flask(__name__)

@app.get("/")
def home():
    return "OK"

def run_keepalive():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

# ---- Main ----
if __name__ == "__main__":
    db_init()
    Thread(target=run_keepalive, daemon=True).start()

    app_ = ApplicationBuilder().token(BOT_TOKEN).build()

    app_.add_handler(CommandHandler("start", start))
    app_.add_handler(CommandHandler("help", help_cmd))
    app_.add_handler(CommandHandler("addadmin", addadmin_cmd))
    app_.add_handler(CommandHandler("rmadmin", rmadmin_cmd))
    app_.add_handler(CommandHandler("ban", ban_cmd))
    app_.add_handler(CommandHandler("unban", unban_cmd))

    app_.add_handler(CallbackQueryHandler(btn))
    app_.add_handler(PollAnswerHandler(poll_answer))
    app_.add_handler(MessageHandler(filters.ALL, text_or_poll))

    # delete webhook if any (polling mode)
    asyncio.get_event_loop().run_until_complete(app_.bot.delete_webhook())

    log.info("Application started")
    app_.run_polling()
