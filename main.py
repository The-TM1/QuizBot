# Madhyamik Helper Quiz Bot — POLLING version
# PTB v20.7, SQLite, Flask keepalive (optional), dotenv for secrets.
# New (this build):
# - Robust admin management (panel + commands), prevent self-remove
# - Strict admin-only edits; non-admin attempts alert all admins (with details)
# - New user join notification to admins (+ total users)
# - Subjects/Chapters pagination + SAFE short callback ids (fix long-name issues)
# - Export JSON filename fixed (quizzes_export.json)
# - Admin panel: Total users

import os, json, time, random, logging, sqlite3, asyncio, re, traceback, hashlib
from threading import Thread
from math import ceil

from dotenv import load_dotenv
load_dotenv("secrets.env")  # loads if present (public repo safe if you don't commit this file)

from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    PollAnswerHandler, ContextTypes, filters
)

# ---------- Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS_ENV = os.getenv("ADMIN_ID", "")  # comma-separated allowed
DB_PATH = os.getenv("DB_PATH", "db.sqlite3")

PAGE_SIZE = 10       # items per page for subjects/chapters
LB_PAGE = 20         # leaderboard page size
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

# ---------- Settings / Admins ----------
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

def is_admin(uid: int) -> bool:
    return uid in admin_ids_from_settings()

def add_admin(uid: int):
    ids = admin_ids_from_settings()
    ids.add(uid)
    sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

def remove_admin(uid: int):
    ids = admin_ids_from_settings()
    if uid in ids:
        ids.remove(uid)
        sset("admin_ids", ",".join(str(x) for x in sorted(ids)))

# ---------- Helpers ----------
async def busy(chat, action=ChatAction.TYPING, secs=0.15):
    await asyncio.sleep(secs)  # keep quiet

def format_uname_row(row):
    if not row: return None
    if row["username"]:
        return f"@{row['username']}"
    name = " ".join(filter(None, [row["first_name"], row["last_name"]]))
    return name or None

def send_admin_alert_sync(text: str):
    for aid in admin_ids_from_settings():
        try:
            from telegram import Bot
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
    """Insert/update user. Returns True if NEW user."""
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
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]

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

# ---------- Menus ----------
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
         InlineKeyboardButton("👥 Total users", callback_data="a:users")],
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

# ---------- Commands ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    uid = update.effective_user.id
    # first user becomes admin if none set
    if not admin_ids_from_settings():
        add_admin(uid)
        log.info("Auto-assigned admin to %s", uid)
    first = update.effective_user.first_name or "there"
    await update.effective_chat.send_message(
        f"Hey {first}, welcome to our *Madhyamik Helper Quiz Bot*! 🎓",
        parse_mode="Markdown",
        reply_markup=main_menu(uid)
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    await update.message.reply_text(
        "Start → Subject → Chapter → Timer (or Without Timer) → I am ready!\n"
        "Use /stop any time to cancel. Admins can manage quizzes, broadcast, and admins."
    )

# ---------- User UI (with pagination + short ids) ----------
async def user_subjects(update_or_query, page=0):
    await busy(update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat)
    subs = list_subjects_with_counts()
    if not subs:
        await edit_or_reply(update_or_query, "Home › Subjects\n\nNo subjects yet. Ask admin to add quizzes.",
                            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
        return
    pages = max(1, ceil(len(subs) / PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    slice_ = subs[page*PAGE_SIZE:(page+1)*PAGE_SIZE]

    # build short-id map
    sub_map = {}
    rows = []
    for s, chs, qs in slice_:
        sid = short_id(f"{s}")
        sub_map[sid] = s
        rows.append([InlineKeyboardButton(f"📚 {s} (chapters: {chs} | quizzes: {qs})", callback_data=f"u:subjid:{sid}")])
    # save map in user_data (per-page)
    if isinstance(update_or_query, Update):
        update_or_query.effective_user  # just to be safe
        context = None
    # store in a safe place:
    if hasattr(update_or_query, "callback_query") and update_or_query.callback_query:
        updater = update_or_query
        upd_ctx = updater  # just handle below in handler using context
    # We cannot access context here; we will set in handlers where we call this

    # Add pagination
    nav = []
    if pages > 1:
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:subjp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:subjp:{page+1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])

    # Attach the map as JSON in a hidden line via message text (safe & simple)
    hidden = json.dumps({"sub_map": sub_map, "page": page}, ensure_ascii=False)
    text = "Home › Subjects\n\nChoose a subject:\n\n" + f"<code>{hidden}</code>"
    await edit_or_reply(update_or_query, text, InlineKeyboardMarkup(rows), parse_mode="HTML")

async def parse_hidden_map(text: str):
    # expects hidden JSON in last <code>{...}</code> line
    try:
        m = re.findall(r"<code>(\{.*\})</code>", text, re.S)
        if not m: return {}
        return json.loads(m[-1])
    except Exception:
        return {}

async def user_chapters(update_or_query, subject: str, page=0):
    await busy(update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat)
    chs = list_chapters_with_counts(subject)
    if not chs:
        rows = [
            [InlineKeyboardButton("⏱ Choose timer", callback_data="u:timer")],
            [InlineKeyboardButton("⬅️ Back", callback_data="u:startback")]
        ]
        await edit_or_reply(update_or_query, f"Home › Subjects › {subject}\n\nNo chapters found.", InlineKeyboardMarkup(rows))
        return

    pages = max(1, ceil(len(chs) / PAGE_SIZE))
    page = max(0, min(page, pages-1))
    slice_ = chs[page*PAGE_SIZE:(page+1)*PAGE_SIZE]

    chap_map = {}
    rows = []
    for c, qs in slice_:
        cid = short_id(f"{subject}:{c}")
        chap_map[cid] = c
        rows.append([InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"u:chapid:{cid}")])

    if pages > 1:
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:chapp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page < pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:chapp:{page+1}"))
        rows.append(nav)

    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:startback")])

    hidden = json.dumps({"chap_map": chap_map, "page": page, "subject": subject}, ensure_ascii=False)
    text = f"Home › Subjects › {subject}\n\nChoose a chapter:\n\n" + f"<code>{hidden}</code>"
    await edit_or_reply(update_or_query, text, InlineKeyboardMarkup(rows), parse_mode="HTML")

async def timer_menu(update_or_query):
    await busy(update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat)
    times = [15, 30, 45, 60]
    rows = [[InlineKeyboardButton(f"{t}s", callback_data=f"u:timer:{t}") for t in times]]
    rows.append([InlineKeyboardButton("Without Timer", callback_data="u:timer:0")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:chapback")])
    await edit_or_reply(update_or_query, "Home › Subjects › Chapter › Timer\n\nChoose time per question:",
                        InlineKeyboardMarkup(rows))

async def pre_quiz_screen(q, context: ContextTypes.DEFAULT_TYPE):
    subj = context.user_data.get("subject")
    chap = context.user_data.get("chapter")
    op = int(context.user_data.get("open_period", DEFAULT_OPEN_PERIOD))
    timer_text = "Without Timer" if op == 0 else f"{op}s"
    txt = (f"Home › Subjects › {subj} › {chap} › Timer\n\n"
           f"Get ready!\n\nSubject: {subj}\nChapter: {chap}\nTimer: {timer_text}\n\n"
           "Press the button when ready. Send /stop to cancel.")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("I am ready!", callback_data="u:ready")],
                               [InlineKeyboardButton("⬅️ Back", callback_data="u:timerback")]])
    await q.message.edit_text(txt, reply_markup=kb)

# ---------- Quiz engine ----------
async def timeout_fallback(bot, session_id: int, poll_id: str, wait_secs: int):
    try:
        await asyncio.sleep(max(1, wait_secs))
        row = conn.execute("SELECT * FROM active_polls WHERE poll_id=?", (poll_id,)).fetchone()
        if not row:
            return  # answered already
        item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND poll_id=?", (row["session_id"], poll_id)).fetchone()
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

        show_idx = idx + 1
        total = len(items)
        display_q = f"[{show_idx}/{total}] {q_text}"

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

        try:
            await bot.send_message(
                srow["chat_id"],
                "Controls:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏹️ Stop quiz", callback_data="u:stop_now"),
                     InlineKeyboardButton("🏠 Main menu", callback_data="u:back")]
                ])
            )
        except Exception:
            pass

        if srow["open_period"] > 0:
            asyncio.create_task(timeout_fallback(bot, session_id, msg.poll.id, srow["open_period"] + 2))

    except Exception as e:
        err = f"send_next_quiz error: {e}"
        log.error(err + "\n" + traceback.format_exc())
        await send_admin_alert(f"[Admin alert] {err}", bot)

async def begin_quiz_session(q, context: ContextTypes.DEFAULT_TYPE):
    try:
        subj = context.user_data.get("subject")
        chap = context.user_data.get("chapter")
        if not subj or not chap:
            await q.message.edit_text("Please choose Subject and Chapter first.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:start")]]))
            return
        uid = q.from_user.id
        chat_id = q.message.chat.id
        op = int(context.user_data.get("open_period", DEFAULT_OPEN_PERIOD))

        context.user_data["last_subject"] = subj
        context.user_data["last_chapter"] = chap
        context.user_data["last_open_period"] = op

        rows = conn.execute("SELECT id FROM quizzes WHERE subject=? AND chapter=?", (subj, chap)).fetchall()
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
            conn.execute(
                "INSERT INTO session_items(session_id,quiz_id,poll_id,message_id,idx) VALUES(?,?,?,?,?)",
                (sid, qid, "", 0, i)
            )
        conn.commit()

        await q.message.edit_text("Quiz started! Good luck! 🎯",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))
        await send_next_quiz(context.bot, sid)

    except Exception as e:
        log.error("begin_quiz_session error: %s\n%s", e, traceback.format_exc())
        await send_admin_alert(f"[Admin alert] begin_quiz_session error: {e}", context.bot)
        try:
            await q.message.reply_text("Couldn't start quiz due to an error. Please try again.")
        except Exception:
            pass

# --- Answers ---
async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ans = update.poll_answer
    pid = ans.poll_id
    chosen = ans.option_ids[0] if ans.option_ids else None
    row = conn.execute("SELECT * FROM active_polls WHERE poll_id=?", (pid,)).fetchone()
    if not row: return
    item = conn.execute("SELECT * FROM session_items WHERE session_id=? AND poll_id=?", (row["session_id"], pid)).fetchone()
    if not item: return
    quiz = conn.execute("SELECT correct FROM quizzes WHERE id=?", (item["quiz_id"],)).fetchone()
    ok = 1 if chosen is not None and int(chosen) == int(quiz["correct"]) else 0
    conn.execute("UPDATE session_items SET chosen=?, is_correct=?, closed_at=? WHERE id=?",
                 (chosen, ok, int(time.time()), item["id"]))
    conn.execute("DELETE FROM active_polls WHERE poll_id=?", (pid,))
    srow = conn.execute("SELECT * FROM sessions WHERE id=?", (row["session_id"],)).fetchone()
    if not srow:
        conn.commit()
        return
    advance = False
    if srow["open_period"] > 0:
        advance = True
    elif chosen is not None:
        advance = True
    if advance:
        conn.execute("UPDATE sessions SET current_index=current_index+1 WHERE id=?", (row["session_id"],))
        conn.commit()
        await send_next_quiz(context.bot, row["session_id"])
    else:
        conn.commit()

  # --- Stats, Leaderboard, Contact ---
async def show_stats(q):
    uid = q.from_user.id
    r = conn.execute(
        "SELECT COUNT(si.id) tot, SUM(si.is_correct) ok "
        "FROM sessions s LEFT JOIN session_items si ON si.session_id=s.id "
        "WHERE s.user_id=?", (uid,)
    ).fetchone()
    tot = r["tot"] or 0
    ok = r["ok"] or 0
    txt = f"Your overall stats:\nCorrect: {ok}\nWrong: {max(0, tot-ok)}"
    await q.message.edit_text(txt, reply_markup=main_menu(uid))

async def leaderboard(q, page=0):
    rows = conn.execute(
        "SELECT s.user_id, COALESCE(SUM(si.is_correct),0) ok "
        "FROM sessions s LEFT JOIN session_items si ON si.session_id=s.id "
        "GROUP BY s.user_id ORDER BY ok DESC"
    ).fetchall()
    if not rows:
        await q.message.edit_text("No data yet.", reply_markup=main_menu(q.from_user.id))
        return
    pages = max(1, ceil(len(rows)/LB_PAGE))
    page = max(0, min(page, pages-1))
    slice_ = rows[page*LB_PAGE:(page+1)*LB_PAGE]
    lines = [f"🏆 Leaderboard page {page+1}/{pages}:"]
    for i,r in enumerate(slice_, start=page*LB_PAGE+1):
        uname = conn.execute("SELECT username,first_name,last_name FROM users WHERE user_id=?", (r["user_id"],)).fetchone()
        uname_fmt = f"@{uname['username']}" if uname and uname["username"] else (uname["first_name"] if uname else str(r["user_id"]))
        lines.append(f"{i}. {uname_fmt} ({r['user_id']}): {r['ok']} correct")
    nav = []
    if pages>1:
        if page>0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"u:lbp:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
        if page<pages-1: nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"u:lbp:{page+1}"))
    kb = [nav] if nav else []
    kb.append([InlineKeyboardButton("⬅️ Back", callback_data="u:back")])
    await q.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb))

async def contact_admin_start(q, context):
    context.user_data["mode"] = "CONTACTING"
    await q.message.edit_text("Please type your message for the admin. Send /cancel to abort.",
                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]]))

async def handle_contact_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("mode")=="CONTACTING":
        context.user_data["mode"]=None
        u=update.effective_user
        uname=f"@{u.username}" if u.username else u.first_name
        msg=f"📩 Message from user\nUsername: {uname}\nUserID: {u.id}\n\n{update.message.text}"
        await send_admin_alert(msg, context.bot)
        await update.message.reply_text("Your message has been sent to the admin.", reply_markup=main_menu(u.id))

# --- Admin callbacks ---
async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    uid=q.from_user.id
    if not is_admin(uid):
        await send_admin_alert(f"⚠️ Unauthorized admin action\nUsername: @{q.from_user.username}\nUserID: {uid}\nAction: {q.data}")
        await q.message.reply_text("Only admin can use this."); return
    action=q.data.split(":",1)[1]
    if action=="panel": await q.message.edit_text("Admin panel:", reply_markup=admin_menu())
    elif action=="back": await q.message.edit_text("Menu:", reply_markup=main_menu(uid))
    elif action=="export":
        path="quizzes_export.json"
        cur=conn.execute("SELECT * FROM quizzes ORDER BY id")
        items=[]
        for r in cur.fetchall():
            d=dict(r); d["options"]=json.loads(r["options_json"]); d.pop("options_json",None); items.append(d)
        with open(path,"w",encoding="utf-8") as f: json.dump(items,f,ensure_ascii=False,indent=2)
        await q.message.reply_document(InputFile(path,filename="quizzes_export.json"),caption="Backup exported.")
    elif action=="count":
        total=conn.execute("SELECT COUNT(*) FROM quizzes").fetchone()[0]
        cats=conn.execute("SELECT subject,chapter,COUNT(*) n FROM quizzes GROUP BY subject,chapter").fetchall()
        lines=[f"Total: {total}"]
        for r in cats: lines.append(f"• {r['subject']} › {r['chapter']}: {r['n']}")
        await q.message.edit_text("\n".join(lines), reply_markup=admin_menu())
    elif action=="users":
        total=conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        await q.message.edit_text(f"Total registered users: {total}", reply_markup=admin_menu())
    elif action=="admins":
        await q.message.edit_text("Admin management:", reply_markup=admins_menu())
    elif action=="admins_list":
        ids=sorted(admin_ids_from_settings())
        lines=["👑 Admins:"]
        for aid in ids: lines.append(f"- {aid}")
        await q.message.edit_text("\n".join(lines), reply_markup=admins_menu())
    elif action=="addadmin": context.user_data["mode"]="ADDADMIN"; await q.message.edit_text("Send the user ID to add as admin. /cancel to abort.", reply_markup=admins_menu())
    elif action=="rmadmin": context.user_data["mode"]="RMADMIN"; await q.message.edit_text("Send the user ID to remove as admin. /cancel to abort.", reply_markup=admins_menu())

# --- Text/Poll handler ---
async def text_or_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    mode=context.user_data.get("mode")
    if mode=="ADDADMIN" and update.message and update.message.text:
        try:
            new_id=int(update.message.text.strip())
            if new_id==uid: await update.message.reply_text("You are already admin.")
            else: add_admin(new_id); await update.message.reply_text(f"Added {new_id} as admin.")
        except: await update.message.reply_text("Invalid ID.")
        context.user_data["mode"]=None; return
    if mode=="RMADMIN" and update.message and update.message.text:
        try:
            rid=int(update.message.text.strip())
            if rid==uid: await update.message.reply_text("You cannot remove yourself!")
            else: remove_admin(rid); await update.message.reply_text(f"Removed {rid} from admins.")
        except: await update.message.reply_text("Invalid ID.")
        context.user_data["mode"]=None; return
    if mode=="CONTACTING": await handle_contact_message(update, context); return

# --- Button handler ---
async def btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer(); data=q.data; uid=q.from_user.id
    if data=="u:help": await q.message.edit_text("Tap Start quiz. Choose Subject, Chapter, Timer then Start.", reply_markup=main_menu(uid))
    elif data=="u:stats": await show_stats(q)
    elif data=="u:lb": await leaderboard(q)
    elif data.startswith("u:lbp:"): page=int(data.split(":")[2]); await leaderboard(q,page)
    elif data=="u:contact": await contact_admin_start(q,context)
    elif data=="u:start": await user_subjects(update)
    elif data.startswith("u:subjp:"): page=int(data.split(":")[2]); await user_subjects(update,page)
    elif data.startswith("u:subjid:"):
        sid=data.split(":")[2]
        m=await parse_hidden_map(q.message.text)
        subj=m.get("sub_map",{}).get(sid)
        if subj: context.user_data["subject"]=subj; await user_chapters(update,subj)
    elif data.startswith("u:chapp:"):
        m=await parse_hidden_map(q.message.text); subj=m.get("subject")
        page=int(data.split(":")[2]); await user_chapters(update,subj,page)
    elif data.startswith("u:chapid:"):
        cid=data.split(":")[2]
        m=await parse_hidden_map(q.message.text); chap=m.get("chap_map",{}).get(cid); subj=m.get("subject")
        if chap and subj: context.user_data["chapter"]=chap; await timer_menu(update)
    elif data.startswith("u:timer:"): t=int(data.split(":")[2]); context.user_data["open_period"]=t; await pre_quiz_screen(q,context)
    elif data=="u:ready": await begin_quiz_session(q,context)
    elif data=="u:retry":
        context.user_data["subject"]=context.user_data.get("last_subject")
        context.user_data["chapter"]=context.user_data.get("last_chapter")
        context.user_data["open_period"]=context.user_data.get("last_open_period")
        await begin_quiz_session(q,context)
    elif data=="u:stop_now":
        conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'",(uid,)); conn.commit()
        await q.message.edit_text("Quiz stopped.", reply_markup=main_menu(uid))
    elif data=="u:back": await q.message.edit_text("Menu:", reply_markup=main_menu(uid))
    elif data.startswith("a:"): await admin_cb(update,context)

# --- Flask keepalive (not used for webhook, just optional) ---
app = Flask(__name__)
@app.get("/")
def home():
    return "OK"
def run_keepalive(): app.run(host="0.0.0.0", port=int(os.getenv("PORT",8080)))

# --- Main ---
if __name__=="__main__":
    db_init()
    Thread(target=run_keepalive, daemon=True).start()
    app_=ApplicationBuilder().token(BOT_TOKEN).build()
    app_.add_handler(CommandHandler("start", start))
    app_.add_handler(CommandHandler("help", help_cmd))
    app_.add_handler(CommandHandler("stop", lambda u,c: conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'",(u.effective_user.id,)) or conn.commit() or u.message.reply_text("Quiz stopped.")))
    app_.add_handler(CallbackQueryHandler(btn))
    app_.add_handler(PollAnswerHandler(poll_answer))
    app_.add_handler(MessageHandler(filters.ALL, text_or_poll))
    app_.run_polling()
