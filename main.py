# Madhyamik Helper Quiz Bot — POLLING version
# PTB v20.7, SQLite, Flask keepalive (optional), dotenv for secrets.
# New:
# - Admin management (list/add/remove) visible in Admin panel
# - Strict admin-only editing; any non-admin attempt alerts all admins
# - Notify admins when a NEW user joins (with total user count)

import os, json, time, random, logging, sqlite3, asyncio, re, traceback
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
async def busy(chat, action=ChatAction.TYPING, secs=0.2):
    await asyncio.sleep(secs)  # keep quiet; no typing action to reduce noise

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
         InlineKeyboardButton("⛔️ Delete last", callback_data="a:dellast")],
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
    is_new = upsert_user(update)
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

# ---------- User UI ----------
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

async def user_chapters(update_or_query, subject: str):
    await busy(update_or_query.effective_chat if isinstance(update_or_query, Update) else update_or_query.callback_query.message.chat)
    chs = list_chapters_with_counts(subject)
    if not chs:
        rows = [
            [InlineKeyboardButton("⏱ Choose timer", callback_data="u:timer")],
            [InlineKeyboardButton("⬅️ Back", callback_data="u:startback")]
        ]
        await edit_or_reply(update_or_query, f"Home › Subjects › {subject}\n\nNo chapters found.", InlineKeyboardMarkup(rows))
        return
    rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"u:chap:{c}")]
            for c, qs in chs]
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="u:startback")])
    await edit_or_reply(update_or_query, f"Home › Subjects › {subject}\n\nChoose a chapter:",
                        InlineKeyboardMarkup(rows))

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

  # ---------- Stats & Leaderboard ----------
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
    if not is_admin(q.from_user.id):
        await q.message.edit_text("This section is for admins.",
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

# ---------- Strict admin gate (alert on misuse) ----------
async def deny_and_alert(update_or_query, action_desc: str, details: str = ""):
    try:
        if hasattr(update_or_query, "callback_query") and update_or_query.callback_query:
            u = update_or_query.callback_query.from_user
            chat = update_or_query.callback_query.message.chat
            await update_or_query.callback_query.message.reply_text("⛔️ Only admin can do this.")
        else:
            u = update_or_query.effective_user
            chat = update_or_query.effective_chat
            await update_or_query.effective_chat.send_message("⛔️ Only admin can do this.")
        uname = f"@{u.username}" if u and u.username else (u.first_name if u else "—")
        extra = f"\nDetails: {details}" if details else ""
        await send_admin_alert(f"⚠️ Non-admin attempted admin action\nUser: {uname} (id:{u.id})\nAction: {action_desc}{extra}")
    except Exception:
        pass

  # ---------- Admin panel ----------
async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer(cache_time=1)
    uid = q.from_user.id
    if not is_admin(uid):
        await deny_and_alert(update, "Open Admin Panel (buttons)")
        return
    act = q.data.split(":", 1)[1]

    if act == "panel":
        await q.message.edit_text("Admin panel:", reply_markup=admin_menu())
    elif act == "back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid))

    elif act == "add":
        subs = list_subjects_with_counts()
        rows = [[InlineKeyboardButton(f"📚 {s} (chapters: {chs} | quizzes: {qs})", callback_data=f"a:add_subj:{s}")]
                for s, chs, qs in subs]
        rows.insert(0, [InlineKeyboardButton("➕ Add new Subject", callback_data="a:newsubj")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:panel")])
        await q.message.edit_text("Admin › Add quiz\n\nChoose a Subject (or add new):", reply_markup=InlineKeyboardMarkup(rows))

    elif act.startswith("add_subj:"):
        subject = act.split(":",1)[1]
        context.user_data["add_subject"] = subject
        chs = list_chapters_with_counts(subject)
        rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"a:add_chap:{c}")] for c, qs in chs]
        rows.insert(0, [InlineKeyboardButton("➕ Add new Chapter", callback_data="a:newchap")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:add")])
        await q.message.edit_text(f"Admin › Add quiz › {subject}\n\nChoose a Chapter (or add new):",
                                  reply_markup=InlineKeyboardMarkup(rows))

    elif act == "newsubj":
        context.user_data["mode"] = "NEW_SUBJECT"
        await q.message.edit_text("Admin › Add quiz\n\nSend the *Subject* name:",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]]),
                                  parse_mode="Markdown")

    elif act == "newchap":
        if not context.user_data.get("add_subject"):
            await q.message.edit_text("Pick a subject first.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:add")]]))
            return
        context.user_data["mode"] = "NEW_CHAPTER"
        await q.message.edit_text("Admin › Add quiz\n\nSend the *Chapter* name:",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"a:add_subj:{context.user_data['add_subject']}")]]),
                                  parse_mode="Markdown")

    elif act.startswith("add_chap:"):
        chapter = act.split(":",1)[1]
        context.user_data["add_chapter"] = chapter
        context.user_data["mode"] = "ADDING"
        sub = context.user_data.get("add_subject")
        await q.message.edit_text(
            f"Admin › Add quiz › {sub} › {chapter}\n\nNow send *Quiz-type* polls to add.\nSend /done when finished.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"a:add_subj:{sub}")]]),
            parse_mode="Markdown"
        )

    elif act == "dellast":
        await q.message.edit_text("Admin › Delete last\n\nDelete the *last quiz you added*?\nThis cannot be undone.",
                                  reply_markup=InlineKeyboardMarkup([
                                      [InlineKeyboardButton("✅ Confirm", callback_data="a:dellast_yes"),
                                       InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]
                                  ]), parse_mode="Markdown")

    elif act == "dellast_yes":
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

    elif act == "export":
        path = "export_quizzes.json"
        cur = conn.execute("SELECT * FROM quizzes ORDER BY id")
        items = []
        for r in cur.fetchall():
            d = dict(r)
            d["options"] = json.loads(r["options_json"])
            d.pop("options_json", None)
            items.append(d)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        await q.message.reply_document(InputFile(path), caption="Backup exported.")

    elif act == "import":
        context.user_data["mode"] = "IMPORT"
        await q.message.edit_text(
            "Admin › Import\n\nSend a .json file to import quizzes. Keys: question, options, correct, explanation?, subject, chapter",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]])
        )

    elif act == "count":
        total = conn.execute("SELECT COUNT(*) t FROM quizzes").fetchone()["t"]
        subs = conn.execute("SELECT subject s, COUNT(*) n FROM quizzes GROUP BY s ORDER BY n DESC").fetchall()
        lines = [f"*Total quizzes: {total}*", "— By subject —"]
        lines += [f"• {r['s']}: {r['n']}" for r in subs]
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]])
        await q.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="Markdown")

    elif act == "broadcast":
        context.user_data["mode"] = "BROADCAST_ENTER"
        await q.message.edit_text("Admin › Broadcast\n\nSend the *text message* to broadcast to all users.",
                                  reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]),
                                  parse_mode="Markdown")

    elif act == "admins":
        await q.message.edit_text("Admin › Admins", reply_markup=admins_menu())

    elif act == "admins_list":
        ids = sorted(admin_ids_from_settings())
        if not ids:
            await q.message.edit_text("No admins configured.", reply_markup=admins_menu())
        else:
            lines = ["👑 *Admins*:"]
            for aid in ids:
                u = conn.execute("SELECT username,first_name,last_name FROM users WHERE user_id=?", (aid,)).fetchone()
                uname = ("@" + (u["username"] or "")) if (u and u["username"]) else (
                    " ".join(filter(None, [u["first_name"] if u else None, u["last_name"] if u else None])) or f"id:{aid}"
                )
                lines.append(f"• {uname} (id:{aid})")
            await q.message.edit_text("\n".join(lines), parse_mode="Markdown", reply_markup=admins_menu())

    elif act == "addadmin":
        context.user_data["mode"] = "ADD_ADMIN"
        await q.message.edit_text("Send the *user id* to add as admin:", parse_mode="Markdown", reply_markup=admins_menu())

    elif act == "rmadmin":
        context.user_data["mode"] = "RM_ADMIN"
        await q.message.edit_text("Send the *user id* to remove from admins:", parse_mode="Markdown", reply_markup=admins_menu())

  # ---------- Text/Poll handler ----------
async def text_or_poll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    uid = update.effective_user.id
    mode = context.user_data.get("mode")

    # ----- Admin modes (strict gate + alert on misuse) -----
    if mode in {"NEW_SUBJECT", "NEW_CHAPTER", "ADDING", "IMPORT", "BROADCAST_ENTER", "BROADCAST_CONFIRM",
                "ADD_ADMIN", "RM_ADMIN", "EDIT_SUB", "EDIT_CHAP", "EDIT_QUIZ"}:
        if not is_admin(uid):
            attempted = mode
            sample = ""
            if update.message:
                if update.message.text: sample = _truncate(update.message.text, 200)
                elif update.message.poll: sample = "[Poll sent]"
                elif update.message.document: sample = f"[File: {update.message.document.file_name}]"
            await deny_and_alert(update, f"Mode={attempted}", details=sample)
            context.user_data["mode"] = None
            return

    if mode == "NEW_SUBJECT" and update.message and update.message.text:
        context.user_data["add_subject"] = update.message.text.strip()
        context.user_data["mode"] = None
        chs = list_chapters_with_counts(context.user_data["add_subject"])
        rows = [[InlineKeyboardButton(f"📖 {c} (quizzes: {qs})", callback_data=f"a:add_chap:{c}")] for c, qs in chs]
        rows.insert(0, [InlineKeyboardButton("➕ Add new Chapter", callback_data="a:newchap")])
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="a:add")])
        await update.message.reply_text(
            f"Admin › Add quiz › {context.user_data['add_subject']}\n\nChoose a Chapter:",
            reply_markup=InlineKeyboardMarkup(rows)
        )
        return

    if mode == "NEW_CHAPTER" and update.message and update.message.text:
        context.user_data["add_chapter"] = update.message.text.strip()
        context.user_data["mode"] = "ADDING"
        sub = context.user_data.get("add_subject")
        await update.message.reply_text(
            f"Admin › Add quiz › {sub} › {context.user_data['add_chapter']}\n\nNow send *Quiz-type* polls to add.\nSend /done when finished.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"a:add_subj:{sub}")]]),
            parse_mode="Markdown"
        )
        return

    if mode == "ADDING" and update.message and update.message.poll:
        poll = update.message.poll
        if poll.type != "quiz":
            await update.message.reply_text("Please send *Quiz-type* polls.", parse_mode="Markdown")
            return
        subject = context.user_data.get("add_subject")
        chapter = context.user_data.get("add_chapter")
        clean_question = re.sub(r'^\s*(\[[^\]]+\]\s*)+', '', poll.question or "").strip()
        q_text, q_opts, q_expl = sanitize_for_poll(clean_question, [o.text for o in poll.options], poll.explanation)
        conn.execute(
            "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (q_text, json.dumps(q_opts, ensure_ascii=False),
             min(int(poll.correct_option_id), max(0, len(q_opts)-1)), q_expl, subject, chapter, int(time.time()), int(uid))
        )
        conn.commit()
        qid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("INSERT INTO admin_log(admin_id,quiz_id,created_at) VALUES(?,?,?)", (uid, qid, int(time.time())))
        conn.commit()
        total = conn.execute("SELECT COUNT(*) t FROM quizzes").fetchone()["t"]
        await update.message.reply_text(f"Saved (#{qid}). Total in DB: {total}")
        return

    if mode == "IMPORT" and update.message and update.message.document:
        doc = update.message.document
        if not doc.file_name.lower().endswith(".json"):
            await update.message.reply_text("Please send a .json file.")
            return
        tgfile = await doc.get_file()
        await tgfile.download_to_drive(custom_path="import.json")
        try:
            data = json.load(open("import.json", "r", encoding="utf-8"))
            count = 0
            for it in data:
                conn.execute(
                    "INSERT INTO quizzes(question,options_json,correct,explanation,subject,chapter,created_at,added_by) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (it["question"], json.dumps(it["options"], ensure_ascii=False),
                     int(it["correct"]), it.get("explanation"), it.get("subject"), it.get("chapter"),
                     int(time.time()), int(uid))
                )
                count += 1
            conn.commit()
            await update.message.reply_text(f"Imported {count} items.")
        except Exception as e:
            await update.message.reply_text("Import error: " + str(e))
        finally:
            context.user_data["mode"] = None
        return

    if mode == "BROADCAST_ENTER" and update.message and update.message.text:
        text = update.message.text.strip()
        context.user_data["broadcast_text"] = text
        context.user_data["mode"] = "BROADCAST_CONFIRM"
        preview = f'Admin Message:\n\n"{text}"'
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data="a:bc_confirm")],
            [InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]
        ])
        await update.message.reply_text("Preview:", reply_markup=None)
        await update.message.reply_text(preview, reply_markup=kb)
        return

    if mode == "ADD_ADMIN" and update.message and update.message.text:
        try:
            new_id = int(update.message.text.strip())
            add_admin(new_id)
            await update.message.reply_text(f"✅ Added admin: {new_id}", reply_markup=admins_menu())
        except Exception:
            await update.message.reply_text("Invalid user id.", reply_markup=admins_menu())
        finally:
            context.user_data["mode"] = None
        return

    if mode == "RM_ADMIN" and update.message and update.message.text:
        try:
            rem_id = int(update.message.text.strip())
            ids = admin_ids_from_settings()
            if rem_id in ids and len(ids) == 1:
                await update.message.reply_text("Cannot remove the last admin.", reply_markup=admins_menu())
            else:
                remove_admin(rem_id)
                await update.message.reply_text(f"🗑 Removed admin: {rem_id}", reply_markup=admins_menu())
        except Exception:
            await update.message.reply_text("Invalid user id.", reply_markup=admins_menu())
        finally:
            context.user_data["mode"] = None
        return

    if mode == "EDIT_SUB" and update.message and update.message.text:
        old = context.user_data.get("edit_sub_old")
        new = update.message.text.strip()
        cnt = conn.execute("SELECT COUNT(*) c FROM quizzes WHERE subject=?", (old,)).fetchone()["c"]
        conn.execute("UPDATE quizzes SET subject=? WHERE subject=?", (new, old))
        conn.commit()
        context.user_data["mode"] = None
        await update.message.reply_text(f"Subject renamed '{old}' → '{new}' ({cnt} quizzes updated).")
        return

    if mode == "EDIT_CHAP" and update.message and update.message.text:
        subj = context.user_data.get("edit_chap_subj")
        old = context.user_data.get("edit_chap_old")
        new = update.message.text.strip()
        cnt = conn.execute("SELECT COUNT(*) c FROM quizzes WHERE subject=? AND chapter=?", (subj, old)).fetchone()["c"]
        conn.execute("UPDATE quizzes SET chapter=? WHERE subject=? AND chapter=?", (new, subj, old))
        conn.commit()
        context.user_data["mode"] = None
        await update.message.reply_text(f"Chapter renamed '{old}' → '{new}' in subject '{subj}' ({cnt} quizzes updated).")
        return

    if mode == "EDIT_QUIZ" and update.message and update.message.poll:
        qid = int(context.user_data["edit_quiz_id"])
        base = conn.execute("SELECT subject, chapter FROM quizzes WHERE id=?", (qid,)).fetchone()
        if not base:
            await update.message.reply_text("Original quiz not found.")
            context.user_data["mode"] = None
            return
        poll = update.message.poll
        if poll.type != "quiz":
            await update.message.reply_text("Please send a *Quiz-type* poll.", parse_mode="Markdown")
            return
        clean_question = re.sub(r'^\s*(\[[^\]]+\]\s*)+', '', poll.question or "").strip()
        q_text, q_opts, q_expl = sanitize_for_poll(clean_question, [o.text for o in poll.options], poll.explanation)
        conn.execute(
            "UPDATE quizzes SET question=?, options_json=?, correct=?, explanation=? WHERE id=?",
            (q_text, json.dumps(q_opts, ensure_ascii=False),
             min(int(poll.correct_option_id), max(0, len(q_opts)-1)), q_expl, qid)
        )
        conn.commit()
        context.user_data["mode"] = None
        await update.message.reply_text(f"Quiz #{qid} updated (kept subject/chapter: '{base['subject']}' / '{base['chapter']}').")
        return

    # ----- User-side plain text shortcuts -----
    if update.message and update.message.text:
        t = update.message.text.strip().lower()
        if t == "/done":
            if is_admin(uid):
                context.user_data["mode"] = None
                await update.message.reply_text("Finished.", reply_markup=admin_menu())
            else:
                await deny_and_alert(update, "/done")
        elif t == "/menu":
            await update.message.reply_text("Menu:", reply_markup=main_menu(uid))

      # ---------- Delete/Edit commands (strict) ----------
async def delsub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/delsub", " ".join(context.args))
        return
    if not context.args:
        await update.message.reply_text("Usage: /delsub <subject>")
        return
    subj = " ".join(context.args).strip()
    cur = conn.execute("SELECT COUNT(*) c FROM quizzes WHERE subject=?", (subj,)).fetchone()
    conn.execute("DELETE FROM quizzes WHERE subject=?", (subj,))
    conn.commit()
    await update.message.reply_text(f"✅ Deleted subject '{subj}' ({cur['c']} quizzes).")

async def delchap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/delchap", " ".join(context.args))
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /delchap <subject> <chapter>")
        return
    subj = context.args[0]
    chap = " ".join(context.args[1:]).strip()
    cur = conn.execute("SELECT COUNT(*) c FROM quizzes WHERE subject=? AND chapter=?", (subj, chap)).fetchone()
    conn.execute("DELETE FROM quizzes WHERE subject=? AND chapter=?", (subj, chap))
    conn.commit()
    await update.message.reply_text(f"✅ Deleted chapter '{chap}' from '{subj}' ({cur['c']} quizzes).")

async def delquiz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/delquiz", " ".join(context.args))
        return
    if not context.args:
        await update.message.reply_text("Usage: /delquiz <quiz_id>")
        return
    try:
        qid = int(context.args[0])
    except:
        await update.message.reply_text("quiz_id must be a number.")
        return
    cur = conn.execute("SELECT subject,chapter FROM quizzes WHERE id=?", (qid,)).fetchone()
    if not cur:
        await update.message.reply_text("Quiz not found.")
        return
    conn.execute("DELETE FROM quizzes WHERE id=?", (qid,))
    conn.commit()
    await update.message.reply_text(f"✅ Deleted quiz #{qid} ({cur['subject']} / {cur['chapter']}).")

async def editsub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/editsub", " ".join(context.args))
        return
    if not context.args:
        await update.message.reply_text("Usage: /editsub <old subject>")
        return
    context.user_data["edit_sub_old"] = " ".join(context.args).strip()
    context.user_data["mode"] = "EDIT_SUB"
    await update.message.reply_text(f"Send the *new subject name* for '{context.user_data['edit_sub_old']}'.", parse_mode="Markdown")

async def editchap_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/editchap", " ".join(context.args))
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /editchap <subject> <old chapter>")
        return
    context.user_data["edit_chap_subj"] = context.args[0]
    context.user_data["edit_chap_old"] = " ".join(context.args[1:]).strip()
    context.user_data["mode"] = "EDIT_CHAP"
    await update.message.reply_text(
        f"Send the *new chapter name* for subject '{context.user_data['edit_chap_subj']}', chapter '{context.user_data['edit_chap_old']}'.",
        parse_mode="Markdown"
    )

async def editquiz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/editquiz", " ".join(context.args))
        return
    if not context.args:
        await update.message.reply_text("Usage: /editquiz <quiz_id>\nThen send a *new Quiz-type poll* to replace it.", parse_mode="Markdown")
        return
    try:
        qid = int(context.args[0])
    except:
        await update.message.reply_text("user id must be a number.")
        return
    if not conn.execute("SELECT 1 FROM quizzes WHERE id=?", (qid,)).fetchone():
        await update.message.reply_text("Quiz not found.")
        return
    context.user_data["edit_quiz_id"] = qid
    context.user_data["mode"] = "EDIT_QUIZ"
    await update.message.reply_text(f"Editing quiz #{qid}. Send a *new Quiz-type poll* now.", parse_mode="Markdown")

# ---------- Admin management commands (also via panel) ----------
async def admins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/admins")
        return
    ids = sorted(admin_ids_from_settings())
    if not ids:
        await update.message.reply_text("No admins configured.")
        return
    lines = ["👑 *Admins*:"]
    for uid in ids:
        u = conn.execute("SELECT username, first_name, last_name FROM users WHERE user_id=?", (uid,)).fetchone()
        uname = ("@" + (u["username"] or "")) if (u and u["username"]) else (
            " ".join(filter(None, [u["first_name"] if u else None, u["last_name"] if u else None])) or f"id:{uid}"
        )
        lines.append(f"• {uname} (id:{uid})")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def addadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/addadmin", " ".join(context.args))
        return
    if not context.args:
        await update.message.reply_text("Usage: /addadmin <user_id>")
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("User id must be a number.")
        return
    add_admin(uid)
    await update.message.reply_text(f"✅ Added admin: {uid}")

async def rmadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await deny_and_alert(update, "/rmadmin", " ".join(context.args))
        return
    if not context.args:
        await update.message.reply_text("Usage: /rmadmin <user_id>")
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("User id must be a number.")
        return
    ids = admin_ids_from_settings()
    if uid in ids and len(ids) == 1:
        await update.message.reply_text("Cannot remove the last admin.")
        return
    remove_admin(uid)
    await update.message.reply_text(f"🗑 Removed admin: {uid}")

# ---------- Buttons ----------
async def btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    q = update.callback_query
    await q.answer(cache_time=1)
    data = q.data
    uid = q.from_user.id

    if data == "u:help":
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
        await q.message.edit_text("Start → Subject → Chapter → Timer (or Without Timer) → I am ready!", reply_markup=kb)

    elif data == "u:stats":
        await show_stats(q)

    elif data == "u:lb":
        if not is_admin(uid):
            await deny_and_alert(update, "Leaderboard (button)")
            return
        await leaderboard(q, page=0)
    elif data.startswith("u:lbp:"):
        if not is_admin(uid):
            await deny_and_alert(update, "Leaderboard page (button)")
            return
        page = int(data.split(":")[2]); await leaderboard(q, page=page)

    elif data == "u:contact":
        context.user_data["mode"] = "CONTACT_ADMIN"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="u:back")]])
        await q.message.edit_text("Type your message for the admin:", reply_markup=kb)

    elif data == "u:start":
        await user_subjects(update)
    elif data.startswith("u:subjp:"):
        page = int(data.split(":")[2]); await user_subjects(update, page)
    elif data.startswith("u:subj:"):
        subject = data.split(":",2)[2]; context.user_data["subject"] = subject; await user_chapters(update, subject)
    elif data == "u:startback":
        await user_subjects(update)

    elif data.startswith("u:chap:"):
        chapter = data.split(":",2)[2]; context.user_data["chapter"] = chapter; await timer_menu(update)
    elif data == "u:chapback":
        subj = context.user_data.get("subject"); await user_chapters(update, subj)

    elif data == "u:timer":
        await timer_menu(update)
    elif data.startswith("u:timer:"):
        context.user_data["open_period"] = int(data.split(":")[2]); await pre_quiz_screen(q, context)
    elif data == "u:timerback":
        await timer_menu(update)

    elif data == "u:ready":
        await begin_quiz_session(q, context)

    elif data == "u:retry":
        context.user_data["subject"] = context.user_data.get("last_subject")
        context.user_data["chapter"] = context.user_data.get("last_chapter")
        context.user_data["open_period"] = context.user_data.get("last_open_period", DEFAULT_OPEN_PERIOD)
        await begin_quiz_session(q, context)

    elif data == "u:stop_now":
        conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
        conn.commit()
        await q.message.edit_text("Quiz stopped.", reply_markup=main_menu(uid))

    elif data == "u:back":
        await q.message.edit_text("Menu:", reply_markup=main_menu(uid))

    elif data.startswith("a:"):
        if not is_admin(uid):
            await deny_and_alert(update, f"Admin button '{data}'")
            return
        act = data.split(":",1)[1]
        if act == "bc_confirm":
            btxt = context.user_data.get("broadcast_text")
            if not btxt:
                await q.message.edit_text("No text to broadcast.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
                return
            full = f'Admin Message:\n\n"{btxt}"'
            rows = conn.execute("SELECT DISTINCT chat_id FROM users WHERE chat_id IS NOT NULL").fetchall()
            sent = 0
            for r in rows:
                try:
                    await context.bot.send_message(int(r["chat_id"]), full)
                    sent += 1
                except Exception:
                    pass
                await asyncio.sleep(0.02)
            context.user_data["broadcast_text"] = None
            context.user_data["mode"] = None
            await q.message.edit_text(f"Broadcast sent to {sent} users.",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="a:panel")]]))
        else:
            await admin_cb(update, context)

# ---------- Stop ----------
async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    conn.execute("UPDATE sessions SET state='stopped' WHERE user_id=? AND state='running'", (uid,))
    conn.commit()
    await update.message.reply_text("Quiz stopped.", reply_markup=main_menu(uid))

# ---------- Flask keepalive (optional) ----------
app = Flask(__name__)
@app.get("/")
def home():
    return "OK", 200

def run_keepalive():
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# ---------- Main ----------
if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN missing in env or secrets.env")

    db_init()
    Thread(target=run_keepalive, daemon=True).start()

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("stop", stop_cmd))

    # delete/edit quiz
    application.add_handler(CommandHandler("delsub", delsub_cmd))
    application.add_handler(CommandHandler("delchap", delchap_cmd))
    application.add_handler(CommandHandler("delquiz", delquiz_cmd))
    application.add_handler(CommandHandler("editsub", editsub_cmd))
    application.add_handler(CommandHandler("editchap", editchap_cmd))
    application.add_handler(CommandHandler("editquiz", editquiz_cmd))

    # admin management (commands)
    application.add_handler(CommandHandler("admins", admins_cmd))
    application.add_handler(CommandHandler("addadmin", addadmin_cmd))
    application.add_handler(CommandHandler("rmadmin", rmadmin_cmd))

    application.add_handler(CallbackQueryHandler(btn))
    application.add_handler(PollAnswerHandler(poll_answer))
    application.add_handler(MessageHandler(filters.ALL, text_or_poll))

    # POLLING mode
    application.run_polling(close_loop=False)
```0
