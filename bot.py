"""
╔══════════════════════════════════════╗
║   🌸  ECONOMY BOT  —  v3.0          ║
║   PostgreSQL · Webhook · Render      ║
╚══════════════════════════════════════╝

Изменения v3.0:
  • SQLite → PostgreSQL (Supabase)
  • polling → Webhook (Flask)
  • Connection pool через psycopg2.pool
  • Все запросы переписаны под %s placeholder
  • Фиксы багов оригинала
"""

# ══════════════════════════════════════════════
# 0. ЗАВИСИМОСТИ
# ══════════════════════════════════════════════

import os, re, time, math, random, threading
from contextlib import contextmanager
from datetime import datetime

import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool

from flask import Flask, request, abort

from dotenv import load_dotenv
import telebot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice
)

load_dotenv()

# ══════════════════════════════════════════════
# 1. КОНФИГ
# ══════════════════════════════════════════════

TOKEN        = os.getenv("BOT_TOKEN")
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
ALERT_CHAT   = int(os.getenv("ALERT_CHAT", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")          # postgres://user:pass@host:5432/db
WEBHOOK_URL  = os.getenv("WEBHOOK_URL", "")       # https://your-app.onrender.com
PORT         = int(os.getenv("PORT", 8443))
CURRENCY     = "💵"
TICKER       = "ECO"

# Кулдауны (секунды)
CD_CLICK   = 5
CD_DAILY   = 86400
CD_WORK    = 14400
CD_MINE    = 3600
CD_LOTTERY = 86400

# Экономика
TRANSFER_FEE      = 0.05
BANK_RATE         = 0.01
LOAN_RATE         = 0.10
LOAN_MAX          = 50_000
CLICK_BASE        = 100
MINE_BASE         = 500
WORK_BASE         = 1_500
TICKET_PRICE      = 500

# Акции
STOCK_UPDATE      = 1800
STOCK_PRICE_START = 10_000
STOCK_VOLATILITY  = 0.04


# ══════════════════════════════════════════════
# 2. БАЗА ДАННЫХ — PostgreSQL
# ══════════════════════════════════════════════

_pg_pool: pg_pool.ThreadedConnectionPool | None = None

def get_pg_pool() -> pg_pool.ThreadedConnectionPool:
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = pg_pool.ThreadedConnectionPool(
            minconn=2, maxconn=10,
            dsn=DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    return _pg_pool


@contextmanager
def db():
    """Контекстный менеджер для работы с БД."""
    pool = get_pg_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def init_db():
    with db() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id              BIGINT PRIMARY KEY,
            name            TEXT    DEFAULT '',
            balance         BIGINT  DEFAULT 0,
            bank            BIGINT  DEFAULT 0,
            xp              BIGINT  DEFAULT 0,
            daily_streak    INT     DEFAULT 0,
            last_click      BIGINT  DEFAULT 0,
            last_daily      BIGINT  DEFAULT 0,
            last_work       BIGINT  DEFAULT 0,
            last_mine       BIGINT  DEFAULT 0,
            click_power     INT     DEFAULT 100,
            total_earned    BIGINT  DEFAULT 0,
            ref_by          BIGINT  DEFAULT 0,
            ref_code        TEXT    UNIQUE,
            premium_until   BIGINT  DEFAULT 0,
            last_interest_calc BIGINT DEFAULT 0,
            created_at      BIGINT  DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS loans (
            user_id  BIGINT PRIMARY KEY,
            amount   BIGINT DEFAULT 0,
            due_at   BIGINT DEFAULT 0,
            taken_at BIGINT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS stocks (
            ticker      TEXT   PRIMARY KEY,
            price       BIGINT DEFAULT 10000,
            prev_price  BIGINT DEFAULT 10000,
            updated_at  BIGINT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS portfolios (
            user_id BIGINT,
            ticker  TEXT,
            shares  INT    DEFAULT 0,
            avg_buy BIGINT DEFAULT 0,
            PRIMARY KEY (user_id, ticker)
        );
        CREATE TABLE IF NOT EXISTS stock_history (
            id     BIGSERIAL PRIMARY KEY,
            ticker TEXT,
            price  BIGINT,
            ts     BIGINT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS lottery (
            id      INT PRIMARY KEY DEFAULT 1,
            jackpot BIGINT DEFAULT 0,
            draw_at BIGINT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS lottery_tickets (
            user_id BIGINT PRIMARY KEY,
            tickets INT    DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS transfers (
            id      BIGSERIAL PRIMARY KEY,
            from_id BIGINT,
            to_id   BIGINT,
            amount  BIGINT,
            fee     BIGINT,
            ts      BIGINT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS items (
            id      SERIAL PRIMARY KEY,
            name    TEXT,
            emoji   TEXT    DEFAULT '📦',
            price   BIGINT,
            supply  INT     DEFAULT -1,
            sold    INT     DEFAULT 0,
            active  SMALLINT DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS inventory (
            user_id BIGINT,
            item_id INT,
            qty     INT DEFAULT 1,
            PRIMARY KEY (user_id, item_id)
        );
        CREATE TABLE IF NOT EXISTS clans (
            id         SERIAL PRIMARY KEY,
            name       TEXT UNIQUE,
            tag        TEXT UNIQUE,
            owner      BIGINT,
            balance    BIGINT  DEFAULT 0,
            level      INT     DEFAULT 1,
            xp         BIGINT  DEFAULT 0,
            created_at BIGINT  DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS clan_members (
            user_id   BIGINT PRIMARY KEY,
            clan_id   INT,
            role      TEXT DEFAULT 'member',
            joined_at BIGINT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS bans (
            user_id BIGINT PRIMARY KEY,
            reason  TEXT,
            by      BIGINT,
            ts      BIGINT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS promo_codes (
            code     TEXT PRIMARY KEY,
            reward   BIGINT,
            max_uses INT    DEFAULT 1,
            uses     INT    DEFAULT 0,
            expires  BIGINT DEFAULT 0,
            active   SMALLINT DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT,
            code    TEXT,
            ts      BIGINT DEFAULT 0,
            PRIMARY KEY (user_id, code)
        );
        CREATE TABLE IF NOT EXISTS donate_packages (
            key    TEXT PRIMARY KEY,
            stars  INT,
            amount BIGINT,
            label  TEXT
        );

        INSERT INTO lottery (id, jackpot, draw_at) VALUES (1, 0, 0)
            ON CONFLICT (id) DO NOTHING;
        INSERT INTO stocks (ticker, price, prev_price, updated_at) VALUES ('ECO', 10000, 10000, 0)
            ON CONFLICT (ticker) DO NOTHING;
        INSERT INTO donate_packages VALUES
            ('s1',   1,    10000, '⭐ 10 000'),
            ('s5',   5,    60000, '⭐ 60 000'),
            ('s15',  15,  250000, '🔥 250 000'),
            ('s50',  50,  900000, '🔥 900 000'),
            ('s150', 150, 3000000,'💎 3 000 000'),
            ('s250', 250, 5500000,'💎 5 500 000')
            ON CONFLICT (key) DO NOTHING;

        CREATE INDEX IF NOT EXISTS ix_users_bal  ON users(balance DESC);
        CREATE INDEX IF NOT EXISTS ix_users_xp   ON users(xp DESC);
        CREATE INDEX IF NOT EXISTS ix_port_user  ON portfolios(user_id);
        CREATE INDEX IF NOT EXISTS ix_hist_tick  ON stock_history(ticker, ts DESC);
        """)
    print("✅ БД инициализирована")


# ══════════════════════════════════════════════
# 3. ФЛУД-ЗАЩИТА
# ══════════════════════════════════════════════

_flood: dict[int, list] = {}
_banned_flood: dict[int, float] = {}
_flock = threading.Lock()
FLOOD_N, FLOOD_W, FLOOD_BAN = 10, 5, 300

def is_flooding(uid: int) -> bool:
    t = time.time()
    with _flock:
        if uid in _banned_flood:
            if t < _banned_flood[uid]:
                return True
            del _banned_flood[uid]
        hist = [x for x in _flood.get(uid, []) if t - x < FLOOD_W]
        hist.append(t)
        _flood[uid] = hist
        if len(hist) >= FLOOD_N:
            _banned_flood[uid] = t + FLOOD_BAN
            return True
    return False


# ══════════════════════════════════════════════
# 4. БОТ + WEBHOOK
# ══════════════════════════════════════════════

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=6)
app = Flask(__name__)

@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    if request.headers.get("content-type") == "application/json":
        json_string = request.get_data().decode("utf-8")
        update = telebot.types.Update.de_json(json_string)
        # Флуд-фильтр
        try:
            uid = None
            if update.message and update.message.from_user:
                uid = update.message.from_user.id
            elif update.callback_query and update.callback_query.from_user:
                uid = update.callback_query.from_user.id
            if uid and is_flooding(uid):
                return "OK", 200
        except Exception:
            pass
        bot.process_new_updates([update])
        return "OK", 200
    abort(403)

@app.route("/", methods=["GET"])
def health():
    return "Bot is running", 200


# ══════════════════════════════════════════════
# 5. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════

def fmt(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n:,}".replace(",", " ")
    return str(n)

def now() -> int:
    return int(time.time())

def cd_str(seconds: int) -> str:
    if seconds <= 0:
        return "готово"
    h, r = divmod(int(seconds), 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}ч {m}м"
    if m:
        return f"{m}м {s}с"
    return f"{s}с"

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def is_banned(uid: int) -> bool:
    with db() as c:
        c.execute("SELECT 1 FROM bans WHERE user_id=%s", (uid,))
        return bool(c.fetchone())

def get_user(uid: int) -> dict | None:
    with db() as c:
        c.execute("SELECT * FROM users WHERE id=%s", (uid,))
        row = c.fetchone()
        return dict(row) if row else None

def ensure_user(uid: int, name: str = "") -> dict:
    with db() as c:
        c.execute(
            """INSERT INTO users (id, name, created_at) VALUES (%s, %s, %s)
               ON CONFLICT (id) DO NOTHING""",
            (uid, name, now())
        )
    return get_user(uid)

def add_balance(uid: int, amount: int):
    with db() as c:
        c.execute(
            """UPDATE users
               SET balance = balance + %s,
                   total_earned = total_earned + GREATEST(0, %s)
               WHERE id = %s""",
            (amount, amount, uid)
        )

def add_xp(uid: int, xp: int):
    with db() as c:
        c.execute("UPDATE users SET xp=xp+%s WHERE id=%s", (xp, uid))

def user_level(xp: int) -> int:
    return max(1, int(math.sqrt(xp / 100)) + 1)

def level_xp(lvl: int) -> int:
    return (lvl - 1) ** 2 * 100

def send_alert(text: str):
    if ALERT_CHAT:
        try:
            bot.send_message(ALERT_CHAT, text, parse_mode="HTML")
        except Exception:
            pass

def is_group(msg) -> bool:
    return msg.chat.type in ("group", "supergroup")

def get_display_name(msg) -> str:
    u = msg.from_user
    if not u:
        return "Игрок"
    return u.first_name or u.username or str(u.id)

def mention(uid: int, name: str) -> str:
    return f'<a href="tg://user?id={uid}">{name}</a>'


# ══════════════════════════════════════════════
# 6. UI — КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def kb(*rows) -> InlineKeyboardMarkup:
    m = InlineKeyboardMarkup()
    for row in rows:
        btns = []
        for item in row:
            if len(item) == 2:
                btns.append(InlineKeyboardButton(item[0], callback_data=item[1]))
            elif item[0] == "url":
                btns.append(InlineKeyboardButton(item[1], url=item[2]))
        m.row(*btns)
    return m

def main_menu(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    u = get_user(uid)
    bal  = u["balance"] if u else 0
    bank = u["bank"]    if u else 0
    lvl  = user_level(u["xp"]) if u else 1
    is_prem = u and u["premium_until"] > now()
    prem_tag = " ⭐" if is_prem else ""

    text = (
        f"<b>💼 Главное меню{prem_tag}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💵 Кошелёк: <b>{fmt(bal)}</b>\n"
        f"🏦 Банк: <b>{fmt(bank)}</b>\n"
        f"⚡ Уровень: <b>{lvl}</b>\n"
    )
    buttons = kb(
        [("💵 Баланс", "menu_balance"), ("📈 Биржа", "menu_stock")],
        [("⚒️ Работа", "menu_work"),   ("🎰 Игры",  "menu_games")],
        [("🏦 Банк",   "menu_bank"),   ("🛍️ Магазин","menu_shop")],
        [("🏆 Топ",    "menu_top"),    ("🎁 Бонус", "menu_bonus")],
        [("👤 Профиль","menu_profile"),("🏰 Клан",  "menu_clan")],
    )
    return text, buttons


# ══════════════════════════════════════════════
# 7. СТАРТ / МЕНЮ
# ══════════════════════════════════════════════

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    uid  = msg.from_user.id
    name = msg.from_user.first_name or "Игрок"
    if is_banned(uid):
        bot.send_message(uid, "🚫 Вы заблокированы.")
        return
    ensure_user(uid, name)

    # Реферал
    parts = msg.text.split()
    if len(parts) > 1:
        ref = parts[1]
        u = get_user(uid)
        if u and not u["ref_by"] and ref != str(uid):
            with db() as c:
                c.execute("SELECT id FROM users WHERE ref_code=%s", (ref,))
                row = c.fetchone()
                if row:
                    ref_uid = row["id"]
                    bonus = 2_000
                    add_balance(uid, bonus)
                    add_balance(ref_uid, bonus)
                    c.execute("UPDATE users SET ref_by=%s WHERE id=%s", (ref_uid, uid))
                    try:
                        bot.send_message(ref_uid,
                            f"🎉 По вашей ссылке пришёл новый игрок!\n"
                            f"Вам начислено <b>{fmt(bonus)} {CURRENCY}</b>",
                            parse_mode="HTML")
                    except Exception:
                        pass

    text, btns = main_menu(uid)
    bot.send_message(uid, text, reply_markup=btns, parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and m.text.lower() in ["меню", "/меню", "menu", "/menu", "назад", "🏠"])
def cmd_menu(msg):
    uid = msg.from_user.id
    if is_banned(uid):
        return
    ensure_user(uid, msg.from_user.first_name or "")
    text, btns = main_menu(uid)
    bot.send_message(uid, text, reply_markup=btns, parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: c.data == "home")
def cb_home(call):
    uid = call.from_user.id
    text, btns = main_menu(uid)
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=btns, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=btns, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ══════════════════════════════════════════════
# 8. БАЛАНС / ПРОФИЛЬ
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_balance")
def cb_balance(call):
    uid = call.from_user.id
    u = get_user(uid)
    if not u:
        bot.answer_callback_query(call.id, "Используй /start")
        return

    lvl = user_level(u["xp"])
    next_lvl_xp = level_xp(lvl + 1)
    cur_xp = u["xp"] - level_xp(lvl)
    xp_range = max(1, next_lvl_xp - level_xp(lvl))
    xp_bar_pct = min(10, int(cur_xp / xp_range * 10))
    bar = "█" * xp_bar_pct + "░" * (10 - xp_bar_pct)

    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()

    loan_text = ""
    if loan and loan["amount"] > 0:
        due = datetime.fromtimestamp(loan["due_at"]).strftime("%d.%m")
        loan_text = f"\n💳 Кредит: <b>{fmt(loan['amount'])} {CURRENCY}</b> до {due}"

    text = (
        f"<b>💵 Баланс</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Кошелёк: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"Банк:    <b>{fmt(u['bank'])} {CURRENCY}</b>\n"
        f"Всего заработано: {fmt(u['total_earned'])}\n"
        f"——\n"
        f"⚡ Уровень {lvl} [{bar}]\n"
        f"XP: {fmt(u['xp'])} / {fmt(next_lvl_xp)}"
        f"{loan_text}"
    )
    buttons = kb(
        [("💸 Перевод", "action_transfer"), ("📜 История", "menu_transfers")],
        [("🏠 Меню", "home")]
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data == "menu_profile")
def cb_profile(call):
    uid = call.from_user.id
    u = get_user(uid)
    if not u:
        bot.answer_callback_query(call.id, "Используй /start")
        return

    lvl = user_level(u["xp"])
    prem_str = (
        datetime.fromtimestamp(u["premium_until"]).strftime("%d.%m.%Y")
        if u["premium_until"] > now() else "нет"
    )

    with db() as c:
        c.execute("""SELECT cl.name FROM clan_members cm
                     JOIN clans cl ON cl.id=cm.clan_id WHERE cm.user_id=%s""", (uid,))
        clan_row = c.fetchone()
        c.execute("SELECT SUM(shares) as s FROM portfolios WHERE user_id=%s", (uid,))
        port_row = c.fetchone()

    clan_str = clan_row["name"] if clan_row else "нет"
    shares = port_row["s"] if port_row and port_row["s"] else 0
    ref_code = u["ref_code"] or "—"

    text = (
        f"<b>👤 Профиль</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Имя: <b>{u['name'] or 'Игрок'}</b>\n"
        f"ID: <code>{uid}</code>\n"
        f"Уровень: <b>{lvl}</b>  |  XP: {fmt(u['xp'])}\n"
        f"Стрик: 🔥 {u['daily_streak']} дней\n"
        f"Premium: {prem_str}\n"
        f"Клан: {clan_str}\n"
        f"Акции: {shares} шт.\n"
        f"Реф-код: <code>{ref_code}</code>"
    )
    buttons = kb(
        [("✏️ Сменить имя", "action_rename"), ("🏠 Меню", "home")]
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ══════════════════════════════════════════════
# 9. КЛИКЕР
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.strip() in ["клик", "/клик", "click"])
def cmd_click(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")
    u = get_user(uid)

    remaining = CD_CLICK - (now() - u["last_click"])
    if remaining > 0:
        bot.send_message(uid, f"⏱ Подожди ещё <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return

    power = u["click_power"]
    streak_bonus = 1.5 if u["daily_streak"] >= 7 else (1.2 if u["daily_streak"] >= 3 else 1.0)
    earn = int(power * streak_bonus * random.uniform(0.8, 1.2))
    add_balance(uid, earn)
    add_xp(uid, 5)
    with db() as c:
        c.execute("UPDATE users SET last_click=%s WHERE id=%s", (now(), uid))

    bot.send_message(uid,
        f"⚡ Клик! +<b>{fmt(earn)} {CURRENCY}</b>\n"
        f"{'🔥 Бонус стрика x'+str(streak_bonus) if streak_bonus > 1 else ''}",
        reply_markup=kb([("⚡ Ещё раз", "action_click"), ("🏠 Меню", "home")]),
        parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: c.data == "action_click")
def cb_click(call):
    uid = call.from_user.id
    u = get_user(uid)
    if not u:
        bot.answer_callback_query(call.id, "Используй /start")
        return
    remaining = CD_CLICK - (now() - u["last_click"])
    if remaining > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(remaining)}", show_alert=False)
        return
    earn = int(u["click_power"] * random.uniform(0.8, 1.2))
    add_balance(uid, earn)
    add_xp(uid, 5)
    with db() as c:
        c.execute("UPDATE users SET last_click=%s WHERE id=%s", (now(), uid))
    bot.answer_callback_query(call.id, f"⚡ +{fmt(earn)} {CURRENCY}", show_alert=False)


# ══════════════════════════════════════════════
# 10. РАБОТА (4 ПРОФЕССИИ)
# ══════════════════════════════════════════════

JOBS = {
    "taxi":  {"name": "🚕 Такси",       "earn": (800,  1_600),  "xp": 20},
    "cargo": {"name": "🚚 Курьер",      "earn": (1_200, 2_400), "xp": 30},
    "trade": {"name": "📊 Трейдер",     "earn": (600,  3_000),  "xp": 25, "risk": True},
    "code":  {"name": "💻 Программист", "earn": (2_000, 4_000), "xp": 40},
}

@bot.callback_query_handler(func=lambda c: c.data == "menu_work")
def cb_work_menu(call):
    uid = call.from_user.id
    u = get_user(uid)
    remaining = CD_WORK - (now() - u["last_work"]) if u else 0

    if remaining > 0:
        text    = f"<b>⚒️ Работа</b>\n\nДоступно через: <b>{cd_str(remaining)}</b>"
        buttons = kb([("🏠 Меню", "home")])
    else:
        text = "<b>⚒️ Выбери профессию:</b>"
        rows = [[(f"{v['name']}  {fmt(v['earn'][0])}–{fmt(v['earn'][1])}", f"do_work_{k}")]
                for k, v in JOBS.items()]
        rows.append([("🏠 Меню", "home")])
        buttons = kb(*rows)

    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("do_work_"))
def cb_do_work(call):
    uid     = call.from_user.id
    job_key = call.data[8:]
    job     = JOBS.get(job_key)
    if not job:
        bot.answer_callback_query(call.id)
        return

    u = get_user(uid)
    remaining = CD_WORK - (now() - u["last_work"])
    if remaining > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(remaining)}", show_alert=True)
        return

    lo, hi = job["earn"]
    earn = random.randint(lo, hi)
    if job.get("risk") and random.random() < 0.3:
        earn = -random.randint(lo // 2, hi // 2)

    add_balance(uid, earn)
    add_xp(uid, job["xp"])
    with db() as c:
        c.execute("UPDATE users SET last_work=%s WHERE id=%s", (now(), uid))

    sign = "+" if earn >= 0 else ""
    text = (
        f"<b>{job['name']}</b>\n"
        f"{'✅' if earn >= 0 else '📉'} Результат: <b>{sign}{fmt(earn)} {CURRENCY}</b>\n"
        f"⭐ +{job['xp']} XP\n\n"
        f"⏱ Следующая работа через <b>{cd_str(CD_WORK)}</b>"
    )
    buttons = kb([("⚒️ Работа", "menu_work"), ("🏠 Меню", "home")])
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ══════════════════════════════════════════════
# 11. МАЙНИНГ
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower() in ["майнинг", "/майнинг", "mine"])
def cmd_mine(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")
    u = get_user(uid)

    remaining = CD_MINE - (now() - u["last_mine"])
    if remaining > 0:
        bot.send_message(uid,
            f"<b>⛏️ Майнинг</b>\n\n⏱ Следующий сбор через: <b>{cd_str(remaining)}</b>",
            parse_mode="HTML")
        return

    with db() as c:
        c.execute("SELECT qty FROM inventory WHERE user_id=%s AND item_id=1", (uid,))
        row = c.fetchone()
    cards = row["qty"] if row else 0

    earn = int((MINE_BASE + cards * 200) * random.uniform(0.9, 1.1))
    add_balance(uid, earn)
    add_xp(uid, 10)
    with db() as c:
        c.execute("UPDATE users SET last_mine=%s WHERE id=%s", (now(), uid))

    bot.send_message(uid,
        f"<b>⛏️ Майнинг</b>\n\n"
        f"💰 Намайнено: <b>+{fmt(earn)} {CURRENCY}</b>\n"
        f"🖥 Видеокарт: {cards}\n"
        f"⏱ Следующий сбор через <b>{cd_str(CD_MINE)}</b>",
        parse_mode="HTML",
        reply_markup=kb([("⛏️ Ещё раз", "mine_again"), ("🏠 Меню", "home")])
    )


@bot.callback_query_handler(func=lambda c: c.data == "mine_again")
def cb_mine_again(call):
    uid = call.from_user.id
    u = get_user(uid)
    remaining = CD_MINE - (now() - u["last_mine"])
    if remaining > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(remaining)}", show_alert=False)
    else:
        bot.answer_callback_query(call.id)
        cmd_mine(call.message)


# ══════════════════════════════════════════════
# 12. ЕЖЕДНЕВНЫЙ БОНУС
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_bonus")
def cb_bonus(call):
    uid = call.from_user.id
    u = get_user(uid)
    if not u:
        bot.answer_callback_query(call.id, "Используй /start")
        return

    remaining = CD_DAILY - (now() - u["last_daily"])

    if remaining > 0:
        text = (
            f"<b>🎁 Ежедневный бонус</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⏱ Следующий бонус через: <b>{cd_str(remaining)}</b>\n"
            f"🔥 Стрик: <b>{u['daily_streak']} дней</b>"
        )
        buttons = kb([("🏠 Меню", "home")])
    else:
        streak = u["daily_streak"]
        bonus  = 1_000 + streak * 100
        text = (
            f"<b>🎁 Ежедневный бонус</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔥 Стрик: <b>{streak} дней</b>\n"
            f"💰 Получишь: <b>+{fmt(bonus)} {CURRENCY}</b>"
        )
        buttons = kb([("✅ Получить", "claim_bonus"), ("🏠 Меню", "home")])

    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data == "claim_bonus")
def cb_claim_bonus(call):
    uid = call.from_user.id
    u = get_user(uid)
    remaining = CD_DAILY - (now() - u["last_daily"])
    if remaining > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(remaining)}", show_alert=True)
        return

    # Стрик: даём 1ч форы
    is_consecutive = (now() - u["last_daily"]) < CD_DAILY + 3600
    streak = (u["daily_streak"] + 1) if is_consecutive else 1
    bonus  = 1_000 + streak * 100

    add_balance(uid, bonus)
    add_xp(uid, 20)
    with db() as c:
        c.execute("UPDATE users SET last_daily=%s, daily_streak=%s WHERE id=%s",
                  (now(), streak, uid))

    text = (
        f"<b>🎁 Бонус получен!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"+<b>{fmt(bonus)} {CURRENCY}</b>\n"
        f"🔥 Стрик: <b>{streak} дней</b>"
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=kb([("🏠 Меню", "home")]), parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=kb([("🏠 Меню", "home")]), parse_mode="HTML")
    bot.answer_callback_query(call.id, "✅ Получено!")


# ══════════════════════════════════════════════
# 13. БАНК — ВКЛАД / КРЕДИТ
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_bank")
def cb_bank(call):
    uid = call.from_user.id
    u = get_user(uid)

    # Начисляем проценты
    if u and u["bank"] > 0:
        days = (now() - (u.get("last_interest_calc") or now())) / 86400
        if days >= 1:
            interest = int(u["bank"] * BANK_RATE * days)
            with db() as c:
                c.execute("UPDATE users SET bank=bank+%s, last_interest_calc=%s WHERE id=%s",
                          (interest, now(), uid))
            u = get_user(uid)

    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()

    loan_str = "нет"
    if loan and loan["amount"] > 0:
        due = datetime.fromtimestamp(loan["due_at"]).strftime("%d.%m")
        loan_str = f"{fmt(loan['amount'])} {CURRENCY} до {due}"

    text = (
        f"<b>🏦 Банк</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"На счёте: <b>{fmt(u['bank'])} {CURRENCY}</b>\n"
        f"💹 Ставка: <b>{BANK_RATE*100:.0f}%</b>/сутки\n"
        f"——\n"
        f"Кредит: <b>{loan_str}</b>\n"
        f"Максимум займа: <b>{fmt(LOAN_MAX)}</b>"
    )
    buttons = kb(
        [("📥 Внести", "bank_deposit"), ("📤 Снять", "bank_withdraw")],
        [("💳 Кредит",  "bank_loan"),   ("💰 Погасить", "bank_repay")],
        [("🏠 Меню", "home")]
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ── Ввод суммы через следующее сообщение ──────

_waiting: dict[int, str] = {}

@bot.callback_query_handler(func=lambda c: c.data in [
    "bank_deposit","bank_withdraw","bank_loan","bank_repay","action_transfer","action_rename"
])
def cb_input_prompt(call):
    uid = call.from_user.id
    _waiting[uid] = call.data
    prompts = {
        "bank_deposit":   "Введи сумму для вклада:",
        "bank_withdraw":  "Введи сумму для снятия:",
        "bank_loan":      f"Введи сумму кредита (макс {fmt(LOAN_MAX)}):",
        "bank_repay":     "Введи сумму для погашения:",
        "action_transfer":"Введи: @username сумма\nПример: @ivan 5000",
        "action_rename":  "Введи новое имя (до 20 символов):",
    }
    bot.answer_callback_query(call.id)
    bot.send_message(uid, prompts.get(call.data, "Введи значение:"),
                     reply_markup=kb([("❌ Отмена", "cancel_input")]))


@bot.callback_query_handler(func=lambda c: c.data == "cancel_input")
def cb_cancel_input(call):
    _waiting.pop(call.from_user.id, None)
    bot.answer_callback_query(call.id, "Отменено")
    text, btns = main_menu(call.from_user.id)
    bot.send_message(call.from_user.id, text, reply_markup=btns, parse_mode="HTML")


@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in _waiting and m.chat.type == "private")
def handle_input(msg):
    uid    = msg.from_user.id
    action = _waiting.pop(uid, None)
    if not action:
        return

    u    = get_user(uid)
    text = msg.text.strip()

    if action == "action_rename":
        name = text[:20]
        with db() as c:
            c.execute("UPDATE users SET name=%s WHERE id=%s", (name, uid))
        bot.send_message(uid, f"✅ Имя изменено на <b>{name}</b>", parse_mode="HTML",
                         reply_markup=kb([("🏠 Меню", "home")]))
        return

    if action == "action_transfer":
        parts = text.split()
        if len(parts) < 2:
            bot.send_message(uid, "❌ Формат: @username сумма")
            return
        target_un = parts[0].lstrip("@")
        try:
            amount = int(parts[1].replace(" ", ""))
        except ValueError:
            bot.send_message(uid, "❌ Неверная сумма")
            return
        if amount <= 0:
            bot.send_message(uid, "❌ Сумма > 0")
            return
        fee   = int(amount * TRANSFER_FEE)
        total = amount + fee
        if u["balance"] < total:
            bot.send_message(uid, f"❌ Нужно {fmt(total)} (комиссия {fmt(fee)})")
            return
        with db() as c:
            c.execute("SELECT id, name FROM users WHERE name ILIKE %s", (f"%{target_un}%",))
            row = c.fetchone()
        if not row:
            bot.send_message(uid, "❌ Пользователь не найден")
            return
        to_uid = row["id"]
        if to_uid == uid:
            bot.send_message(uid, "❌ Нельзя переводить самому себе")
            return
        add_balance(uid, -total)
        add_balance(to_uid, amount)
        with db() as c:
            c.execute("INSERT INTO transfers (from_id,to_id,amount,fee,ts) VALUES (%s,%s,%s,%s,%s)",
                      (uid, to_uid, amount, fee, now()))
        try:
            bot.send_message(to_uid, f"💸 Вам перевели <b>{fmt(amount)} {CURRENCY}</b>", parse_mode="HTML")
        except Exception:
            pass
        bot.send_message(uid,
            f"✅ Переведено <b>{fmt(amount)}</b> + комиссия <b>{fmt(fee)}</b>",
            parse_mode="HTML",
            reply_markup=kb([("🏠 Меню", "home")]))
        return

    # Банк
    try:
        amount = int(text.replace(" ", "").replace(",", ""))
    except ValueError:
        bot.send_message(uid, "❌ Введи число")
        return
    if amount <= 0:
        bot.send_message(uid, "❌ Сумма > 0")
        return

    if action == "bank_deposit":
        if u["balance"] < amount:
            bot.send_message(uid, "❌ Недостаточно на кошельке")
            return
        with db() as c:
            c.execute("UPDATE users SET balance=balance-%s, bank=bank+%s, last_interest_calc=%s WHERE id=%s",
                      (amount, amount, now(), uid))
        bot.send_message(uid, f"✅ Внесено <b>{fmt(amount)} {CURRENCY}</b> на депозит",
                         parse_mode="HTML", reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))

    elif action == "bank_withdraw":
        u = get_user(uid)
        if u["bank"] < amount:
            bot.send_message(uid, "❌ Недостаточно в банке")
            return
        with db() as c:
            c.execute("UPDATE users SET balance=balance+%s, bank=bank-%s WHERE id=%s",
                      (amount, amount, uid))
        bot.send_message(uid, f"✅ Снято <b>{fmt(amount)} {CURRENCY}</b>",
                         parse_mode="HTML", reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))

    elif action == "bank_loan":
        if amount > LOAN_MAX:
            bot.send_message(uid, f"❌ Максимум кредита: {fmt(LOAN_MAX)}")
            return
        with db() as c:
            c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
            existing = c.fetchone()
        if existing and existing["amount"] > 0:
            bot.send_message(uid, "❌ У тебя уже есть непогашенный кредит")
            return
        repay  = int(amount * (1 + LOAN_RATE))
        due    = now() + 7 * 86400
        add_balance(uid, amount)
        with db() as c:
            c.execute("""INSERT INTO loans (user_id, amount, due_at, taken_at) VALUES (%s,%s,%s,%s)
                         ON CONFLICT (user_id) DO UPDATE SET amount=%s, due_at=%s, taken_at=%s""",
                      (uid, repay, due, now(), repay, due, now()))
        bot.send_message(uid,
            f"💳 Кредит <b>{fmt(amount)}</b> выдан!\n"
            f"Вернуть: <b>{fmt(repay)} {CURRENCY}</b> до {datetime.fromtimestamp(due).strftime('%d.%m')}",
            parse_mode="HTML", reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))

    elif action == "bank_repay":
        with db() as c:
            c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
            loan = c.fetchone()
        if not loan or not loan["amount"]:
            bot.send_message(uid, "❌ Нет активного кредита")
            return
        debt = loan["amount"]
        pay  = min(amount, debt)
        if u["balance"] < pay:
            bot.send_message(uid, f"❌ Недостаточно средств")
            return
        add_balance(uid, -pay)
        new_debt = debt - pay
        with db() as c:
            if new_debt <= 0:
                c.execute("DELETE FROM loans WHERE user_id=%s", (uid,))
                bot.send_message(uid, "✅ Кредит полностью погашен!", parse_mode="HTML",
                                 reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))
            else:
                c.execute("UPDATE loans SET amount=%s WHERE user_id=%s", (new_debt, uid))
                bot.send_message(uid,
                    f"✅ Погашено <b>{fmt(pay)}</b>. Остаток: <b>{fmt(new_debt)} {CURRENCY}</b>",
                    parse_mode="HTML", reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))


# ══════════════════════════════════════════════
# 14. БИРЖА
# ══════════════════════════════════════════════

def get_stock() -> dict:
    with db() as c:
        c.execute("SELECT * FROM stocks WHERE ticker=%s", (TICKER,))
        row = c.fetchone()
    return dict(row) if row else {"price": STOCK_PRICE_START, "prev_price": STOCK_PRICE_START}

def stock_trend(n=10) -> list:
    with db() as c:
        c.execute("SELECT price, ts FROM stock_history WHERE ticker=%s ORDER BY ts DESC LIMIT %s",
                  (TICKER, n))
        rows = c.fetchall()
    return list(reversed(rows))


@bot.callback_query_handler(func=lambda c: c.data == "menu_stock")
def cb_stock(call):
    uid = call.from_user.id
    st  = get_stock()
    price = st["price"]
    prev  = st["prev_price"]
    chg   = (price - prev) / prev * 100
    arrow = "📈" if chg >= 0 else "📉"

    with db() as c:
        c.execute("SELECT shares, avg_buy FROM portfolios WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        port = c.fetchone()

    port_str = "нет"
    if port and port["shares"]:
        pnl = (price - port["avg_buy"]) * port["shares"]
        pnl_str = f"+{fmt(pnl)}" if pnl >= 0 else fmt(pnl)
        port_str = f"{port['shares']} шт. (P&L: {pnl_str})"

    text = (
        f"<b>📈 Биржа — {TICKER}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Цена: <b>{fmt(price)} {CURRENCY}</b>  {arrow} {chg:+.1f}%\n"
        f"Портфель: <b>{port_str}</b>"
    )
    buttons = kb(
        [("🛒 Купить", "stock_buy"), ("💰 Продать", "stock_sell")],
        [("📊 История", "stock_history"), ("🏠 Меню", "home")]
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data in ["stock_buy", "stock_sell"])
def cb_stock_action(call):
    uid = call.from_user.id
    _waiting[uid] = call.data
    st  = get_stock()
    action_str = "купить" if call.data == "stock_buy" else "продать"
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        f"Цена: <b>{fmt(st['price'])} {CURRENCY}</b>\nСколько акций {action_str}?",
        parse_mode="HTML",
        reply_markup=kb([("❌ Отмена", "cancel_input")]))


@bot.message_handler(func=lambda m: m.from_user and _waiting.get(m.from_user.id) in ["stock_buy","stock_sell"] and m.chat.type == "private")
def handle_stock_input(msg):
    uid    = msg.from_user.id
    action = _waiting.pop(uid, None)
    try:
        qty = int(msg.text.strip())
    except ValueError:
        bot.send_message(uid, "❌ Введи число акций")
        return
    if qty <= 0:
        bot.send_message(uid, "❌ Количество > 0")
        return

    st    = get_stock()
    price = st["price"]
    u     = get_user(uid)

    if action == "stock_buy":
        total = price * qty
        fee   = int(total * 0.02)
        cost  = total + fee
        if u["balance"] < cost:
            bot.send_message(uid, f"❌ Нужно {fmt(cost)} (включая комиссию {fmt(fee)})")
            return
        add_balance(uid, -cost)
        with db() as c:
            c.execute("""INSERT INTO portfolios (user_id, ticker, shares, avg_buy)
                         VALUES (%s, %s, %s, %s)
                         ON CONFLICT (user_id, ticker) DO UPDATE SET
                             avg_buy = (portfolios.avg_buy * portfolios.shares + %s * %s) / (portfolios.shares + %s),
                             shares  = portfolios.shares + %s""",
                      (uid, TICKER, qty, price, price, qty, qty, qty))
        _market_impact(qty, "buy")
        bot.send_message(uid,
            f"✅ Куплено <b>{qty} акций {TICKER}</b>\n"
            f"По {fmt(price)} + комиссия {fmt(fee)}\nИтого: {fmt(cost)} {CURRENCY}",
            parse_mode="HTML",
            reply_markup=kb([("📈 Биржа","menu_stock"),("🏠 Меню","home")]))

    elif action == "stock_sell":
        with db() as c:
            c.execute("SELECT shares, avg_buy FROM portfolios WHERE user_id=%s AND ticker=%s", (uid, TICKER))
            port = c.fetchone()
        if not port or port["shares"] < qty:
            bot.send_message(uid, "❌ Недостаточно акций")
            return
        total = price * qty
        fee   = int(total * 0.02)
        gain  = total - fee
        pnl   = (price - port["avg_buy"]) * qty
        add_balance(uid, gain)
        with db() as c:
            new_shares = port["shares"] - qty
            if new_shares == 0:
                c.execute("DELETE FROM portfolios WHERE user_id=%s AND ticker=%s", (uid, TICKER))
            else:
                c.execute("UPDATE portfolios SET shares=%s WHERE user_id=%s AND ticker=%s",
                          (new_shares, uid, TICKER))
        _market_impact(qty, "sell")
        pnl_str = f"+{fmt(pnl)}" if pnl >= 0 else fmt(pnl)
        bot.send_message(uid,
            f"✅ Продано <b>{qty} акций {TICKER}</b>\n"
            f"Получено: <b>{fmt(gain)} {CURRENCY}</b>\nP&L: <b>{pnl_str}</b>",
            parse_mode="HTML",
            reply_markup=kb([("📈 Биржа","menu_stock"),("🏠 Меню","home")]))


def _market_impact(qty: int, direction: str):
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=%s", (TICKER,))
        row = c.fetchone()
        if not row:
            return
        price  = row["price"]
        impact = int(price * 0.001 * qty)
        new_price = max(100, price + (impact if direction == "buy" else -impact))
        c.execute("UPDATE stocks SET prev_price=price, price=%s WHERE ticker=%s", (new_price, TICKER))
        c.execute("INSERT INTO stock_history (ticker, price, ts) VALUES (%s, %s, %s)",
                  (TICKER, new_price, now()))


@bot.callback_query_handler(func=lambda c: c.data == "stock_history")
def cb_stock_history(call):
    uid     = call.from_user.id
    history = stock_trend(n=10)
    if not history:
        bot.answer_callback_query(call.id, "История пуста")
        return

    lines = []
    for i, row in enumerate(history):
        dt = datetime.fromtimestamp(row["ts"]).strftime("%d.%m %H:%M")
        p  = row["price"]
        if i > 0:
            prev = history[i-1]["price"]
            chg  = (p - prev) / prev * 100
            icon = "🟢" if p >= prev else "🔴"
            lines.append(f"{icon} {dt}  <b>{fmt(p)}</b>  {chg:+.1f}%")
        else:
            lines.append(f"⬜ {dt}  <b>{fmt(p)}</b>")

    text = f"<b>📊 История {TICKER}</b>\n\n" + "\n".join(lines)
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=kb([("📈 Биржа","menu_stock"),("🏠 Меню","home")]),
                              parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, parse_mode="HTML",
                         reply_markup=kb([("📈 Биржа","menu_stock"),("🏠 Меню","home")]))
    bot.answer_callback_query(call.id)


def _stock_scheduler():
    print(f"[stocks] планировщик запущен ({STOCK_UPDATE//60}мин)")
    while True:
        time.sleep(STOCK_UPDATE)
        try:
            with db() as c:
                c.execute("SELECT price FROM stocks WHERE ticker=%s", (TICKER,))
                row = c.fetchone()
                if not row:
                    continue
                old   = row["price"]
                drift = (STOCK_PRICE_START - old) * 0.01
                vol   = old * STOCK_VOLATILITY
                new   = max(100, int(old + drift + random.gauss(0, vol)))
                c.execute("UPDATE stocks SET prev_price=price, price=%s, updated_at=%s WHERE ticker=%s",
                          (new, now(), TICKER))
                c.execute("INSERT INTO stock_history (ticker, price, ts) VALUES (%s, %s, %s)",
                          (TICKER, new, now()))
            chg = (new - old) / old * 100
            if abs(chg) >= 2:
                arrow = "📈" if new > old else "📉"
                send_alert(f"{arrow} {TICKER}: {fmt(old)} → <b>{fmt(new)}</b> ({chg:+.1f}%)")
        except Exception as e:
            print(f"[stocks] ошибка: {e}")


# ══════════════════════════════════════════════
# 15. ИГРЫ
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_games")
def cb_games_menu(call):
    uid  = call.from_user.id
    text = "<b>🎰 Игры</b>\n━━━━━━━━━━━━━━━━━━\nВыбери игру:"
    buttons = kb(
        [("🎲 Кубик",   "game_dice"),  ("🎰 Слоты",  "game_slots")],
        [("🎡 Рулетка", "game_roul"),  ("💣 Мины",   "game_mines")],
        [("🏆 Лотерея", "game_lotto"), ("⚡ Краш",   "game_crash")],
        [("🏠 Меню", "home")]
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ── Кубик ──────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data == "game_dice")
def cb_dice_menu(call):
    _waiting[call.from_user.id] = "game_dice"
    bot.answer_callback_query(call.id)
    bot.send_message(call.from_user.id, "<b>🎲 Кубик</b>\nВведи ставку:",
                     parse_mode="HTML", reply_markup=kb([("❌ Отмена", "cancel_input")]))


@bot.message_handler(func=lambda m: m.from_user and _waiting.get(m.from_user.id) == "game_dice" and m.chat.type == "private")
def handle_dice_game(msg):
    uid = msg.from_user.id
    _waiting.pop(uid, None)
    u = get_user(uid)
    try:
        bet = int(msg.text.strip())
    except ValueError:
        bot.send_message(uid, "❌ Введи число")
        return
    if bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка (баланс: {fmt(u['balance'])})")
        return

    sent = bot.send_dice(uid, emoji="🎲")
    time.sleep(3)
    val = sent.dice.value
    if val >= 4:
        win = int(bet * 1.5)
        add_balance(uid, win - bet)
        result = f"🎲 Выпало <b>{val}</b> — победа! +{fmt(win-bet)} {CURRENCY}"
    else:
        add_balance(uid, -bet)
        result = f"🎲 Выпало <b>{val}</b> — проигрыш. -{fmt(bet)} {CURRENCY}"

    bot.send_message(uid, result, parse_mode="HTML",
                     reply_markup=kb([("🎲 Ещё","game_dice"),("🎮 Игры","menu_games"),("🏠 Меню","home")]))


# ── Слоты ──────────────────────────────────

SLOT_SYMBOLS = ["🍒","🍋","🍊","🍇","⭐","💎"]
SLOT_PAYOUTS = {
    "💎💎💎": 10, "⭐⭐⭐": 7, "🍇🍇🍇": 5,
    "🍊🍊🍊": 4,  "🍋🍋🍋": 3, "🍒🍒🍒": 2,
}

@bot.callback_query_handler(func=lambda c: c.data == "game_slots")
def cb_slots_menu(call):
    _waiting[call.from_user.id] = "game_slots"
    bot.answer_callback_query(call.id)
    bot.send_message(call.from_user.id, "<b>🎰 Слоты</b>\nВведи ставку:",
                     parse_mode="HTML", reply_markup=kb([("❌ Отмена", "cancel_input")]))


@bot.message_handler(func=lambda m: m.from_user and _waiting.get(m.from_user.id) == "game_slots" and m.chat.type == "private")
def handle_slots_game(msg):
    uid = msg.from_user.id
    _waiting.pop(uid, None)
    u = get_user(uid)
    try:
        bet = int(msg.text.strip())
    except ValueError:
        bot.send_message(uid, "❌ Введи число")
        return
    if bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка (баланс: {fmt(u['balance'])})")
        return

    reels = [random.choice(SLOT_SYMBOLS) for _ in range(3)]
    combo = "".join(reels)
    mult  = SLOT_PAYOUTS.get(combo, 0)

    if mult:
        win = bet * mult
        add_balance(uid, win - bet)
        result = f"🎰 {combo}\n🎉 Джекпот x{mult}! +{fmt(win-bet)} {CURRENCY}"
    else:
        add_balance(uid, -bet)
        result = f"🎰 {combo}\n😔 Нет совпадений. -{fmt(bet)} {CURRENCY}"

    bot.send_message(uid, result, parse_mode="HTML",
                     reply_markup=kb([("🎰 Ещё","game_slots"),("🎮 Игры","menu_games"),("🏠 Меню","home")]))


# ── Рулетка ────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data == "game_roul")
def cb_roulette_menu(call):
    uid = call.from_user.id
    _waiting[uid] = "game_roulette"
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        "<b>🎡 Рулетка</b>\n"
        "🔴 Красное (x2)   ⬛ Чёрное (x2)\n🟢 Зеро (x14)\n\n"
        "Введи: <code>цвет сумма</code>\nПример: <code>красное 1000</code>",
        parse_mode="HTML", reply_markup=kb([("❌ Отмена","cancel_input")]))


@bot.message_handler(func=lambda m: m.from_user and _waiting.get(m.from_user.id) == "game_roulette" and m.chat.type == "private")
def handle_roulette(msg):
    uid = msg.from_user.id
    _waiting.pop(uid, None)
    u = get_user(uid)
    parts = msg.text.strip().lower().split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: красное 1000")
        return
    color_map = {
        "красное":"red","красный":"red","r":"red",
        "чёрное":"black","черное":"black","b":"black",
        "зеро":"zero","0":"zero"
    }
    color = color_map.get(parts[0])
    if not color:
        bot.send_message(uid, "❌ Выбери: красное / чёрное / зеро")
        return
    try:
        bet = int(parts[1])
    except ValueError:
        bot.send_message(uid, "❌ Неверная ставка")
        return
    if bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка (баланс: {fmt(u['balance'])})")
        return

    num    = random.randint(0, 36)
    actual = "zero" if num == 0 else ("red" if num % 2 == 1 else "black")
    mults  = {"red": 2, "black": 2, "zero": 14}
    icons  = {"red": "🔴", "black": "⬛", "zero": "🟢"}

    if actual == color:
        win = bet * mults[color]
        add_balance(uid, win - bet)
        result = f"{icons[actual]} Выпало {num} — победа! +{fmt(win-bet)} {CURRENCY}"
    else:
        add_balance(uid, -bet)
        result = f"{icons[actual]} Выпало {num} — проигрыш. -{fmt(bet)} {CURRENCY}"

    bot.send_message(uid, result, parse_mode="HTML",
                     reply_markup=kb([("🎡 Рулетка","game_roul"),("🎮 Игры","menu_games"),("🏠 Меню","home")]))


# ── Лотерея ────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data == "game_lotto")
def cb_lottery(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
        lotto = c.fetchone()
        c.execute("SELECT tickets FROM lottery_tickets WHERE user_id=%s", (uid,))
        my = c.fetchone()

    jackpot    = lotto["jackpot"] if lotto else 0
    draw_at    = lotto["draw_at"] if lotto else 0
    my_tickets = my["tickets"] if my else 0
    draw_str   = datetime.fromtimestamp(draw_at).strftime("%d.%m %H:%M") if draw_at > now() else "скоро"

    text = (
        f"<b>🏆 Лотерея</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Джекпот: <b>{fmt(jackpot)} {CURRENCY}</b>\n"
        f"Розыгрыш: <b>{draw_str}</b>\n"
        f"Билет: <b>{fmt(TICKET_PRICE)}</b>\n"
        f"Ваши билеты: <b>{my_tickets}</b>"
    )
    buttons = kb(
        [("🎟 Купить 1", "lotto_buy_1"), ("🎟 Купить 5", "lotto_buy_5")],
        [("🏠 Меню", "home")]
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data in ["lotto_buy_1","lotto_buy_5"])
def cb_buy_tickets(call):
    uid  = call.from_user.id
    qty  = 1 if call.data == "lotto_buy_1" else 5
    cost = TICKET_PRICE * qty
    u    = get_user(uid)
    if u["balance"] < cost:
        bot.answer_callback_query(call.id, f"❌ Нужно {fmt(cost)}", show_alert=True)
        return
    add_balance(uid, -cost)
    with db() as c:
        c.execute("""INSERT INTO lottery_tickets (user_id, tickets) VALUES (%s, %s)
                     ON CONFLICT (user_id) DO UPDATE SET tickets = lottery_tickets.tickets + %s""",
                  (uid, qty, qty))
        c.execute("UPDATE lottery SET jackpot=jackpot+%s WHERE id=1", (cost,))
    bot.answer_callback_query(call.id, f"✅ Куплено {qty} билетов!")
    cb_lottery(call)


# ── Краш ───────────────────────────────────

_crash_games: dict[int, dict] = {}

@bot.callback_query_handler(func=lambda c: c.data == "game_crash")
def cb_crash_menu(call):
    uid = call.from_user.id
    _waiting[uid] = "game_crash"
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        "<b>⚡ Краш</b>\nМножитель растёт пока не крашнется.\nУспей забрать!\n\nВведи ставку:",
        parse_mode="HTML", reply_markup=kb([("❌ Отмена","cancel_input")]))


@bot.message_handler(func=lambda m: m.from_user and _waiting.get(m.from_user.id) == "game_crash" and m.chat.type == "private")
def handle_crash_bet(msg):
    uid = msg.from_user.id
    _waiting.pop(uid, None)
    u = get_user(uid)
    try:
        bet = int(msg.text.strip())
    except ValueError:
        bot.send_message(uid, "❌ Введи число")
        return
    if bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка (баланс: {fmt(u['balance'])})")
        return

    crash_at = round(max(1.01, random.expovariate(1) + 1.0), 2)
    add_balance(uid, -bet)
    _crash_games[uid] = {"bet": bet, "crash": crash_at}

    buttons = kb(
        [("💰 x1.5", f"crash_take_{uid}_1.5"), ("💰 x2.0", f"crash_take_{uid}_2.0")],
        [("💰 x3.0", f"crash_take_{uid}_3.0"), ("💰 x5.0", f"crash_take_{uid}_5.0")],
    )
    bot.send_message(uid,
        f"⚡ Ставка: <b>{fmt(bet)}</b>\nМножитель растёт... Забери вовремя!",
        parse_mode="HTML", reply_markup=buttons)


@bot.callback_query_handler(func=lambda c: c.data.startswith("crash_take_"))
def cb_crash_take(call):
    parts = call.data.split("_")
    owner = int(parts[2])
    mult  = float(parts[3])
    uid   = call.from_user.id

    if uid != owner:
        bot.answer_callback_query(call.id, "Это не твоя игра")
        return

    game = _crash_games.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "Игра уже завершена")
        return

    bet      = game["bet"]
    crash_at = game["crash"]

    if mult <= crash_at:
        win = int(bet * mult)
        add_balance(uid, win)
        result = f"✅ Забрал x{mult}! +<b>{fmt(win)} {CURRENCY}</b>"
    else:
        result = f"💥 Краш на x{crash_at}! Проигрыш -{fmt(bet)} {CURRENCY}"

    try:
        bot.edit_message_text(result, uid, call.message.message_id, parse_mode="HTML",
                              reply_markup=kb([("⚡ Ещё","game_crash"),("🎮 Игры","menu_games"),("🏠 Меню","home")]))
    except Exception:
        bot.send_message(uid, result, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ── Мины (новая игра) ──────────────────────

@bot.callback_query_handler(func=lambda c: c.data == "game_mines")
def cb_mines_menu(call):
    _waiting[call.from_user.id] = "game_mines"
    bot.answer_callback_query(call.id)
    bot.send_message(call.from_user.id,
        "<b>💣 Мины</b>\n5×5 поле, 5 мин.\nНажимай клетки — получай множитель.\nВведи ставку:",
        parse_mode="HTML", reply_markup=kb([("❌ Отмена","cancel_input")]))

_mine_games: dict[int, dict] = {}

@bot.message_handler(func=lambda m: m.from_user and _waiting.get(m.from_user.id) == "game_mines" and m.chat.type == "private")
def handle_mines_bet(msg):
    uid = msg.from_user.id
    _waiting.pop(uid, None)
    u = get_user(uid)
    try:
        bet = int(msg.text.strip())
    except ValueError:
        bot.send_message(uid, "❌ Введи число")
        return
    if bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка (баланс: {fmt(u['balance'])})")
        return

    mines = random.sample(range(25), 5)
    add_balance(uid, -bet)
    _mine_games[uid] = {"bet": bet, "mines": mines, "opened": [], "mult": 1.0}
    _send_mines_board(uid, msg.chat.id)


def _send_mines_board(uid: int, chat_id: int):
    game = _mine_games.get(uid)
    if not game:
        return

    rows = []
    for row in range(5):
        r = []
        for col in range(5):
            idx = row * 5 + col
            if idx in game["opened"]:
                r.append((f"💎", f"mines_noop"))
            else:
                r.append((f"⬜", f"mines_{uid}_{idx}"))
        rows.append(r)

    rows.append([
        (f"💰 Забрать x{game['mult']:.2f}", f"mines_cashout_{uid}"),
    ])

    bot.send_message(chat_id,
        f"<b>💣 Мины</b>\nСтавка: <b>{fmt(game['bet'])}</b>  |  Множитель: <b>x{game['mult']:.2f}</b>\n"
        f"Открыто: {len(game['opened'])} клеток",
        parse_mode="HTML", reply_markup=kb(*rows))


@bot.callback_query_handler(func=lambda c: re.match(r'^mines_\d+_\d+$', c.data))
def cb_mines_open(call):
    parts = call.data.split("_")
    owner = int(parts[1])
    idx   = int(parts[2])
    uid   = call.from_user.id

    if uid != owner:
        bot.answer_callback_query(call.id, "Это не твоя игра")
        return

    game = _mine_games.get(uid)
    if not game:
        bot.answer_callback_query(call.id, "Игра не найдена")
        return

    if idx in game["mines"]:
        _mine_games.pop(uid, None)
        bot.answer_callback_query(call.id, "💥 МИНА!", show_alert=True)
        try:
            bot.edit_message_text(
                f"💥 <b>Мина!</b> Проигрыш -{fmt(game['bet'])} {CURRENCY}",
                uid, call.message.message_id, parse_mode="HTML",
                reply_markup=kb([("💣 Ещё","game_mines"),("🎮 Игры","menu_games")]))
        except Exception:
            pass
    else:
        game["opened"].append(idx)
        safe = 25 - 5  # всего безопасных
        opened = len(game["opened"])
        game["mult"] = round(1.0 + opened * (5 / safe), 2)
        bot.answer_callback_query(call.id, f"💎 Безопасно! x{game['mult']:.2f}")
        try:
            bot.delete_message(uid, call.message.message_id)
        except Exception:
            pass
        _send_mines_board(uid, call.message.chat.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_cashout_"))
def cb_mines_cashout(call):
    owner = int(call.data[14:])
    uid   = call.from_user.id
    if uid != owner:
        bot.answer_callback_query(call.id, "Это не твоя игра")
        return
    game = _mine_games.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "Игра не найдена")
        return
    win = int(game["bet"] * game["mult"])
    add_balance(uid, win)
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text(
            f"✅ Забрал x{game['mult']:.2f}!\n+<b>{fmt(win)} {CURRENCY}</b>",
            uid, call.message.message_id, parse_mode="HTML",
            reply_markup=kb([("💣 Ещё","game_mines"),("🎮 Игры","menu_games"),("🏠 Меню","home")]))
    except Exception:
        bot.send_message(uid, f"✅ Выигрыш: {fmt(win)} {CURRENCY}", parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: c.data == "mines_noop")
def cb_mines_noop(call):
    bot.answer_callback_query(call.id)


# ══════════════════════════════════════════════
# 16. МАГАЗИН
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_shop")
def cb_shop(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("SELECT * FROM items WHERE active=1 ORDER BY price")
        items = c.fetchall()

    if not items:
        text = "<b>🛍️ Магазин</b>\n\nПусто."
        try:
            bot.edit_message_text(text, uid, call.message.message_id,
                                  reply_markup=kb([("🏠 Меню","home")]), parse_mode="HTML")
        except Exception:
            bot.send_message(uid, text, reply_markup=kb([("🏠 Меню","home")]), parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    text = "<b>🛍️ Магазин</b>\n━━━━━━━━━━━━━━━━━━\n"
    rows = []
    for item in items:
        avail = "∞" if item["supply"] == -1 else str(item["supply"] - item["sold"])
        text += f"{item['emoji']} <b>{item['name']}</b> — {fmt(item['price'])} {CURRENCY} [{avail}]\n"
        rows.append([(f"🛒 {item['emoji']} {item['name']}", f"buy_item_{item['id']}")])
    rows.append([("🏠 Меню","home")])

    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=kb(*rows), parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=kb(*rows), parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("buy_item_"))
def cb_buy_item(call):
    uid     = call.from_user.id
    item_id = int(call.data[9:])

    with db() as c:
        c.execute("SELECT * FROM items WHERE id=%s AND active=1", (item_id,))
        item = c.fetchone()

    if not item:
        bot.answer_callback_query(call.id, "❌ Товар недоступен", show_alert=True)
        return
    if item["supply"] != -1 and item["sold"] >= item["supply"]:
        bot.answer_callback_query(call.id, "❌ Товар распродан", show_alert=True)
        return

    u = get_user(uid)
    if u["balance"] < item["price"]:
        bot.answer_callback_query(call.id,
            f"❌ Нужно {fmt(item['price'])}, у тебя {fmt(u['balance'])}", show_alert=True)
        return

    add_balance(uid, -item["price"])
    with db() as c:
        c.execute("""INSERT INTO inventory (user_id, item_id, qty) VALUES (%s, %s, 1)
                     ON CONFLICT (user_id, item_id) DO UPDATE SET qty = inventory.qty + 1""",
                  (uid, item_id))
        if item["supply"] != -1:
            c.execute("UPDATE items SET sold=sold+1 WHERE id=%s", (item_id,))

    bot.answer_callback_query(call.id, f"✅ Куплено: {item['emoji']} {item['name']}")


# ══════════════════════════════════════════════
# 17. ТОП
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_top")
def cb_top(call):
    uid = call.from_user.id
    buttons = kb(
        [("💵 По балансу","top_balance"), ("⭐ По XP","top_xp")],
        [("📈 Акционеры","top_stocks"),   ("🏠 Меню","home")]
    )
    try:
        bot.edit_message_text("<b>🏆 Рейтинги</b>", uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, "<b>🏆 Рейтинги</b>", reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("top_"))
def cb_top_category(call):
    uid     = call.from_user.id
    cat     = call.data[4:]
    medals  = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

    with db() as c:
        if cat == "balance":
            c.execute("SELECT name, balance FROM users ORDER BY balance DESC LIMIT 10")
            rows = c.fetchall()
            title  = "💵 Топ по балансу"
            val_fn = lambda r: fmt(r["balance"])
        elif cat == "xp":
            c.execute("SELECT name, xp FROM users ORDER BY xp DESC LIMIT 10")
            rows = c.fetchall()
            title  = "⭐ Топ по XP"
            val_fn = lambda r: fmt(r["xp"])
        elif cat == "stocks":
            c.execute("""SELECT u.name, SUM(p.shares) AS total
                         FROM portfolios p JOIN users u ON u.id=p.user_id
                         WHERE p.ticker=%s GROUP BY u.name ORDER BY total DESC LIMIT 10""", (TICKER,))
            rows = c.fetchall()
            title  = "📈 Топ акционеров"
            val_fn = lambda r: f"{r['total']} акций"
        else:
            bot.answer_callback_query(call.id)
            return

    lines = [f"{medals[i]} {r['name'] or 'Игрок'} — <b>{val_fn(r)}</b>"
             for i, r in enumerate(rows)]
    text = f"<b>{title}</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines) if lines else f"<b>{title}</b>\n\nПусто"

    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=kb([("🏆 Топ","menu_top"),("🏠 Меню","home")]),
                              parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, parse_mode="HTML",
                         reply_markup=kb([("🏆 Топ","menu_top"),("🏠 Меню","home")]))
    bot.answer_callback_query(call.id)


# ══════════════════════════════════════════════
# 18. КЛАН
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_clan")
def cb_clan(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("""SELECT cl.*, cm.role FROM clan_members cm
                     JOIN clans cl ON cl.id=cm.clan_id WHERE cm.user_id=%s""", (uid,))
        clan = c.fetchone()

    if not clan:
        text = (
            "<b>🏰 Клан</b>\n━━━━━━━━━━━━━━━━━━\n"
            "Ты не состоишь в клане.\n\nСоздай или вступи в клан!"
        )
        buttons = kb([("⚔️ Создать клан","clan_create")], [("🏠 Меню","home")])
    else:
        with db() as c:
            c.execute("SELECT COUNT(*) AS cnt FROM clan_members WHERE clan_id=%s", (clan["id"],))
            members = c.fetchone()["cnt"]

        text = (
            f"<b>🏰 {clan['name']}</b> [{clan['tag']}]\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Уровень: <b>{clan['level']}</b>\n"
            f"Казна: <b>{fmt(clan['balance'])} {CURRENCY}</b>\n"
            f"Участников: <b>{members}</b>\n"
            f"Ваша роль: <b>{clan['role']}</b>"
        )
        rows = [[("👥 Участники", f"clan_members_{clan['id']}"),
                 ("💰 Взнос",     f"clan_donate_{clan['id']}")]]
        if clan["role"] in ("owner","admin"):
            rows.append([("⚙️ Управление", f"clan_manage_{clan['id']}")])
        rows.append([("🚪 Покинуть", f"clan_leave_{clan['id']}"), ("🏠 Меню","home")])
        buttons = kb(*rows)

    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data == "clan_create")
def cb_clan_create(call):
    _waiting[call.from_user.id] = "clan_create"
    bot.answer_callback_query(call.id)
    bot.send_message(call.from_user.id,
        f"Введи название и тег клана через пробел:\nПример: <code>Легион LEG</code>\nСтоимость: <b>5 000 {CURRENCY}</b>",
        parse_mode="HTML", reply_markup=kb([("❌ Отмена","cancel_input")]))


@bot.message_handler(func=lambda m: m.from_user and _waiting.get(m.from_user.id) == "clan_create" and m.chat.type == "private")
def handle_clan_create(msg):
    uid = msg.from_user.id
    _waiting.pop(uid, None)
    parts = msg.text.strip().split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: Название ТЕГ")
        return
    tag  = parts[-1].upper()[:5]
    name = " ".join(parts[:-1])[:30]
    cost = 5_000
    u = get_user(uid)
    if u["balance"] < cost:
        bot.send_message(uid, f"❌ Нужно {fmt(cost)} {CURRENCY}")
        return
    try:
        add_balance(uid, -cost)
        with db() as c:
            c.execute("INSERT INTO clans (name,tag,owner,created_at) VALUES (%s,%s,%s,%s)",
                      (name, tag, uid, now()))
            c.execute("SELECT id FROM clans WHERE tag=%s", (tag,))
            clan_id = c.fetchone()["id"]
            c.execute("INSERT INTO clan_members (user_id,clan_id,role,joined_at) VALUES (%s,%s,%s,%s)",
                      (uid, clan_id, "owner", now()))
        bot.send_message(uid, f"✅ Клан <b>{name}</b> [{tag}] создан!", parse_mode="HTML",
                         reply_markup=kb([("🏰 Клан","menu_clan"),("🏠 Меню","home")]))
    except Exception:
        bot.send_message(uid, "❌ Название или тег уже заняты")
        add_balance(uid, cost)


@bot.callback_query_handler(func=lambda c: c.data.startswith("clan_leave_"))
def cb_clan_leave(call):
    uid     = call.from_user.id
    clan_id = int(call.data[11:])
    with db() as c:
        c.execute("SELECT role FROM clan_members WHERE user_id=%s AND clan_id=%s", (uid, clan_id))
        row = c.fetchone()
    if row and row["role"] == "owner":
        bot.answer_callback_query(call.id, "❌ Владелец не может покинуть клан. Передай права.", show_alert=True)
        return
    with db() as c:
        c.execute("DELETE FROM clan_members WHERE user_id=%s", (uid,))
    bot.answer_callback_query(call.id, "✅ Вы покинули клан")
    cb_clan(call)


# ══════════════════════════════════════════════
# 19. ДОНАТ (Telegram Stars)
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().strip() == "донат")
def cmd_donate(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")
    with db() as c:
        c.execute("SELECT key, stars, amount, label FROM donate_packages ORDER BY stars")
        pkgs = c.fetchall()

    text = "<b>💎 Донат</b>\n━━━━━━━━━━━━━━━━━━\nПополни баланс через Telegram Stars:"
    rows = [[(p["label"], f"donate_{p['key']}")] for p in pkgs]
    rows.append([("🏠 Меню","home")])
    bot.send_message(uid, text, reply_markup=kb(*rows), parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: c.data.startswith("donate_"))
def cb_donate(call):
    uid = call.from_user.id
    key = call.data[7:]
    with db() as c:
        c.execute("SELECT * FROM donate_packages WHERE key=%s", (key,))
        pkg = c.fetchone()
    if not pkg:
        bot.answer_callback_query(call.id, "❌ Пакет не найден")
        return
    bot.answer_callback_query(call.id)
    bot.send_invoice(uid,
        title=f"Пополнение {pkg['label']}",
        description=f"+{fmt(pkg['amount'])} {CURRENCY}",
        payload=f"donate_{key}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(f"{pkg['label']}", pkg["stars"])])


@bot.pre_checkout_query_handler(func=lambda q: True)
def pre_checkout(query):
    bot.answer_pre_checkout_query(query.id, ok=True)


@bot.message_handler(content_types=["successful_payment"])
def successful_payment(msg):
    uid     = msg.from_user.id
    payload = msg.successful_payment.invoice_payload
    key     = payload[7:]
    with db() as c:
        c.execute("SELECT amount FROM donate_packages WHERE key=%s", (key,))
        pkg = c.fetchone()
    if pkg:
        add_balance(uid, pkg["amount"])
        bot.send_message(uid,
            f"✅ <b>Пополнение прошло!</b>\n+{fmt(pkg['amount'])} {CURRENCY}",
            parse_mode="HTML", reply_markup=kb([("🏠 Меню","home")]))
        send_alert(f"💎 Донат: uid={uid} +{fmt(pkg['amount'])}")


# ══════════════════════════════════════════════
# 20. ПРОМОКОД
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("промо "))
def cmd_promo(msg):
    uid  = msg.from_user.id
    code = msg.text.strip().split(None, 1)[1].strip().upper()
    ensure_user(uid, msg.from_user.first_name or "")

    with db() as c:
        c.execute("SELECT * FROM promo_codes WHERE code=%s AND active=1", (code,))
        promo = c.fetchone()

    if not promo:
        bot.send_message(uid, "❌ Промокод не найден")
        return
    if promo["expires"] and promo["expires"] < now():
        bot.send_message(uid, "❌ Промокод истёк")
        return
    if promo["max_uses"] > 0 and promo["uses"] >= promo["max_uses"]:
        bot.send_message(uid, "❌ Промокод исчерпан")
        return

    with db() as c:
        try:
            c.execute("INSERT INTO promo_uses (user_id,code,ts) VALUES (%s,%s,%s)",
                      (uid, code, now()))
        except Exception:
            bot.send_message(uid, "❌ Этот промокод ты уже использовал")
            return
        c.execute("UPDATE promo_codes SET uses=uses+1 WHERE code=%s", (code,))

    add_balance(uid, promo["reward"])
    bot.send_message(uid, f"✅ Промокод активирован!\n+<b>{fmt(promo['reward'])} {CURRENCY}</b>",
                     parse_mode="HTML", reply_markup=kb([("🏠 Меню","home")]))


# ══════════════════════════════════════════════
# 21. РЕФЕРАЛЬНАЯ ССЫЛКА
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().strip() in ["реферал","рефка","ref"])
def cmd_referral(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")

    with db() as c:
        c.execute("SELECT ref_code FROM users WHERE id=%s", (uid,))
        row = c.fetchone()

    ref_code = row["ref_code"] if row and row["ref_code"] else None
    if not ref_code:
        ref_code = f"R{uid}"
        with db() as c:
            c.execute("UPDATE users SET ref_code=%s WHERE id=%s", (ref_code, uid))

    with db() as c:
        c.execute("SELECT COUNT(*) AS cnt FROM users WHERE ref_by=%s", (uid,))
        referrals = c.fetchone()["cnt"]

    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={ref_code}"

    bot.send_message(uid,
        f"<b>🔗 Реферальная программа</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Ты пригласил: <b>{referrals}</b> игроков\n"
        f"Бонус за приглашение: <b>2 000 {CURRENCY}</b> тебе и другу\n\n"
        f"Твоя ссылка:\n{link}",
        parse_mode="HTML", reply_markup=kb([("🏠 Меню","home")]))


# ══════════════════════════════════════════════
# 22. ПЕРЕВОД (текстовая команда)
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().split()[0] in ["перевод","дать","pay"] if m.text else False)
def cmd_transfer(msg):
    if is_group(msg):
        return  # в группе обрабатывается отдельным хендлером
    uid = msg.from_user.id
    _waiting[uid] = "action_transfer"
    bot.send_message(uid,
        f"Введи: @username сумма\nПример: @ivan 5000\n\nКомиссия: {TRANSFER_FEE*100:.0f}%",
        reply_markup=kb([("❌ Отмена","cancel_input")]))


# ══════════════════════════════════════════════
# 23. ЛОТЕРЕЯ — ПЛАНИРОВЩИК
# ══════════════════════════════════════════════

def _lottery_scheduler():
    print("[lottery] планировщик запущен")
    while True:
        time.sleep(60)
        try:
            with db() as c:
                c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
                lotto = c.fetchone()
            if not lotto or lotto["draw_at"] == 0:
                # Устанавливаем следующий розыгрыш через 24ч
                with db() as c:
                    c.execute("UPDATE lottery SET draw_at=%s WHERE id=1", (now() + 86400,))
                continue
            if lotto["draw_at"] > now():
                continue
            if lotto["jackpot"] == 0:
                with db() as c:
                    c.execute("UPDATE lottery SET draw_at=%s WHERE id=1", (now() + 86400,))
                continue

            with db() as c:
                c.execute("SELECT user_id, tickets FROM lottery_tickets WHERE tickets > 0")
                participants = c.fetchall()

            if not participants:
                with db() as c:
                    c.execute("UPDATE lottery SET draw_at=%s WHERE id=1", (now() + 86400,))
                continue

            pool_list = []
            for p in participants:
                pool_list.extend([p["user_id"]] * p["tickets"])

            winner = random.choice(pool_list)
            jackpot = lotto["jackpot"]
            add_balance(winner, jackpot)

            with db() as c:
                c.execute("DELETE FROM lottery_tickets")
                c.execute("UPDATE lottery SET jackpot=0, draw_at=%s WHERE id=1", (now() + 86400,))

            send_alert(f"🏆 Лотерея! Победитель uid={winner} +{fmt(jackpot)} {CURRENCY}")
            try:
                bot.send_message(winner,
                    f"🎉 Вы выиграли лотерею!\n+<b>{fmt(jackpot)} {CURRENCY}</b>",
                    parse_mode="HTML")
            except Exception:
                pass
        except Exception as e:
            print(f"[lottery] ошибка: {e}")


# ══════════════════════════════════════════════
# 24. АДМИН-КОМАНДЫ
# ══════════════════════════════════════════════

def admin_only(fn):
    def wrapper(msg):
        if not is_admin(msg.from_user.id):
            return
        fn(msg)
    return wrapper


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("выдать ") and is_admin(m.from_user.id))
@admin_only
def cmd_give(msg):
    parts = msg.text.split()
    try:
        uid_ = int(parts[1])
        amt  = int(parts[2])
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: выдать <uid> <сумма>")
        return
    add_balance(uid_, amt)
    bot.reply_to(msg, f"✅ Выдано {fmt(amt)} {CURRENCY} игроку {uid_}")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("забрать ") and is_admin(m.from_user.id))
@admin_only
def cmd_take(msg):
    parts = msg.text.split()
    try:
        uid_ = int(parts[1])
        amt  = int(parts[2])
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: забрать <uid> <сумма>")
        return
    add_balance(uid_, -amt)
    bot.reply_to(msg, f"✅ Забрано {fmt(amt)} {CURRENCY} у игрока {uid_}")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("бан ") and is_admin(m.from_user.id))
@admin_only
def cmd_ban(msg):
    parts = msg.text.split(None, 2)
    try:
        uid_ = int(parts[1])
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: бан <uid> [причина]")
        return
    reason = parts[2] if len(parts) > 2 else "Нарушение правил"
    with db() as c:
        c.execute("INSERT INTO bans (user_id,reason,by,ts) VALUES (%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET reason=%s",
                  (uid_, reason, msg.from_user.id, now(), reason))
    bot.reply_to(msg, f"🚫 Игрок {uid_} заблокирован.")
    try:
        bot.send_message(uid_, f"🚫 Вы заблокированы.\nПричина: {reason}")
    except Exception:
        pass


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("разбан ") and is_admin(m.from_user.id))
@admin_only
def cmd_unban(msg):
    parts = msg.text.split()
    try:
        uid_ = int(parts[1])
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: разбан <uid>")
        return
    with db() as c:
        c.execute("DELETE FROM bans WHERE user_id=%s", (uid_,))
    bot.reply_to(msg, f"✅ Игрок {uid_} разблокирован.")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("промо создать ") and is_admin(m.from_user.id))
@admin_only
def cmd_create_promo(msg):
    parts = msg.text.split()
    try:
        code = parts[2].upper()
        amt  = int(parts[3])
        uses = int(parts[4]) if len(parts) > 4 else 1
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: промо создать КОД СУММА [uses]")
        return
    with db() as c:
        try:
            c.execute("INSERT INTO promo_codes (code,reward,max_uses) VALUES (%s,%s,%s)",
                      (code, amt, uses))
        except Exception:
            bot.reply_to(msg, "❌ Код уже существует")
            return
    bot.reply_to(msg, f"✅ Промокод <code>{code}</code> создан. +{fmt(amt)}, uses={uses}", parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("добавить товар ") and is_admin(m.from_user.id))
@admin_only
def cmd_add_item(msg):
    parts = msg.text.split(None, 5)
    try:
        emoji  = parts[2]
        name   = parts[3]
        price  = int(parts[4])
        supply = int(parts[5]) if len(parts) > 5 else -1
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: добавить товар EMOJI ИМЯ ЦЕНА [supply]")
        return
    with db() as c:
        c.execute("INSERT INTO items (name,emoji,price,supply) VALUES (%s,%s,%s,%s)",
                  (name, emoji, price, supply))
    bot.reply_to(msg, f"✅ Товар {emoji} {name} добавлен ({fmt(price)} {CURRENCY})")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("стат") and is_admin(m.from_user.id))
@admin_only
def cmd_stats(msg):
    with db() as c:
        c.execute("SELECT COUNT(*) AS cnt FROM users")
        users = c.fetchone()["cnt"]
        c.execute("SELECT SUM(balance) AS s FROM users")
        total_bal = c.fetchone()["s"] or 0
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        jackpot = (c.fetchone() or {}).get("jackpot", 0)

    bot.reply_to(msg,
        f"<b>📊 Статистика</b>\n"
        f"Игроков: <b>{users}</b>\n"
        f"Деньги в обороте: <b>{fmt(total_bal)} {CURRENCY}</b>\n"
        f"Джекпот: <b>{fmt(jackpot)} {CURRENCY}</b>",
        parse_mode="HTML")


# ══════════════════════════════════════════════
# 25. ГРУППОВЫЕ КОМАНДЫ
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["баланс","б","/б","/баланс"])
def group_balance(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)
    u = get_user(uid)
    if not u:
        return
    lvl = user_level(u["xp"])
    bot.reply_to(msg,
        f"👤 {mention(uid, name)}\n"
        f"💵 <b>{fmt(u['balance'])}</b>  🏦 {fmt(u['bank'])}\n"
        f"⚡ Уровень <b>{lvl}</b>",
        parse_mode="HTML")


@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(топ|/топ|top|/top)(\s+\S+)?$', m.text.lower().strip()))
def group_top(msg):
    parts = msg.text.strip().lower().split()
    cat   = parts[1] if len(parts) > 1 else "баланс"
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

    with db() as c:
        if cat in ("xp","опыт","уровень"):
            c.execute("SELECT name, xp FROM users ORDER BY xp DESC LIMIT 10")
            rows  = c.fetchall()
            title = "⭐ Топ по XP"
            val   = lambda r: f"{fmt(r['xp'])} XP"
        elif cat in ("акции","stocks"):
            c.execute("""SELECT u.name, SUM(p.shares) AS s
                         FROM portfolios p JOIN users u ON u.id=p.user_id
                         WHERE p.ticker=%s GROUP BY u.name ORDER BY s DESC LIMIT 10""", (TICKER,))
            rows  = c.fetchall()
            title = "📈 Топ акционеров"
            val   = lambda r: f"{r['s']} акций"
        else:
            c.execute("SELECT name, balance FROM users ORDER BY balance DESC LIMIT 10")
            rows  = c.fetchall()
            title = "💵 Топ по балансу"
            val   = lambda r: fmt(r["balance"])

    if not rows:
        bot.reply_to(msg, "Список пуст.")
        return
    lines = [f"{medals[i]} <b>{r['name'] or 'Игрок'}</b> — {val(r)}" for i, r in enumerate(rows)]
    bot.send_message(msg.chat.id,
        f"<b>{title}</b>\n━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
        parse_mode="HTML")


@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["акции","биржа","акция"])
def group_stocks(msg):
    st    = get_stock()
    price = st["price"]
    prev  = st["prev_price"]
    chg   = (price - prev) / prev * 100
    arrow = "📈" if chg >= 0 else "📉"
    bot.send_message(msg.chat.id,
        f"<b>📈 Биржа — {TICKER}</b>\n"
        f"Цена: <b>{fmt(price)} {CURRENCY}</b>  {arrow} {chg:+.1f}%\n\n"
        f"Купить/продать: в <b>личку</b> → меню → 📈 Биржа",
        parse_mode="HTML")


@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(рул|рулетка)\s+\S+\s+\d+', m.text.lower().strip()))
def group_roulette(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)

    parts = msg.text.strip().lower().split()
    color_map = {
        "красное":"red","красный":"red","red":"red","r":"red",
        "чёрное":"black","черное":"black","black":"black","b":"black",
        "зеро":"zero","0":"zero","zero":"zero",
    }
    color = color_map.get(parts[1])
    if not color:
        bot.reply_to(msg, "❌ Цвет: красное / чёрное / зеро")
        return
    try:
        bet = int(parts[2])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: рул красное 1000")
        return
    if bet <= 0:
        bot.reply_to(msg, "❌ Ставка > 0")
        return

    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: <b>{fmt(u['balance'])}</b>", parse_mode="HTML")
        return

    num    = random.randint(0, 36)
    actual = "zero" if num == 0 else ("red" if num % 2 == 1 else "black")
    mults  = {"red": 2, "black": 2, "zero": 14}
    icons  = {"red": "🔴", "black": "⬛", "zero": "🟢"}

    if actual == color:
        win = bet * mults[color]
        add_balance(uid, win - bet)
        result = (f"✅ {mention(uid, name)} поставил на {icons[color]} и выиграл!\n"
                  f"🎲 Выпало <b>{num}</b> {icons[actual]}\n"
                  f"💰 +<b>{fmt(win - bet)} {CURRENCY}</b>")
    else:
        add_balance(uid, -bet)
        result = (f"😔 {mention(uid, name)} поставил на {icons[color]} и проиграл.\n"
                  f"🎲 Выпало <b>{num}</b> {icons[actual]}\n"
                  f"💸 -<b>{fmt(bet)} {CURRENCY}</b>")

    bot.send_message(msg.chat.id, result, parse_mode="HTML")


@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(кости|куб|кубик|dice)\s+\d+', m.text.lower().strip()))
def group_dice(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)
    try:
        bet = int(msg.text.strip().split()[1])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: кости 1000")
        return
    if bet <= 0:
        bot.reply_to(msg, "❌ Ставка > 0")
        return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: <b>{fmt(u['balance'])}</b>", parse_mode="HTML")
        return

    add_balance(uid, -bet)
    sent = bot.send_dice(msg.chat.id, emoji="🎲", reply_to_message_id=msg.message_id)
    time.sleep(3)
    val = sent.dice.value
    if val >= 4:
        win = int(bet * 2)
        add_balance(uid, win)
        bot.send_message(msg.chat.id,
            f"🎲 {mention(uid, name)} бросил <b>{val}</b> — победа!\n"
            f"💰 +<b>{fmt(win - bet)} {CURRENCY}</b>", parse_mode="HTML")
    else:
        bot.send_message(msg.chat.id,
            f"🎲 {mention(uid, name)} бросил <b>{val}</b> — мимо.\n"
            f"💸 -<b>{fmt(bet)} {CURRENCY}</b>", parse_mode="HTML")


@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(слот|слоты|slots?)\s+\d+', m.text.lower().strip()))
def group_slots(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)
    try:
        bet = int(msg.text.strip().split()[1])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: слот 1000")
        return
    if bet <= 0:
        bot.reply_to(msg, "❌ Ставка > 0")
        return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}")
        return

    reels = [random.choice(SLOT_SYMBOLS) for _ in range(3)]
    combo = "".join(reels)
    mult  = SLOT_PAYOUTS.get(combo, 0)
    if mult:
        win = bet * mult
        add_balance(uid, win - bet)
        bot.send_message(msg.chat.id,
            f"🎰 {mention(uid, name)}: {combo}\n🎉 Джекпот x{mult}! +<b>{fmt(win - bet)} {CURRENCY}</b>",
            parse_mode="HTML")
    else:
        add_balance(uid, -bet)
        bot.send_message(msg.chat.id,
            f"🎰 {mention(uid, name)}: {combo}\n😔 Нет совпадений. -<b>{fmt(bet)} {CURRENCY}</b>",
            parse_mode="HTML")


# ── Дуэль ──────────────────────────────────

_duels: dict[int, dict] = {}

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(дуэль|дуэл|duel)\s+\d+', m.text.lower().strip()))
def group_duel_create(msg):
    uid   = msg.from_user.id
    cname = get_display_name(msg)
    ensure_user(uid, cname)

    try:
        bet = int(msg.text.strip().split()[1])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: дуэль 1000")
        return
    if bet <= 0:
        bot.reply_to(msg, "❌ Ставка > 0")
        return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}")
        return
    if uid in _duels:
        bot.reply_to(msg, "❌ У тебя уже есть активная дуэль")
        return

    opp_uid, opp_name = None, "любой игрок"
    if msg.reply_to_message and msg.reply_to_message.from_user:
        ru = msg.reply_to_message.from_user
        if ru.id != uid and not ru.is_bot:
            opp_uid  = ru.id
            opp_name = ru.first_name or str(ru.id)
            ensure_user(opp_uid, opp_name)

    _duels[uid] = {
        "bet": bet, "opponent": opp_uid, "cname": cname, "oname": opp_name,
        "chat_id": msg.chat.id, "expires": now() + 60
    }

    target_str = mention(opp_uid, opp_name) if opp_uid else "<b>любой игрок</b>"
    kb_duel = InlineKeyboardMarkup()
    kb_duel.add(InlineKeyboardButton("⚔️ Принять дуэль", callback_data=f"duel_accept_{uid}"))

    sent = bot.send_message(msg.chat.id,
        f"⚔️ {mention(uid, cname)} вызывает {target_str} на дуэль!\n"
        f"Ставка: <b>{fmt(bet)} {CURRENCY}</b>\n⏱ 60 секунд на принятие",
        parse_mode="HTML", reply_markup=kb_duel)
    _duels[uid]["msg_id"] = sent.message_id

    def _cancel():
        time.sleep(62)
        if uid in _duels:
            _duels.pop(uid, None)
            try:
                bot.edit_message_text(
                    f"⚔️ Дуэль от {mention(uid, cname)} отменена — никто не принял.",
                    msg.chat.id, sent.message_id, parse_mode="HTML")
            except Exception:
                pass
    threading.Thread(target=_cancel, daemon=True).start()


@bot.callback_query_handler(func=lambda c: c.data.startswith("duel_accept_"))
def cb_duel_accept(call):
    challenger_uid = int(call.data[12:])
    opp_uid        = call.from_user.id
    opp_name       = call.from_user.first_name or str(opp_uid)

    duel = _duels.get(challenger_uid)
    if not duel:
        bot.answer_callback_query(call.id, "Дуэль уже завершена")
        return
    if duel["expires"] < now():
        _duels.pop(challenger_uid, None)
        bot.answer_callback_query(call.id, "Время вышло")
        return
    if challenger_uid == opp_uid:
        bot.answer_callback_query(call.id, "❌ Нельзя принять свою дуэль")
        return
    if duel["opponent"] and duel["opponent"] != opp_uid:
        bot.answer_callback_query(call.id, "❌ Эта дуэль не для тебя")
        return

    ensure_user(opp_uid, opp_name)
    bet     = duel["bet"]
    cname   = duel["cname"]
    chat_id = duel["chat_id"]

    ou = get_user(opp_uid)
    cu = get_user(challenger_uid)
    if ou["balance"] < bet:
        bot.answer_callback_query(call.id, f"❌ Нужно {fmt(bet)}", show_alert=True)
        return
    if cu["balance"] < bet:
        bot.answer_callback_query(call.id, "❌ У организатора недостаточно средств", show_alert=True)
        _duels.pop(challenger_uid, None)
        return

    _duels.pop(challenger_uid, None)
    bot.answer_callback_query(call.id)

    c_roll, o_roll = random.randint(1, 6), random.randint(1, 6)
    rerolls = 0
    while c_roll == o_roll and rerolls < 5:
        c_roll, o_roll = random.randint(1, 6), random.randint(1, 6)
        rerolls += 1

    if c_roll > o_roll:
        w_uid, w_name = challenger_uid, cname
        l_uid         = opp_uid
    else:
        w_uid, w_name = opp_uid, opp_name
        l_uid         = challenger_uid

    add_balance(l_uid, -bet)
    add_balance(w_uid,  bet)

    try:
        bot.edit_message_text(
            f"⚔️ <b>Дуэль!</b>\n"
            f"{mention(challenger_uid, cname)}: 🎲 <b>{c_roll}</b>\n"
            f"{mention(opp_uid, opp_name)}: 🎲 <b>{o_roll}</b>\n\n"
            f"🏆 Победил {mention(w_uid, w_name)}!\n💰 +<b>{fmt(bet)} {CURRENCY}</b>",
            chat_id, call.message.message_id, parse_mode="HTML")
    except Exception:
        bot.send_message(chat_id,
            f"🏆 Победил {mention(w_uid, w_name)}! +{fmt(bet)} {CURRENCY}", parse_mode="HTML")


# ── КНБ ────────────────────────────────────

_knb_games: dict[int, dict] = {}
KNB_BEATS = {"камень":"ножницы","ножницы":"бумага","бумага":"камень"}
KNB_EMOJI = {"камень":"🪨","ножницы":"✂️","бумага":"📄"}

@bot.message_handler(func=lambda m: is_group(m) and m.reply_to_message and
    m.text and re.match(r'^(кнб|рпс|knb)\s+\d+', m.text.lower().strip()))
def group_knb_create(msg):
    uid   = msg.from_user.id
    cname = get_display_name(msg)
    ensure_user(uid, cname)

    ru = msg.reply_to_message.from_user
    if not ru or ru.id == uid or ru.is_bot:
        bot.reply_to(msg, "❌ Сделай reply на сообщение соперника")
        return
    try:
        bet = int(msg.text.strip().split()[1])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: кнб 1000 (reply)")
        return
    if bet <= 0:
        bot.reply_to(msg, "❌ Ставка > 0")
        return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}")
        return

    ouid  = ru.id
    oname = ru.first_name or str(ouid)
    ensure_user(ouid, oname)

    _knb_games[uid] = {
        "bet": bet, "chat": msg.chat.id,
        "c_uid": uid, "c_name": cname, "c_choice": None,
        "o_uid": ouid, "o_name": oname, "o_choice": None,
        "expires": now() + 120
    }

    kb_knb = InlineKeyboardMarkup(row_width=3)
    kb_knb.add(
        InlineKeyboardButton("🪨", callback_data=f"knb_{uid}_камень"),
        InlineKeyboardButton("✂️", callback_data=f"knb_{uid}_ножницы"),
        InlineKeyboardButton("📄", callback_data=f"knb_{uid}_бумага"),
    )
    bot.send_message(msg.chat.id,
        f"🪨✂️📄 <b>КНБ!</b>\n{mention(uid, cname)} vs {mention(ouid, oname)}\n"
        f"Ставка: <b>{fmt(bet)} {CURRENCY}</b>\nОба выбирают втайне 👇",
        parse_mode="HTML", reply_markup=kb_knb)


@bot.callback_query_handler(func=lambda c: re.match(r'^knb_\d+_\S+$', c.data))
def cb_knb_choice(call):
    parts   = call.data.split("_")
    game_id = int(parts[1])
    choice  = parts[2]
    uid     = call.from_user.id

    game = _knb_games.get(game_id)
    if not game:
        bot.answer_callback_query(call.id, "Игра завершена")
        return
    if game["expires"] < now():
        _knb_games.pop(game_id, None)
        bot.answer_callback_query(call.id, "Время вышло")
        return

    if uid == game["c_uid"]:
        if game["c_choice"]:
            bot.answer_callback_query(call.id, "Ты уже выбрал!")
            return
        game["c_choice"] = choice
        bot.answer_callback_query(call.id, f"Ты выбрал {KNB_EMOJI[choice]} — ждём соперника")
    elif uid == game["o_uid"]:
        if game["o_choice"]:
            bot.answer_callback_query(call.id, "Ты уже выбрал!")
            return
        game["o_choice"] = choice
        bot.answer_callback_query(call.id, f"Ты выбрал {KNB_EMOJI[choice]} — ждём соперника")
    else:
        bot.answer_callback_query(call.id, "Ты не участник этой игры")
        return

    if not game["c_choice"] or not game["o_choice"]:
        return

    _knb_games.pop(game_id, None)
    cc, oc = game["c_choice"], game["o_choice"]
    bet    = game["bet"]

    if cc == oc:
        result = f"🤝 Ничья!\n{KNB_EMOJI[cc]} vs {KNB_EMOJI[oc]}\nСтавки возвращены."
    elif KNB_BEATS[cc] == oc:
        add_balance(game["o_uid"], -bet)
        add_balance(game["c_uid"],  bet)
        result = (f"🏆 Победил {mention(game['c_uid'], game['c_name'])}!\n"
                  f"{KNB_EMOJI[cc]} бьёт {KNB_EMOJI[oc]}\n💰 +<b>{fmt(bet)} {CURRENCY}</b>")
    else:
        add_balance(game["c_uid"], -bet)
        add_balance(game["o_uid"],  bet)
        result = (f"🏆 Победил {mention(game['o_uid'], game['o_name'])}!\n"
                  f"{KNB_EMOJI[oc]} бьёт {KNB_EMOJI[cc]}\n💰 +<b>{fmt(bet)} {CURRENCY}</b>")

    try:
        bot.edit_message_text(
            f"🪨✂️📄 <b>КНБ — результат</b>\n"
            f"{mention(game['c_uid'], game['c_name'])}: {KNB_EMOJI[cc]}\n"
            f"{mention(game['o_uid'], game['o_name'])}: {KNB_EMOJI[oc]}\n\n{result}",
            game["chat"], call.message.message_id, parse_mode="HTML")
    except Exception:
        bot.send_message(game["chat"], result, parse_mode="HTML")


# ── Чек ────────────────────────────────────

_checks: dict[str, dict] = {}

@bot.message_handler(func=lambda m: m.text and re.match(r'^чек\s+\d+(\s+\d+)?$', m.text.lower().strip()))
def cmd_check_create(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)
    parts = msg.text.strip().split()
    try:
        amount = int(parts[1])
        uses   = int(parts[2]) if len(parts) > 2 else 1
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: чек 5000 [кол-во активаций]")
        return
    if amount <= 0 or uses <= 0:
        bot.reply_to(msg, "❌ Значения > 0")
        return
    total = amount * uses
    u = get_user(uid)
    if u["balance"] < total:
        bot.reply_to(msg, f"❌ Нужно {fmt(total)}, баланс: {fmt(u['balance'])}")
        return
    add_balance(uid, -total)
    code = f"CHK{''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ23456789', k=6))}"
    _checks[code] = {"amount": amount, "uses": uses, "left": uses, "uid": uid}
    bot.send_message(msg.chat.id,
        f"💸 {mention(uid, name)} создал чек!\n"
        f"💵 <b>{fmt(amount)} {CURRENCY}</b> × {uses} активаций\n\n"
        f"Код: <code>{code}</code>\nАктивировать: <code>активировать {code}</code>",
        parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and re.match(r'^активировать\s+\S+', m.text.lower().strip()))
def cmd_check_use(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)
    code = msg.text.strip().split()[1].upper()
    chk  = _checks.get(code)
    if not chk:
        bot.reply_to(msg, "❌ Чек не найден или уже использован")
        return
    if chk["uid"] == uid:
        bot.reply_to(msg, "❌ Нельзя активировать свой чек")
        return

    amount = chk["amount"]
    add_balance(uid, amount)
    chk["left"] -= 1
    extra = " — чек исчерпан" if chk["left"] <= 0 else f" — осталось {chk['left']} активаций"
    if chk["left"] <= 0:
        _checks.pop(code, None)

    bot.reply_to(msg,
        f"✅ {mention(uid, name)} активировал чек!\n"
        f"💰 +<b>{fmt(amount)} {CURRENCY}</b>{extra}",
        parse_mode="HTML")


# ── Перевод в группе ────────────────────────

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(дать|перевод|pay)\s+', m.text.lower().strip()))
def group_transfer(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)

    parts  = msg.text.strip().split()
    to_uid, to_name, amount = None, None, None

    if msg.reply_to_message and msg.reply_to_message.from_user:
        ru = msg.reply_to_message.from_user
        if ru.id == uid or ru.is_bot:
            bot.reply_to(msg, "❌ Некорректный получатель")
            return
        to_uid  = ru.id
        to_name = ru.first_name or str(ru.id)
        ensure_user(to_uid, to_name)
        try:
            amount = int(parts[1])
        except (ValueError, IndexError):
            bot.reply_to(msg, "❌ Формат: дать 5000 (reply)")
            return
    else:
        if len(parts) < 3:
            bot.reply_to(msg, "❌ Формат: дать @username 5000")
            return
        uname = parts[1].lstrip("@")
        with db() as c:
            c.execute("SELECT id, name FROM users WHERE name ILIKE %s", (f"%{uname}%",))
            row = c.fetchone()
        if not row:
            bot.reply_to(msg, "❌ Игрок не найден")
            return
        to_uid, to_name = row["id"], row["name"]
        if to_uid == uid:
            bot.reply_to(msg, "❌ Нельзя переводить самому себе")
            return
        try:
            amount = int(parts[2])
        except (ValueError, IndexError):
            bot.reply_to(msg, "❌ Укажи сумму")
            return

    if amount <= 0:
        bot.reply_to(msg, "❌ Сумма > 0")
        return

    fee   = int(amount * TRANSFER_FEE)
    total = amount + fee
    u = get_user(uid)
    if u["balance"] < total:
        bot.reply_to(msg, f"❌ Нужно {fmt(total)} (комиссия {fmt(fee)}), баланс: {fmt(u['balance'])}")
        return

    add_balance(uid, -total)
    add_balance(to_uid, amount)
    with db() as c:
        c.execute("INSERT INTO transfers (from_id,to_id,amount,fee,ts) VALUES (%s,%s,%s,%s,%s)",
                  (uid, to_uid, amount, fee, now()))

    bot.send_message(msg.chat.id,
        f"💸 {mention(uid, name)} → {mention(to_uid, to_name)}\n"
        f"<b>{fmt(amount)} {CURRENCY}</b>  (комиссия {fmt(fee)})",
        parse_mode="HTML")


# ── Краш в группе ──────────────────────────

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(краш|crash)\s+\d+', m.text.lower().strip()))
def group_crash(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)
    try:
        bet = int(msg.text.strip().split()[1])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: краш 1000")
        return
    if bet <= 0:
        bot.reply_to(msg, "❌ Ставка > 0")
        return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}")
        return

    crash_at = round(max(1.01, random.expovariate(0.8) + 1.0), 2)
    add_balance(uid, -bet)

    kb_crash = InlineKeyboardMarkup(row_width=3)
    kb_crash.add(
        InlineKeyboardButton("💰 x1.5", callback_data=f"grcrash_{uid}_1.5_{bet}_{crash_at}"),
        InlineKeyboardButton("💰 x2.0", callback_data=f"grcrash_{uid}_2.0_{bet}_{crash_at}"),
        InlineKeyboardButton("💰 x3.0", callback_data=f"grcrash_{uid}_3.0_{bet}_{crash_at}"),
    )
    bot.send_message(msg.chat.id,
        f"⚡ {mention(uid, name)} запустил краш!\n"
        f"Ставка: <b>{fmt(bet)} {CURRENCY}</b>\nМножитель растёт... Успей забрать!",
        parse_mode="HTML", reply_markup=kb_crash)


@bot.callback_query_handler(func=lambda c: c.data.startswith("grcrash_"))
def cb_group_crash(call):
    parts    = call.data.split("_")
    owner    = int(parts[1])
    mult     = float(parts[2])
    bet      = int(parts[3])
    crash_at = float(parts[4])
    uid      = call.from_user.id

    if uid != owner:
        bot.answer_callback_query(call.id, "Это не твоя игра")
        return

    name = call.from_user.first_name or str(uid)
    if mult <= crash_at:
        win = int(bet * mult)
        add_balance(uid, win)
        result = f"✅ {mention(uid, name)} забрал x{mult}! +<b>{fmt(win)} {CURRENCY}</b>"
    else:
        result = f"💥 Краш на x{crash_at}! {mention(uid, name)} проиграл -{fmt(bet)} {CURRENCY}"

    try:
        bot.edit_message_text(result, call.message.chat.id, call.message.message_id, parse_mode="HTML")
    except Exception:
        bot.send_message(call.message.chat.id, result, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ── Лотерея в группе ───────────────────────

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(лот|лотерея)\s+\d+', m.text.lower().strip()))
def group_lottery(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)
    try:
        qty = max(1, min(int(msg.text.strip().split()[1]), 100))
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: лот 3")
        return

    cost = TICKET_PRICE * qty
    u = get_user(uid)
    if u["balance"] < cost:
        bot.reply_to(msg, f"❌ Нужно {fmt(cost)}, баланс: {fmt(u['balance'])}")
        return

    add_balance(uid, -cost)
    with db() as c:
        c.execute("""INSERT INTO lottery_tickets (user_id, tickets) VALUES (%s, %s)
                     ON CONFLICT (user_id) DO UPDATE SET tickets = lottery_tickets.tickets + %s""",
                  (uid, qty, qty))
        c.execute("UPDATE lottery SET jackpot=jackpot+%s WHERE id=1", (cost,))
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        jackpot = c.fetchone()["jackpot"]

    bot.reply_to(msg,
        f"🎟 {mention(uid, name)} купил <b>{qty}</b> билетов!\nДжекпот: <b>{fmt(jackpot)} {CURRENCY}</b>",
        parse_mode="HTML")


# ── Помощь в группе ────────────────────────

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["/help","помощь","/помощь"])
def group_help(msg):
    bot.send_message(msg.chat.id,
        "<b>📋 Команды бота</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        "<b>Инфо</b>\n"
        "баланс — твой кошелёк\n"
        "топ — топ по балансу\n"
        "топ xp — топ по уровню\n"
        "акции — текущая цена\n\n"
        "<b>Игры</b>\n"
        "рул красное 1000 — рулетка\n"
        "кости 500 — кубик\n"
        "слот 1000 — слоты\n"
        "краш 1000 — краш\n"
        "дуэль 1000 @username — дуэль\n"
        "кнб 500 (reply) — камень-ножницы-бумага\n"
        "лот 3 — купить 3 лотерейных билета\n\n"
        "<b>Переводы</b>\n"
        "дать @username 5000\n"
        "дать 5000 (reply)\n"
        "чек 5000 — создать чек\n"
        "активировать КОД\n\n"
        "<b>Промокод</b>\n"
        "промо КОД\n\n"
        "Банк, магазин, клан — в <b>личку</b> → /start",
        parse_mode="HTML")


# ══════════════════════════════════════════════
# 26. ЗАПУСК
# ══════════════════════════════════════════════

if __name__ == "__main__":
    init_db()

    threading.Thread(target=_stock_scheduler,  daemon=True).start()
    threading.Thread(target=_lottery_scheduler, daemon=True).start()

    # Устанавливаем webhook
    if WEBHOOK_URL:
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=f"{WEBHOOK_URL}/{TOKEN}")
        print(f"✅ Webhook установлен: {WEBHOOK_URL}/{TOKEN}")
    else:
        print("⚠️ WEBHOOK_URL не задан — используй polling для локального запуска")

    print(f"🚀 Бот запущен на порту {PORT}")
    app.run(host="0.0.0.0", port=PORT)
