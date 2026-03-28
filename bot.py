"""
╔══════════════════════════════════════╗
║   🌸  ECONOMY BOT  —  v2.0 CLEAN    ║
║   Minimalist · Smart · Fast         ║
╚══════════════════════════════════════╝

Архитектура:
  • Один файл, чёткие блоки
  • SQLite + connection pool
  • Inline-кнопки везде (минимум reply-клавиатуры)
  • Единая функция главного меню
  • Все деньги — целые числа (💵)
"""

# ══════════════════════════════════════════════
# 0. ЗАВИСИМОСТИ
# ══════════════════════════════════════════════

import os, re, time, json, math, random, threading
from contextlib import contextmanager
from datetime import datetime, timedelta
import sqlite3
import requests as _req
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

TOKEN       = os.getenv("BOT_TOKEN")
ADMIN_IDS   = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
ALERT_CHAT  = int(os.getenv("ALERT_CHAT", "0"))   # канал для уведомлений
DB_FILE     = "economy.db"
CURRENCY    = "💵"
TICKER      = "ECO"          # тикер акции

# Кулдауны (секунды)
CD_CLICK        = 5
CD_DAILY        = 86400
CD_WORK         = 14400      # 4 часа
CD_MINE         = 3600       # 1 час
CD_LOTTERY      = 86400

# Экономика
TRANSFER_FEE    = 0.05       # 5%
BANK_RATE       = 0.01       # 1% в сутки на вклад
LOAN_RATE       = 0.10       # 10% за срок
LOAN_MAX        = 50_000
CLICK_BASE      = 100
MINE_BASE       = 500
WORK_BASE       = 1_500

# Акции
STOCK_UPDATE    = 1800       # обновление цены каждые 30 мин
STOCK_PRICE_START = 10_000
STOCK_VOLATILITY  = 0.04    # ±4% за шаг

# ══════════════════════════════════════════════
# 2. БАЗА ДАННЫХ
# ══════════════════════════════════════════════

class Pool:
    def __init__(self, path, size=8):
        self._path, self._size = path, size
        self._pool, self._lock = [], threading.Lock()

    def get(self):
        with self._lock:
            if self._pool:
                return self._pool.pop()
        c = sqlite3.connect(self._path, timeout=30, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA foreign_keys=ON")
        return c

    def put(self, c):
        with self._lock:
            if len(self._pool) < self._size:
                self._pool.append(c)
            else:
                c.close()

_pool = Pool(DB_FILE)

@contextmanager
def db():
    c = _pool.get()
    cur = c.cursor()
    try:
        yield cur
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        _pool.put(c)


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY,
            name        TEXT    DEFAULT '',
            balance     INTEGER DEFAULT 0,
            bank        INTEGER DEFAULT 0,
            xp          INTEGER DEFAULT 0,
            daily_streak INTEGER DEFAULT 0,
            last_click   INTEGER DEFAULT 0,
            last_daily   INTEGER DEFAULT 0,
            last_work    INTEGER DEFAULT 0,
            last_mine    INTEGER DEFAULT 0,
            click_power  INTEGER DEFAULT 100,
            total_earned INTEGER DEFAULT 0,
            ref_by       INTEGER DEFAULT 0,
            ref_code     TEXT    UNIQUE,
            premium_until INTEGER DEFAULT 0,
            created_at   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS loans (
            user_id     INTEGER PRIMARY KEY,
            amount      INTEGER DEFAULT 0,
            due_at      INTEGER DEFAULT 0,
            taken_at    INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS stocks (
            ticker      TEXT    PRIMARY KEY,
            price       INTEGER DEFAULT 10000,
            prev_price  INTEGER DEFAULT 10000,
            updated_at  INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS portfolios (
            user_id INTEGER,
            ticker  TEXT,
            shares  INTEGER DEFAULT 0,
            avg_buy INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, ticker)
        );
        CREATE TABLE IF NOT EXISTS stock_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT,
            price       INTEGER,
            ts          INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS lottery (
            id          INTEGER PRIMARY KEY DEFAULT 1,
            jackpot     INTEGER DEFAULT 0,
            draw_at     INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS lottery_tickets (
            user_id INTEGER PRIMARY KEY,
            tickets INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS transfers (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            from_id  INTEGER,
            to_id    INTEGER,
            amount   INTEGER,
            fee      INTEGER,
            ts       INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS items (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT,
            emoji    TEXT DEFAULT '📦',
            price    INTEGER,
            supply   INTEGER DEFAULT -1,
            sold     INTEGER DEFAULT 0,
            active   INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS inventory (
            user_id INTEGER,
            item_id INTEGER,
            qty     INTEGER DEFAULT 1,
            PRIMARY KEY (user_id, item_id)
        );
        CREATE TABLE IF NOT EXISTS clans (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT UNIQUE,
            tag      TEXT UNIQUE,
            owner    INTEGER,
            balance  INTEGER DEFAULT 0,
            level    INTEGER DEFAULT 1,
            xp       INTEGER DEFAULT 0,
            created_at INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS clan_members (
            user_id INTEGER PRIMARY KEY,
            clan_id INTEGER,
            role    TEXT DEFAULT 'member',
            joined_at INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY,
            reason  TEXT,
            by      INTEGER,
            ts      INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS promo_codes (
            code    TEXT PRIMARY KEY,
            reward  INTEGER,
            max_uses INTEGER DEFAULT 1,
            uses    INTEGER DEFAULT 0,
            expires INTEGER DEFAULT 0,
            active  INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS promo_uses (
            user_id INTEGER,
            code    TEXT,
            ts      INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, code)
        );
        CREATE TABLE IF NOT EXISTS donate_packages (
            key     TEXT PRIMARY KEY,
            stars   INTEGER,
            amount  INTEGER,
            label   TEXT
        );
        INSERT OR IGNORE INTO lottery (id, jackpot, draw_at) VALUES (1, 0, 0);
        INSERT OR IGNORE INTO stocks (ticker, price, prev_price, updated_at)
            VALUES ('ECO', 10000, 10000, 0);
        INSERT OR IGNORE INTO donate_packages VALUES
            ('s1',   1,    10000, '⭐ 10 000'),
            ('s5',   5,    60000, '⭐ 60 000'),
            ('s15',  15,  250000, '🔥 250 000'),
            ('s50',  50,  900000, '🔥 900 000'),
            ('s150', 150, 3000000,'💎 3 000 000'),
            ('s250', 250, 5500000,'💎 5 500 000');

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
_banned: dict[int, float] = {}
_flock = threading.Lock()
FLOOD_N, FLOOD_W, FLOOD_BAN = 10, 5, 300

def is_flooding(uid: int) -> bool:
    now = time.time()
    with _flock:
        if uid in _banned:
            if now < _banned[uid]:
                return True
            del _banned[uid]
        hist = [t for t in _flood.get(uid, []) if now - t < FLOOD_W]
        hist.append(now)
        _flood[uid] = hist
        if len(hist) >= FLOOD_N:
            _banned[uid] = now + FLOOD_BAN
            return True
    return False


# ══════════════════════════════════════════════
# 4. БОТ
# ══════════════════════════════════════════════

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=6)

# Патч process_new_messages — флуд + activity
_orig_pnm = bot.process_new_messages
def _safe_pnm(msgs):
    ok = []
    for m in msgs:
        try:
            uid = m.from_user.id if m.from_user else None
            if uid and is_flooding(uid):
                continue
            if uid:
                with db() as c:
                    c.execute("UPDATE users SET balance=balance WHERE id=?", (uid,))  # touch
        except Exception:
            pass
        ok.append(m)
    if ok:
        _orig_pnm(ok)
bot.process_new_messages = _safe_pnm


# ══════════════════════════════════════════════
# 5. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════

def fmt(n: int) -> str:
    """10 000 вместо 10000"""
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
    h, r = divmod(seconds, 3600)
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
        c.execute("SELECT 1 FROM bans WHERE user_id=?", (uid,))
        return bool(c.fetchone())

def get_user(uid: int) -> dict | None:
    with db() as c:
        c.execute("SELECT * FROM users WHERE id=?", (uid,))
        row = c.fetchone()
        return dict(row) if row else None

def ensure_user(uid: int, name: str = "") -> dict:
    with db() as c:
        c.execute(
            "INSERT OR IGNORE INTO users (id, name, created_at) VALUES (?,?,?)",
            (uid, name, now())
        )
    return get_user(uid)

def add_balance(uid: int, amount: int):
    with db() as c:
        c.execute("UPDATE users SET balance=balance+?, total_earned=total_earned+MAX(0,?) WHERE id=?",
                  (amount, amount, uid))

def add_xp(uid: int, xp: int):
    with db() as c:
        c.execute("UPDATE users SET xp=xp+? WHERE id=?", (xp, uid))

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


# ══════════════════════════════════════════════
# 6. UI — КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def kb(*rows: list[tuple]) -> InlineKeyboardMarkup:
    """
    kb(
        [("Текст", "callback"), ("Текст2", "cb2")],
        [("Назад", "back")]
    )
    """
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
    bal = u["balance"] if u else 0
    bank = u["bank"] if u else 0
    lvl = user_level(u["xp"]) if u else 1
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
                c.execute("SELECT id FROM users WHERE ref_code=?", (ref,))
                row = c.fetchone()
                if row:
                    ref_uid = row["id"]
                    bonus = 2_000
                    add_balance(uid, bonus)
                    add_balance(ref_uid, bonus)
                    c.execute("UPDATE users SET ref_by=? WHERE id=?", (ref_uid, uid))
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
    xp_bar_pct = min(100, int((u["xp"] - level_xp(lvl)) / max(1, next_lvl_xp - level_xp(lvl)) * 10))
    bar = "█" * xp_bar_pct + "░" * (10 - xp_bar_pct)

    # Лоан
    loan_text = ""
    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=?", (uid,))
        loan = c.fetchone()
    if loan and loan["amount"] > 0:
        due = datetime.fromtimestamp(loan["due_at"]).strftime("%d.%m")
        loan_text = f"\n⚠️ Долг: <b>{fmt(loan['amount'])}</b> до {due}"

    text = (
        f"<b>💵 Баланс</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Кошелёк: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"Банк: <b>{fmt(u['bank'])} {CURRENCY}</b>\n"
        f"Всего заработано: <b>{fmt(u['total_earned'])}</b>\n"
        f"{loan_text}\n"
        f"⚡ Уровень <b>{lvl}</b>  [{bar}]\n"
        f"Опыт: {fmt(u['xp'])} / {fmt(next_lvl_xp)}"
    )
    buttons = kb(
        [("💸 Перевод", "action_transfer"), ("🔄 Обновить", "menu_balance")],
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
    is_prem = u["premium_until"] > now()
    prem_str = f"⭐ до {datetime.fromtimestamp(u['premium_until']).strftime('%d.%m.%y')}" if is_prem else "—"

    # Клан
    with db() as c:
        c.execute("""SELECT cl.name, cm.role FROM clan_members cm
                     JOIN clans cl ON cl.id=cm.clan_id WHERE cm.user_id=?""", (uid,))
        clan = c.fetchone()
    clan_str = f"[{clan['name']}] ({clan['role']})" if clan else "—"

    # Акции
    with db() as c:
        c.execute("SELECT SUM(shares) FROM portfolios WHERE user_id=?", (uid,))
        shares = c.fetchone()[0] or 0

    # Реф-код
    with db() as c:
        c.execute("SELECT ref_code FROM users WHERE id=?", (uid,))
        row = c.fetchone()
        ref_code = row["ref_code"] if row and row["ref_code"] else "—"

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
        bot.send_message(uid,
            f"⏱ Подожди ещё <b>{cd_str(remaining)}</b>",
            parse_mode="HTML")
        return

    power = u["click_power"]
    # Стрик бонус
    streak_bonus = 1.0
    if u["daily_streak"] >= 7:
        streak_bonus = 1.5
    elif u["daily_streak"] >= 3:
        streak_bonus = 1.2

    earn = int(power * streak_bonus * random.uniform(0.8, 1.2))
    add_balance(uid, earn)
    add_xp(uid, 5)

    with db() as c:
        c.execute("UPDATE users SET last_click=?, total_clicks=COALESCE(total_clicks,0)+1 WHERE id=?",
                  (now(), uid))

    btn = kb([("⚡ Ещё раз", "action_click"), ("🏠 Меню", "home")])
    bot.send_message(uid,
        f"⚡ Клик! +<b>{fmt(earn)} {CURRENCY}</b>\n"
        f"{'🔥 Бонус стрика x'+str(streak_bonus) if streak_bonus > 1 else ''}",
        reply_markup=btn, parse_mode="HTML")


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

    power = u["click_power"]
    earn = int(power * random.uniform(0.8, 1.2))
    add_balance(uid, earn)
    add_xp(uid, 5)
    with db() as c:
        c.execute("UPDATE users SET last_click=? WHERE id=?", (now(), uid))
    bot.answer_callback_query(call.id, f"⚡ +{fmt(earn)} {CURRENCY}", show_alert=False)


# ══════════════════════════════════════════════
# 10. РАБОТА (4 ПРОФЕССИИ)
# ══════════════════════════════════════════════

JOBS = {
    "taxi":   {"name": "🚕 Такси",    "earn": (800,  1_600),  "xp": 20},
    "cargo":  {"name": "🚚 Курьер",   "earn": (1_200, 2_400), "xp": 30},
    "trade":  {"name": "📊 Трейдер",  "earn": (600,  3_000),  "xp": 25, "risk": True},
    "code":   {"name": "💻 Программист","earn":(2_000, 4_000), "xp": 40},
}

@bot.callback_query_handler(func=lambda c: c.data == "menu_work")
def cb_work_menu(call):
    uid = call.from_user.id
    u = get_user(uid)
    remaining = CD_WORK - (now() - u["last_work"]) if u else 0

    if remaining > 0:
        text = f"<b>⚒️ Работа</b>\n\nДоступно через: <b>{cd_str(remaining)}</b>"
        buttons = kb([("🏠 Меню", "home")])
    else:
        text = "<b>⚒️ Выбери профессию:</b>"
        rows = [[( f"{v['name']}  {fmt(v['earn'][0])}–{fmt(v['earn'][1])}", f"do_work_{k}")]
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
    uid  = call.from_user.id
    job_key = call.data[8:]
    job = JOBS.get(job_key)
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
    if job.get("risk"):
        # трейдер может потерять
        if random.random() < 0.3:
            earn = -random.randint(lo // 2, hi // 2)

    add_balance(uid, earn)
    add_xp(uid, job["xp"])
    with db() as c:
        c.execute("UPDATE users SET last_work=? WHERE id=?", (now(), uid))

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

    # Бонус от видеокарт (если есть item с id=1 — видеокарта)
    with db() as c:
        c.execute("SELECT qty FROM inventory WHERE user_id=? AND item_id=1", (uid,))
        row = c.fetchone()
    cards = row["qty"] if row else 0

    earn = MINE_BASE + cards * 200
    earn = int(earn * random.uniform(0.9, 1.1))
    add_balance(uid, earn)
    add_xp(uid, 10)
    with db() as c:
        c.execute("UPDATE users SET last_mine=? WHERE id=?", (now(), uid))

    bot.send_message(uid,
        f"<b>⛏️ Майнинг</b>\n\n"
        f"💰 Намайнено: <b>+{fmt(earn)} {CURRENCY}</b>\n"
        f"🖥 Видеокарт: {cards}\n"
        f"⏱ Следующий сбор через <b>{cd_str(CD_MINE)}</b>",
        parse_mode="HTML",
        reply_markup=kb([("⛏️ Собрать ещё", "mine_again"), ("🏠 Меню", "home")])
    )


@bot.callback_query_handler(func=lambda c: c.data == "mine_again")
def cb_mine_again(call):
    uid = call.from_user.id
    u = get_user(uid)
    remaining = CD_MINE - (now() - u["last_mine"])
    if remaining > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(remaining)}", show_alert=False)
    else:
        cmd_mine(call.message)
        bot.answer_callback_query(call.id)


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
        base   = 1_000
        bonus  = base + streak * 100
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

    # Стрик
    is_consecutive = (now() - u["last_daily"]) < CD_DAILY + 3600
    streak = (u["daily_streak"] + 1) if is_consecutive else 1
    bonus  = 1_000 + streak * 100

    add_balance(uid, bonus)
    add_xp(uid, 20)
    with db() as c:
        c.execute("UPDATE users SET last_daily=?, daily_streak=? WHERE id=?",
                  (now(), streak, uid))

    text = (
        f"<b>🎁 Бонус получен!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"+<b>{fmt(bonus)} {CURRENCY}</b>\n"
        f"🔥 Стрик: <b>{streak} дней</b>"
    )
    buttons = kb([("🏠 Меню", "home")])
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id, "✅ Получено!")


# ══════════════════════════════════════════════
# 13. БАНК — ВКЛАД / КРЕДИТ
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_bank")
def cb_bank(call):
    uid = call.from_user.id
    u = get_user(uid)

    # Начисляем проценты по вкладу
    if u and u["bank"] > 0:
        days = (now() - u.get("last_interest_calc", now())) / 86400
        if days >= 1:
            interest = int(u["bank"] * BANK_RATE * days)
            with db() as c:
                c.execute("UPDATE users SET bank=bank+?, last_interest_calc=? WHERE id=?",
                          (interest, now(), uid))
            u = get_user(uid)

    # Кредит
    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=?", (uid,))
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


# Ввод суммы вклада/снятия/кредита через следующее сообщение

_waiting: dict[int, str] = {}  # uid -> action

@bot.callback_query_handler(func=lambda c: c.data in ["bank_deposit","bank_withdraw","bank_loan","bank_repay","action_transfer","action_rename"])
def cb_input_prompt(call):
    uid = call.from_user.id
    action = call.data
    _waiting[uid] = action

    prompts = {
        "bank_deposit":   "Введи сумму для вклада:",
        "bank_withdraw":  "Введи сумму для снятия:",
        "bank_loan":      f"Введи сумму кредита (макс {fmt(LOAN_MAX)}):",
        "bank_repay":     "Введи сумму для погашения:",
        "action_transfer":"Введи: @username сумма\nПример: @ivan 5000",
        "action_rename":  "Введи новое имя (до 20 символов):",
    }
    bot.answer_callback_query(call.id)
    bot.send_message(uid, prompts.get(action, "Введи значение:"),
                     reply_markup=kb([("❌ Отмена", "cancel_input")]))


@bot.callback_query_handler(func=lambda c: c.data == "cancel_input")
def cb_cancel_input(call):
    uid = call.from_user.id
    _waiting.pop(uid, None)
    bot.answer_callback_query(call.id, "Отменено")
    text, btns = main_menu(uid)
    bot.send_message(uid, text, reply_markup=btns, parse_mode="HTML")


@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in _waiting and m.chat.type == "private")
def handle_input(msg):
    uid    = msg.from_user.id
    action = _waiting.pop(uid, None)
    if not action:
        return

    u = get_user(uid)
    text = msg.text.strip()

    # ── Rename ──
    if action == "action_rename":
        name = text[:20]
        with db() as c:
            c.execute("UPDATE users SET name=? WHERE id=?", (name, uid))
        bot.send_message(uid, f"✅ Имя изменено на <b>{name}</b>", parse_mode="HTML",
                         reply_markup=kb([("🏠 Меню", "home")]))
        return

    # ── Transfer ──
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
            bot.send_message(uid, "❌ Сумма должна быть > 0")
            return
        fee = int(amount * TRANSFER_FEE)
        total = amount + fee
        if u["balance"] < total:
            bot.send_message(uid, f"❌ Недостаточно средств. Нужно: {fmt(total)} (включая комиссию {fmt(fee)})")
            return
        with db() as c:
            c.execute("SELECT id, name FROM users WHERE name=? OR id=?", (target_un, 0))
            # поиск по username tg — упрощённо по name
            c.execute("SELECT id, name FROM users WHERE name LIKE ?", (f"%{target_un}%",))
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
            c.execute("INSERT INTO transfers (from_id,to_id,amount,fee,ts) VALUES (?,?,?,?,?)",
                      (uid, to_uid, amount, fee, now()))
        try:
            bot.send_message(to_uid,
                f"💸 Вам перевели <b>{fmt(amount)} {CURRENCY}</b>",
                parse_mode="HTML")
        except Exception:
            pass
        bot.send_message(uid,
            f"✅ Переведено <b>{fmt(amount)}</b> + комиссия <b>{fmt(fee)}</b>",
            parse_mode="HTML",
            reply_markup=kb([("🏠 Меню", "home")]))
        return

    # ── Bank operations ──
    try:
        amount = int(text.replace(" ", "").replace(",", ""))
    except ValueError:
        bot.send_message(uid, "❌ Введи число")
        return

    if amount <= 0:
        bot.send_message(uid, "❌ Сумма должна быть > 0")
        return

    if action == "bank_deposit":
        if u["balance"] < amount:
            bot.send_message(uid, "❌ Недостаточно на кошельке")
            return
        with db() as c:
            c.execute("UPDATE users SET balance=balance-?, bank=bank+?, last_interest_calc=? WHERE id=?",
                      (amount, amount, now(), uid))
        bot.send_message(uid, f"✅ Внесено на вклад: <b>{fmt(amount)} {CURRENCY}</b>",
                         parse_mode="HTML", reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))

    elif action == "bank_withdraw":
        u = get_user(uid)
        if u["bank"] < amount:
            bot.send_message(uid, "❌ Недостаточно в банке")
            return
        with db() as c:
            c.execute("UPDATE users SET balance=balance+?, bank=bank-? WHERE id=?",
                      (amount, amount, uid))
        bot.send_message(uid, f"✅ Снято с вклада: <b>{fmt(amount)} {CURRENCY}</b>",
                         parse_mode="HTML", reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))

    elif action == "bank_loan":
        with db() as c:
            c.execute("SELECT amount FROM loans WHERE user_id=?", (uid,))
            existing = c.fetchone()
        if existing and existing["amount"] > 0:
            bot.send_message(uid, "❌ У тебя уже есть кредит. Сначала погаси его.")
            return
        if amount > LOAN_MAX:
            bot.send_message(uid, f"❌ Максимум кредита: {fmt(LOAN_MAX)}")
            return
        if u["bank"] < 500:
            bot.send_message(uid, "❌ На вкладе должно быть хотя бы 500 для получения кредита")
            return
        total_debt = int(amount * (1 + LOAN_RATE))
        due = now() + 7 * 86400  # 7 дней
        add_balance(uid, amount)
        with db() as c:
            c.execute("INSERT OR REPLACE INTO loans (user_id,amount,due_at,taken_at) VALUES (?,?,?,?)",
                      (uid, total_debt, due, now()))
        bot.send_message(uid,
            f"✅ Кредит выдан: <b>{fmt(amount)} {CURRENCY}</b>\n"
            f"Вернуть <b>{fmt(total_debt)}</b> до {datetime.fromtimestamp(due).strftime('%d.%m.%Y')}",
            parse_mode="HTML", reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))

    elif action == "bank_repay":
        with db() as c:
            c.execute("SELECT amount FROM loans WHERE user_id=?", (uid,))
            loan = c.fetchone()
        if not loan or loan["amount"] == 0:
            bot.send_message(uid, "❌ У тебя нет кредита")
            return
        debt = loan["amount"]
        pay  = min(amount, debt)
        if u["balance"] < pay:
            bot.send_message(uid, "❌ Недостаточно средств")
            return
        add_balance(uid, -pay)
        new_debt = debt - pay
        with db() as c:
            if new_debt <= 0:
                c.execute("DELETE FROM loans WHERE user_id=?", (uid,))
                msg_extra = "\n🎉 Кредит погашен полностью!"
            else:
                c.execute("UPDATE loans SET amount=? WHERE user_id=?", (new_debt, uid))
                msg_extra = f"\nОстаток долга: <b>{fmt(new_debt)}</b>"
        bot.send_message(uid,
            f"✅ Погашено: <b>{fmt(pay)} {CURRENCY}</b>{msg_extra}",
            parse_mode="HTML", reply_markup=kb([("🏦 Банк","menu_bank"),("🏠 Меню","home")]))


# ══════════════════════════════════════════════
# 14. БИРЖА — АКЦИИ
# ══════════════════════════════════════════════

def get_stock() -> dict:
    with db() as c:
        c.execute("SELECT * FROM stocks WHERE ticker=?", (TICKER,))
        return dict(c.fetchone())


def stock_trend(ticker: str = TICKER, n: int = 10) -> list:
    with db() as c:
        c.execute("SELECT price, ts FROM stock_history WHERE ticker=? ORDER BY ts DESC LIMIT ?",
                  (ticker, n))
        return list(reversed(c.fetchall()))


@bot.callback_query_handler(func=lambda c: c.data == "menu_stock")
def cb_stock(call):
    uid = call.from_user.id
    st = get_stock()
    price = st["price"]
    prev  = st["prev_price"]
    chg   = (price - prev) / prev * 100

    # Портфель пользователя
    with db() as c:
        c.execute("SELECT shares, avg_buy FROM portfolios WHERE user_id=? AND ticker=?", (uid, TICKER))
        port = c.fetchone()

    # Мини-график
    history = stock_trend(n=8)
    if len(history) >= 2:
        mini = ""
        prices = [r["price"] for r in history]
        lo, hi = min(prices), max(prices)
        for p in prices:
            if hi == lo:
                mini += "━"
            elif p >= hi * 0.8:
                mini += "▲"
            elif p <= lo * 1.2:
                mini += "▼"
            else:
                mini += "─"
    else:
        mini = "—"

    arrow = "📈" if chg >= 0 else "📉"
    port_str = ""
    if port and port["shares"] > 0:
        pnl = (price - port["avg_buy"]) * port["shares"]
        pnl_str = f"+{fmt(pnl)}" if pnl >= 0 else fmt(pnl)
        port_str = (
            f"\n——\n"
            f"📂 Ваш портфель: {port['shares']} акций\n"
            f"Средняя покупка: {fmt(port['avg_buy'])}\n"
            f"P&L: <b>{pnl_str} {CURRENCY}</b>"
        )

    text = (
        f"<b>📈 Биржа — {TICKER}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Цена: <b>{fmt(price)} {CURRENCY}</b>\n"
        f"{arrow} Изменение: <b>{chg:+.1f}%</b>\n"
        f"График: <code>{mini}</code>"
        f"{port_str}"
    )
    buttons = kb(
        [("🛒 Купить", "stock_buy"), ("💰 Продать", "stock_sell")],
        [("📊 История", "stock_history"), ("🔄 Обновить", "menu_stock")],
        [("🏠 Меню", "home")]
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data in ["stock_buy", "stock_sell"])
def cb_stock_action(call):
    uid    = call.from_user.id
    action = call.data
    _waiting[uid] = action
    st = get_stock()
    price = st["price"]

    if action == "stock_buy":
        u = get_user(uid)
        max_shares = u["balance"] // price
        bot.answer_callback_query(call.id)
        bot.send_message(uid,
            f"🛒 Цена: <b>{fmt(price)}</b>\n"
            f"Можешь купить: <b>{max_shares} акций</b>\n\n"
            f"Введи количество акций:",
            parse_mode="HTML",
            reply_markup=kb([("❌ Отмена", "cancel_input")]))
    else:
        with db() as c:
            c.execute("SELECT shares FROM portfolios WHERE user_id=? AND ticker=?", (uid, TICKER))
            port = c.fetchone()
        shares = port["shares"] if port else 0
        bot.answer_callback_query(call.id)
        bot.send_message(uid,
            f"💰 Цена: <b>{fmt(price)}</b>\n"
            f"У тебя: <b>{shares} акций</b>\n\n"
            f"Введи количество для продажи:",
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
        bot.send_message(uid, "❌ Количество должно быть > 0")
        return

    st    = get_stock()
    price = st["price"]
    u     = get_user(uid)

    if action == "stock_buy":
        total = price * qty
        fee   = int(total * 0.02)  # 2% комиссия биржи
        cost  = total + fee
        if u["balance"] < cost:
            bot.send_message(uid, f"❌ Нужно {fmt(cost)} (включая комиссию {fmt(fee)})")
            return
        add_balance(uid, -cost)
        with db() as c:
            c.execute("""INSERT INTO portfolios (user_id,ticker,shares,avg_buy) VALUES (?,?,?,?)
                         ON CONFLICT(user_id,ticker) DO UPDATE SET
                         avg_buy=(avg_buy*shares+?*?)/(shares+?),
                         shares=shares+?""",
                      (uid, TICKER, qty, price, price, qty, qty, qty))
        # Влияем на цену
        _market_impact(qty, "buy")
        bot.send_message(uid,
            f"✅ Куплено <b>{qty} акций {TICKER}</b>\n"
            f"По {fmt(price)} + комиссия {fmt(fee)}\n"
            f"Итого: {fmt(cost)} {CURRENCY}",
            parse_mode="HTML",
            reply_markup=kb([("📈 Биржа","menu_stock"),("🏠 Меню","home")]))

    elif action == "stock_sell":
        with db() as c:
            c.execute("SELECT shares, avg_buy FROM portfolios WHERE user_id=? AND ticker=?", (uid, TICKER))
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
                c.execute("DELETE FROM portfolios WHERE user_id=? AND ticker=?", (uid, TICKER))
            else:
                c.execute("UPDATE portfolios SET shares=? WHERE user_id=? AND ticker=?",
                          (new_shares, uid, TICKER))
        _market_impact(qty, "sell")
        pnl_str = f"+{fmt(pnl)}" if pnl >= 0 else fmt(pnl)
        bot.send_message(uid,
            f"✅ Продано <b>{qty} акций {TICKER}</b>\n"
            f"Получено: <b>{fmt(gain)} {CURRENCY}</b>\n"
            f"P&L: <b>{pnl_str}</b>",
            parse_mode="HTML",
            reply_markup=kb([("📈 Биржа","menu_stock"),("🏠 Меню","home")]))


def _market_impact(qty: int, direction: str):
    """Сделки немного сдвигают цену."""
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
        row = c.fetchone()
        if not row:
            return
        price = row["price"]
        impact = price * 0.001 * qty  # 0.1% за акцию
        new_price = max(100, price + (impact if direction == "buy" else -impact))
        new_price = int(new_price)
        c.execute("UPDATE stocks SET prev_price=price, price=? WHERE ticker=?", (new_price, TICKER))
        c.execute("INSERT INTO stock_history (ticker,price,ts) VALUES (?,?,?)", (TICKER, new_price, now()))


@bot.callback_query_handler(func=lambda c: c.data == "stock_history")
def cb_stock_history(call):
    uid = call.from_user.id
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
    """Обновляет цену каждые STOCK_UPDATE секунд."""
    print(f"[stocks] планировщик запущен ({STOCK_UPDATE//60}мин)")
    while True:
        time.sleep(STOCK_UPDATE)
        try:
            with db() as c:
                c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
                row = c.fetchone()
                if not row:
                    continue
                old = row["price"]
                # Случайное блуждание + возврат к среднему
                drift = (STOCK_PRICE_START - old) * 0.01
                vol   = old * STOCK_VOLATILITY
                new   = max(100, int(old + drift + random.gauss(0, vol)))
                c.execute("UPDATE stocks SET prev_price=price, price=?, updated_at=? WHERE ticker=?",
                          (new, now(), TICKER))
                c.execute("INSERT INTO stock_history (ticker,price,ts) VALUES (?,?,?)",
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
    uid = call.from_user.id
    text = (
        "<b>🎰 Игры</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Выбери игру:"
    )
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
    bot.send_message(call.from_user.id,
        "<b>🎲 Кубик</b>\nВведи ставку:",
        parse_mode="HTML",
        reply_markup=kb([("❌ Отмена", "cancel_input")]))


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
    time.sleep(2.5)
    val = sent.dice.value
    if val >= 4:
        win = int(bet * 1.5)
        add_balance(uid, win - bet)
        result = f"🎲 Выпало <b>{val}</b> — победа! +{fmt(win-bet)} {CURRENCY}"
    else:
        add_balance(uid, -bet)
        result = f"🎲 Выпало <b>{val}</b> — проигрыш. -{fmt(bet)} {CURRENCY}"

    bot.send_message(uid, result, parse_mode="HTML",
                     reply_markup=kb([("🎲 Ещё", "game_dice"),("🎮 Игры","menu_games"),("🏠 Меню","home")]))


# ── Слоты ──────────────────────────────────

SLOT_SYMBOLS = ["🍒", "🍋", "🍊", "🍇", "⭐", "💎"]
SLOT_PAYOUTS = {
    "💎💎💎": 10, "⭐⭐⭐": 7, "🍇🍇🍇": 5,
    "🍊🍊🍊": 4, "🍋🍋🍋": 3, "🍒🍒🍒": 2,
}

@bot.callback_query_handler(func=lambda c: c.data == "game_slots")
def cb_slots_menu(call):
    _waiting[call.from_user.id] = "game_slots"
    bot.answer_callback_query(call.id)
    bot.send_message(call.from_user.id,
        "<b>🎰 Слоты</b>\nВведи ставку:",
        parse_mode="HTML",
        reply_markup=kb([("❌ Отмена", "cancel_input")]))


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
    text = (
        "<b>🎡 Рулетка</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔴 Красное (x2)   ⬛ Чёрное (x2)\n"
        "🟢 Зеро (x14)\n\n"
        "Введи: <code>цвет сумма</code>\n"
        "Пример: <code>красное 1000</code>"
    )
    bot.answer_callback_query(call.id)
    _waiting[uid] = "game_roulette"
    bot.send_message(uid, text, parse_mode="HTML",
                     reply_markup=kb([("❌ Отмена","cancel_input")]))


@bot.message_handler(func=lambda m: m.from_user and _waiting.get(m.from_user.id) == "game_roulette" and m.chat.type == "private")
def handle_roulette(msg):
    uid = msg.from_user.id
    _waiting.pop(uid, None)
    u = get_user(uid)
    parts = msg.text.strip().lower().split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: красное 1000")
        return
    color_input = parts[0]
    try:
        bet = int(parts[1])
    except ValueError:
        bot.send_message(uid, "❌ Неверная ставка")
        return
    if bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка (баланс: {fmt(u['balance'])})")
        return

    color_map = {"красное": "red", "красный": "red", "r": "red",
                 "чёрное": "black", "черное": "black", "b": "black",
                 "зеро": "zero", "0": "zero"}
    color = color_map.get(color_input)
    if not color:
        bot.send_message(uid, "❌ Выбери: красное / чёрное / зеро")
        return

    num = random.randint(0, 36)
    if num == 0:
        actual = "zero"
    elif num % 2 == 1:
        actual = "red"
    else:
        actual = "black"

    mults = {"red": 2, "black": 2, "zero": 14}
    icons = {"red": "🔴", "black": "⬛", "zero": "🟢"}

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

TICKET_PRICE = 500

@bot.callback_query_handler(func=lambda c: c.data == "game_lotto")
def cb_lottery(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
        lotto = c.fetchone()
        c.execute("SELECT tickets FROM lottery_tickets WHERE user_id=?", (uid,))
        my = c.fetchone()

    jackpot   = lotto["jackpot"] if lotto else 0
    draw_at   = lotto["draw_at"] if lotto else 0
    my_tickets = my["tickets"] if my else 0

    draw_str = datetime.fromtimestamp(draw_at).strftime("%d.%m %H:%M") if draw_at > now() else "скоро"

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
    uid = call.from_user.id
    qty = 1 if call.data == "lotto_buy_1" else 5
    cost = TICKET_PRICE * qty
    u = get_user(uid)
    if u["balance"] < cost:
        bot.answer_callback_query(call.id, f"❌ Нужно {fmt(cost)}", show_alert=True)
        return
    add_balance(uid, -cost)
    with db() as c:
        c.execute("INSERT INTO lottery_tickets (user_id,tickets) VALUES (?,?) ON CONFLICT(user_id) DO UPDATE SET tickets=tickets+?",
                  (uid, qty, qty))
        c.execute("UPDATE lottery SET jackpot=jackpot+? WHERE id=1", (cost,))
    bot.answer_callback_query(call.id, f"✅ Куплено {qty} билетов!")
    cb_lottery(call)


# ── Краш ───────────────────────────────────

_crash_games: dict[int, dict] = {}  # uid -> {bet, multiplier, active}

@bot.callback_query_handler(func=lambda c: c.data == "game_crash")
def cb_crash_menu(call):
    uid = call.from_user.id
    _waiting[uid] = "game_crash"
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        "<b>⚡ Краш</b>\n"
        "Множитель растёт пока не крашнется.\n"
        "Успей забрать вовремя!\n\n"
        "Введи ставку:",
        parse_mode="HTML",
        reply_markup=kb([("❌ Отмена","cancel_input")]))


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

    # Генерируем точку краша (экспоненциальное распределение)
    crash_at = max(1.01, random.expovariate(1) + 1.0)
    crash_at = round(crash_at, 2)

    add_balance(uid, -bet)
    _crash_games[uid] = {"bet": bet, "crash": crash_at, "ts": now()}

    m = bot.send_message(uid,
        f"⚡ Ставка: <b>{fmt(bet)}</b>\n"
        f"Множитель растёт... Забери вовремя!\n\n"
        f"x1.00 → x{crash_at:.2f} (краш)\n\n"
        f"Нажми <b>Забрать</b> когда хочешь:",
        parse_mode="HTML",
        reply_markup=kb([("💰 Забрать x1.5", f"crash_cashout_1.5_{uid}"),
                         ("💰 Забрать x2.0", f"crash_cashout_2.0_{uid}"),
                         ("💰 Забрать x3.0", f"crash_cashout_3.0_{uid}")]))


@bot.callback_query_handler(func=lambda c: c.data.startswith("crash_cashout_"))
def cb_crash_cashout(call):
    uid = call.from_user.id
    parts   = call.data.split("_")
    mult    = float(parts[2])
    game_uid = int(parts[3])

    if game_uid != uid:
        bot.answer_callback_query(call.id, "Это не ваша игра")
        return

    game = _crash_games.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "Игра уже завершена")
        return

    if mult <= game["crash"]:
        win = int(game["bet"] * mult)
        add_balance(uid, win)
        text = f"✅ Забрал x{mult}! +{fmt(win)} {CURRENCY}"
    else:
        text = f"💥 Краш на x{game['crash']}! Проигрыш."

    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=kb([("⚡ Ещё","game_crash"),("🎮 Игры","menu_games"),("🏠 Меню","home")]),
                              parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ── Мины ───────────────────────────────────

_mines_games: dict[int, dict] = {}

@bot.callback_query_handler(func=lambda c: c.data == "game_mines")
def cb_mines_menu(call):
    uid = call.from_user.id
    _waiting[uid] = "game_mines"
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        "<b>💣 Мины</b>\n"
        "Поле 3×3 — 2 мины.\n"
        "Открывай клетки и забирай выигрыш!\n\n"
        "Введи ставку:",
        parse_mode="HTML",
        reply_markup=kb([("❌ Отмена","cancel_input")]))


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

    mines = random.sample(range(9), 2)
    _mines_games[uid] = {"bet": bet, "mines": mines, "opened": [], "active": True}
    add_balance(uid, -bet)
    _send_mines_board(uid, msg.chat.id)


def _send_mines_board(uid: int, chat_id: int, msg_id: int = None):
    game = _mines_games.get(uid)
    if not game:
        return

    opened = game["opened"]
    mult   = 1.0 + len(opened) * 0.5

    rows = []
    board_row = []
    for i in range(9):
        if i in opened:
            board_row.append(("💎", f"mines_nop_{i}"))
        else:
            board_row.append(("⬜", f"mines_open_{uid}_{i}"))
        if len(board_row) == 3:
            rows.append(board_row)
            board_row = []

    rows.append([("💰 Забрать", f"mines_cashout_{uid}"), ("🏠 Меню","home")])

    text = (
        f"<b>💣 Мины</b>  Ставка: {fmt(game['bet'])}\n"
        f"Открыто: {len(opened)}/7  Множитель: x{mult:.1f}\n"
        f"Потенциал: <b>{fmt(int(game['bet']*mult))}</b>"
    )

    if msg_id:
        try:
            bot.edit_message_text(text, chat_id, msg_id,
                                  reply_markup=kb(*rows), parse_mode="HTML")
            return
        except Exception:
            pass
    bot.send_message(chat_id, text, reply_markup=kb(*rows), parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_open_"))
def cb_mines_open(call):
    uid = call.from_user.id
    parts = call.data.split("_")
    game_uid = int(parts[2])
    cell = int(parts[3])

    if game_uid != uid:
        bot.answer_callback_query(call.id)
        return

    game = _mines_games.get(uid)
    if not game or not game["active"]:
        bot.answer_callback_query(call.id, "Игра завершена")
        return

    if cell in game["mines"]:
        # Взрыв
        _mines_games.pop(uid, None)
        mines = game["mines"]
        # Показываем мины
        rows = []
        row = []
        for i in range(9):
            if i in mines:
                row.append(("💥", f"mines_nop_{i}"))
            elif i in game["opened"]:
                row.append(("💎", f"mines_nop_{i}"))
            else:
                row.append(("⬜", f"mines_nop_{i}"))
            if len(row) == 3:
                rows.append(row)
                row = []
        rows.append([("💣 Ещё","game_mines"),("🏠 Меню","home")])
        try:
            bot.edit_message_text(
                f"💥 <b>БУМ!</b> Ты нашёл мину!\nПроигрыш: -{fmt(game['bet'])} {CURRENCY}",
                uid, call.message.message_id,
                reply_markup=kb(*rows), parse_mode="HTML")
        except Exception:
            bot.send_message(uid, "💥 БУМ! Мина!", parse_mode="HTML")
        bot.answer_callback_query(call.id, "💥 Мина!")
        return

    game["opened"].append(cell)
    if len(game["opened"]) >= 7:
        # Все безопасные клетки открыты — победа
        mult = 1.0 + 7 * 0.5
        win  = int(game["bet"] * mult)
        add_balance(uid, win)
        _mines_games.pop(uid, None)
        bot.answer_callback_query(call.id, f"🎉 Ты открыл все! +{fmt(win)}")
        bot.send_message(uid,
            f"🎉 <b>Победа!</b> Открыл все клетки!\n+{fmt(win)} {CURRENCY}",
            parse_mode="HTML",
            reply_markup=kb([("💣 Ещё","game_mines"),("🎮 Игры","menu_games")]))
        return

    bot.answer_callback_query(call.id, "💎 Безопасно!")
    _send_mines_board(uid, call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_cashout_"))
def cb_mines_cashout(call):
    uid      = call.from_user.id
    game_uid = int(call.data.split("_")[2])
    if game_uid != uid:
        bot.answer_callback_query(call.id)
        return

    game = _mines_games.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "Игра уже завершена")
        return

    opened = len(game["opened"])
    mult = 1.0 + opened * 0.5
    win  = int(game["bet"] * mult)
    add_balance(uid, win)

    try:
        bot.edit_message_text(
            f"💰 <b>Забрал!</b> x{mult:.1f}\n+{fmt(win)} {CURRENCY}",
            uid, call.message.message_id,
            reply_markup=kb([("💣 Ещё","game_mines"),("🎮 Игры","menu_games"),("🏠 Меню","home")]),
            parse_mode="HTML")
    except Exception:
        bot.send_message(uid, f"💰 +{fmt(win)} {CURRENCY}", parse_mode="HTML")
    bot.answer_callback_query(call.id, f"✅ +{fmt(win)}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_nop_"))
def cb_mines_nop(call):
    bot.answer_callback_query(call.id)


# ══════════════════════════════════════════════
# 16. МАГАЗИН
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_shop")
def cb_shop(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("SELECT id, emoji, name, price, supply, sold FROM items WHERE active=1 ORDER BY price")
        items = c.fetchall()

    if not items:
        try:
            bot.edit_message_text("🛍️ <b>Магазин пуст</b>", uid, call.message.message_id,
                                  reply_markup=kb([("🏠 Меню","home")]), parse_mode="HTML")
        except Exception:
            bot.send_message(uid, "🛍️ Магазин пуст",
                             reply_markup=kb([("🏠 Меню","home")]))
        bot.answer_callback_query(call.id)
        return

    lines = []
    rows  = []
    for item in items:
        supply_str = f" ({item['supply']-item['sold']} шт.)" if item["supply"] != -1 else ""
        lines.append(f"{item['emoji']} <b>{item['name']}</b> — {fmt(item['price'])} {CURRENCY}{supply_str}")
        rows.append([(f"{item['emoji']} Купить", f"buy_item_{item['id']}")])

    rows.append([("🏠 Меню","home")])
    text = "<b>🛍️ Магазин</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines)

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
        c.execute("SELECT * FROM items WHERE id=? AND active=1", (item_id,))
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
        c.execute("""INSERT INTO inventory (user_id, item_id, qty) VALUES (?,?,1)
                     ON CONFLICT(user_id,item_id) DO UPDATE SET qty=qty+1""", (uid, item_id))
        if item["supply"] != -1:
            c.execute("UPDATE items SET sold=sold+1 WHERE id=?", (item_id,))

    bot.answer_callback_query(call.id, f"✅ Куплено: {item['emoji']} {item['name']}")


# ══════════════════════════════════════════════
# 17. ТОП
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_top")
def cb_top(call):
    uid = call.from_user.id
    text = "<b>🏆 Рейтинги</b>"
    buttons = kb(
        [("💵 По балансу","top_balance"), ("⭐ По XP","top_xp")],
        [("📈 Акционеры","top_stocks"), ("🏠 Меню","home")]
    )
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=buttons, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=buttons, parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("top_"))
def cb_top_category(call):
    uid = call.from_user.id
    cat = call.data[4:]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

    with db() as c:
        if cat == "balance":
            c.execute("SELECT name, balance FROM users ORDER BY balance DESC LIMIT 10")
            rows = c.fetchall()
            title, val_fn = "💵 Топ по балансу", lambda r: fmt(r["balance"])
        elif cat == "xp":
            c.execute("SELECT name, xp FROM users ORDER BY xp DESC LIMIT 10")
            rows = c.fetchall()
            title, val_fn = "⭐ Топ по XP", lambda r: fmt(r["xp"])
        elif cat == "stocks":
            c.execute("""SELECT u.name, SUM(p.shares) as total
                         FROM portfolios p JOIN users u ON u.id=p.user_id
                         WHERE p.ticker=? GROUP BY p.user_id ORDER BY total DESC LIMIT 10""", (TICKER,))
            rows = c.fetchall()
            title, val_fn = "📈 Топ акционеров", lambda r: f"{r['total']} акций"
        else:
            bot.answer_callback_query(call.id)
            return

    lines = []
    for i, row in enumerate(rows):
        name = row["name"] or f"Игрок"
        lines.append(f"{medals[i]} {name} — <b>{val_fn(row)}</b>")

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
                     JOIN clans cl ON cl.id=cm.clan_id WHERE cm.user_id=?""", (uid,))
        clan = c.fetchone()

    if not clan:
        text = (
            "<b>🏰 Клан</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "Ты не состоишь в клане.\n\n"
            "Создай или вступи в клан!"
        )
        buttons = kb(
            [("⚔️ Создать клан", "clan_create")],
            [("🏠 Меню", "home")]
        )
    else:
        with db() as c:
            c.execute("SELECT COUNT(*) as cnt FROM clan_members WHERE clan_id=?", (clan["id"],))
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
        if clan["role"] in ("owner", "admin"):
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
    uid = call.from_user.id
    _waiting[uid] = "clan_create"
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        "Введи название и тег клана через пробел:\n"
        "Пример: <code>Легион LEG</code>\n"
        "Стоимость: <b>5 000 {CURRENCY}</b>",
        parse_mode="HTML",
        reply_markup=kb([("❌ Отмена","cancel_input")]))


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
            c.execute("INSERT INTO clans (name,tag,owner,created_at) VALUES (?,?,?,?)",
                      (name, tag, uid, now()))
            clan_id = c.lastrowid
            c.execute("INSERT INTO clan_members (user_id,clan_id,role,joined_at) VALUES (?,?,?,?)",
                      (uid, clan_id, "owner", now()))
        bot.send_message(uid,
            f"✅ Клан <b>{name}</b> [{tag}] создан!",
            parse_mode="HTML",
            reply_markup=kb([("🏰 Клан","menu_clan"),("🏠 Меню","home")]))
    except Exception:
        bot.send_message(uid, "❌ Название или тег уже заняты")
        add_balance(uid, cost)


@bot.callback_query_handler(func=lambda c: c.data.startswith("clan_leave_"))
def cb_clan_leave(call):
    uid     = call.from_user.id
    clan_id = int(call.data[11:])
    with db() as c:
        c.execute("SELECT role FROM clan_members WHERE user_id=? AND clan_id=?", (uid, clan_id))
        row = c.fetchone()
    if row and row["role"] == "owner":
        bot.answer_callback_query(call.id, "❌ Владелец не может покинуть клан. Передай права.", show_alert=True)
        return
    with db() as c:
        c.execute("DELETE FROM clan_members WHERE user_id=?", (uid,))
    bot.answer_callback_query(call.id, "✅ Вы покинули клан")
    cb_clan(call)


# ══════════════════════════════════════════════
# 19. ДОНАТ
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().strip() == "донат")
def cmd_donate(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")
    with db() as c:
        c.execute("SELECT key, stars, amount, label FROM donate_packages ORDER BY stars")
        pkgs = c.fetchall()

    text = "<b>💎 Донат</b>\n━━━━━━━━━━━━━━━━━━\nПополни баланс через Telegram Stars:"
    rows = [[( f"{p['label']}", f"donate_{p['key']}")] for p in pkgs]
    rows.append([("🏠 Меню","home")])
    bot.send_message(uid, text, reply_markup=kb(*rows), parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: c.data.startswith("donate_"))
def cb_donate(call):
    uid = call.from_user.id
    key = call.data[7:]
    with db() as c:
        c.execute("SELECT * FROM donate_packages WHERE key=?", (key,))
        pkg = c.fetchone()
    if not pkg:
        bot.answer_callback_query(call.id, "❌ Пакет не найден")
        return
    bot.answer_callback_query(call.id)
    bot.send_invoice(
        uid,
        title=f"Пополнение {pkg['label']}",
        description=f"+{fmt(pkg['amount'])} {CURRENCY}",
        payload=f"donate_{key}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(f"{pkg['label']}", pkg["stars"])]
    )


@bot.pre_checkout_query_handler(func=lambda q: True)
def pre_checkout(query):
    bot.answer_pre_checkout_query(query.id, ok=True)


@bot.message_handler(content_types=["successful_payment"])
def successful_payment(msg):
    uid     = msg.from_user.id
    payload = msg.successful_payment.invoice_payload
    key     = payload[7:]
    with db() as c:
        c.execute("SELECT amount FROM donate_packages WHERE key=?", (key,))
        pkg = c.fetchone()
    if pkg:
        add_balance(uid, pkg["amount"])
        bot.send_message(uid,
            f"✅ <b>Пополнение прошло!</b>\n+{fmt(pkg['amount'])} {CURRENCY}",
            parse_mode="HTML",
            reply_markup=kb([("🏠 Меню","home")]))
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
        c.execute("SELECT * FROM promo_codes WHERE code=? AND active=1", (code,))
        promo = c.fetchone()

    if not promo:
        bot.send_message(uid, "❌ Промокод не найден или истёк")
        return

    if promo["expires"] and promo["expires"] < now():
        bot.send_message(uid, "❌ Промокод истёк")
        return

    if promo["max_uses"] > 0 and promo["uses"] >= promo["max_uses"]:
        bot.send_message(uid, "❌ Промокод исчерпан")
        return

    with db() as c:
        try:
            c.execute("INSERT INTO promo_uses (user_id,code,ts) VALUES (?,?,?)",
                      (uid, code, now()))
        except Exception:
            bot.send_message(uid, "❌ Этот промокод ты уже использовал")
            return
        c.execute("UPDATE promo_codes SET uses=uses+1 WHERE code=?", (code,))

    add_balance(uid, promo["reward"])
    bot.send_message(uid,
        f"✅ Промокод активирован!\n+<b>{fmt(promo['reward'])} {CURRENCY}</b>",
        parse_mode="HTML",
        reply_markup=kb([("🏠 Меню","home")]))


# ══════════════════════════════════════════════
# 21. РЕФЕРАЛЬНАЯ ССЫЛКА
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().strip() in ["реферал", "рефка", "ref"])
def cmd_referral(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")

    with db() as c:
        c.execute("SELECT ref_code FROM users WHERE id=?", (uid,))
        row = c.fetchone()
        ref_code = row["ref_code"] if row and row["ref_code"] else None

    if not ref_code:
        ref_code = f"R{uid}"
        with db() as c:
            c.execute("UPDATE users SET ref_code=? WHERE id=?", (ref_code, uid))

    with db() as c:
        c.execute("SELECT COUNT(*) as cnt FROM users WHERE ref_by=?", (uid,))
        referrals = c.fetchone()["cnt"]

    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={ref_code}"

    bot.send_message(uid,
        f"<b>🔗 Реферальная программа</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Ты пригласил: <b>{referrals}</b> игроков\n"
        f"Бонус за приглашение: <b>2 000 {CURRENCY}</b> тебе и другу\n\n"
        f"Твоя ссылка:\n{link}",
        parse_mode="HTML",
        reply_markup=kb([("🏠 Меню","home")]))


# ══════════════════════════════════════════════
# 22. ПЕРЕВОД (текстовая команда)
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().split()[0] in ["перевод","дать","pay"] if m.text else False)
def cmd_transfer(msg):
    uid = msg.from_user.id
    _waiting[uid] = "action_transfer"
    bot.send_message(uid,
        "Введи: @username сумма\nПример: @ivan 5000\n\n"
        f"Комиссия: {TRANSFER_FEE*100:.0f}%",
        reply_markup=kb([("❌ Отмена","cancel_input")]))


# ══════════════════════════════════════════════
# 23. АДМИН-КОМАНДЫ
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
        uid_  = int(parts[1])
        amt   = int(parts[2])
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
        uid_  = int(parts[1])
        amt   = int(parts[2])
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
        c.execute("INSERT OR REPLACE INTO bans (user_id,reason,by,ts) VALUES (?,?,?,?)",
                  (uid_, reason, msg.from_user.id, now()))
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
        c.execute("DELETE FROM bans WHERE user_id=?", (uid_,))
    bot.reply_to(msg, f"✅ Игрок {uid_} разблокирован.")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("промо создать ") and is_admin(m.from_user.id))
@admin_only
def cmd_create_promo(msg):
    # промо создать КОД 5000 [uses]
    parts = msg.text.split()
    try:
        code  = parts[2].upper()
        amt   = int(parts[3])
        uses  = int(parts[4]) if len(parts) > 4 else 1
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: промо создать КОД СУММА [uses]")
        return
    with db() as c:
        try:
            c.execute("INSERT INTO promo_codes (code,reward,max_uses) VALUES (?,?,?)",
                      (code, amt, uses))
        except Exception:
            bot.reply_to(msg, "❌ Код уже существует")
            return
    bot.reply_to(msg, f"✅ Промокод <code>{code}</code> создан. +{fmt(amt)}, uses={uses}", parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("добавить товар ") and is_admin(m.from_user.id))
@admin_only
def cmd_add_item(msg):
    # добавить товар 💻 Ноутбук 10000 [supply]
    parts = msg.text.split(None, 4)
    try:
        emoji  = parts[2]
        name   = parts[3]
        price  = int(parts[4].split()[0])
        supply = int(parts[4].split()[1]) if len(parts[4].split()) > 1 else -1
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: добавить товар EMOJI ИМЯ ЦЕНА [supply]")
        return
    with db() as c:
        c.execute("INSERT INTO items (emoji,name,price,supply) VALUES (?,?,?,?)",
                  (emoji, name, price, supply))
    bot.reply_to(msg, f"✅ Товар {emoji} {name} добавлен по цене {fmt(price)}")


@bot.message_handler(func=lambda m: m.text and m.text.lower().strip() == "стат" and is_admin(m.from_user.id))
@admin_only
def cmd_stat(msg):
    with db() as c:
        c.execute("SELECT COUNT(*) FROM users")
        users = c.fetchone()[0]
        c.execute("SELECT COALESCE(SUM(balance),0) FROM users")
        total_bal = c.fetchone()[0]
        c.execute("SELECT COALESCE(SUM(bank),0) FROM users")
        total_bank = c.fetchone()[0]
        c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
        stock = c.fetchone()
        price = stock["price"] if stock else 0
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        lotto = c.fetchone()
        jackpot = lotto["jackpot"] if lotto else 0

    bot.reply_to(msg,
        f"<b>📊 Статистика</b>\n"
        f"Игроков: <b>{users}</b>\n"
        f"Кошельки: <b>{fmt(total_bal)}</b>\n"
        f"Вклады: <b>{fmt(total_bank)}</b>\n"
        f"Акция {TICKER}: <b>{fmt(price)}</b>\n"
        f"Лото джекпот: <b>{fmt(jackpot)}</b>",
        parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("дивиденды ") and is_admin(m.from_user.id))
@admin_only
def cmd_dividends(msg):
    try:
        per_share = int(msg.text.split()[1])
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: дивиденды СУММА_ЗА_АКЦИЮ")
        return

    with db() as c:
        c.execute("SELECT user_id, shares FROM portfolios WHERE ticker=?", (TICKER,))
        holders = c.fetchall()

    if not holders:
        bot.reply_to(msg, "❌ Нет держателей акций")
        return

    total = 0
    for h in holders:
        pay = h["shares"] * per_share
        add_balance(h["user_id"], pay)
        total += pay
        try:
            bot.send_message(h["user_id"],
                f"💰 Дивиденды {TICKER}: <b>+{fmt(pay)}</b> ({h['shares']} акций × {fmt(per_share)})",
                parse_mode="HTML")
        except Exception:
            pass

    bot.reply_to(msg,
        f"✅ Дивиденды выплачены!\n"
        f"Держателей: {len(holders)}\n"
        f"Итого: {fmt(total)} {CURRENCY}",
        parse_mode="HTML")
    send_alert(f"💰 Дивиденды {TICKER}: {fmt(per_share)}/акцию, итого {fmt(total)}")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("рассылка") and is_admin(m.from_user.id))
@admin_only
def cmd_broadcast(msg):
    text = msg.text[8:].strip()
    if not text:
        bot.reply_to(msg, "Формат: рассылка Текст сообщения")
        return
    with db() as c:
        c.execute("SELECT id FROM users")
        users = [r["id"] for r in c.fetchall()]

    sent, fail = 0, 0
    for uid_ in users:
        try:
            bot.send_message(uid_, text, parse_mode="HTML")
            sent += 1
        except Exception:
            fail += 1
        time.sleep(0.05)

    bot.reply_to(msg, f"✅ Отправлено: {sent}, ошибок: {fail}")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("установить балансе ") and is_admin(m.from_user.id))
@admin_only
def cmd_set_balance(msg):
    parts = msg.text.split()
    try:
        uid_  = int(parts[2])
        amt   = int(parts[3])
    except (IndexError, ValueError):
        bot.reply_to(msg, "Формат: установить балансе <uid> <сумма>")
        return
    with db() as c:
        c.execute("UPDATE users SET balance=? WHERE id=?", (amt, uid_))
    bot.reply_to(msg, f"✅ Баланс игрока {uid_} = {fmt(amt)}")


# ══════════════════════════════════════════════
# 24. ПОМОЩЬ
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower() in ["помощь","help","/help"])
def cmd_help(msg):
    text = (
        "<b>📞 Помощь</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<b>Основные команды:</b>\n"
        "меню — главное меню\n"
        "клик — кликер\n"
        "майнинг — добыча монет\n"
        "перевод — перевести деньги\n"
        "реферал — реферальная ссылка\n"
        "промо КОД — активировать промокод\n"
        "донат — пополнить баланс\n\n"
        "<b>В меню доступно:</b>\n"
        "💵 Баланс · 📈 Биржа · ⚒️ Работа\n"
        "🎰 Игры · 🏦 Банк · 🛍️ Магазин\n"
        "🏆 Топ · 🎁 Бонус · 🏰 Клан"
    )
    bot.send_message(msg.chat.id, text, parse_mode="HTML",
                     reply_markup=kb([("🏠 Меню","home")]))


# ══════════════════════════════════════════════
# 25. ЛОТЕРЕЯ — ПЛАНИРОВЩИК РОЗЫГРЫША
# ══════════════════════════════════════════════

def _lottery_scheduler():
    """Розыгрыш лотереи раз в сутки."""
    while True:
        time.sleep(3600)
        try:
            with db() as c:
                c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
                lotto = c.fetchone()
            if not lotto:
                continue
            if lotto["draw_at"] > now():
                continue

            # Розыгрыш
            with db() as c:
                c.execute("SELECT user_id, tickets FROM lottery_tickets WHERE tickets > 0")
                participants = c.fetchall()

            if not participants:
                with db() as c:
                    c.execute("UPDATE lottery SET draw_at=? WHERE id=1", (now() + 86400,))
                continue

            pool = []
            for p in participants:
                pool.extend([p["user_id"]] * p["tickets"])

            winner = random.choice(pool)
            jackpot = lotto["jackpot"]
            add_balance(winner, jackpot)

            with db() as c:
                c.execute("UPDATE lottery SET jackpot=0, draw_at=? WHERE id=1", (now() + 86400,))
                c.execute("DELETE FROM lottery_tickets")

            with db() as c:
                c.execute("SELECT name FROM users WHERE id=?", (winner,))
                row = c.fetchone()
            name = row["name"] if row else f"#{winner}"

            send_alert(
                f"🏆 <b>Лотерея!</b> Победитель: <b>{name}</b>\n"
                f"Выигрыш: <b>{fmt(jackpot)} {CURRENCY}</b>"
            )
            try:
                bot.send_message(winner,
                    f"🎉 <b>Вы выиграли лотерею!</b>\n+{fmt(jackpot)} {CURRENCY}",
                    parse_mode="HTML")
            except Exception:
                pass
        except Exception as e:
            print(f"[lottery] ошибка: {e}")


# ══════════════════════════════════════════════
# 26. ГРУППОВЫЕ КОМАНДЫ
# ══════════════════════════════════════════════
#
# Логика разделения:
#   ГРУППА  — игры командами, топ, баланс, акции
#   ЛИЧКА   — банк, магазин, клан, бонус, профиль, донат
#
# ══════════════════════════════════════════════

GROUP_TYPES = {"group", "supergroup"}

def is_group(msg) -> bool:
    return msg.chat.type in GROUP_TYPES

def group_only(fn):
    """Декоратор — только для групп."""
    def wrapper(msg):
        if not is_group(msg):
            bot.send_message(msg.chat.id,
                "⚠️ Эта команда работает только в группе.")
            return
        fn(msg)
    return wrapper

def get_display_name(msg) -> str:
    u = msg.from_user
    return u.first_name or u.username or str(u.id)

def mention(uid: int, name: str) -> str:
    return f'<a href="tg://user?id={uid}">{name}</a>'


# ── Баланс в чате ──────────────────────────────────────────

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["баланс", "б", "/б", "/баланс"])
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


# ── Топ в чате ─────────────────────────────────────────────

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(топ|/топ|top|/top)(\s+\S+)?$', m.text.lower().strip()))
def group_top(msg):
    parts = msg.text.strip().lower().split()
    cat   = parts[1] if len(parts) > 1 else "баланс"

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

    with db() as c:
        if cat in ("xp", "опыт", "уровень"):
            c.execute("SELECT name, xp FROM users ORDER BY xp DESC LIMIT 10")
            rows  = c.fetchall()
            title = "⭐ Топ по XP"
            val   = lambda r: f"{fmt(r['xp'])} XP"
        elif cat in ("акции", "stocks"):
            c.execute("""SELECT u.name, SUM(p.shares) AS s
                         FROM portfolios p JOIN users u ON u.id=p.user_id
                         WHERE p.ticker=? GROUP BY p.user_id ORDER BY s DESC LIMIT 10""", (TICKER,))
            rows  = c.fetchall()
            title = "📈 Топ акционеров"
            val   = lambda r: f"{r['s']} акций"
        else:
            c.execute("SELECT name, balance FROM users ORDER BY balance DESC LIMIT 10")
            rows  = c.fetchall()
            title = "💵 Топ по балансу"
            val   = lambda r: fmt(r['balance'])

    if not rows:
        bot.reply_to(msg, "Список пуст.")
        return

    lines = [f"{medals[i]} <b>{r['name'] or 'Игрок'}</b> — {val(r)}"
             for i, r in enumerate(rows)]
    bot.send_message(msg.chat.id,
        f"<b>{title}</b>\n━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
        parse_mode="HTML")


# ── Акции в чате ───────────────────────────────────────────

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["акции", "биржа", "акция"])
def group_stocks(msg):
    st    = get_stock()
    price = st["price"]
    prev  = st["prev_price"]
    chg   = (price - prev) / prev * 100
    arrow = "📈" if chg >= 0 else "📉"

    history = stock_trend(n=8)
    mini = ""
    if len(history) >= 2:
        prices = [r["price"] for r in history]
        lo, hi = min(prices), max(prices)
        for p in prices:
            if hi == lo:       mini += "━"
            elif p >= hi*0.8:  mini += "▲"
            elif p <= lo*1.2:  mini += "▼"
            else:              mini += "─"

    bot.send_message(msg.chat.id,
        f"<b>📈 Биржа — {TICKER}</b>\n"
        f"Цена: <b>{fmt(price)} {CURRENCY}</b>  {arrow} {chg:+.1f}%\n"
        f"<code>{mini}</code>\n\n"
        f"Купить/продать: пиши боту в <b>личку</b> → меню → 📈 Биржа",
        parse_mode="HTML")


# ── Рулетка в чате ─────────────────────────────────────────
# Формат: рул красное 1000 | рул чёрное 500 | рул зеро 200

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(рул|рулетка)\s+\S+\s+\d+', m.text.lower().strip()))
def group_roulette(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)

    parts = msg.text.strip().lower().split()
    color_input = parts[1]
    try:
        bet = int(parts[2])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: рул красное 1000")
        return

    if bet <= 0:
        bot.reply_to(msg, "❌ Ставка должна быть > 0")
        return

    color_map = {
        "красное": "red", "красный": "red", "red": "red", "r": "red",
        "чёрное": "black", "черное": "black", "black": "black", "b": "black",
        "зеро": "zero", "зеленое": "zero", "0": "zero", "zero": "zero",
    }
    color = color_map.get(color_input)
    if not color:
        bot.reply_to(msg, "❌ Цвет: красное / чёрное / зеро")
        return

    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg,
            f"❌ Недостаточно средств. Баланс: <b>{fmt(u['balance'])}</b>",
            parse_mode="HTML")
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


# ── Кости в чате ───────────────────────────────────────────
# Формат: кости 500 | куб 1000

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
        bot.reply_to(msg, "❌ Ставка должна быть > 0")
        return

    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg,
            f"❌ Баланс: <b>{fmt(u['balance'])}</b>",
            parse_mode="HTML")
        return

    add_balance(uid, -bet)
    sent = bot.send_dice(msg.chat.id, emoji="🎲",
                         reply_to_message_id=msg.message_id)
    time.sleep(3)
    val = sent.dice.value

    if val >= 4:
        win = int(bet * 2)
        add_balance(uid, win)
        bot.send_message(msg.chat.id,
            f"🎲 {mention(uid, name)} бросил <b>{val}</b> — победа!\n"
            f"💰 +<b>{fmt(win - bet)} {CURRENCY}</b>",
            parse_mode="HTML")
    else:
        bot.send_message(msg.chat.id,
            f"🎲 {mention(uid, name)} бросил <b>{val}</b> — мимо.\n"
            f"💸 -<b>{fmt(bet)} {CURRENCY}</b>",
            parse_mode="HTML")


# ── Слоты в чате ───────────────────────────────────────────
# Формат: слот 500

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
            f"🎰 {mention(uid, name)}: {combo}\n"
            f"🎉 Джекпот x{mult}! +<b>{fmt(win - bet)} {CURRENCY}</b>",
            parse_mode="HTML")
    else:
        add_balance(uid, -bet)
        bot.send_message(msg.chat.id,
            f"🎰 {mention(uid, name)}: {combo}\n"
            f"😔 Нет совпадений. -<b>{fmt(bet)} {CURRENCY}</b>",
            parse_mode="HTML")


# ── Дуэль в чате ───────────────────────────────────────────
# Формат: дуэль 1000 @username (или reply)

_duels: dict[int, dict] = {}   # challenger_uid -> {bet, opponent, msg_id, chat_id}

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(дуэль|дуэл|duel)\s+\d+', m.text.lower().strip()))
def group_duel_create(msg):
    uid      = msg.from_user.id
    cname    = get_display_name(msg)
    ensure_user(uid, cname)

    parts = msg.text.strip().split()
    try:
        bet = int(parts[1])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: дуэль 1000 @username или reply")
        return

    if bet <= 0:
        bot.reply_to(msg, "❌ Ставка > 0")
        return

    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}")
        return

    # Определяем соперника
    opponent_uid  = None
    opponent_name = "любой игрок"

    if msg.reply_to_message and msg.reply_to_message.from_user:
        ru = msg.reply_to_message.from_user
        if ru.id == uid:
            bot.reply_to(msg, "❌ Нельзя вызвать самого себя")
            return
        opponent_uid  = ru.id
        opponent_name = ru.first_name or ru.username or str(ru.id)
        ensure_user(opponent_uid, opponent_name)
    elif len(parts) > 2:
        uname = parts[2].lstrip("@")
        with db() as c:
            c.execute("SELECT id, name FROM users WHERE name LIKE ?", (f"%{uname}%",))
            row = c.fetchone()
        if row:
            opponent_uid  = row["id"]
            opponent_name = row["name"]

    if uid in _duels:
        bot.reply_to(msg, "❌ У тебя уже есть активная дуэль")
        return

    _duels[uid] = {
        "bet": bet, "opponent": opponent_uid,
        "cname": cname, "oname": opponent_name,
        "chat_id": msg.chat.id, "expires": now() + 60
    }

    target_str = mention(opponent_uid, opponent_name) if opponent_uid else "<b>любой игрок</b>"

    kb_duel = InlineKeyboardMarkup()
    kb_duel.add(InlineKeyboardButton("⚔️ Принять дуэль", callback_data=f"duel_accept_{uid}"))

    sent = bot.send_message(msg.chat.id,
        f"⚔️ {mention(uid, cname)} вызывает {target_str} на дуэль!\n"
        f"Ставка: <b>{fmt(bet)} {CURRENCY}</b>\n"
        f"⏱ 60 секунд на принятие",
        parse_mode="HTML", reply_markup=kb_duel)

    _duels[uid]["msg_id"] = sent.message_id

    # Автоотмена через 60 сек
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
    opponent_uid   = call.from_user.id
    oname          = get_display_name(call.message) if False else (
        call.from_user.first_name or call.from_user.username or str(opponent_uid))

    duel = _duels.get(challenger_uid)
    if not duel:
        bot.answer_callback_query(call.id, "Дуэль уже завершена или отменена")
        return

    if duel["expires"] < now():
        _duels.pop(challenger_uid, None)
        bot.answer_callback_query(call.id, "Время вышло")
        return

    if challenger_uid == opponent_uid:
        bot.answer_callback_query(call.id, "❌ Нельзя принять свою дуэль")
        return

    if duel["opponent"] and duel["opponent"] != opponent_uid:
        bot.answer_callback_query(call.id, "❌ Эта дуэль не для тебя")
        return

    ensure_user(opponent_uid, oname)
    bet   = duel["bet"]
    cname = duel["cname"]
    chat_id = duel["chat_id"]

    ou = get_user(opponent_uid)
    cu = get_user(challenger_uid)

    if ou["balance"] < bet:
        bot.answer_callback_query(call.id, f"❌ Нужно {fmt(bet)}, у тебя {fmt(ou['balance'])}", show_alert=True)
        return
    if cu["balance"] < bet:
        bot.answer_callback_query(call.id, "❌ У организатора недостаточно средств", show_alert=True)
        _duels.pop(challenger_uid, None)
        return

    _duels.pop(challenger_uid, None)
    bot.answer_callback_query(call.id)

    # Бросаем кости
    c_roll = random.randint(1, 6)
    o_roll = random.randint(1, 6)
    rerolls = 0
    while c_roll == o_roll and rerolls < 5:
        c_roll = random.randint(1, 6)
        o_roll = random.randint(1, 6)
        rerolls += 1

    if c_roll > o_roll:
        winner_uid, winner_name = challenger_uid, cname
        loser_uid,  loser_name  = opponent_uid,   oname
    else:
        winner_uid, winner_name = opponent_uid,   oname
        loser_uid,  loser_name  = challenger_uid, cname

    add_balance(loser_uid,  -bet)
    add_balance(winner_uid,  bet)

    try:
        bot.edit_message_text(
            f"⚔️ <b>Дуэль!</b>\n"
            f"{mention(challenger_uid, cname)}: 🎲 <b>{c_roll}</b>\n"
            f"{mention(opponent_uid, oname)}: 🎲 <b>{o_roll}</b>\n\n"
            f"🏆 Победил {mention(winner_uid, winner_name)}!\n"
            f"💰 +<b>{fmt(bet)} {CURRENCY}</b>",
            chat_id, call.message.message_id, parse_mode="HTML")
    except Exception:
        bot.send_message(chat_id,
            f"🏆 Победил {mention(winner_uid, winner_name)}! +{fmt(bet)} {CURRENCY}",
            parse_mode="HTML")


# ── КНБ в чате ─────────────────────────────────────────────
# Формат: кнб 500 (reply на сообщение соперника)

_knb_games: dict[int, dict] = {}   # challenger -> {bet, opponent, choices}

KNB_BEATS = {"камень": "ножницы", "ножницы": "бумага", "бумага": "камень"}
KNB_EMOJI = {"камень": "🪨", "ножницы": "✂️", "бумага": "📄"}

@bot.message_handler(func=lambda m: is_group(m) and m.reply_to_message and
    m.text and re.match(r'^(кнб|рпс|knb)\s+\d+', m.text.lower().strip()))
def group_knb_create(msg):
    uid  = msg.from_user.id
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
    oname = ru.first_name or ru.username or str(ouid)
    ensure_user(ouid, oname)

    game_id = uid
    _knb_games[game_id] = {
        "bet": bet, "chat": msg.chat.id,
        "c_uid": uid, "c_name": cname, "c_choice": None,
        "o_uid": ouid, "o_name": oname, "o_choice": None,
        "expires": now() + 120
    }

    kb_knb = InlineKeyboardMarkup(row_width=3)
    kb_knb.add(
        InlineKeyboardButton("🪨", callback_data=f"knb_{game_id}_камень"),
        InlineKeyboardButton("✂️", callback_data=f"knb_{game_id}_ножницы"),
        InlineKeyboardButton("📄", callback_data=f"knb_{game_id}_бумага"),
    )

    bot.send_message(msg.chat.id,
        f"🪨✂️📄 <b>КНБ!</b>\n"
        f"{mention(uid, cname)} vs {mention(ouid, oname)}\n"
        f"Ставка: <b>{fmt(bet)} {CURRENCY}</b>\n\n"
        f"Оба выбирают втайне 👇",
        parse_mode="HTML", reply_markup=kb_knb)


@bot.callback_query_handler(func=lambda c: re.match(r'^knb_\d+_\S+$', c.data))
def cb_knb_choice(call):
    parts    = call.data.split("_")
    game_id  = int(parts[1])
    choice   = parts[2]
    uid      = call.from_user.id

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

    # Оба выбрали — определяем победителя
    if not game["c_choice"] or not game["o_choice"]:
        return

    _knb_games.pop(game_id, None)
    cc, oc = game["c_choice"], game["o_choice"]
    bet    = game["bet"]

    if cc == oc:
        result = (f"🤝 Ничья!\n"
                  f"{KNB_EMOJI[cc]} vs {KNB_EMOJI[oc]}\n"
                  f"Ставки возвращены.")
    elif KNB_BEATS[cc] == oc:
        add_balance(game["o_uid"], -bet)
        add_balance(game["c_uid"],  bet)
        result = (f"🏆 Победил {mention(game['c_uid'], game['c_name'])}!\n"
                  f"{KNB_EMOJI[cc]} бьёт {KNB_EMOJI[oc]}\n"
                  f"💰 +<b>{fmt(bet)} {CURRENCY}</b>")
    else:
        add_balance(game["c_uid"], -bet)
        add_balance(game["o_uid"],  bet)
        result = (f"🏆 Победил {mention(game['o_uid'], game['o_name'])}!\n"
                  f"{KNB_EMOJI[oc]} бьёт {KNB_EMOJI[cc]}\n"
                  f"💰 +<b>{fmt(bet)} {CURRENCY}</b>")

    try:
        bot.edit_message_text(
            f"🪨✂️📄 <b>КНБ — результат</b>\n"
            f"{mention(game['c_uid'], game['c_name'])}: {KNB_EMOJI[cc]}\n"
            f"{mention(game['o_uid'], game['o_name'])}: {KNB_EMOJI[oc]}\n\n"
            f"{result}",
            game["chat"], call.message.message_id, parse_mode="HTML")
    except Exception:
        bot.send_message(game["chat"], result, parse_mode="HTML")


# ── Чек (перевод через код) в чате ─────────────────────────
# Формат: чек 5000 — создаёт код, любой может активировать

_checks: dict[str, dict] = {}   # code -> {amount, uid, uses}

@bot.message_handler(func=lambda m: m.text and
    re.match(r'^чек\s+\d+(\s+\d+)?$', m.text.lower().strip()))
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
        bot.reply_to(msg, "❌ Значения должны быть > 0")
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
        f"Код: <code>{code}</code>\n"
        f"Активировать: <code>активировать {code}</code>",
        parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and
    re.match(r'^активировать\s+\S+', m.text.lower().strip()))
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

    if chk["left"] <= 0:
        _checks.pop(code, None)
        extra = " — чек исчерпан"
    else:
        extra = f" — осталось {chk['left']} активаций"

    bot.reply_to(msg,
        f"✅ {mention(uid, name)} активировал чек!\n"
        f"💰 +<b>{fmt(amount)} {CURRENCY}</b>{extra}",
        parse_mode="HTML")


# ── Перевод в чате ─────────────────────────────────────────
# Формат: дать @username 5000 | reply + дать 5000

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(дать|перевод|pay)\s+', m.text.lower().strip()))
def group_transfer(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)

    parts = msg.text.strip().split()
    to_uid, to_name, amount = None, None, None

    # Reply
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
        # дать @username 5000
        if len(parts) < 3:
            bot.reply_to(msg, "❌ Формат: дать @username 5000")
            return
        uname = parts[1].lstrip("@")
        with db() as c:
            c.execute("SELECT id, name FROM users WHERE name LIKE ?", (f"%{uname}%",))
            row = c.fetchone()
        if not row:
            bot.reply_to(msg, "❌ Игрок не найден")
            return
        to_uid  = row["id"]
        to_name = row["name"]
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

    add_balance(uid,   -total)
    add_balance(to_uid, amount)
    with db() as c:
        c.execute("INSERT INTO transfers (from_id,to_id,amount,fee,ts) VALUES (?,?,?,?,?)",
                  (uid, to_uid, amount, fee, now()))

    bot.send_message(msg.chat.id,
        f"💸 {mention(uid, name)} → {mention(to_uid, to_name)}\n"
        f"<b>{fmt(amount)} {CURRENCY}</b>  (комиссия {fmt(fee)})",
        parse_mode="HTML")


# ── Краш в чате ────────────────────────────────────────────
# Формат: краш 1000

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

    crash_at = max(1.01, random.expovariate(0.8) + 1.0)
    crash_at = round(crash_at, 2)
    add_balance(uid, -bet)

    kb_crash = InlineKeyboardMarkup(row_width=3)
    kb_crash.add(
        InlineKeyboardButton(f"💰 x1.5", callback_data=f"grcrash_{uid}_1.5_{bet}_{crash_at}"),
        InlineKeyboardButton(f"💰 x2.0", callback_data=f"grcrash_{uid}_2.0_{bet}_{crash_at}"),
        InlineKeyboardButton(f"💰 x3.0", callback_data=f"grcrash_{uid}_3.0_{bet}_{crash_at}"),
    )

    bot.send_message(msg.chat.id,
        f"⚡ {mention(uid, name)} запустил краш!\n"
        f"Ставка: <b>{fmt(bet)} {CURRENCY}</b>\n"
        f"Множитель растёт... Успей забрать!",
        parse_mode="HTML", reply_markup=kb_crash)


@bot.callback_query_handler(func=lambda c: c.data.startswith("grcrash_"))
def cb_group_crash(call):
    parts     = call.data.split("_")
    owner_uid = int(parts[1])
    mult      = float(parts[2])
    bet       = int(parts[3])
    crash_at  = float(parts[4])
    uid       = call.from_user.id

    if uid != owner_uid:
        bot.answer_callback_query(call.id, "Это не твоя игра")
        return

    name = call.from_user.first_name or str(uid)

    if mult <= crash_at:
        win = int(bet * mult)
        add_balance(uid, win)
        result = f"✅ {mention(uid, name)} забрал x{mult}! +<b>{fmt(win)} {CURRENCY}</b>"
    else:
        result = f"💥 Краш на x{crash_at}! {mention(uid, name)} проиграл -{fmt(bet)}"

    try:
        bot.edit_message_text(result, call.message.chat.id, call.message.message_id,
                              parse_mode="HTML")
    except Exception:
        bot.send_message(call.message.chat.id, result, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ── Лотерея в чате ─────────────────────────────────────────
# Формат: лот 3 — купить 3 билета

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r'^(лот|лотерея)\s+\d+', m.text.lower().strip()))
def group_lottery(msg):
    uid  = msg.from_user.id
    name = get_display_name(msg)
    ensure_user(uid, name)

    try:
        qty = int(msg.text.strip().split()[1])
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Формат: лот 3 (количество билетов)")
        return

    qty  = max(1, min(qty, 100))
    cost = TICKET_PRICE * qty
    u = get_user(uid)
    if u["balance"] < cost:
        bot.reply_to(msg, f"❌ Нужно {fmt(cost)}, баланс: {fmt(u['balance'])}")
        return

    add_balance(uid, -cost)
    with db() as c:
        c.execute("""INSERT INTO lottery_tickets (user_id, tickets) VALUES (?,?)
                     ON CONFLICT(user_id) DO UPDATE SET tickets=tickets+?""",
                  (uid, qty, qty))
        c.execute("UPDATE lottery SET jackpot=jackpot+? WHERE id=1", (cost,))
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        jackpot = c.fetchone()["jackpot"]

    bot.reply_to(msg,
        f"🎟 {mention(uid, name)} купил <b>{qty}</b> билетов!\n"
        f"Джекпот: <b>{fmt(jackpot)} {CURRENCY}</b>",
        parse_mode="HTML")


# ── Помощь по групповым командам ───────────────────────────

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["/help", "помощь", "/помощь"])
def group_help(msg):
    bot.send_message(msg.chat.id,
        "<b>📋 Команды бота</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        "<b>Инфо</b>\n"
        "баланс — твой кошелёк\n"
        "топ — топ по балансу\n"
        "топ xp — топ по уровню\n"
        "топ акции — топ акционеров\n"
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
        "дать 5000 (reply на сообщение)\n"
        "чек 5000 — создать чек\n"
        "активировать КОД — использовать чек\n\n"
        "<b>Промокод</b>\n"
        "промо КОД\n\n"
        "Банк, магазин, клан — в <b>личке</b> → /start",
        parse_mode="HTML")


# ══════════════════════════════════════════════
# 27. ЗАПУСК
# ══════════════════════════════════════════════

if __name__ == "__main__":
    init_db()

    threading.Thread(target=_stock_scheduler,  daemon=True).start()
    threading.Thread(target=_lottery_scheduler, daemon=True).start()

    print("🚀 Бот запущен")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)

