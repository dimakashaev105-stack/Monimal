"""
╔══════════════════════════════════════╗
║   🌸  FECTIZ BOT  —  v5.0           ║
║   PostgreSQL · Чаты + ЛС · Inline  ║
╚══════════════════════════════════════╝
"""

import os, time, math, random, threading
from contextlib import contextmanager
from datetime import datetime
import psycopg2, psycopg2.pool, psycopg2.extras
from dotenv import load_dotenv
from telebot import TeleBot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, LabeledPrice
)
from http.server import HTTPServer, BaseHTTPRequestHandler

load_dotenv()

# ═══════════════════════════════════════════════════════════════
# 0. КОНФИГ
# ═══════════════════════════════════════════════════════════════

TOKEN        = os.getenv("BOT_TOKEN")
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")
PORT         = int(os.environ.get("PORT", 8080))

CURRENCY = "🌸"
TICKER   = "FECTZ"

CD_CLICK         = 5
CD_DAILY_NORMAL  = 1200
CD_DAILY_PREMIUM = 600
CD_MINE          = 3600
CD_TRANSFER      = 60
GAME_COOLDOWN    = 3

TRANSFER_FEE         = 0.10
PREMIUM_TRANSFER_FEE = 0.05
CLICK_BASE  = 100
MINE_BASE   = 250
WORK_BASE   = 1500
DAILY_BASE  = 5000
BANK_RATE   = 0.005
LOAN_MAX    = 100_000
LOAN_RATE   = 0.10
LOAN_TERM   = 7

STOCK_PRICE_START = 10_000
STOCK_VOLATILITY  = 0.04
STOCK_MAX_PER_USER= 5_000
STOCK_SELL_FEE    = 0.03
STOCK_COOLDOWN    = 600
STOCK_UPDATE_SEC  = 1800

LOTTERY_TICKET_PRICE = 500
LOTTERY_INTERVAL     = 86400

TAXI_ROUTES = [
    {"name": "📍 Центр → Аэропорт",       "base": 1500, "time": 5},
    {"name": "🏠 Жилой р-н → Офис",        "base": 1000, "time": 4},
    {"name": "🎓 Университет → ТЦ",         "base": 800,  "time": 3},
    {"name": "🏥 Больница → Вокзал",        "base": 1200, "time": 4},
    {"name": "🏢 Бизнес-центр → Ресторан", "base": 600,  "time": 3},
    {"name": "🌃 Ночной рейс",              "base": 2000, "time": 6},
    {"name": "🚄 Вокзал → Гостиница",       "base": 400,  "time": 3},
]

SLOT_SYMBOLS = ["🍒","🍋","🍊","🍇","⭐","💎","🔔","7️⃣"]
SLOT_PAYOUTS = {
    "💎💎💎": 10, "⭐⭐⭐": 7, "7️⃣7️⃣7️⃣": 5,
    "🍇🍇🍇": 4,  "🍊🍊🍊": 3, "🍋🍋🍋": 2,
    "🍒🍒🍒": 1.5,"🔔🔔🔔": 2.5,
}

RED_NUMBERS = [1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]

DONATE_PACKAGES = {
    "s1":   {"stars": 1,   "amount": 10_000,   "label": "⭐ 10 000"},
    "s5":   {"stars": 5,   "amount": 66_000,   "label": "⭐ 66 000"},
    "s15":  {"stars": 15,  "amount": 266_000,  "label": "🔥 266 000"},
    "s50":  {"stars": 50,  "amount": 1_000_000,"label": "🔥 1 000 000"},
    "s150": {"stars": 150, "amount": 4_000_000,"label": "💎 4 000 000"},
    "s250": {"stars": 250, "amount": 8_000_000,"label": "💎 8 000 000"},
}

# ═══════════════════════════════════════════════════════════════
# 1. HTTP-СЕРВЕР ДЛЯ RENDER
# ═══════════════════════════════════════════════════════════════

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header("Content-type","text/plain")
        self.end_headers(); self.wfile.write(b"OK")
    def log_message(self, *a): pass

def run_http():
    HTTPServer(("0.0.0.0", PORT), HealthHandler).serve_forever()

threading.Thread(target=run_http, daemon=True).start()

# ═══════════════════════════════════════════════════════════════
# 2. БОТ
# ═══════════════════════════════════════════════════════════════

_temp = TeleBot(TOKEN)
try:
    _temp.delete_webhook(); print("✅ Webhook удалён")
except Exception as e:
    print(f"⚠️ {e}")

bot = TeleBot(TOKEN, threaded=True, num_threads=8)

# ═══════════════════════════════════════════════════════════════
# 3. БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════

_pg_pool = None

def get_pg_pool():
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=10,
            dsn=DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    return _pg_pool

@contextmanager
def db():
    pool = get_pg_pool()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); pool.putconn(conn)

def init_db():
    with db() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            BIGINT PRIMARY KEY,
            name          TEXT DEFAULT '',
            balance       BIGINT DEFAULT 0,
            bank          BIGINT DEFAULT 0,
            xp            BIGINT DEFAULT 0,
            daily_streak  INTEGER DEFAULT 0,
            last_click    BIGINT DEFAULT 0,
            last_daily    BIGINT DEFAULT 0,
            last_work     BIGINT DEFAULT 0,
            last_mine     BIGINT DEFAULT 0,
            last_transfer BIGINT DEFAULT 0,
            click_power   INTEGER DEFAULT 100,
            total_earned  BIGINT DEFAULT 0,
            premium_until BIGINT DEFAULT 0,
            video_cards   INTEGER DEFAULT 0,
            games_won     INTEGER DEFAULT 0,
            games_lost    INTEGER DEFAULT 0,
            total_won     BIGINT DEFAULT 0,
            total_lost    BIGINT DEFAULT 0,
            created_at    BIGINT DEFAULT 0,
            ref_code      TEXT UNIQUE,
            ref_by        BIGINT DEFAULT 0,
            registered    BOOLEAN DEFAULT FALSE
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS loans (
            user_id BIGINT PRIMARY KEY,
            amount  BIGINT DEFAULT 0,
            due_at  BIGINT DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            ticker     TEXT PRIMARY KEY,
            price      BIGINT DEFAULT 10000,
            prev_price BIGINT DEFAULT 10000,
            updated_at BIGINT DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock_portfolio (
            user_id    BIGINT,
            ticker     TEXT,
            shares     INTEGER DEFAULT 0,
            avg_buy    BIGINT DEFAULT 0,
            last_trade BIGINT DEFAULT 0,
            PRIMARY KEY (user_id, ticker)
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock_history (
            id SERIAL PRIMARY KEY, ticker TEXT,
            price BIGINT, ts BIGINT DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS lottery (
            id INTEGER PRIMARY KEY DEFAULT 1,
            jackpot BIGINT DEFAULT 0,
            draw_at BIGINT DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS lottery_tickets (
            user_id BIGINT PRIMARY KEY,
            tickets INTEGER DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id SERIAL PRIMARY KEY,
            from_id BIGINT, to_id BIGINT,
            amount BIGINT, fee BIGINT,
            ts BIGINT DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS game_history (
            id SERIAL PRIMARY KEY,
            user_id BIGINT, game TEXT,
            bet BIGINT, win BIGINT,
            result TEXT, ts BIGINT DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock_trades (
            id SERIAL PRIMARY KEY,
            user_id BIGINT, ticker TEXT,
            action TEXT, amount INTEGER,
            price BIGINT, fee BIGINT DEFAULT 0,
            total BIGINT, created_at BIGINT DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            reward BIGINT, max_uses INTEGER DEFAULT 1,
            uses INTEGER DEFAULT 0,
            expires BIGINT DEFAULT 0,
            active BOOLEAN DEFAULT TRUE
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT, code TEXT,
            ts BIGINT DEFAULT 0,
            PRIMARY KEY (user_id, code)
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS checks (
            code TEXT PRIMARY KEY,
            amount BIGINT,
            max_activations INTEGER,
            current_activations INTEGER DEFAULT 0,
            password TEXT,
            created_by BIGINT,
            created_at BIGINT DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS check_activations (
            user_id BIGINT, check_code TEXT,
            activated_at BIGINT DEFAULT 0,
            PRIMARY KEY (user_id, check_code)
        )""")
        # Добавляем колонку registered если нет
        c.execute("""
        ALTER TABLE users ADD COLUMN IF NOT EXISTS registered BOOLEAN DEFAULT FALSE
        """)
        next_draw = now() + LOTTERY_INTERVAL
        c.execute(
            "INSERT INTO lottery (id,jackpot,draw_at) VALUES (1,0,%s) ON CONFLICT DO NOTHING",
            (next_draw,)
        )
        c.execute(
            "INSERT INTO stocks (ticker,price,prev_price,updated_at) VALUES (%s,10000,10000,0) ON CONFLICT DO NOTHING",
            (TICKER,)
        )
    print("✅ БД инициализирована")

# ═══════════════════════════════════════════════════════════════
# 4. УТИЛИТЫ
# ═══════════════════════════════════════════════════════════════

def fmt(n) -> str:
    n = int(n or 0)
    if n >= 1_000_000_000: return f"{n/1_000_000_000:.1f}B"
    if n >= 1_000_000:     return f"{n/1_000_000:.1f}M"
    if n >= 1_000:         return f"{n:,}".replace(",", " ")
    return str(n)

def now() -> int: return int(time.time())

def cd_str(sec: int) -> str:
    if sec <= 0: return "готово"
    h, r = divmod(int(sec), 3600)
    m, s = divmod(r, 60)
    if h: return f"{h}ч {m}м"
    if m: return f"{m}м {s}с"
    return f"{s}с"

def is_admin(uid: int) -> bool: return uid in ADMIN_IDS

def is_premium(uid: int) -> bool:
    with db() as c:
        c.execute("SELECT premium_until FROM users WHERE id=%s", (uid,))
        r = c.fetchone()
        return bool(r and r["premium_until"] > now())

def get_user(uid: int):
    with db() as c:
        c.execute("SELECT * FROM users WHERE id=%s", (uid,))
        return c.fetchone()

def ensure_user(uid: int, name: str = ""):
    with db() as c:
        c.execute(
            "INSERT INTO users (id,name,created_at) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (uid, name, now())
        )
        c.execute("SELECT ref_code FROM users WHERE id=%s", (uid,))
        r = c.fetchone()
        if r and not r["ref_code"]:
            c.execute("UPDATE users SET ref_code=%s WHERE id=%s", (f"REF{uid}", uid))
    return get_user(uid)

def update_balance(uid: int, amount: int):
    with db() as c:
        c.execute(
            "UPDATE users SET balance=balance+%s, total_earned=total_earned+GREATEST(0,%s) WHERE id=%s",
            (amount, amount, uid)
        )

def add_xp(uid: int, xp: int):
    with db() as c:
        c.execute("UPDATE users SET xp=xp+%s WHERE id=%s", (xp, uid))

def user_level(xp) -> int:
    return max(1, int(math.sqrt(int(xp or 0) / 100)) + 1)

def level_xp(lvl: int) -> int:
    return (lvl - 1) ** 2 * 100

def parse_bet(text: str, balance) -> int | None:
    t = text.lower().strip().replace(" ", "")
    bal = int(balance or 0)
    if t in ["все", "all", "всё"]: return bal
    if t.endswith("к") and t[:-1].replace(".", "").isdigit():
        return int(float(t[:-1]) * 1000)
    if t.endswith("м") and t[:-1].replace(".", "").isdigit():
        return int(float(t[:-1]) * 1_000_000)
    try: return int(t)
    except: return None

def record_game(uid: int, game: str, bet: int, win: int, result: str):
    with db() as c:
        c.execute(
            "INSERT INTO game_history (user_id,game,bet,win,result,ts) VALUES (%s,%s,%s,%s,%s,%s)",
            (uid, game, bet, win, result, now())
        )
        if win > 0:
            c.execute("UPDATE users SET games_won=games_won+1, total_won=total_won+%s WHERE id=%s", (win, uid))
        else:
            c.execute("UPDATE users SET games_lost=games_lost+1, total_lost=total_lost+%s WHERE id=%s", (bet, uid))

def is_group(msg) -> bool:
    return msg.chat.type in ["group", "supergroup"]

def reply(msg, text, markup=None, **kw):
    """Отправить в тот же чат (ЛС или группа)."""
    return bot.send_message(msg.chat.id, text, reply_markup=markup, **kw)

def need_reg(uid: int) -> bool:
    """Проверить — зарегистрирован ли пользователь."""
    u = get_user(uid)
    return not u or not u.get("registered")

# ═══════════════════════════════════════════════════════════════
# 5. КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════

def main_menu() -> ReplyKeyboardMarkup:
    """Компактное главное меню."""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
    kb.row(KeyboardButton("👤 Я"), KeyboardButton("💰 Баланс"), KeyboardButton("🎁 Бонус"))
    kb.row(KeyboardButton("⚒️ Работа"), KeyboardButton("🎰 Игры"), KeyboardButton("🏦 Банк"))
    kb.row(KeyboardButton("📈 Биржа"), KeyboardButton("🏆 Топ"), KeyboardButton("📋 Ещё"))
    return kb

def more_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.row(KeyboardButton("💎 Донат"), KeyboardButton("🔗 Реферал"))
    kb.row(KeyboardButton("📞 Помощь"), KeyboardButton("💸 Переводы"))
    kb.row(KeyboardButton("🏠 Главное меню"))
    return kb

def work_inline(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👆 Кликер", callback_data=f"work_click_{uid}"),
        InlineKeyboardButton("⛏️ Майнинг", callback_data=f"work_mine_{uid}"),
        InlineKeyboardButton("🚗 Такси", callback_data=f"work_taxi_{uid}"),
    )
    return kb

def games_inline(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    games = [
        ("🎲 Кости",    f"game_info_dice_{uid}"),
        ("🎰 Слоты",    f"game_info_slots_{uid}"),
        ("🎡 Рулетка",  f"game_info_roulette_{uid}"),
        ("💣 Мины",     f"game_info_mines_{uid}"),
        ("🚀 Краш",     f"game_info_crash_{uid}"),
        ("🎟️ Лотерея",  f"game_info_lottery_{uid}"),
        ("🎯 Дартс",    f"game_info_darts_{uid}"),
        ("🎳 Боулинг",  f"game_info_bowling_{uid}"),
        ("🏀 Баскет",   f"game_info_basketball_{uid}"),
        ("⚽ Футбол",   f"game_info_football_{uid}"),
    ]
    for name, cb in games:
        kb.add(InlineKeyboardButton(name, callback_data=cb))
    return kb

def bank_inline(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📥 Вклад", callback_data=f"bank_dep_{uid}"),
        InlineKeyboardButton("📤 Снять", callback_data=f"bank_wd_{uid}"),
        InlineKeyboardButton("💳 Кредит", callback_data=f"bank_loan_{uid}"),
        InlineKeyboardButton("✅ Погасить", callback_data=f"bank_repay_{uid}"),
    )
    return kb

def balance_inline(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
        InlineKeyboardButton("🔄 Обновить", callback_data=f"bal_refresh_{uid}"),
        InlineKeyboardButton("📊 Профиль", callback_data=f"bal_profile_{uid}"),
        InlineKeyboardButton("💸 Перевод", callback_data=f"bal_transfer_{uid}"),
    )
    return kb

# ═══════════════════════════════════════════════════════════════
# 6. РЕГИСТРАЦИЯ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    uid  = msg.from_user.id
    name = msg.from_user.first_name or "Игрок"

    parts = msg.text.split()
    param = parts[1] if len(parts) > 1 else None

    # Активация чека через /start CHKxxxxxx
    if param and param.upper().startswith("CHK"):
        u = ensure_user(uid, name)
        if need_reg(uid):
            bot.send_message(
                msg.chat.id,
                f"👋 <b>Привет, {name}!</b>\nЧтобы активировать чек, сначала зарегистрируйся!\n\nНажми /start без параметров.",
                parse_mode="HTML"
            )
            return
        _activate_check_by_code(uid, param.upper(), msg.chat.id)
        return

    # Уже зарегистрирован
    u = get_user(uid)
    if u and u.get("registered"):
        # Реферал
        if param and not u["ref_by"] and param != str(uid):
            with db() as c:
                c.execute("SELECT id FROM users WHERE ref_code=%s", (param,))
                row = c.fetchone()
                if row:
                    ref_uid = row["id"]
                    update_balance(uid, 1000)
                    update_balance(ref_uid, 2000)
                    add_xp(ref_uid, 500)
                    c.execute("UPDATE users SET ref_by=%s WHERE id=%s", (ref_uid, uid))
                    try:
                        bot.send_message(ref_uid, f"🎉 По вашей ссылке пришёл новый игрок!\n+2 000 {CURRENCY}", parse_mode="HTML")
                    except: pass
        bot.send_message(
            msg.chat.id,
            f"🌸 <b>С возвращением, {name}!</b>",
            reply_markup=main_menu(), parse_mode="HTML"
        )
        return

    # Не зарегистрирован — показываем приветствие
    ensure_user(uid, name)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🌸 Начать игру!", callback_data=f"register_{uid}_{param or ''}"))
    bot.send_message(
        msg.chat.id,
        f"🌸 <b>Добро пожаловать в FECTIZ!</b>\n\n"
        f"💰 Экономическая игра с:\n"
        f"• Играми на деньги\n"
        f"• Биржей акций\n"
        f"• Банком и кредитами\n"
        f"• Ежедневными бонусами\n"
        f"• Лотереями и чеками\n\n"
        f"Нажми кнопку чтобы начать, <b>{name}!</b>",
        reply_markup=kb, parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda c: c.data.startswith("register_"))
def cb_register(call):
    uid  = call.from_user.id
    parts = call.data.split("_", 2)
    target_uid = int(parts[1])
    ref_param  = parts[2] if len(parts) > 2 else ""

    if uid != target_uid:
        bot.answer_callback_query(call.id, "❌ Это не твоя кнопка")
        return

    with db() as c:
        c.execute("UPDATE users SET registered=TRUE WHERE id=%s", (uid,))

    # Стартовый баланс
    update_balance(uid, 5000)

    # Реферал
    if ref_param and ref_param != str(uid):
        with db() as c:
            c.execute("SELECT id FROM users WHERE ref_code=%s", (ref_param,))
            row = c.fetchone()
            if row:
                ref_uid = row["id"]
                update_balance(uid, 1000)
                update_balance(ref_uid, 2000)
                add_xp(ref_uid, 500)
                c.execute("UPDATE users SET ref_by=%s WHERE id=%s", (ref_uid, uid))
                try:
                    bot.send_message(ref_uid, f"🎉 Новый игрок по вашей ссылке!\n+2 000 {CURRENCY}", parse_mode="HTML")
                except: pass

    bot.edit_message_text(
        f"✅ <b>Регистрация прошла успешно!</b>\n\n"
        f"💰 На твой счёт зачислено: <b>5 000 {CURRENCY}</b>\n\n"
        f"Используй меню ниже для навигации 👇",
        call.message.chat.id, call.message.message_id,
        parse_mode="HTML"
    )
    bot.send_message(
        call.message.chat.id,
        f"🌸 <b>Добро пожаловать, {call.from_user.first_name}!</b>",
        reply_markup=main_menu(), parse_mode="HTML"
    )
    bot.answer_callback_query(call.id, "✅ Зарегистрирован!")

def check_reg(msg) -> bool:
    """Возвращает True если пользователь зарегистрирован, иначе шлёт сообщение."""
    uid = msg.from_user.id
    if need_reg(uid):
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🌸 Зарегистрироваться", url=f"https://t.me/{bot.get_me().username}?start=reg"))
        bot.send_message(
            msg.chat.id,
            "❌ Ты не зарегистрирован!\nНажми /start в личке бота чтобы зарегистрироваться.",
            reply_markup=kb, parse_mode="HTML"
        )
        return False
    return True

# ═══════════════════════════════════════════════════════════════
# 7. ГЛАВНОЕ МЕНЮ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🏠 Главное меню")
def back_to_menu(msg):
    ensure_user(msg.from_user.id, msg.from_user.first_name or "Игрок")
    bot.send_message(msg.chat.id, "🏠 Главное меню:", reply_markup=main_menu())

@bot.message_handler(func=lambda m: m.text == "📋 Ещё")
def cmd_more(msg):
    bot.send_message(msg.chat.id, "📋 Дополнительно:", reply_markup=more_menu())

# ═══════════════════════════════════════════════════════════════
# 8. Я (Профиль)
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text in ["👤 Я", "я", "/me"])
def cmd_me(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    _send_profile(uid, msg.chat.id)

def _send_profile(uid: int, chat_id: int):
    u = get_user(uid)
    lvl      = user_level(u["xp"])
    xp_cur   = int(u["xp"]) - level_xp(lvl)
    xp_need  = level_xp(lvl + 1) - level_xp(lvl)
    bar      = "▓" * int((xp_cur / max(1, xp_need)) * 10) + "░" * (10 - int((xp_cur / max(1, xp_need)) * 10))
    prem     = "💎 Premium" if is_premium(uid) else "👤 Обычный"
    created  = datetime.fromtimestamp(u["created_at"]).strftime("%d.%m.%Y") if u["created_at"] else "—"
    total_g  = int(u["games_won"]) + int(u["games_lost"])
    wr       = f"{int(u['games_won'])/total_g*100:.1f}%" if total_g else "—"
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()
    loan_txt = f"\n⚠️ Кредит: {fmt(loan['amount'])} {CURRENCY}" if loan and loan["amount"] > 0 else ""
    bot.send_message(
        chat_id,
        f"<b>👤 {u['name'] or 'Игрок'}</b>  {prem}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎖 Уровень: <b>{lvl}</b>  [{bar}]\n"
        f"⭐ Опыт: {fmt(u['xp'])} / {fmt(level_xp(lvl+1))}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Баланс: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"📈 Всего заработано: {fmt(u['total_earned'])} {CURRENCY}"
        f"{loan_txt}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎮 Побед: {u['games_won']} | Поражений: {u['games_lost']} | WR: {wr}\n"
        f"🔥 Стрик: {u['daily_streak']} дней\n"
        f"📅 В игре с: {created}",
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda c: c.data.startswith("bal_profile_"))
def cb_profile(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌ Не твой запрос"); return
    _send_profile(uid, call.message.chat.id)
    bot.answer_callback_query(call.id)

# ═══════════════════════════════════════════════════════════════
# 9. БАЛАНС
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text in ["💰 Баланс", "б", "/bal", "/balance"])
def cmd_balance(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    _send_balance(uid, msg.chat.id)

def _send_balance(uid: int, chat_id: int, message_id: int = None):
    u = get_user(uid)
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()
    loan_txt = f"\n⚠️ Кредит: <b>{fmt(loan['amount'])} {CURRENCY}</b>" if loan and loan["amount"] > 0 else ""
    prem_txt = " 💎" if is_premium(uid) else ""
    text = (
        f"<b>💰 Баланс{prem_txt}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👛 Кошелёк: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"🏦 Депозит:  {fmt(u['bank'])} {CURRENCY}\n"
        f"💎 Итого:    {fmt(int(u['balance'])+int(u['bank']))} {CURRENCY}"
        f"{loan_txt}"
    )
    kb = balance_inline(uid)
    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=kb, parse_mode="HTML")
            return
        except: pass
    bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("bal_refresh_"))
def cb_bal_refresh(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌ Не твой запрос"); return
    _send_balance(uid, call.message.chat.id, call.message.message_id)
    bot.answer_callback_query(call.id, "🔄 Обновлено!")

@bot.callback_query_handler(func=lambda c: c.data.startswith("bal_transfer_"))
def cb_bal_transfer(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌ Не твой запрос"); return
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        "💸 Формат перевода:\n<code>перевод @username 5000</code>\nили\n<code>перевод ID 5000</code>",
        parse_mode="HTML"
    )

# ═══════════════════════════════════════════════════════════════
# 10. РАБОТА (inline кнопки)
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "⚒️ Работа")
def cmd_work_menu(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    bot.send_message(
        msg.chat.id,
        "⚒️ <b>Работа</b>\nВыбери чем заняться:",
        reply_markup=work_inline(uid), parse_mode="HTML"
    )

# --- КЛИКЕР ---

@bot.callback_query_handler(func=lambda c: c.data.startswith("work_click_"))
def cb_click(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌ Не твоя кнопка"); return
    u = ensure_user(uid, call.from_user.first_name or "Игрок")
    remaining = CD_CLICK - (now() - int(u["last_click"]))
    if remaining > 0:
        bot.answer_callback_query(call.id, f"⏱ Подожди {cd_str(remaining)}", show_alert=False)
        return
    lvl  = user_level(u["xp"])
    earn = max(50, CLICK_BASE + lvl * 5 + random.randint(-20, 50))
    update_balance(uid, earn)
    add_xp(uid, 5 + lvl // 10)
    with db() as c:
        c.execute("UPDATE users SET last_click=%s WHERE id=%s", (now(), uid))
    # Обновляем кнопку
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"👆 Ещё клик! (+{fmt(earn)} {CURRENCY})", callback_data=f"work_click_{uid}"))
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb)
    except: pass
    bot.answer_callback_query(call.id, f"⚡ +{fmt(earn)} {CURRENCY}!", show_alert=False)

# --- МАЙНИНГ ---

@bot.callback_query_handler(func=lambda c: c.data.startswith("work_mine_"))
def cb_mine(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌ Не твоя кнопка"); return
    u = ensure_user(uid, call.from_user.first_name or "Игрок")
    remaining = CD_MINE - (now() - int(u["last_mine"]))
    if remaining > 0:
        bot.answer_callback_query(call.id, f"⛏️ Через {cd_str(remaining)}", show_alert=True)
        return
    cards = int(u.get("video_cards") or 0)
    earn  = max(100, MINE_BASE + cards * 200 + random.randint(-50, 150))
    update_balance(uid, earn)
    add_xp(uid, 10 + cards // 5)
    with db() as c:
        c.execute("UPDATE users SET last_mine=%s WHERE id=%s", (now(), uid))
    bot.answer_callback_query(call.id, f"⛏️ Намайнено +{fmt(earn)} {CURRENCY}!", show_alert=True)
    # Предложить видеокарту
    u2 = get_user(uid)
    card_price = 5000 * (2 ** cards)
    if int(u2["balance"]) >= card_price:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(f"🖥 Купить карту за {fmt(card_price)}", callback_data=f"buy_card_{uid}"))
        bot.send_message(call.message.chat.id,
            f"⛏️ <b>Майнинг</b>: +{fmt(earn)} {CURRENCY}\n🖥 Видеокарт: {cards}\n\n"
            f"💡 Купи видеокарту за {fmt(card_price)} {CURRENCY}?",
            reply_markup=kb, parse_mode="HTML")
    else:
        bot.send_message(call.message.chat.id,
            f"⛏️ <b>Майнинг</b>: +{fmt(earn)} {CURRENCY}\n🖥 Видеокарт: {cards}",
            parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("buy_card_"))
def cb_buy_card(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌ Не твой запрос"); return
    u = get_user(uid)
    cards = int(u.get("video_cards") or 0)
    price = 5000 * (2 ** cards)
    if int(u["balance"]) < price:
        bot.answer_callback_query(call.id, f"❌ Не хватает {fmt(price)}"); return
    update_balance(uid, -price)
    with db() as c:
        c.execute("UPDATE users SET video_cards=video_cards+1 WHERE id=%s", (uid,))
    bot.answer_callback_query(call.id, f"✅ Куплена видеокарта! Теперь {cards+1} шт.")
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)

# --- ТАКСИ ---

active_rides = {}

@bot.callback_query_handler(func=lambda c: c.data.startswith("work_taxi_"))
def cb_taxi(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌ Не твоя кнопка"); return
    u = ensure_user(uid, call.from_user.first_name or "Игрок")

    if uid in active_rides:
        data = active_rides[uid]
        left = data["time"] * 60 - (now() - data["start"])
        if left > 0:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton(f"✅ Завершить (+{fmt(data['earn'])} {CURRENCY})", callback_data=f"taxi_finish_{uid}"))
            bot.answer_callback_query(call.id)
            bot.send_message(call.message.chat.id,
                f"🚕 Поездка ещё идёт!\n⏱ Осталось: <b>{cd_str(left)}</b>",
                reply_markup=kb, parse_mode="HTML")
            return
        else:
            data2 = active_rides.pop(uid)
            update_balance(uid, data2["earn"])
            add_xp(uid, 30)
            bot.send_message(call.message.chat.id,
                f"🚕 Прошлая поездка завершена!\n💰 +{fmt(data2['earn'])} {CURRENCY}", parse_mode="HTML")

    route = random.choice(TAXI_ROUTES)
    lvl   = user_level(u["xp"])
    earn  = int(route["base"] * random.uniform(0.9, 1.2) * (1 + lvl * 0.01))
    active_rides[uid] = {"route": route, "earn": earn, "start": now(), "time": route["time"]}

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"✅ Завершить (+{fmt(earn)} {CURRENCY})", callback_data=f"taxi_finish_{uid}"))

    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
        f"🚕 <b>Новый заказ!</b>\n\n"
        f"Маршрут: {route['name']}\n"
        f"Время: ~{route['time']} мин\n"
        f"Оплата: <b>{fmt(earn)} {CURRENCY}</b>\n\n"
        f"Нажми кнопку через {route['time']} мин для полной оплаты:",
        reply_markup=kb, parse_mode="HTML")

    def auto_finish():
        time.sleep(route["time"] * 60 + 30)
        if uid in active_rides:
            d = active_rides.pop(uid)
            update_balance(uid, d["earn"])
            add_xp(uid, 30)
            try:
                bot.send_message(uid, f"🚕 <b>Поездка завершена автоматически!</b>\n💰 +{fmt(d['earn'])} {CURRENCY}", parse_mode="HTML")
            except: pass
    threading.Thread(target=auto_finish, daemon=True).start()

@bot.callback_query_handler(func=lambda c: c.data.startswith("taxi_finish_"))
def cb_taxi_finish(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌ Не твоя поездка"); return
    if uid not in active_rides:
        bot.answer_callback_query(call.id, "❌ Поездка не найдена"); return
    data    = active_rides.pop(uid)
    elapsed = now() - data["start"]
    ratio   = elapsed / (data["time"] * 60)
    total   = data["earn"] if ratio >= 1 else int(data["earn"] * max(0.5, ratio))
    update_balance(uid, total)
    add_xp(uid, 30)
    pct = min(100, int(ratio * 100))
    bot.edit_message_text(
        f"🚕 <b>Поездка завершена!</b>\n💰 +{fmt(total)} {CURRENCY} ({pct}% маршрута)\n⭐ +30 опыта",
        call.message.chat.id, call.message.message_id, parse_mode="HTML"
    )
    bot.answer_callback_query(call.id, f"✅ +{fmt(total)}")

# ═══════════════════════════════════════════════════════════════
# 11. ИГРЫ (inline меню + ставки)
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🎰 Игры")
def cmd_games_menu(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    u = get_user(uid)
    bot.send_message(
        msg.chat.id,
        f"🎰 <b>ИГРЫ</b>\n💰 Баланс: <b>{fmt(u['balance'])} {CURRENCY}</b>\n\nВыбери игру:",
        reply_markup=games_inline(uid), parse_mode="HTML"
    )

GAME_HELP = {
    "dice":       ("🎲 Кости",    "кости 1000 чет",    "Ставка + тип: чет, нечет, малые, большие, число 1-6"),
    "slots":      ("🎰 Слоты",    "слоты 5000",         "Просто введи ставку, 3 барабана прокрутятся"),
    "roulette":   ("🎡 Рулетка",  "рулетка 1000 красное", "Ставка + цвет: красное(×2), черное(×2), зеленое(×36), или число 0-36"),
    "mines":      ("💣 Мины",     "мины 5000 3",         "Ставка + кол-во мин 1-10. Открывай клетки, забирай!"),
    "crash":      ("🚀 Краш",     "краш 10000 3.0",      "Ставка + множитель 1.1-10. Успей до краша!"),
    "lottery":    ("🎟️ Лотерея",  "лотерея 1000",        f"Купить билеты по {LOTTERY_TICKET_PRICE}. Розыгрыш раз в сутки"),
    "darts":      ("🎯 Дартс",    "дартс 2000",          "Яблочко ×5, кольцо — возврат, промах — ×2 потеря"),
    "bowling":    ("🎳 Боулинг",  "боулинг 1000",        "Страйк ×3, спэр ×1.5, 9 кеглей — возврат, промах — потеря"),
    "basketball": ("🏀 Баскет",   "баскетбол 1500",      "Попадание ×2.5, трёхочковый ×3, промах — потеря"),
    "football":   ("⚽ Футбол",   "футбол 800",          "Гол ×2, штанга — возврат, мимо — потеря"),
}

@bot.callback_query_handler(func=lambda c: c.data.startswith("game_info_"))
def cb_game_info(call):
    uid = call.from_user.id
    parts = call.data.split("_")
    # game_info_GAME_UID
    game_key = parts[2]
    target   = int(parts[3])
    if uid != target:
        bot.answer_callback_query(call.id, "❌ Не твой запрос"); return

    info = GAME_HELP.get(game_key)
    if not info:
        bot.answer_callback_query(call.id); return

    title, example, desc = info
    u = get_user(uid)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("◀️ Назад к играм", callback_data=f"games_back_{uid}"))

    bot.edit_message_text(
        f"<b>{title}</b>\n\n"
        f"📋 {desc}\n\n"
        f"💰 Твой баланс: <b>{fmt(u['balance'])} {CURRENCY}</b>\n\n"
        f"✏️ Введи команду:\n<code>{example}</code>",
        call.message.chat.id, call.message.message_id,
        reply_markup=kb, parse_mode="HTML"
    )
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("games_back_"))
def cb_games_back(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]):
        bot.answer_callback_query(call.id, "❌"); return
    u = get_user(uid)
    try:
        bot.edit_message_text(
            f"🎰 <b>ИГРЫ</b>\n💰 Баланс: <b>{fmt(u['balance'])} {CURRENCY}</b>\n\nВыбери игру:",
            call.message.chat.id, call.message.message_id,
            reply_markup=games_inline(uid), parse_mode="HTML"
        )
    except: pass
    bot.answer_callback_query(call.id)

# ═══════════════════════════════════════════════════════════════
# 12. ИГРЫ — МЕХАНИКА
# ═══════════════════════════════════════════════════════════════

def game_check(msg, parts_needed=2):
    """Проверка: зарегистрирован + нужное кол-во аргументов."""
    if not check_reg(msg): return None
    uid = msg.from_user.id
    return ensure_user(uid, msg.from_user.first_name or "Игрок")

# КОСТИ
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("кости"))
def cmd_dice(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 2:
        reply(msg, "❌ Формат: <code>кости 1000 чет</code>", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    bet_type = parts[2].lower() if len(parts) > 2 else "чет"
    result = random.randint(1, 6)
    icons = {1:"1️⃣",2:"2️⃣",3:"3️⃣",4:"4️⃣",5:"5️⃣",6:"6️⃣"}
    win, mult = False, 1
    if   bet_type in ["чет","ч","even"]:    win = result % 2 == 0; mult = 2
    elif bet_type in ["нечет","н","odd"]:   win = result % 2 == 1; mult = 2
    elif bet_type in ["малые","мал"]:       win = result in [1,2,3]; mult = 2
    elif bet_type in ["большие","бол"]:     win = result in [4,5,6]; mult = 2
    elif bet_type.isdigit() and 1 <= int(bet_type) <= 6:
        win = result == int(bet_type); mult = 6
    else:
        reply(msg, "❌ Тип: чет, нечет, малые, большие, число 1-6", parse_mode="HTML"); return
    if win:
        profit = bet * mult - bet
        update_balance(uid, profit)
        record_game(uid, "кости", bet, profit, f"{result}")
        reply(msg, f"🎲 {icons[result]} <b>Победа!</b> +{fmt(profit)} {CURRENCY}", parse_mode="HTML")
    else:
        update_balance(uid, -bet)
        record_game(uid, "кости", bet, 0, f"{result}")
        reply(msg, f"🎲 {icons[result]} Проигрыш. -{fmt(bet)} {CURRENCY}", parse_mode="HTML")

# СЛОТЫ
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("слоты"))
def cmd_slots(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 2:
        reply(msg, "❌ Формат: <code>слоты 1000</code>", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    reels = [random.choice(SLOT_SYMBOLS) for _ in range(3)]
    combo = "".join(reels)
    mult  = SLOT_PAYOUTS.get(combo, 0)
    if not mult and (reels[0]==reels[1] or reels[1]==reels[2] or reels[0]==reels[2]):
        mult = 0.5
    if mult:
        profit = int(bet * mult) - bet
        update_balance(uid, profit)
        record_game(uid, "слоты", bet, max(0, profit), combo)
        if mult >= 1:
            reply(msg, f"🎰 {combo}\n🎉 ×{mult}! {'+' if profit>=0 else ''}{fmt(profit)} {CURRENCY}", parse_mode="HTML")
        else:
            reply(msg, f"🎰 {combo}\n⚡ Возврат {fmt(int(bet*mult))} {CURRENCY}", parse_mode="HTML")
    else:
        update_balance(uid, -bet)
        record_game(uid, "слоты", bet, 0, combo)
        reply(msg, f"🎰 {combo}\n😔 Нет совпадений. -{fmt(bet)} {CURRENCY}", parse_mode="HTML")

# ДАРТС
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("дартс"))
def cmd_darts(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 2:
        reply(msg, "❌ Формат: <code>дартс 1000</code>", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    r = random.randint(1, 6)
    if r == 6:
        profit = bet * 4
        update_balance(uid, profit); record_game(uid, "дартс", bet, profit, "яблочко")
        reply(msg, f"🎯 <b>ЯБЛОЧКО!</b> ×5! +{fmt(profit)} {CURRENCY}", parse_mode="HTML")
    elif r in [4, 5]:
        record_game(uid, "дартс", bet, 0, "кольцо")
        reply(msg, "🎯 Попадание в кольцо! Ставка возвращена.", parse_mode="HTML")
    else:
        loss = min(bet * 2, int(u["balance"]))
        update_balance(uid, -loss); record_game(uid, "дартс", bet, 0, "промах")
        reply(msg, f"💥 ПРОМАХ! -{fmt(loss)} {CURRENCY}", parse_mode="HTML")

# БОУЛИНГ
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("боулинг"))
def cmd_bowling(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 2:
        reply(msg, "❌ Формат: <code>боулинг 1000</code>", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    r = random.randint(1, 6)
    if r == 6:
        profit = bet * 2
        update_balance(uid, profit); record_game(uid, "боулинг", bet, profit, "страйк")
        reply(msg, f"🎳 <b>СТРАЙК!</b> ×3! +{fmt(profit)} {CURRENCY}", parse_mode="HTML")
    elif r == 5:
        profit = int(bet * 0.5)
        update_balance(uid, profit); record_game(uid, "боулинг", bet, profit, "спэр")
        reply(msg, f"🎳 Спэр! ×1.5! +{fmt(profit)} {CURRENCY}", parse_mode="HTML")
    elif r >= 3:
        record_game(uid, "боулинг", bet, 0, "9 кеглей")
        reply(msg, "🎳 9 кеглей! Ставка возвращена.", parse_mode="HTML")
    else:
        update_balance(uid, -bet); record_game(uid, "боулинг", bet, 0, "промах")
        reply(msg, f"🎳 Промах! -{fmt(bet)} {CURRENCY}", parse_mode="HTML")

# БАСКЕТБОЛ
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("баскетбол"))
def cmd_basketball(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 2:
        reply(msg, "❌ Формат: <code>баскетбол 1000</code>", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    r = random.randint(1, 6)
    if r == 6:
        profit = bet * 2
        update_balance(uid, profit); record_game(uid, "баскетбол", bet, profit, "трёхочковый")
        reply(msg, f"🏀 <b>ТРЁХОЧКОВЫЙ!</b> ×3! +{fmt(profit)} {CURRENCY}", parse_mode="HTML")
    elif r in [4, 5]:
        profit = int(bet * 1.5)
        update_balance(uid, profit); record_game(uid, "баскетбол", bet, profit, "попадание")
        reply(msg, f"🏀 <b>ПОПАДАНИЕ!</b> ×2.5! +{fmt(profit)} {CURRENCY}", parse_mode="HTML")
    else:
        update_balance(uid, -bet); record_game(uid, "баскетбол", bet, 0, "промах")
        reply(msg, f"🏀 Промах! -{fmt(bet)} {CURRENCY}", parse_mode="HTML")

# ФУТБОЛ
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("футбол"))
def cmd_football(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 2:
        reply(msg, "❌ Формат: <code>футбол 1000</code>", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    r = random.randint(1, 6)
    if r in [3, 4]:
        update_balance(uid, bet); record_game(uid, "футбол", bet, bet, "гол")
        reply(msg, f"⚽ <b>ГОЛ!</b> ×2! +{fmt(bet)} {CURRENCY}", parse_mode="HTML")
    elif r == 5:
        record_game(uid, "футбол", bet, 0, "штанга")
        reply(msg, "⚽ В ШТАНГУ! Ставка возвращена.", parse_mode="HTML")
    else:
        update_balance(uid, -bet); record_game(uid, "футбол", bet, 0, "мимо")
        reply(msg, f"⚽ Мимо! -{fmt(bet)} {CURRENCY}", parse_mode="HTML")

# КРАШ
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("краш"))
def cmd_crash(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 3:
        reply(msg, "❌ Формат: <code>краш 1000 2.5</code>", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    try:
        target = round(float(parts[2]), 2)
        if target < 1.1 or target > 10: raise ValueError
    except:
        reply(msg, "❌ Множитель от 1.1 до 10", parse_mode="HTML"); return
    update_balance(uid, -bet)
    crash_at = max(1.01, min(20, round(random.expovariate(0.8) + 1.0, 2)))
    if crash_at >= target:
        win    = int(bet * target)
        update_balance(uid, win)
        profit = win - bet
        record_game(uid, "краш", bet, profit, f"{crash_at:.2f}x")
        reply(msg, f"🚀 <b>Краш на {crash_at:.2f}x!</b>\n✅ Твой ×{target} выжил!\n💰 +{fmt(profit)} {CURRENCY}", parse_mode="HTML")
    else:
        record_game(uid, "краш", bet, 0, f"{crash_at:.2f}x")
        reply(msg, f"💥 <b>Краш на {crash_at:.2f}x!</b>\n❌ Твой ×{target} не успел!\n📉 -{fmt(bet)} {CURRENCY}", parse_mode="HTML")

# РУЛЕТКА
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("рулетка"))
def cmd_roulette(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 3:
        reply(msg, "❌ Формат: <code>рулетка 1000 красное</code>", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    bt = parts[2].lower()
    result = random.randint(0, 36)
    win, mult = False, 1
    if   bt in ["красное","крас","к","red"]:  win = result in RED_NUMBERS; mult = 2
    elif bt in ["черное","чёрное","черн","black"]: win = result!=0 and result not in RED_NUMBERS; mult = 2
    elif bt in ["зеленое","зёленое","зел","green"]: win = result==0; mult = 36
    elif bt in ["чет","even"]: win = result!=0 and result%2==0; mult = 2
    elif bt in ["нечет","odd"]: win = result%2==1; mult = 2
    elif bt.isdigit() and 0 <= int(bt) <= 36: win = result==int(bt); mult = 36
    else:
        reply(msg, "❌ Ставка: красное, черное, зеленое, чет, нечет, 0-36", parse_mode="HTML"); return
    color = "🔴" if result in RED_NUMBERS else "⚫" if result != 0 else "🟢"
    if win:
        profit = bet * mult - bet
        update_balance(uid, profit); record_game(uid, "рулетка", bet, profit, f"{result}")
        reply(msg, f"🎡 <b>{color} {result}</b>\n🎉 Победа! +{fmt(profit)} {CURRENCY}", parse_mode="HTML")
    else:
        update_balance(uid, -bet); record_game(uid, "рулетка", bet, 0, f"{result}")
        reply(msg, f"🎡 <b>{color} {result}</b>\n😔 Проигрыш. -{fmt(bet)} {CURRENCY}", parse_mode="HTML")

# МИНЫ
mines_games = {}

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("мины"))
def cmd_mines(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 3:
        reply(msg, "❌ Формат: <code>мины 1000 3</code>", parse_mode="HTML"); return
    if uid in mines_games:
        reply(msg, "⚠️ У тебя уже есть активная игра! Завершите её.", parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    try:
        mc = int(parts[2])
        if mc < 1 or mc > 10: raise ValueError
    except:
        reply(msg, "❌ Мин от 1 до 10", parse_mode="HTML"); return
    mines_games[uid] = {
        "bet": bet, "mines": random.sample(range(25), mc),
        "opened": [], "mines_count": mc, "chat_id": msg.chat.id
    }
    update_balance(uid, -bet)
    show_mines_board(uid, msg.chat.id)

def mines_mult(opened: int, mc: int) -> float:
    if opened == 0: return 1.0
    m = 1.0
    for i in range(opened):
        m *= (25 - mc - i) / (25 - i)
    return max(1.01, round(1.0 / m * 0.97, 2))

def show_mines_board(uid: int, chat_id: int, message_id: int = None):
    game = mines_games.get(uid)
    if not game: return
    mult  = mines_mult(len(game["opened"]), game["mines_count"])
    pot   = int(game["bet"] * mult)
    text  = (
        f"💣 <b>Мины</b> | Ставка: {fmt(game['bet'])} | Мин: {game['mines_count']}\n"
        f"✅ Открыто: {len(game['opened'])}/25 | 💰 Потенциал: {fmt(pot)} (×{mult})"
    )
    kb = InlineKeyboardMarkup()
    for i in range(0, 25, 5):
        row = []
        for j in range(5):
            cell = i + j
            if cell in game["opened"]:
                row.append(InlineKeyboardButton("💎", callback_data=f"mno_{cell}"))
            else:
                row.append(InlineKeyboardButton("⬜", callback_data=f"mop_{uid}_{cell}"))
        kb.row(*row)
    kb.row(
        InlineKeyboardButton(f"💰 Забрать {fmt(pot)}", callback_data=f"mco_{uid}"),
        InlineKeyboardButton("🏃 Выход",              callback_data=f"mex_{uid}")
    )
    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=kb, parse_mode="HTML"); return
        except: pass
    bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("mop_"))
def cb_mines_open(call):
    uid  = call.from_user.id
    p    = call.data.split("_")
    g_uid= int(p[1]); cell = int(p[2])
    if g_uid != uid: bot.answer_callback_query(call.id, "❌ Не твоя игра"); return
    game = mines_games.get(uid)
    if not game: bot.answer_callback_query(call.id, "❌ Игра не найдена"); return
    if cell in game["opened"]: bot.answer_callback_query(call.id); return
    if cell in game["mines"]:
        kb = InlineKeyboardMarkup()
        for i in range(0, 25, 5):
            row = []
            for j in range(5):
                c2 = i + j
                if c2 == cell:       row.append(InlineKeyboardButton("💥", callback_data="mno_0"))
                elif c2 in game["mines"]: row.append(InlineKeyboardButton("💣", callback_data="mno_0"))
                elif c2 in game["opened"]: row.append(InlineKeyboardButton("💎", callback_data="mno_0"))
                else:                row.append(InlineKeyboardButton("⬜", callback_data="mno_0"))
            kb.row(*row)
        record_game(uid, "мины", game["bet"], 0, f"мина {cell}, открыто {len(game['opened'])}")
        del mines_games[uid]
        bot.edit_message_text(
            f"💥 <b>БУМ!</b> Мина!\nПотеряно: {fmt(game['bet'])} {CURRENCY}\nОткрыто: {len(game['opened'])}",
            call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="HTML"
        )
        bot.answer_callback_query(call.id, "💥 МИНА!")
        return
    game["opened"].append(cell)
    safe_total = 25 - game["mines_count"]
    if len(game["opened"]) >= safe_total:
        mult = mines_mult(len(game["opened"]), game["mines_count"])
        win  = int(game["bet"] * mult)
        update_balance(uid, win)
        record_game(uid, "мины", game["bet"], win - game["bet"], "все безопасные")
        bot.edit_message_text(
            f"🎉 <b>ПОБЕДА! Все мины обойдены!</b>\n+{fmt(win)} {CURRENCY} (×{mult})",
            call.message.chat.id, call.message.message_id, parse_mode="HTML"
        )
        del mines_games[uid]
        bot.answer_callback_query(call.id, f"🎉 +{fmt(win)}")
        return
    bot.answer_callback_query(call.id, "💎 Безопасно!")
    show_mines_board(uid, call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("mco_"))
def cb_mines_cashout(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[1]): bot.answer_callback_query(call.id, "❌"); return
    game = mines_games.pop(uid, None)
    if not game: bot.answer_callback_query(call.id, "❌ Игра не найдена"); return
    if len(game["opened"]) == 0:
        update_balance(uid, game["bet"])
        bot.edit_message_text("🏃 Отмена. Ставка возвращена.", call.message.chat.id, call.message.message_id, parse_mode="HTML")
        bot.answer_callback_query(call.id, "Возвращено"); return
    mult = mines_mult(len(game["opened"]), game["mines_count"])
    win  = int(game["bet"] * mult)
    update_balance(uid, win)
    record_game(uid, "мины", game["bet"], win - game["bet"], f"кешаут {len(game['opened'])}")
    bot.edit_message_text(
        f"💰 <b>Кешаут!</b> {len(game['opened'])} клеток\n+{fmt(win)} {CURRENCY} (×{mult})",
        call.message.chat.id, call.message.message_id, parse_mode="HTML"
    )
    bot.answer_callback_query(call.id, f"✅ +{fmt(win)}")

@bot.callback_query_handler(func=lambda c: c.data.startswith("mex_"))
def cb_mines_exit(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[1]): bot.answer_callback_query(call.id, "❌"); return
    game = mines_games.pop(uid, None)
    if not game: bot.answer_callback_query(call.id, "❌"); return
    update_balance(uid, game["bet"])
    bot.edit_message_text(f"🏃 Выход. Возвращено {fmt(game['bet'])} {CURRENCY}", call.message.chat.id, call.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(call.id, "✅")

@bot.callback_query_handler(func=lambda c: c.data.startswith("mno_"))
def cb_mines_noop(call): bot.answer_callback_query(call.id)

# ЛОТЕРЕЯ
@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("лотерея"))
def cmd_lottery(msg):
    uid = msg.from_user.id
    u = game_check(msg)
    if not u: return
    parts = msg.text.split()
    if len(parts) < 2:
        with db() as c:
            c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
            lotto = c.fetchone()
            c.execute("SELECT tickets FROM lottery_tickets WHERE user_id=%s", (uid,))
            my = c.fetchone()
        jackpot  = lotto["jackpot"] if lotto else 0
        draw_at  = lotto["draw_at"] if lotto else 0
        my_t     = my["tickets"] if my else 0
        reply(msg,
            f"🎟️ <b>Лотерея</b>\n━━━━━━━━━━━━━━━━━━\n"
            f"💰 Джекпот: <b>{fmt(jackpot)} {CURRENCY}</b>\n"
            f"⏰ Розыгрыш: <b>{cd_str(max(0, int(draw_at)-now()))}</b>\n"
            f"🎫 Твоих билетов: <b>{my_t}</b>\n"
            f"💵 Цена билета: {fmt(LOTTERY_TICKET_PRICE)}\n\n"
            f"Введи: <code>лотерея 1000</code>",
            parse_mode="HTML"); return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        reply(msg, f"❌ Неверная сумма. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    tickets = bet // LOTTERY_TICKET_PRICE
    if tickets == 0:
        reply(msg, f"❌ Минимум {fmt(LOTTERY_TICKET_PRICE)} за 1 билет", parse_mode="HTML"); return
    cost = tickets * LOTTERY_TICKET_PRICE
    update_balance(uid, -cost)
    with db() as c:
        c.execute("INSERT INTO lottery_tickets (user_id,tickets) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET tickets=lottery_tickets.tickets+%s",
                  (uid, tickets, tickets))
        c.execute("UPDATE lottery SET jackpot=jackpot+%s WHERE id=1", (cost,))
        c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
        row = c.fetchone()
    reply(msg,
        f"🎟️ Куплено <b>{tickets}</b> билет(ов) за {fmt(cost)} {CURRENCY}\n"
        f"💰 Джекпот: <b>{fmt(row['jackpot'])} {CURRENCY}</b>\n"
        f"⏰ Розыгрыш: {cd_str(max(0, int(row['draw_at'])-now()))}",
        parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 13. БОНУС
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🎁 Бонус")
def cmd_bonus(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    cd = CD_DAILY_PREMIUM if is_premium(uid) else CD_DAILY_NORMAL
    remaining = cd - (now() - int(u["last_daily"]))
    if remaining > 0:
        reply(msg, f"🎁 Следующий бонус через: <b>{cd_str(remaining)}</b>", parse_mode="HTML"); return
    streak = int(u["daily_streak"])
    lvl    = user_level(u["xp"])
    bonus  = int(DAILY_BASE * (1 + min(streak * 0.1, 2) + lvl * 0.02))
    update_balance(uid, bonus)
    add_xp(uid, 50 + lvl)
    with db() as c:
        c.execute("UPDATE users SET last_daily=%s, daily_streak=daily_streak+1 WHERE id=%s", (now(), uid))
    extra = ""
    if streak > 0 and streak % 7 == 0:
        update_balance(uid, bonus)
        extra = f"\n🎊 Неделя подряд! +{fmt(bonus)} доп.!"
    reply(msg,
        f"🎁 <b>Ежедневный бонус!</b>\n"
        f"🔥 Стрик: {streak+1} дней\n"
        f"💰 +<b>{fmt(bonus)} {CURRENCY}</b>\n"
        f"⭐ +{50+lvl} опыта{extra}",
        parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 14. БАНК
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🏦 Банк")
def cmd_bank(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()
    loan_txt = ""
    if loan and loan["amount"] > 0:
        due = datetime.fromtimestamp(loan["due_at"]).strftime("%d.%m")
        overdue = now() > loan["due_at"]
        loan_txt = f"\n{'🔴' if overdue else '⚠️'} Долг: <b>{fmt(loan['amount'])}</b> до {due}"
    reply(msg,
        f"<b>🏦 Банк</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"💰 Кошелёк: {fmt(u['balance'])} {CURRENCY}\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"📈 Ставка: {BANK_RATE*100:.2f}%/3ч{loan_txt}\n\n"
        f"Выбери действие:",
        reply_markup=bank_inline(uid), parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("bank_"))
def cb_bank(call):
    uid  = call.from_user.id
    p    = call.data.split("_")
    act  = p[1]
    if uid != int(p[2]): bot.answer_callback_query(call.id, "❌ Не твой запрос"); return
    bot.answer_callback_query(call.id)
    if act == "dep":
        m = bot.send_message(call.message.chat.id, "📥 Сколько положить на депозит?\nФормат: <code>5000</code> или <code>все</code>", parse_mode="HTML")
        bot.register_next_step_handler(m, lambda msg: _bank_deposit(msg, uid))
    elif act == "wd":
        m = bot.send_message(call.message.chat.id, "📤 Сколько снять с депозита?\nФормат: <code>3000</code> или <code>все</code>", parse_mode="HTML")
        bot.register_next_step_handler(m, lambda msg: _bank_withdraw(msg, uid))
    elif act == "loan":
        m = bot.send_message(call.message.chat.id, f"💳 Введи сумму кредита (макс {fmt(LOAN_MAX)}):", parse_mode="HTML")
        bot.register_next_step_handler(m, lambda msg: _bank_loan(msg, uid))
    elif act == "repay":
        m = bot.send_message(call.message.chat.id, "✅ Сколько погасить?", parse_mode="HTML")
        bot.register_next_step_handler(m, lambda msg: _bank_repay(msg, uid))

def _bank_deposit(msg, uid):
    u = get_user(uid)
    amount = parse_bet(msg.text.strip(), u["balance"])
    if not amount or amount <= 0 or amount > int(u["balance"]):
        bot.send_message(msg.chat.id, f"❌ Неверная сумма. Баланс: {fmt(u['balance'])} {CURRENCY}", parse_mode="HTML"); return
    update_balance(uid, -amount)
    with db() as c:
        c.execute("UPDATE users SET bank=bank+%s WHERE id=%s", (amount, uid))
    bot.send_message(msg.chat.id, f"✅ Внесено: <b>{fmt(amount)} {CURRENCY}</b>", parse_mode="HTML")

def _bank_withdraw(msg, uid):
    u = get_user(uid)
    amount = parse_bet(msg.text.strip(), u["bank"])
    if not amount or amount <= 0 or amount > int(u["bank"]):
        bot.send_message(msg.chat.id, f"❌ Неверная сумма. Депозит: {fmt(u['bank'])} {CURRENCY}", parse_mode="HTML"); return
    update_balance(uid, amount)
    with db() as c:
        c.execute("UPDATE users SET bank=bank-%s WHERE id=%s", (amount, uid))
    bot.send_message(msg.chat.id, f"✅ Снято: <b>{fmt(amount)} {CURRENCY}</b>", parse_mode="HTML")

def _bank_loan(msg, uid):
    u = get_user(uid)
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
        ex = c.fetchone()
    if ex and ex["amount"] > 0:
        bot.send_message(msg.chat.id, "❌ У тебя уже есть кредит. Сначала погаси."); return
    amount = parse_bet(msg.text.strip(), u["balance"])
    if not amount or amount <= 0 or amount > LOAN_MAX:
        bot.send_message(msg.chat.id, f"❌ Сумма от 1 до {fmt(LOAN_MAX)}"); return
    total = int(amount * (1 + LOAN_RATE))
    due   = now() + LOAN_TERM * 86400
    update_balance(uid, amount)
    with db() as c:
        c.execute("INSERT INTO loans (user_id,amount,due_at) VALUES (%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET amount=%s, due_at=%s",
                  (uid, total, due, total, due))
    bot.send_message(msg.chat.id,
        f"✅ Кредит: <b>{fmt(amount)} {CURRENCY}</b>\n"
        f"💳 Вернуть: {fmt(total)} ({LOAN_RATE*100:.0f}% комиссия)\n"
        f"📅 Срок: {datetime.fromtimestamp(due).strftime('%d.%m.%Y')}",
        parse_mode="HTML")

def _bank_repay(msg, uid):
    u = get_user(uid)
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()
    if not loan or loan["amount"] == 0:
        bot.send_message(msg.chat.id, "❌ У тебя нет кредита"); return
    amount = parse_bet(msg.text.strip(), u["balance"])
    if not amount or amount <= 0:
        bot.send_message(msg.chat.id, "❌ Неверная сумма"); return
    pay = min(amount, int(loan["amount"]))
    if int(u["balance"]) < pay:
        bot.send_message(msg.chat.id, f"❌ Не хватает {fmt(pay)} {CURRENCY}"); return
    update_balance(uid, -pay)
    new_debt = int(loan["amount"]) - pay
    with db() as c:
        if new_debt <= 0:
            c.execute("DELETE FROM loans WHERE user_id=%s", (uid,))
            bot.send_message(msg.chat.id, "✅ <b>Кредит полностью погашен!</b> 🎉", parse_mode="HTML")
        else:
            c.execute("UPDATE loans SET amount=%s WHERE user_id=%s", (new_debt, uid))
            bot.send_message(msg.chat.id, f"✅ Погашено: {fmt(pay)} {CURRENCY}\nОсталось: <b>{fmt(new_debt)}</b>", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 15. БИРЖА
# ═══════════════════════════════════════════════════════════════

def get_stock_price():
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=%s", (TICKER,))
        r = c.fetchone()
        return int(r["price"]) if r else STOCK_PRICE_START

def update_stock_price(impact: float = 0):
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=%s", (TICKER,))
        r = c.fetchone(); price = int(r["price"])
        drift    = (STOCK_PRICE_START - price) * 0.01
        new_price= max(100, int(price * (1 + drift + impact + random.uniform(-STOCK_VOLATILITY, STOCK_VOLATILITY))))
        c.execute("UPDATE stocks SET prev_price=price, price=%s, updated_at=%s WHERE ticker=%s", (new_price, now(), TICKER))
        c.execute("INSERT INTO stock_history (ticker,price,ts) VALUES (%s,%s,%s)", (TICKER, new_price, now()))

@bot.message_handler(func=lambda m: m.text == "📈 Биржа")
def cmd_stock(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    price = get_stock_price()
    with db() as c:
        c.execute("SELECT shares, avg_buy, last_trade FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        port = c.fetchone()
        c.execute("SELECT prev_price FROM stocks WHERE ticker=%s", (TICKER,))
        sr = c.fetchone()
    prev = int(sr["prev_price"]) if sr else price
    chg  = (price - prev) / prev * 100 if prev else 0
    icon = "🟢" if price >= prev else "🔴"
    text = f"<b>📈 Биржа — {TICKER}</b>\n━━━━━━━━━━━━━━━━━━\nЦена: <b>{fmt(price)} {CURRENCY}</b>  {icon} {chg:+.1f}%\n"
    if port and port["shares"] > 0:
        pnl = (price - int(port["avg_buy"])) * int(port["shares"])
        text += f"📂 Портфель: <b>{int(port['shares'])}</b> акций\n"
        text += f"{'📈' if pnl>=0 else '📉'} P&L: {'+' if pnl>=0 else ''}{fmt(pnl)} {CURRENCY}\n"
    cd_left = 0
    if port and port["last_trade"]:
        cd_left = max(0, STOCK_COOLDOWN - (now() - int(port["last_trade"])))
    if cd_left > 0:
        text += f"⏳ Кулдаун: {cd_str(cd_left)}\n"
    text += f"\nКоманды:\n<code>купить 10</code> — купить акции\n<code>продать 5</code> — продать акции"
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🛒 Купить", callback_data=f"stock_buy_{uid}"),
        InlineKeyboardButton("💰 Продать", callback_data=f"stock_sell_{uid}"),
        InlineKeyboardButton("📊 История", callback_data=f"stock_hist_{uid}"),
    )
    reply(msg, text, markup=kb, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("stock_"))
def cb_stock(call):
    uid = call.from_user.id
    p   = call.data.split("_")
    act = p[1]
    if uid != int(p[2]): bot.answer_callback_query(call.id, "❌"); return
    bot.answer_callback_query(call.id)
    if act == "buy":
        m = bot.send_message(call.message.chat.id, "🛒 Сколько акций купить?\nФормат: <code>купить 10</code>", parse_mode="HTML")
        bot.register_next_step_handler(m, lambda msg: _stock_buy_step(msg, uid))
    elif act == "sell":
        m = bot.send_message(call.message.chat.id, "💰 Сколько акций продать?\nФормат: <code>продать 5</code>", parse_mode="HTML")
        bot.register_next_step_handler(m, lambda msg: _stock_sell_step(msg, uid))
    elif act == "hist":
        _send_stock_history(uid, call.message.chat.id)

def _stock_buy_step(msg, uid):
    try:
        qty = int(msg.text.strip().split()[-1])
        if qty <= 0: raise ValueError
    except:
        bot.send_message(msg.chat.id, "❌ Введи число акций"); return
    price = get_stock_price(); total = price * qty
    u = get_user(uid)
    with db() as c:
        c.execute("SELECT last_trade, shares FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        row = c.fetchone()
        if row and row["last_trade"]:
            cd = STOCK_COOLDOWN - (now() - int(row["last_trade"]))
            if cd > 0:
                bot.send_message(msg.chat.id, f"⏳ Кулдаун: {cd_str(cd)}"); return
        cur = int(row["shares"]) if row else 0
        if cur + qty > STOCK_MAX_PER_USER:
            bot.send_message(msg.chat.id, f"❌ Макс. {STOCK_MAX_PER_USER} акций"); return
    if int(u["balance"]) < total:
        bot.send_message(msg.chat.id, f"❌ Нужно {fmt(total)} {CURRENCY}, у тебя {fmt(u['balance'])}"); return
    with db() as c:
        c.execute("SELECT shares, avg_buy FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        port = c.fetchone()
        old_s = int(port["shares"]) if port else 0
        old_a = int(port["avg_buy"]) if port else 0
        new_a = (old_a * old_s + price * qty) // (old_s + qty)
        c.execute("""INSERT INTO stock_portfolio (user_id,ticker,shares,avg_buy,last_trade) VALUES (%s,%s,%s,%s,%s)
                     ON CONFLICT (user_id,ticker) DO UPDATE SET shares=stock_portfolio.shares+%s, avg_buy=%s, last_trade=%s""",
                  (uid, TICKER, qty, new_a, now(), qty, new_a, now()))
    update_balance(uid, -total)
    update_stock_price(0.0005 * qty)
    bot.send_message(msg.chat.id, f"✅ Куплено <b>{qty}</b> акций {TICKER}\n💰 Потрачено: {fmt(total)} {CURRENCY}", parse_mode="HTML")

def _stock_sell_step(msg, uid):
    try:
        qty = int(msg.text.strip().split()[-1])
        if qty <= 0: raise ValueError
    except:
        bot.send_message(msg.chat.id, "❌ Введи число акций"); return
    price = get_stock_price()
    with db() as c:
        c.execute("SELECT shares, avg_buy, last_trade FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        port = c.fetchone()
    if not port or int(port["shares"]) < qty:
        cur = int(port["shares"]) if port else 0
        bot.send_message(msg.chat.id, f"❌ У тебя только {cur} акций"); return
    if port["last_trade"] and STOCK_COOLDOWN - (now() - int(port["last_trade"])) > 0:
        cd = STOCK_COOLDOWN - (now() - int(port["last_trade"]))
        bot.send_message(msg.chat.id, f"⏳ Кулдаун: {cd_str(cd)}"); return
    total = price * qty; fee = int(total * STOCK_SELL_FEE); net = total - fee
    pnl   = net - int(port["avg_buy"]) * qty
    update_balance(uid, net)
    with db() as c:
        ns = int(port["shares"]) - qty
        if ns == 0:
            c.execute("DELETE FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        else:
            c.execute("UPDATE stock_portfolio SET shares=%s, last_trade=%s WHERE user_id=%s AND ticker=%s", (ns, now(), uid, TICKER))
    update_stock_price(-0.0005 * qty)
    pnl_icon = "📈" if pnl >= 0 else "📉"
    bot.send_message(msg.chat.id,
        f"✅ Продано <b>{qty}</b> акций\n💰 Получено: {fmt(net)} (комиссия {fmt(fee)})\n"
        f"{pnl_icon} P&L: {'+' if pnl>=0 else ''}{fmt(pnl)} {CURRENCY}", parse_mode="HTML")

def _send_stock_history(uid, chat_id):
    with db() as c:
        c.execute("SELECT price, ts FROM stock_history WHERE ticker=%s ORDER BY ts DESC LIMIT 10", (TICKER,))
        history = list(reversed(c.fetchall()))
    if not history:
        bot.send_message(chat_id, "📊 История пуста"); return
    lines = []
    for i, row in enumerate(history):
        p  = int(row["price"])
        dt = datetime.fromtimestamp(int(row["ts"])).strftime("%d.%m %H:%M")
        if i > 0:
            pp  = int(history[i-1]["price"])
            chg = (p - pp) / pp * 100
            lines.append(f"{'🟢' if p>=pp else '🔴'} {dt}  <b>{fmt(p)}</b>  ({chg:+.1f}%)")
        else:
            lines.append(f"⬜ {dt}  <b>{fmt(p)}</b>")
    bot.send_message(chat_id, f"<b>📊 История {TICKER}</b>\n\n" + "\n".join(lines), parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("купить "))
def cmd_buy_stock(msg):
    if not check_reg(msg): return
    parts = msg.text.split()
    try: qty = int(parts[1])
    except: bot.send_message(msg.chat.id, "❌ Формат: <code>купить 10</code>", parse_mode="HTML"); return
    _stock_buy_step(msg, msg.from_user.id)

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("продать "))
def cmd_sell_stock(msg):
    if not check_reg(msg): return
    parts = msg.text.split()
    try: qty = int(parts[1])
    except: bot.send_message(msg.chat.id, "❌ Формат: <code>продать 5</code>", parse_mode="HTML"); return
    _stock_sell_step(msg, msg.from_user.id)

# ═══════════════════════════════════════════════════════════════
# 16. ТОП
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🏆 Топ")
def cmd_top(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("💰 Баланс",   callback_data="top_balance"),
        InlineKeyboardButton("⭐ Опыт",     callback_data="top_xp"),
        InlineKeyboardButton("🎮 Победы",   callback_data="top_wins"),
        InlineKeyboardButton("📈 Заработок",callback_data="top_earned"),
    )
    reply(msg, "🏆 <b>Таблица лидеров</b>\nВыбери категорию:", markup=kb, parse_mode="HTML")

def build_top(field, label, fmt_fn=None):
    with db() as c:
        c.execute(f"SELECT name, {field} FROM users ORDER BY {field} DESC LIMIT 10")
        rows = c.fetchall()
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    text = f"<b>🏆 ТОП — {label}</b>\n━━━━━━━━━━━━━━━━━━\n"
    for i, row in enumerate(rows):
        val = fmt_fn(row[field]) if fmt_fn else str(row[field])
        text += f"{medals[i]} {row['name'] or 'Игрок'} — <b>{val}</b>\n"
    return text

@bot.callback_query_handler(func=lambda c: c.data.startswith("top_"))
def cb_top(call):
    uid = call.from_user.id; cat = call.data[4:]
    kb  = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("💰 Баланс",    callback_data="top_balance"),
        InlineKeyboardButton("⭐ Опыт",      callback_data="top_xp"),
        InlineKeyboardButton("🎮 Победы",    callback_data="top_wins"),
        InlineKeyboardButton("📈 Заработок", callback_data="top_earned"),
    )
    if cat == "balance":
        text = build_top("balance", "Баланс", lambda v: f"{fmt(v)} {CURRENCY}")
    elif cat == "xp":
        text = build_top("xp", "Опыт", lambda v: f"{fmt(v)} XP (ур.{user_level(v)})")
    elif cat == "wins":
        text = build_top("games_won", "Победы", lambda v: f"{v} побед")
    elif cat == "earned":
        text = build_top("total_earned", "Заработок", lambda v: f"{fmt(v)} {CURRENCY}")
    else:
        bot.answer_callback_query(call.id); return
    u = get_user(uid)
    if u: text += f"\n👤 Ты: {fmt(u.get(cat if cat!='wins' else 'games_won', 0))}"
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="HTML")
    except:
        bot.send_message(call.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
    bot.answer_callback_query(call.id)

# ═══════════════════════════════════════════════════════════════
# 17. ПЕРЕВОДЫ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "💸 Переводы")
def cmd_transfer_menu(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    u = get_user(uid)
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💸 Перевести", callback_data=f"tr_start_{uid}"))
    reply(msg,
        f"<b>💸 Переводы</b>\n"
        f"Баланс: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"Комиссия: {PREMIUM_TRANSFER_FEE*100:.0f}% (Premium) / {TRANSFER_FEE*100:.0f}% (обычный)\n\n"
        f"Или введи: <code>перевод @username 5000</code>",
        markup=kb, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("tr_start_"))
def cb_tr_start(call):
    uid = call.from_user.id
    if uid != int(call.data.split("_")[2]): bot.answer_callback_query(call.id, "❌"); return
    bot.answer_callback_query(call.id)
    m = bot.send_message(call.message.chat.id, "💸 Введи:\n<code>перевод @username 5000</code>\nили\n<code>перевод ID 5000</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("перевод"))
def cmd_transfer(msg):
    uid = msg.from_user.id
    u   = ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    parts = msg.text.split()
    if len(parts) < 3:
        reply(msg, "❌ Формат: <code>перевод @username 5000</code>", parse_mode="HTML"); return
    target = parts[1].lstrip("@")
    amount = parse_bet(parts[2], u["balance"])
    if not amount or amount <= 0:
        reply(msg, "❌ Неверная сумма", parse_mode="HTML"); return
    with db() as c:
        if target.isdigit():
            c.execute("SELECT id, name FROM users WHERE id=%s", (int(target),))
        else:
            c.execute("SELECT id, name FROM users WHERE name ILIKE %s", (f"%{target}%",))
        row = c.fetchone()
    if not row:
        reply(msg, "❌ Пользователь не найден", parse_mode="HTML"); return
    to_uid = row["id"]
    if to_uid == uid:
        reply(msg, "❌ Нельзя переводить самому себе", parse_mode="HTML"); return
    fee_rate = PREMIUM_TRANSFER_FEE if is_premium(uid) else TRANSFER_FEE
    fee   = int(amount * fee_rate)
    total = amount + fee
    if int(u["balance"]) < total:
        reply(msg, f"❌ Нужно {fmt(total)} {CURRENCY} (включая комиссию {fmt(fee)})", parse_mode="HTML"); return
    last_tr = int(u.get("last_transfer") or 0)
    if now() - last_tr < CD_TRANSFER:
        reply(msg, f"⏱ Подожди {cd_str(CD_TRANSFER-(now()-last_tr))} между переводами", parse_mode="HTML"); return
    update_balance(uid, -total)
    update_balance(to_uid, amount)
    with db() as c:
        c.execute("UPDATE users SET last_transfer=%s WHERE id=%s", (now(), uid))
        c.execute("INSERT INTO transfers (from_id,to_id,amount,fee,ts) VALUES (%s,%s,%s,%s,%s)", (uid, to_uid, amount, fee, now()))
    reply(msg, f"✅ Переведено <b>{fmt(amount)} {CURRENCY}</b> → {row['name'] or target}\n💸 Комиссия: {fmt(fee)}", parse_mode="HTML")
    try:
        bot.send_message(to_uid, f"💰 Тебе перевели <b>{fmt(amount)} {CURRENCY}</b>!", parse_mode="HTML")
    except: pass

# ═══════════════════════════════════════════════════════════════
# 18. РЕФЕРАЛ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🔗 Реферал")
def cmd_referral(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    with db() as c:
        c.execute("SELECT ref_code FROM users WHERE id=%s", (uid,))
        row = c.fetchone()
        ref_code = row["ref_code"] if row else f"REF{uid}"
        c.execute("SELECT COUNT(*) FROM users WHERE ref_by=%s", (uid,))
        refs = c.fetchone()["count"]
    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={ref_code}"
    reply(msg,
        f"<b>🔗 Реферальная программа</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"👥 Приглашено: <b>{refs}</b> игроков\n"
        f"🎁 Тебе: +2 000 {CURRENCY} за каждого\n"
        f"🎁 Другу: +1 000 {CURRENCY} при старте\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔗 Твоя ссылка:\n<code>{link}</code>",
        parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 19. ДОНАТ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "💎 Донат")
def cmd_donate(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    kb = InlineKeyboardMarkup()
    for key, pkg in DONATE_PACKAGES.items():
        kb.add(InlineKeyboardButton(f"{pkg['label']} — {pkg['stars']} ⭐", callback_data=f"donate_{key}"))
    reply(msg,
        "<b>💎 Пополнение за Telegram Stars</b>\n━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(f"{p['label']} → {p['stars']} ⭐" for p in DONATE_PACKAGES.values()),
        markup=kb, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("donate_"))
def cb_donate(call):
    uid = call.from_user.id
    key = call.data[7:]
    pkg = DONATE_PACKAGES.get(key)
    if not pkg:
        bot.answer_callback_query(call.id, "❌"); return
    bot.send_invoice(
        uid, title=f"Пополнение {fmt(pkg['amount'])} {CURRENCY}",
        description=f"Получи {fmt(pkg['amount'])} монет в FECTIZ BOT",
        payload=f"donate_{key}", provider_token="",
        currency="XTR", prices=[LabeledPrice(f"{pkg['stars']} ⭐", pkg["stars"])]
    )
    bot.answer_callback_query(call.id)

@bot.pre_checkout_query_handler(func=lambda q: True)
def pre_checkout(query): bot.answer_pre_checkout_query(query.id, ok=True)

@bot.message_handler(content_types=["successful_payment"])
def successful_payment(msg):
    uid = msg.from_user.id
    key = msg.successful_payment.invoice_payload[7:]
    pkg = DONATE_PACKAGES.get(key)
    if pkg:
        update_balance(uid, pkg["amount"])
        bot.send_message(uid, f"✅ <b>Пополнение!</b> +{fmt(pkg['amount'])} {CURRENCY}\nСпасибо! 💎", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 20. ЧЕКИ
# ═══════════════════════════════════════════════════════════════

def _activate_check_by_code(uid: int, code: str, chat_id: int):
    with db() as c:
        c.execute("SELECT * FROM checks WHERE code=%s", (code,))
        check = c.fetchone()
    if not check:
        bot.send_message(chat_id, "❌ Чек не найден", parse_mode="HTML"); return
    if int(check["current_activations"]) >= int(check["max_activations"]):
        bot.send_message(chat_id, "❌ Чек уже исчерпан", parse_mode="HTML"); return
    if check.get("password"):
        m = bot.send_message(chat_id, "🔒 Введите пароль для активации чека:")
        bot.register_next_step_handler(m, lambda msg: _check_pw(msg, uid, code, check["password"]))
        return
    _do_activate_check(uid, code, check, chat_id)

def _check_pw(msg, uid, code, expected):
    if msg.text and msg.text.strip() == expected:
        with db() as c:
            c.execute("SELECT * FROM checks WHERE code=%s", (code,))
            check = c.fetchone()
        if check: _do_activate_check(uid, code, check, msg.chat.id)
    else:
        bot.send_message(msg.chat.id, "❌ Неверный пароль")

def _do_activate_check(uid, code, check, chat_id):
    with db() as c:
        try:
            c.execute("INSERT INTO check_activations (user_id,check_code,activated_at) VALUES (%s,%s,%s)", (uid, code, now()))
        except:
            bot.send_message(chat_id, "❌ Ты уже активировал этот чек"); return
        c.execute("UPDATE checks SET current_activations=current_activations+1 WHERE code=%s", (code,))
    update_balance(uid, check["amount"])
    remaining = int(check["max_activations"]) - int(check["current_activations"]) - 1
    bot.send_message(chat_id,
        f"✅ <b>Чек активирован!</b>\n💰 +{fmt(check['amount'])} {CURRENCY}\n🔄 Осталось: {remaining}",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("чек "))
def cmd_check_create(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    parts = msg.text.split()
    if len(parts) < 3:
        reply(msg,
            "❌ Формат: <code>чек СУММА КОЛ-ВО [пароль]</code>\n"
            "Пример: <code>чек 5000 3</code>",
            parse_mode="HTML"); return
    amount = parse_bet(parts[1], u["balance"])
    if not amount or amount <= 0:
        reply(msg, "❌ Неверная сумма", parse_mode="HTML"); return
    try:
        uses = int(parts[2])
        if uses < 1 or uses > 100: raise ValueError
    except:
        reply(msg, "❌ Кол-во активаций от 1 до 100", parse_mode="HTML"); return
    password = parts[3] if len(parts) > 3 else None
    total    = amount * uses
    if int(u["balance"]) < total:
        reply(msg, f"❌ Нужно {fmt(total)} {CURRENCY} (на {uses} активаций)", parse_mode="HTML"); return
    code = f"CHK{random.randint(100000, 999999)}"
    update_balance(uid, -total)
    with db() as c:
        c.execute("INSERT INTO checks (code,amount,max_activations,password,created_by,created_at) VALUES (%s,%s,%s,%s,%s,%s)",
                  (code, amount, uses, password, uid, now()))
    bot_info = bot.get_me()
    link     = f"https://t.me/{bot_info.username}?start={code}"
    pw_txt   = f"\n🔒 Пароль: <code>{password}</code>" if password else ""
    reply(msg,
        f"✅ <b>Чек создан!</b>\n"
        f"💵 {fmt(amount)} {CURRENCY} × {uses} активаций\n"
        f"💎 Заморожено: {fmt(total)}{pw_txt}\n\n"
        f"🔗 Ссылка:\n<code>{link}</code>",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("активировать "))
def cmd_activate_check(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    code = msg.text.split(None, 1)[1].strip().upper()
    _activate_check_by_code(uid, code, msg.chat.id)

# ═══════════════════════════════════════════════════════════════
# 21. ПРОМОКОДЫ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("промо "))
def cmd_promo(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if not check_reg(msg): return
    code = msg.text.split(None, 1)[1].strip().upper()
    with db() as c:
        c.execute("SELECT * FROM promo_codes WHERE code=%s AND active=TRUE", (code,))
        promo = c.fetchone()
    if not promo:
        reply(msg, "❌ Промокод не найден", parse_mode="HTML"); return
    if promo["expires"] and int(promo["expires"]) > 0 and int(promo["expires"]) < now():
        reply(msg, "❌ Промокод истёк", parse_mode="HTML"); return
    if int(promo["uses"]) >= int(promo["max_uses"]):
        reply(msg, "❌ Промокод исчерпан", parse_mode="HTML"); return
    with db() as c:
        try:
            c.execute("INSERT INTO promo_uses (user_id,code,ts) VALUES (%s,%s,%s)", (uid, code, now()))
        except:
            reply(msg, "❌ Ты уже использовал этот промокод", parse_mode="HTML"); return
        c.execute("UPDATE promo_codes SET uses=uses+1 WHERE code=%s", (code,))
    update_balance(uid, promo["reward"])
    reply(msg, f"✅ Промокод <b>{code}</b> активирован!\n💰 +{fmt(promo['reward'])} {CURRENCY}", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 22. ПОМОЩЬ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "📞 Помощь")
def cmd_help(msg):
    reply(msg,
        "<b>📞 ПОМОЩЬ — FECTIZ BOT v5.0</b>\n\n"
        "<b>👤 Я</b> — профиль, статистика\n"
        "<b>💰 Баланс</b> — кошелёк + депозит\n"
        "<b>🎁 Бонус</b> — ежедневная награда\n\n"
        "<b>⚒️ РАБОТА</b> (кнопками):\n"
        "• 👆 Кликер — каждые 5 сек\n"
        "• ⛏️ Майнинг — каждый час\n"
        "• 🚗 Такси — нажми завершить\n\n"
        "<b>🎰 ИГРЫ</b> (текстом):\n"
        "<code>кости 1000 чет</code>\n"
        "<code>слоты 5000</code>\n"
        "<code>рулетка 1000 красное</code>\n"
        "<code>краш 10000 3.0</code>\n"
        "<code>мины 5000 3</code>\n"
        "<code>дартс/боулинг/баскетбол/футбол 1000</code>\n"
        "<code>лотерея 1000</code>\n\n"
        "<b>🏦 БАНК</b> (через кнопки или текст):\n"
        "<code>вклад 5000 / снять 3000</code>\n"
        "<code>кредит 10000 / погасить 5000</code>\n\n"
        "<b>💸 ПЕРЕВОДЫ:</b>\n"
        "<code>перевод @username 5000</code>\n\n"
        "<b>📋 ЧЕК:</b>\n"
        "<code>чек 5000 3</code> — создать чек\n"
        "<code>активировать КОД</code>\n\n"
        "<b>🎟️ ПРОМО:</b>\n"
        "<code>промо КОД</code>\n\n"
        "💡 Суммы: 100к, 1.5м, все",
        parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 23. АДМИН
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("выдать ") and is_admin(m.from_user.id))
def cmd_give(msg):
    parts = msg.text.split()
    if len(parts) < 3: bot.reply_to(msg, "❌ выдать @user сумма"); return
    target = parts[1].lstrip("@")
    try: amount = int(parts[2])
    except: bot.reply_to(msg, "❌ Неверная сумма"); return
    with db() as c:
        c.execute("SELECT id, name FROM users WHERE id=%s" if target.isdigit() else "SELECT id, name FROM users WHERE name ILIKE %s",
                  (int(target),) if target.isdigit() else (f"%{target}%",))
        row = c.fetchone()
    if not row: bot.reply_to(msg, "❌ Не найден"); return
    update_balance(row["id"], amount)
    bot.reply_to(msg, f"✅ Выдано {fmt(amount)} {CURRENCY} → {row['name']}")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("забрать ") and is_admin(m.from_user.id))
def cmd_take(msg):
    parts = msg.text.split()
    if len(parts) < 3: bot.reply_to(msg, "❌ забрать @user сумма"); return
    target = parts[1].lstrip("@")
    try: amount = int(parts[2])
    except: bot.reply_to(msg, "❌ Неверная сумма"); return
    with db() as c:
        c.execute("SELECT id, name FROM users WHERE id=%s" if target.isdigit() else "SELECT id, name FROM users WHERE name ILIKE %s",
                  (int(target),) if target.isdigit() else (f"%{target}%",))
        row = c.fetchone()
    if not row: bot.reply_to(msg, "❌ Не найден"); return
    update_balance(row["id"], -amount)
    bot.reply_to(msg, f"✅ Забрано {fmt(amount)} {CURRENCY} у {row['name']}")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("выдать_премиум ") and is_admin(m.from_user.id))
def cmd_give_premium(msg):
    parts = msg.text.split()
    if len(parts) < 3: bot.reply_to(msg, "❌ выдать_премиум @user дней"); return
    target = parts[1].lstrip("@")
    try: days = int(parts[2])
    except: bot.reply_to(msg, "❌ Укажи дни"); return
    with db() as c:
        c.execute("SELECT id, name FROM users WHERE id=%s" if target.isdigit() else "SELECT id, name FROM users WHERE name ILIKE %s",
                  (int(target),) if target.isdigit() else (f"%{target}%",))
        row = c.fetchone()
    if not row: bot.reply_to(msg, "❌ Не найден"); return
    with db() as c:
        c.execute("UPDATE users SET premium_until=GREATEST(premium_until,%s)+%s WHERE id=%s", (now(), days*86400, row["id"]))
    bot.reply_to(msg, f"✅ Премиум {row['name']} +{days} дней")
    try: bot.send_message(row["id"], f"💎 Вам выдан Premium на {days} дней!", parse_mode="HTML")
    except: pass

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("создать промо ") and is_admin(m.from_user.id))
def cmd_create_promo(msg):
    parts = msg.text.split()
    if len(parts) < 4: bot.reply_to(msg, "❌ создать промо КОД СУММА [uses]"); return
    code = parts[2].upper()
    try:
        reward = int(parts[3])
        uses   = int(parts[4]) if len(parts) > 4 else 1
    except: bot.reply_to(msg, "❌ Неверные параметры"); return
    with db() as c:
        try: c.execute("INSERT INTO promo_codes (code,reward,max_uses) VALUES (%s,%s,%s)", (code, reward, uses))
        except: bot.reply_to(msg, "❌ Промокод уже существует"); return
    bot.reply_to(msg, f"✅ <code>{code}</code> создан, {fmt(reward)} {CURRENCY}, {uses} раз", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "стат" and is_admin(m.from_user.id))
def cmd_stat(msg):
    with db() as c:
        c.execute("SELECT COUNT(*) FROM users"); users = c.fetchone()["count"]
        c.execute("SELECT COALESCE(SUM(balance),0) FROM users"); bal = c.fetchone()["coalesce"]
        c.execute("SELECT COALESCE(SUM(bank),0) FROM users"); bank = c.fetchone()["coalesce"]
        c.execute("SELECT COUNT(*) FROM users WHERE last_daily > %s", (now()-86400,)); daily = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) FROM game_history WHERE ts > %s", (now()-86400,)); games = c.fetchone()["count"]
    bot.send_message(msg.chat.id,
        f"<b>📊 Стат</b>\n👥 Игроков: {users}\n🟢 Активны 24ч: {daily}\n"
        f"🎮 Игр сегодня: {games}\n💰 Кошельки: {fmt(bal)}\n🏦 Вклады: {fmt(bank)}",
        parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 24. ПЛАНИРОВЩИКИ
# ═══════════════════════════════════════════════════════════════

def stock_scheduler():
    while True:
        time.sleep(STOCK_UPDATE_SEC)
        try: update_stock_price()
        except Exception as e: print(f"[stocks] {e}")

def interest_scheduler():
    while True:
        time.sleep(10800)
        try:
            with db() as c:
                c.execute("UPDATE users SET bank=FLOOR(bank + bank*%s) WHERE bank > 0", (BANK_RATE,))
            print(f"[interest] начислены в {datetime.now().strftime('%H:%M')}")
        except Exception as e: print(f"[interest] {e}")

def loan_overdue_scheduler():
    while True:
        time.sleep(43200)
        try:
            with db() as c:
                c.execute("UPDATE loans SET amount=FLOOR(amount*1.05) WHERE due_at<%s AND amount>0", (now(),))
        except Exception as e: print(f"[loans] {e}")

def run_lottery_draw():
    try:
        with db() as c:
            c.execute("SELECT jackpot FROM lottery WHERE id=1")
            lotto = c.fetchone()
            if not lotto or lotto["jackpot"] == 0:
                c.execute("UPDATE lottery SET draw_at=%s WHERE id=1", (now()+LOTTERY_INTERVAL,)); return
            c.execute("SELECT user_id, tickets FROM lottery_tickets WHERE tickets > 0")
            parts = c.fetchall()
            if not parts:
                c.execute("UPDATE lottery SET draw_at=%s WHERE id=1", (now()+LOTTERY_INTERVAL,)); return
            pool   = []
            for p in parts: pool.extend([p["user_id"]] * p["tickets"])
            winner = random.choice(pool)
            jackpot= lotto["jackpot"]
            update_balance(winner, jackpot)
            c.execute("UPDATE lottery SET jackpot=0, draw_at=%s WHERE id=1", (now()+LOTTERY_INTERVAL,))
            c.execute("DELETE FROM lottery_tickets")
            try:
                bot.send_message(winner,
                    f"🎉🎊 <b>ВЫ ВЫИГРАЛИ ЛОТЕРЕЮ!</b>\n💰 +{fmt(jackpot)} {CURRENCY}\n🎟️ Участников: {len(pool)}\n🍀 Удача!",
                    parse_mode="HTML")
            except: pass
    except Exception as e: print(f"[lottery] {e}")

def lottery_scheduler():
    while True:
        try:
            with db() as c:
                c.execute("SELECT draw_at FROM lottery WHERE id=1")
                row = c.fetchone()
            sleep_time = max(10, int(row["draw_at"]) - now()) if row else LOTTERY_INTERVAL
            time.sleep(min(sleep_time, 3600))
            with db() as c:
                c.execute("SELECT draw_at FROM lottery WHERE id=1")
                row2 = c.fetchone()
            if row2 and int(row2["draw_at"]) <= now():
                run_lottery_draw()
        except Exception as e:
            print(f"[lottery_scheduler] {e}"); time.sleep(60)

# ═══════════════════════════════════════════════════════════════
# 25. ЗАПУСК
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    init_db()
    threading.Thread(target=stock_scheduler,      daemon=True).start()
    threading.Thread(target=interest_scheduler,   daemon=True).start()
    threading.Thread(target=lottery_scheduler,    daemon=True).start()
    threading.Thread(target=loan_overdue_scheduler, daemon=True).start()
    print("🚀 FECTIZ BOT v5.0 запущен!")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)