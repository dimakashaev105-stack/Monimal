"""
╔══════════════════════════════════════╗
║   🌸  FECTIZ BOT  —  v4.1           ║
║   PostgreSQL · Все игры · Удобно   ║
╚══════════════════════════════════════╝
"""

import os, re, time, json, math, random, threading, string
from contextlib import contextmanager
from datetime import datetime, timedelta
import psycopg2
import psycopg2.pool
import psycopg2.extras
from dotenv import load_dotenv
from telebot import TeleBot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton,
    LabeledPrice
)
from http.server import HTTPServer, BaseHTTPRequestHandler

load_dotenv()

# ═══════════════════════════════════════════════════════════════
# 0. УДАЛЯЕМ WEBHOOK
# ═══════════════════════════════════════════════════════════════

TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")

_temp_bot = TeleBot(TOKEN)
try:
    _temp_bot.delete_webhook()
    print("✅ Webhook удалён")
except Exception as e:
    print(f"⚠️ Ошибка удаления webhook: {e}")

bot = TeleBot(TOKEN, threaded=True, num_threads=8)

# ═══════════════════════════════════════════════════════════════
# 1. HTTP-СЕРВЕР ДЛЯ RENDER
# ═══════════════════════════════════════════════════════════════

PORT = int(os.environ.get("PORT", 8080))

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ["/health", "/"]:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Bot is running")
        else:
            self.send_response(404)
            self.end_headers()
    def log_message(self, format, *args):
        pass

def run_http_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
        print(f"✅ HTTP-сервер запущен на порту {PORT}")
        server.serve_forever()
    except Exception as e:
        print(f"⚠️ Ошибка HTTP-сервера: {e}")

threading.Thread(target=run_http_server, daemon=True).start()

# ═══════════════════════════════════════════════════════════════
# 2. КОНФИГ
# ═══════════════════════════════════════════════════════════════

CURRENCY = "🌸"
TICKER = "FECTZ"

CD_CLICK = 5
CD_DAILY_NORMAL = 1200
CD_DAILY_PREMIUM = 600
CD_WORK = 14400
CD_MINE = 3600
CD_TRANSFER = 60
GAME_COOLDOWN = 3

TRANSFER_FEE = 0.10
PREMIUM_TRANSFER_FEE = 0.05
CLICK_BASE = 100
MINE_BASE = 250
WORK_BASE = 1500
DAILY_BASE = 5000
BANK_RATE = 0.005
LOAN_MAX = 100000
LOAN_RATE = 0.10
LOAN_TERM = 7

STOCK_PRICE_START = 10000
STOCK_VOLATILITY = 0.04
STOCK_MAX_PER_USER = 5000
STOCK_SELL_FEE = 0.03
STOCK_COOLDOWN = 600
STOCK_UPDATE_SEC = 1800

LOTTERY_TICKET_PRICE = 500
LOTTERY_INTERVAL = 86400  # 24 часа

# ═══════════════════════════════════════════════════════════════
# 3. БАЗА ДАННЫХ — PostgreSQL
# ═══════════════════════════════════════════════════════════════

_pg_pool = None

def get_pg_pool():
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
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
        conn.rollback()
        raise
    finally:
        cur.close()
        pool.putconn(conn)

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
            ref_by        BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS loans (
            user_id BIGINT PRIMARY KEY,
            amount  BIGINT DEFAULT 0,
            due_at  BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            ticker     TEXT PRIMARY KEY,
            price      BIGINT DEFAULT 10000,
            prev_price BIGINT DEFAULT 10000,
            updated_at BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock_portfolio (
            user_id    BIGINT,
            ticker     TEXT,
            shares     INTEGER DEFAULT 0,
            avg_buy    BIGINT DEFAULT 0,
            last_trade BIGINT DEFAULT 0,
            PRIMARY KEY (user_id, ticker)
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock_history (
            id     SERIAL PRIMARY KEY,
            ticker TEXT,
            price  BIGINT,
            ts     BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS lottery (
            id      INTEGER PRIMARY KEY DEFAULT 1,
            jackpot BIGINT DEFAULT 0,
            draw_at BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS lottery_tickets (
            user_id BIGINT PRIMARY KEY,
            tickets INTEGER DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id      SERIAL PRIMARY KEY,
            from_id BIGINT,
            to_id   BIGINT,
            amount  BIGINT,
            fee     BIGINT,
            ts      BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS game_history (
            id      SERIAL PRIMARY KEY,
            user_id BIGINT,
            game    TEXT,
            bet     BIGINT,
            win     BIGINT,
            result  TEXT,
            ts      BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock_trades (
            id         SERIAL PRIMARY KEY,
            user_id    BIGINT,
            ticker     TEXT,
            action     TEXT,
            amount     INTEGER,
            price      BIGINT,
            fee        BIGINT DEFAULT 0,
            total      BIGINT,
            created_at BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code     TEXT PRIMARY KEY,
            reward   BIGINT,
            max_uses INTEGER DEFAULT 1,
            uses     INTEGER DEFAULT 0,
            expires  BIGINT DEFAULT 0,
            active   BOOLEAN DEFAULT TRUE
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT,
            code    TEXT,
            ts      BIGINT DEFAULT 0,
            PRIMARY KEY (user_id, code)
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS checks (
            code                TEXT PRIMARY KEY,
            amount              BIGINT,
            max_activations     INTEGER,
            current_activations INTEGER DEFAULT 0,
            password            TEXT,
            created_by          BIGINT,
            created_at          BIGINT DEFAULT 0
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS check_activations (
            user_id       BIGINT,
            check_code    TEXT,
            activated_at  BIGINT DEFAULT 0,
            PRIMARY KEY (user_id, check_code)
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS donate_packages (
            key    TEXT PRIMARY KEY,
            stars  INTEGER,
            amount BIGINT,
            label  TEXT
        )
        """)
        # Инициализация данных
        next_draw = now() + LOTTERY_INTERVAL
        c.execute(
            "INSERT INTO lottery (id, jackpot, draw_at) VALUES (1, 0, %s) ON CONFLICT DO NOTHING",
            (next_draw,)
        )
        c.execute("""
            INSERT INTO stocks (ticker, price, prev_price, updated_at)
            VALUES (%s, 10000, 10000, 0) ON CONFLICT DO NOTHING
        """, (TICKER,))
        for row in [
            ('s1',   1,    10000,   '⭐ 10 000'),
            ('s5',   5,    66000,   '⭐ 66 000'),
            ('s15',  15,   266000,  '🔥 266 000'),
            ('s50',  50,   1000000, '🔥 1 000 000'),
            ('s150', 150,  4000000, '💎 4 000 000'),
            ('s250', 250,  8000000, '💎 8 000 000'),
        ]:
            c.execute(
                "INSERT INTO donate_packages (key, stars, amount, label) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                row
            )
    print("✅ БД инициализирована (PostgreSQL)")

# ═══════════════════════════════════════════════════════════════
# 4. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════

def fmt(n) -> str:
    n = int(n) if n else 0
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B"
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

def is_premium(uid: int) -> bool:
    with db() as c:
        c.execute("SELECT premium_until FROM users WHERE id=%s", (uid,))
        row = c.fetchone()
        return bool(row and row["premium_until"] > now())

def get_user(uid: int):
    with db() as c:
        c.execute("SELECT * FROM users WHERE id=%s", (uid,))
        return c.fetchone()

def ensure_user(uid: int, name: str = ""):
    with db() as c:
        c.execute(
            "INSERT INTO users (id, name, created_at) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (uid, name, now())
        )
        c.execute("SELECT ref_code FROM users WHERE id=%s", (uid,))
        row = c.fetchone()
        if row and not row["ref_code"]:
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
    text = text.lower().strip().replace(" ", "")
    balance = int(balance or 0)
    if text in ["все", "all"]:
        return balance
    if text.endswith("к") and text[:-1].replace(".", "").isdigit():
        return int(float(text[:-1]) * 1000)
    if text.endswith("м") and text[:-1].replace(".", "").isdigit():
        return int(float(text[:-1]) * 1_000_000)
    if text.endswith("б") and text[:-1].replace(".", "").isdigit():
        return int(float(text[:-1]) * 1_000_000_000)
    try:
        return int(text)
    except:
        return None

def record_game(uid: int, game: str, bet: int, win: int, result: str):
    """Записать результат игры и обновить статистику."""
    with db() as c:
        c.execute(
            "INSERT INTO game_history (user_id, game, bet, win, result, ts) VALUES (%s,%s,%s,%s,%s,%s)",
            (uid, game, bet, win, result, now())
        )
        if win > 0:
            c.execute(
                "UPDATE users SET games_won=games_won+1, total_won=total_won+%s WHERE id=%s",
                (win, uid)
            )
        else:
            c.execute(
                "UPDATE users SET games_lost=games_lost+1, total_lost=total_lost+%s WHERE id=%s",
                (bet, uid)
            )

# ═══════════════════════════════════════════════════════════════
# 5. КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════

def main_menu() -> ReplyKeyboardMarkup:
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
    buttons = [
        "👤 Профиль", "💰 Баланс", "⚒️ Работа",
        "🎰 Игры", "🏦 Банк", "📈 Биржа",
        "🎁 Бонус", "🏆 Топ", "💎 Донат",
        "📞 Помощь", "🔗 Реферал"
    ]
    for i in range(0, len(buttons), 3):
        markup.add(*[KeyboardButton(btn) for btn in buttons[i:i+3]])
    return markup

def work_menu() -> ReplyKeyboardMarkup:
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for btn in ["👆 Кликер", "⛏️ Майнинг", "🚗 Такси", "🏠 Главное меню"]:
        markup.add(KeyboardButton(btn))
    return markup

def games_menu() -> ReplyKeyboardMarkup:
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for btn in [
        "🎲 Кости", "🎰 Слоты", "🎯 Дартс", "🎳 Боулинг",
        "🏀 Баскетбол", "⚽ Футбол", "🚀 Краш", "🎡 Рулетка",
        "💣 Мины", "🎟️ Лотерея", "🏠 Главное меню"
    ]:
        markup.add(KeyboardButton(btn))
    return markup

# ═══════════════════════════════════════════════════════════════
# 6. ОСНОВНЫЕ КОМАНДЫ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(commands=["start", "меню"])
def cmd_start(msg):
    uid = msg.from_user.id
    name = msg.from_user.first_name or "Игрок"
    ensure_user(uid, name)

    parts = msg.text.split()
    if len(parts) > 1:
        param = parts[1]

        # Активация чека через /start CHKxxxxxx
        if param.upper().startswith("CHK"):
            _activate_check_by_code(uid, param.upper(), msg.chat.id)
            return

        # Реферальная ссылка
        u = get_user(uid)
        if u and not u["ref_by"] and param != str(uid):
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
                        bot.send_message(
                            ref_uid,
                            f"🎉 По вашей ссылке пришёл новый игрок!\n+2 000 {CURRENCY}, +500 опыта",
                            parse_mode="HTML"
                        )
                    except:
                        pass

    bot.send_message(
        uid,
        f"🌸 <b>Добро пожаловать, {name}!</b>\n\nВыбери раздел в меню 👇",
        reply_markup=main_menu(),
        parse_mode="HTML"
    )

@bot.message_handler(func=lambda m: m.text == "🏠 Главное меню")
def back_to_menu(msg):
    bot.send_message(msg.chat.id, "Главное меню:", reply_markup=main_menu(), parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "👤 Профиль")
def cmd_profile(msg):
    uid = msg.from_user.id
    u = get_user(uid) or ensure_user(uid, msg.from_user.first_name or "Игрок")
    lvl = user_level(u["xp"])
    xp_cur = int(u["xp"]) - level_xp(lvl)
    xp_need = level_xp(lvl + 1) - level_xp(lvl)
    bar_filled = int((xp_cur / max(1, xp_need)) * 10)
    bar = "▓" * bar_filled + "░" * (10 - bar_filled)
    prem = "💎 Премиум" if is_premium(uid) else "👤 Обычный"
    created = datetime.fromtimestamp(u["created_at"]).strftime("%d.%m.%Y") if u["created_at"] else "—"
    total_games = int(u["games_won"]) + int(u["games_lost"])
    winrate = f"{int(u['games_won']) / total_games * 100:.1f}%" if total_games > 0 else "—"
    text = (
        f"<b>👤 Профиль — {u['name'] or 'Игрок'}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎖 Уровень: <b>{lvl}</b>  [{bar}]\n"
        f"⭐ Опыт: {fmt(u['xp'])} | До {lvl+1} ур.: {fmt(max(0, xp_need - xp_cur))}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Баланс: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"📈 Заработано всего: {fmt(u['total_earned'])} {CURRENCY}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎮 Побед: {u['games_won']} | Поражений: {u['games_lost']} | Винрейт: {winrate}\n"
        f"🏆 Выиграно: {fmt(u['total_won'])} | Проиграно: {fmt(u['total_lost'])}\n"
        f"🔥 Стрик бонуса: {u['daily_streak']} дней\n"
        f"🖥 Видеокарт: {u['video_cards']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{prem}\n"
        f"📅 В игре с: {created}"
    )
    bot.send_message(uid, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "💰 Баланс")
def cmd_balance(msg):
    uid = msg.from_user.id
    u = get_user(uid) or ensure_user(uid, msg.from_user.first_name or "Игрок")
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()
    loan_text = f"\n⚠️ Кредит: <b>{fmt(loan['amount'])} {CURRENCY}</b>" if loan and loan["amount"] > 0 else ""
    text = (
        f"<b>💰 Баланс</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👛 Кошелёк: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"💎 Всего: {fmt(int(u['balance']) + int(u['bank']))} {CURRENCY}"
        f"{loan_text}"
    )
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 7. РАБОТА — меню
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "⚒️ Работа")
def cmd_work_menu(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    bot.send_message(msg.chat.id, "⚒️ <b>Выбери работу:</b>", reply_markup=work_menu(), parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "👆 Кликер")
def cmd_click(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    remaining = CD_CLICK - (now() - int(u["last_click"]))
    if remaining > 0:
        bot.send_message(uid, f"⏱ Клик через: <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return
    lvl = user_level(u["xp"])
    earn = max(50, CLICK_BASE + lvl * 5 + random.randint(-20, 50))
    update_balance(uid, earn)
    add_xp(uid, 5 + lvl // 10)
    with db() as c:
        c.execute("UPDATE users SET last_click=%s WHERE id=%s", (now(), uid))
    bot.send_message(uid, f"⚡ <b>Клик!</b> +<b>{fmt(earn)} {CURRENCY}</b>\n⭐ +{5 + lvl // 10} опыта", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 8. МАЙНИНГ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "⛏️ Майнинг")
def cmd_mine(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    remaining = CD_MINE - (now() - int(u["last_mine"]))
    if remaining > 0:
        bot.send_message(uid, f"⛏️ <b>Майнинг</b>\n\n⏱ Следующий сбор через: <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return
    cards = int(u.get("video_cards") or 0)
    earn = max(100, MINE_BASE + cards * 200 + random.randint(-50, 150))
    update_balance(uid, earn)
    add_xp(uid, 10 + cards // 5)
    with db() as c:
        c.execute("UPDATE users SET last_mine=%s WHERE id=%s", (now(), uid))
    bot.send_message(uid, f"⛏️ <b>Майнинг</b>\n\n💰 Намайнено: <b>+{fmt(earn)} {CURRENCY}</b>\n🖥 Видеокарт: {cards}\n⭐ +{10 + cards // 5} опыта", parse_mode="HTML")
    card_price = 5000 * (2 ** cards)
    u2 = get_user(uid)
    if int(u2["balance"]) >= card_price:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(f"🖥 Купить карту за {fmt(card_price)}", callback_data=f"buy_card_{uid}"))
        bot.send_message(uid, f"💡 Улучши майнинг — купи видеокарту за <b>{fmt(card_price)} {CURRENCY}</b>?", reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("buy_card_"))
def cb_buy_card(call):
    uid = call.from_user.id
    if int(call.data.split("_")[2]) != uid:
        bot.answer_callback_query(call.id, "❌ Не твой запрос")
        return
    u = get_user(uid)
    cards = int(u.get("video_cards") or 0)
    price = 5000 * (2 ** cards)
    if int(u["balance"]) < price:
        bot.answer_callback_query(call.id, f"❌ Не хватает {fmt(price)}")
        return
    update_balance(uid, -price)
    with db() as c:
        c.execute("UPDATE users SET video_cards=video_cards+1 WHERE id=%s", (uid,))
    bot.answer_callback_query(call.id, f"✅ Куплена видеокарта! Теперь у тебя {cards + 1} карт(ы)")
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)

# ═══════════════════════════════════════════════════════════════
# 9. ТАКСИ
# ═══════════════════════════════════════════════════════════════

TAXI_ROUTES = [
    {"name": "📍 Центр → Аэропорт",        "base": 1500, "time": 5},
    {"name": "🏠 Жилой р-н → Офис",         "base": 1000, "time": 4},
    {"name": "🎓 Университет → ТЦ",          "base": 800,  "time": 3},
    {"name": "🏥 Больница → Вокзал",         "base": 1200, "time": 4},
    {"name": "🏢 Бизнес-центр → Ресторан",  "base": 600,  "time": 3},
    {"name": "🛍️ ТЦ → Кинотеатр",           "base": 500,  "time": 3},
    {"name": "🌃 Ночной рейс",               "base": 2000, "time": 6},
    {"name": "🚄 Вокзал → Гостиница",        "base": 400,  "time": 3},
]
active_rides = {}

@bot.message_handler(func=lambda m: m.text == "🚗 Такси")
def cmd_taxi(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    if uid in active_rides:
        data = active_rides[uid]
        left = data["time"] * 60 - (now() - data["start"])
        if left > 0:
            bot.send_message(uid, f"⚠️ У тебя уже есть активная поездка!\n⏱ Осталось: <b>{cd_str(left)}</b>\n\nНапиши <b>завершить</b> чтобы закончить.", parse_mode="HTML")
            return
        else:
            # поездка уже истекла, но не была завершена — выдаём награду
            data2 = active_rides.pop(uid)
            update_balance(uid, data2["earn"])
            add_xp(uid, 30)
            bot.send_message(uid, f"🚕 Прошлая поездка завершена автоматически!\n💰 +{fmt(data2['earn'])} {CURRENCY}", parse_mode="HTML")

    route = random.choice(TAXI_ROUTES)
    u = get_user(uid)
    lvl = user_level(u["xp"])
    earn = int(route["base"] * random.uniform(0.9, 1.2) * (1 + lvl * 0.01))
    active_rides[uid] = {"route": route, "earn": earn, "start": now(), "time": route["time"]}

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Завершить поездку", callback_data=f"taxi_finish_{uid}"))

    bot.send_message(uid,
        f"🚕 <b>Новый заказ!</b>\n\n"
        f"Маршрут: {route['name']}\n"
        f"Время: ~{route['time']} мин\n"
        f"Оплата: <b>{fmt(earn)} {CURRENCY}</b>\n\n"
        f"Нажми кнопку или напиши <b>завершить</b> через {route['time']} мин для полной оплаты.",
        reply_markup=markup,
        parse_mode="HTML"
    )

    def auto_finish():
        time.sleep(route["time"] * 60 + 60)
        if uid in active_rides:
            data = active_rides.pop(uid)
            update_balance(uid, data["earn"])
            add_xp(uid, 30)
            try:
                bot.send_message(uid, f"🚕 <b>Поездка завершена!</b>\n💰 +{fmt(data['earn'])} {CURRENCY}\n⭐ +30 опыта", parse_mode="HTML")
            except:
                pass
    threading.Thread(target=auto_finish, daemon=True).start()

@bot.callback_query_handler(func=lambda c: c.data.startswith("taxi_finish_"))
def cb_taxi_finish(call):
    uid = call.from_user.id
    game_uid = int(call.data.split("_")[2])
    if game_uid != uid:
        bot.answer_callback_query(call.id, "❌ Не твоя поездка")
        return
    if uid not in active_rides:
        bot.answer_callback_query(call.id, "❌ Поездка не найдена или уже завершена")
        return
    data = active_rides.pop(uid)
    elapsed = now() - data["start"]
    time_spent = elapsed / 60
    if time_spent >= data["time"]:
        total = data["earn"]
        text = f"🚕 <b>Поездка завершена!</b>\n💰 +{fmt(total)} {CURRENCY}\n⭐ +30 опыта"
    else:
        # досрочно — чуть меньше
        ratio = time_spent / data["time"]
        total = int(data["earn"] * max(0.5, ratio))
        text = f"🚕 <b>Досрочное завершение!</b>\n💰 +{fmt(total)} {CURRENCY} ({int(ratio*100)}% маршрута)\n⭐ +30 опыта"
    update_balance(uid, total)
    add_xp(uid, 30)
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(call.id, f"✅ +{fmt(total)}")

@bot.message_handler(func=lambda m: m.text and m.text.lower() in ["завершить", "закончить", "готово"])
def cmd_finish_ride(msg):
    uid = msg.from_user.id
    if uid in active_rides:
        data = active_rides.pop(uid)
        elapsed = now() - data["start"]
        time_spent = elapsed / 60
        if time_spent < data["time"]:
            ratio = time_spent / data["time"]
            total = int(data["earn"] * max(0.5, ratio))
            text = f"🚕 <b>Поездка завершена досрочно!</b>\n💰 +{fmt(total)} {CURRENCY} ({int(ratio*100)}% маршрута)"
        else:
            total = data["earn"]
            text = f"🚕 <b>Поездка завершена!</b>\n💰 +{fmt(total)} {CURRENCY}"
        update_balance(uid, total)
        add_xp(uid, 30)
        bot.send_message(uid, text + "\n⭐ +30 опыта", parse_mode="HTML")
    else:
        bot.send_message(uid, "❌ У тебя нет активной поездки", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 10. ИГРЫ — меню и кнопки
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🎰 Игры")
def cmd_games_menu(msg):
    ensure_user(msg.from_user.id, msg.from_user.first_name or "Игрок")
    text = (
        "🎰 <b>ИГРЫ</b>\n━━━━━━━━━━━━━━━━━━\n"
        "Выбери игру из меню или введи команду:\n\n"
        "<code>кости 1000 чет</code> — кубики\n"
        "<code>слоты 5000</code> — автоматы\n"
        "<code>дартс 2000</code> — дартс\n"
        "<code>боулинг 1000</code> — боулинг\n"
        "<code>баскетбол 1500</code> — баскетбол\n"
        "<code>футбол 800</code> — футбол\n"
        "<code>краш 10000 3.0</code> — краш\n"
        "<code>рулетка 1000 красное</code> — рулетка\n"
        "<code>мины 5000 3</code> — мины\n"
        "<code>лотерея 1000</code> — лотерея"
    )
    bot.send_message(msg.chat.id, text, reply_markup=games_menu(), parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎲 Кости")
def btn_dice(msg):
    bot.send_message(msg.chat.id,
        "🎲 <b>Кости</b>\nФормат: <code>кости СТАВКА ТИП</code>\n"
        "Типы: чет, нечет, малые (1-3), большие (4-6), число 1-6\n"
        "Пример: <code>кости 1000 чет</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎰 Слоты")
def btn_slots(msg):
    bot.send_message(msg.chat.id,
        "🎰 <b>Слоты</b>\nФормат: <code>слоты СТАВКА</code>\n"
        "Пример: <code>слоты 5000</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎯 Дартс")
def btn_darts(msg):
    bot.send_message(msg.chat.id,
        "🎯 <b>Дартс</b>\nФормат: <code>дартс СТАВКА</code>\n"
        "Попадание — ×5, кольцо — возврат, промах — ×2 потеря\n"
        "Пример: <code>дартс 2000</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎳 Боулинг")
def btn_bowling(msg):
    bot.send_message(msg.chat.id,
        "🎳 <b>Боулинг</b>\nФормат: <code>боулинг СТАВКА</code>\n"
        "Страйк ×3, спэр ×1.5, 9 кеглей — возврат, промах — потеря\n"
        "Пример: <code>боулинг 1000</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🏀 Баскетбол")
def btn_basketball(msg):
    bot.send_message(msg.chat.id,
        "🏀 <b>Баскетбол</b>\nФормат: <code>баскетбол СТАВКА</code>\n"
        "Попадание ×2.5 (шанс ~33%), промах — потеря\n"
        "Пример: <code>баскетбол 1500</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "⚽ Футбол")
def btn_football(msg):
    bot.send_message(msg.chat.id,
        "⚽ <b>Футбол</b>\nФормат: <code>футбол СТАВКА</code>\n"
        "Гол ×2 (шанс ~33%), мимо — потеря\n"
        "Пример: <code>футбол 800</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🚀 Краш")
def btn_crash(msg):
    bot.send_message(msg.chat.id,
        "🚀 <b>Краш</b>\nФормат: <code>краш СТАВКА МНОЖИТЕЛЬ</code>\n"
        "Множитель 1.1–10.0\nПример: <code>краш 10000 3.0</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎡 Рулетка")
def btn_roulette(msg):
    bot.send_message(msg.chat.id,
        "🎡 <b>Рулетка</b>\nФормат: <code>рулетка СТАВКА ЦВЕТ</code>\n"
        "Цвета: красное (×2), черное (×2), зеленое (×36), или число 0-36 (×36)\n"
        "Пример: <code>рулетка 1000 красное</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "💣 Мины")
def btn_mines(msg):
    bot.send_message(msg.chat.id,
        "💣 <b>Мины</b>\nФормат: <code>мины СТАВКА КОЛ-ВО_МИН</code>\n"
        "Мин: 1-10. Чем больше мин — тем выше множитель за каждую клетку\n"
        "Пример: <code>мины 5000 3</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎟️ Лотерея")
def btn_lottery_info(msg):
    uid = msg.from_user.id
    with db() as c:
        c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
        lotto = c.fetchone()
        c.execute("SELECT tickets FROM lottery_tickets WHERE user_id=%s", (uid,))
        my_tickets = c.fetchone()
    jackpot = lotto["jackpot"] if lotto else 0
    draw_at = lotto["draw_at"] if lotto else 0
    my = my_tickets["tickets"] if my_tickets else 0
    time_left = max(0, int(draw_at) - now())
    bot.send_message(msg.chat.id,
        f"🎟️ <b>Лотерея</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"💰 Джекпот: <b>{fmt(jackpot)} {CURRENCY}</b>\n"
        f"⏰ Розыгрыш через: <b>{cd_str(time_left)}</b>\n"
        f"🎫 Твоих билетов: <b>{my}</b>\n"
        f"💵 Цена билета: {fmt(LOTTERY_TICKET_PRICE)}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Формат: <code>лотерея СУММА</code>\nПример: <code>лотерея 1000</code>",
        parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 11. КОСТИ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("кости"))
def cmd_dice(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>кости 1000 чет</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    bet_type = parts[2].lower() if len(parts) > 2 else "чет"
    result = random.randint(1, 6)
    win, multiplier = False, 1
    if bet_type in ["чет", "even", "ч"]:
        win = result % 2 == 0; multiplier = 2
    elif bet_type in ["нечет", "odd", "н"]:
        win = result % 2 == 1; multiplier = 2
    elif bet_type in ["малые", "мал", "small"]:
        win = result in [1, 2, 3]; multiplier = 2
    elif bet_type in ["большие", "бол", "big"]:
        win = result in [4, 5, 6]; multiplier = 2
    elif bet_type.isdigit() and 1 <= int(bet_type) <= 6:
        win = result == int(bet_type); multiplier = 6
    else:
        bot.send_message(uid, "❌ Тип ставки: чет, нечет, малые, большие, число 1-6", parse_mode="HTML")
        return
    dice_icons = {1:"1️⃣",2:"2️⃣",3:"3️⃣",4:"4️⃣",5:"5️⃣",6:"6️⃣"}
    if win:
        profit = bet * multiplier - bet
        update_balance(uid, profit)
        record_game(uid, "кости", bet, profit, f"выпало {result}")
        text = f"🎲 {dice_icons[result]} <b>Победа!</b> +{fmt(profit)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        record_game(uid, "кости", bet, 0, f"выпало {result}")
        text = f"🎲 {dice_icons[result]} Проигрыш. -{fmt(bet)} {CURRENCY}"
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 12. СЛОТЫ
# ═══════════════════════════════════════════════════════════════

SLOT_SYMBOLS = ["🍒", "🍋", "🍊", "🍇", "⭐", "💎", "🔔", "7️⃣"]
SLOT_PAYOUTS = {
    "💎💎💎": 10, "⭐⭐⭐": 7, "7️⃣7️⃣7️⃣": 5, "🍇🍇🍇": 4,
    "🍊🍊🍊": 3, "🍋🍋🍋": 2, "🍒🍒🍒": 1.5, "🔔🔔🔔": 2.5,
}

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("слоты"))
def cmd_slots(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>слоты 1000</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    reels = [random.choice(SLOT_SYMBOLS) for _ in range(3)]
    combo = "".join(reels)
    mult = SLOT_PAYOUTS.get(combo, 0)
    # Частичный выигрыш — 2 одинаковых
    if not mult:
        if reels[0] == reels[1] or reels[1] == reels[2] or reels[0] == reels[2]:
            mult = 0.5  # возврат половины
    if mult:
        profit = int(bet * mult) - bet
        update_balance(uid, profit)
        record_game(uid, "слоты", bet, max(0, profit), combo)
        if mult >= 1:
            text = f"🎰 {combo}\n🎉 Выигрыш ×{mult}! {'+' if profit>=0 else ''}{fmt(profit)} {CURRENCY}"
        else:
            text = f"🎰 {combo}\n⚡ Частичный возврат! {fmt(int(bet*mult))} {CURRENCY} обратно"
    else:
        update_balance(uid, -bet)
        record_game(uid, "слоты", bet, 0, combo)
        text = f"🎰 {combo}\n😔 Нет совпадений. -{fmt(bet)} {CURRENCY}"
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 13. ДАРТС
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("дартс"))
def cmd_darts(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>дартс 1000</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    result = random.randint(1, 6)
    if result == 6:
        # Яблочко — ×5
        profit = bet * 4
        update_balance(uid, profit)
        record_game(uid, "дартс", bet, profit, "яблочко")
        text = f"🎯 <b>ЯБЛОЧКО!</b> ×5! +{fmt(profit)} {CURRENCY}"
    elif result in [4, 5]:
        # Попадание в кольцо — ставка возвращается
        record_game(uid, "дартс", bet, 0, "кольцо")
        text = f"🎯 Попадание в кольцо! Ставка возвращена."
    else:
        # Промах — теряешь ×2
        loss = bet * 2
        actual_loss = min(loss, int(u["balance"]))
        update_balance(uid, -actual_loss)
        record_game(uid, "дартс", bet, 0, "промах")
        text = f"💥 ПРОМАХ! -{fmt(actual_loss)} {CURRENCY}"
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 14. БОУЛИНГ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("боулинг"))
def cmd_bowling(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>боулинг 1000</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    result = random.randint(1, 6)
    if result == 6:
        profit = bet * 2
        update_balance(uid, profit)
        record_game(uid, "боулинг", bet, profit, "страйк")
        text = f"🎳 <b>СТРАЙК!</b> ×3! +{fmt(profit)} {CURRENCY}"
    elif result == 5:
        profit = int(bet * 0.5)
        update_balance(uid, profit)
        record_game(uid, "боулинг", bet, profit, "спэр")
        text = f"🎳 Спэр! ×1.5! +{fmt(profit)} {CURRENCY}"
    elif result >= 3:
        # 9 кеглей — ставка возвращается
        record_game(uid, "боулинг", bet, 0, "9 кеглей")
        text = f"🎳 9 кеглей! Ставка возвращена."
    else:
        update_balance(uid, -bet)
        record_game(uid, "боулинг", bet, 0, "промах")
        text = f"🎳 Промах! -{fmt(bet)} {CURRENCY}"
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 15. БАСКЕТБОЛ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("баскетбол"))
def cmd_basketball(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>баскетбол 1000</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    result = random.randint(1, 6)
    if result in [4, 5]:
        profit = int(bet * 1.5)
        update_balance(uid, profit)
        record_game(uid, "баскетбол", bet, profit, "попадание")
        text = f"🏀 <b>ПОПАДАНИЕ!</b> ×2.5! +{fmt(profit)} {CURRENCY}"
    elif result == 6:
        # трёхочковый
        profit = bet * 2
        update_balance(uid, profit)
        record_game(uid, "баскетбол", bet, profit, "трёхочковый")
        text = f"🏀 <b>ТРЁХОЧКОВЫЙ!</b> ×3! +{fmt(profit)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        record_game(uid, "баскетбол", bet, 0, "промах")
        text = f"🏀 Промах! -{fmt(bet)} {CURRENCY}"
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 16. ФУТБОЛ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("футбол"))
def cmd_football(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>футбол 1000</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    result = random.randint(1, 6)
    if result in [3, 4]:
        update_balance(uid, bet)
        record_game(uid, "футбол", bet, bet, "гол")
        text = f"⚽ <b>ГОЛ!</b> ×2! +{fmt(bet)} {CURRENCY}"
    elif result == 5:
        # штанга — возврат
        record_game(uid, "футбол", bet, 0, "штанга")
        text = f"⚽ В ШТАНГУ! Ставка возвращена."
    else:
        update_balance(uid, -bet)
        record_game(uid, "футбол", bet, 0, "мимо")
        text = f"⚽ Мимо! -{fmt(bet)} {CURRENCY}"
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 17. КРАШ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("краш"))
def cmd_crash(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 3:
        bot.send_message(uid, "❌ Формат: <code>краш 1000 2.5</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    try:
        target_mult = round(float(parts[2]), 2)
    except:
        bot.send_message(uid, "❌ Неверный множитель. Пример: 2.5", parse_mode="HTML")
        return
    if target_mult < 1.1 or target_mult > 10:
        bot.send_message(uid, "❌ Множитель от 1.1 до 10", parse_mode="HTML")
        return
    # Списываем ставку ОДИН РАЗ
    update_balance(uid, -bet)
    crash_at = max(1.01, min(20, round(random.expovariate(0.8) + 1.0, 2)))
    if crash_at >= target_mult:
        win = int(bet * target_mult)
        update_balance(uid, win)
        profit = win - bet
        record_game(uid, "краш", bet, profit, f"краш {crash_at:.2f}x → цель {target_mult}x")
        text = (
            f"🚀 <b>Краш на {crash_at:.2f}x!</b>\n"
            f"✅ Твой множитель {target_mult}x выжил!\n"
            f"💰 Выигрыш: +{fmt(profit)} {CURRENCY}"
        )
    else:
        record_game(uid, "краш", bet, 0, f"краш {crash_at:.2f}x → цель {target_mult}x")
        text = (
            f"💥 <b>Краш на {crash_at:.2f}x!</b>\n"
            f"❌ Твой множитель {target_mult}x не успел!\n"
            f"📉 Потеря: -{fmt(bet)} {CURRENCY}"
        )
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 18. РУЛЕТКА
# ═══════════════════════════════════════════════════════════════

RED_NUMBERS = [1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("рулетка"))
def cmd_roulette(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 3:
        bot.send_message(uid, "❌ Формат: <code>рулетка 1000 красное</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    bet_type = parts[2].lower()
    result = random.randint(0, 36)
    win, multiplier = False, 1
    if bet_type in ["красное", "крас", "к", "red", "r"]:
        win = result in RED_NUMBERS; multiplier = 2
    elif bet_type in ["черное", "черн", "чёрное", "black", "b"]:
        win = result != 0 and result not in RED_NUMBERS; multiplier = 2
    elif bet_type in ["зеленое", "зёленое", "зел", "з", "green", "g"]:
        win = result == 0; multiplier = 36
    elif bet_type in ["чет", "чётное", "even"]:
        win = result != 0 and result % 2 == 0; multiplier = 2
    elif bet_type in ["нечет", "нечётное", "odd"]:
        win = result % 2 == 1; multiplier = 2
    elif bet_type.isdigit() and 0 <= int(bet_type) <= 36:
        win = result == int(bet_type); multiplier = 36
    else:
        bot.send_message(uid, "❌ Ставка: красное, черное, зеленое, чет, нечет, или число 0-36", parse_mode="HTML")
        return
    color = "🔴" if result in RED_NUMBERS else "⚫" if result != 0 else "🟢"
    if win:
        profit = bet * multiplier - bet
        update_balance(uid, profit)
        record_game(uid, "рулетка", bet, profit, f"{result}")
        text = f"🎡 <b>Выпало {color}{result}</b>\n🎉 Победа! +{fmt(profit)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        record_game(uid, "рулетка", bet, 0, f"{result}")
        text = f"🎡 <b>Выпало {color}{result}</b>\n😔 Проигрыш. -{fmt(bet)} {CURRENCY}"
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 19. МИНЫ
# ═══════════════════════════════════════════════════════════════

mines_games = {}

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("мины"))
def cmd_mines(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 3:
        bot.send_message(uid, "❌ Формат: <code>мины 1000 3</code>", parse_mode="HTML")
        return
    if uid in mines_games:
        bot.send_message(uid, "⚠️ У тебя уже есть активная игра в мины! Сначала завершите её (нажми 💰 Забрать или 🏃 Выход).", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    try:
        mines_count = int(parts[2])
        if mines_count < 1 or mines_count > 10:
            bot.send_message(uid, "❌ Мин от 1 до 10", parse_mode="HTML")
            return
    except:
        bot.send_message(uid, "❌ Укажи количество мин числом", parse_mode="HTML")
        return
    mine_positions = random.sample(range(25), mines_count)
    mines_games[uid] = {"bet": bet, "mines": mine_positions, "opened": [], "mines_count": mines_count}
    update_balance(uid, -bet)
    show_mines_board(uid, msg.chat.id)

def mines_multiplier(opened: int, mines_count: int) -> float:
    """Чем больше мин и чем больше открыто — тем выше множитель."""
    if opened == 0:
        return 1.0
    safe = 25 - mines_count
    mult = 1.0
    for i in range(opened):
        mult *= (25 - mines_count - i) / (25 - i)
    return max(1.01, round(1.0 / mult * 0.97, 2))  # 3% хаус-эдж

def show_mines_board(uid, chat_id, message_id=None):
    game = mines_games.get(uid)
    if not game:
        return
    mult = mines_multiplier(len(game["opened"]), game["mines_count"])
    potential = int(game["bet"] * mult)
    text = (
        f"💣 <b>Мины</b> | Ставка: {fmt(game['bet'])} | Мин: {game['mines_count']}\n"
        f"✅ Открыто: {len(game['opened'])}/25 | 💰 Потенциал: {fmt(potential)} (×{mult})"
    )
    markup = InlineKeyboardMarkup()
    for i in range(0, 25, 5):
        row = []
        for j in range(5):
            cell = i + j
            if cell in game["opened"]:
                row.append(InlineKeyboardButton("💎", callback_data=f"mines_no_{cell}"))
            else:
                row.append(InlineKeyboardButton("⬜", callback_data=f"mines_open_{uid}_{cell}"))
        markup.row(*row)
    markup.row(
        InlineKeyboardButton(f"💰 Забрать {fmt(potential)}", callback_data=f"mines_cashout_{uid}"),
        InlineKeyboardButton("🏃 Выход", callback_data=f"mines_exit_{uid}")
    )
    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="HTML")
            return
        except:
            pass
    bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_open_"))
def cb_mines_open(call):
    uid = call.from_user.id
    parts = call.data.split("_")
    game_uid = int(parts[2])
    cell = int(parts[3])
    if game_uid != uid:
        bot.answer_callback_query(call.id, "❌ Не твоя игра")
        return
    game = mines_games.get(uid)
    if not game:
        bot.answer_callback_query(call.id, "❌ Игра не найдена")
        return
    if cell in game["opened"]:
        bot.answer_callback_query(call.id)
        return
    if cell in game["mines"]:
        # Показываем все мины
        markup = InlineKeyboardMarkup()
        for i in range(0, 25, 5):
            row = []
            for j in range(5):
                c2 = i + j
                if c2 == cell:
                    row.append(InlineKeyboardButton("💥", callback_data="mines_no_0"))
                elif c2 in game["mines"]:
                    row.append(InlineKeyboardButton("💣", callback_data="mines_no_0"))
                elif c2 in game["opened"]:
                    row.append(InlineKeyboardButton("💎", callback_data="mines_no_0"))
                else:
                    row.append(InlineKeyboardButton("⬜", callback_data="mines_no_0"))
            markup.row(*row)
        record_game(uid, "мины", game["bet"], 0, f"мина на клетке {cell}, открыто {len(game['opened'])}")
        bot.edit_message_text(
            f"💥 <b>БУМ!</b> Ты наступил на мину!\nПотеряно: {fmt(game['bet'])} {CURRENCY}\nОткрыто безопасных: {len(game['opened'])}",
            call.message.chat.id, call.message.message_id,
            reply_markup=markup, parse_mode="HTML"
        )
        del mines_games[uid]
        bot.answer_callback_query(call.id, "💥 МИНА!")
        return
    game["opened"].append(cell)
    # Если открыты все безопасные клетки
    safe_total = 25 - game["mines_count"]
    if len(game["opened"]) >= safe_total:
        mult = mines_multiplier(len(game["opened"]), game["mines_count"])
        win = int(game["bet"] * mult)
        update_balance(uid, win)
        record_game(uid, "мины", game["bet"], win - game["bet"], f"все {len(game['opened'])} безопасных")
        bot.edit_message_text(
            f"🎉 <b>ПОБЕДА! Все мины обойдены!</b>\nОткрыто: {len(game['opened'])}/{safe_total}\n+{fmt(win)} {CURRENCY} (×{mult})",
            call.message.chat.id, call.message.message_id, parse_mode="HTML"
        )
        del mines_games[uid]
        bot.answer_callback_query(call.id, f"🎉 +{fmt(win)}")
        return
    bot.answer_callback_query(call.id, "💎 Безопасно!")
    show_mines_board(uid, call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_cashout_"))
def cb_mines_cashout(call):
    uid = call.from_user.id
    game_uid = int(call.data.split("_")[2])
    if game_uid != uid:
        bot.answer_callback_query(call.id, "❌ Не твоя игра")
        return
    game = mines_games.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "❌ Игра не найдена")
        return
    if len(game["opened"]) == 0:
        # Ничего не открыли — возврат ставки
        update_balance(uid, game["bet"])
        bot.edit_message_text("🏃 Игра отменена. Ставка возвращена.",
                              call.message.chat.id, call.message.message_id, parse_mode="HTML")
        bot.answer_callback_query(call.id, "Ставка возвращена")
        return
    mult = mines_multiplier(len(game["opened"]), game["mines_count"])
    win = int(game["bet"] * mult)
    profit = win - game["bet"]
    update_balance(uid, win)
    record_game(uid, "мины", game["bet"], profit, f"кешаут на {len(game['opened'])} клетках")
    bot.edit_message_text(
        f"💰 <b>Кешаут!</b>\n✅ Открыто: {len(game['opened'])} клеток\n+{fmt(win)} {CURRENCY} (×{mult})",
        call.message.chat.id, call.message.message_id, parse_mode="HTML"
    )
    bot.answer_callback_query(call.id, f"✅ +{fmt(win)}")

@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_exit_"))
def cb_mines_exit(call):
    uid = call.from_user.id
    game_uid = int(call.data.split("_")[2])
    if game_uid != uid:
        bot.answer_callback_query(call.id, "❌ Не твоя игра")
        return
    game = mines_games.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "❌ Игра не найдена")
        return
    # Возвращаем ставку при выходе без открытых клеток
    update_balance(uid, game["bet"])
    bot.edit_message_text(
        f"🏃 Выход из игры. Ставка возвращена: {fmt(game['bet'])} {CURRENCY}",
        call.message.chat.id, call.message.message_id, parse_mode="HTML"
    )
    bot.answer_callback_query(call.id, "✅ Возвращено")

@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_no_"))
def cb_mines_no(call):
    bot.answer_callback_query(call.id)

# ═══════════════════════════════════════════════════════════════
# 20. ЛОТЕРЕЯ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("лотерея"))
def cmd_lottery(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>лотерея 500</code>", parse_mode="HTML")
        return
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная сумма. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    tickets = bet // LOTTERY_TICKET_PRICE
    if tickets == 0:
        bot.send_message(uid, f"❌ Минимум {fmt(LOTTERY_TICKET_PRICE)} за 1 билет", parse_mode="HTML")
        return
    cost = tickets * LOTTERY_TICKET_PRICE
    update_balance(uid, -cost)
    with db() as c:
        c.execute("""
            INSERT INTO lottery_tickets (user_id, tickets) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET tickets=lottery_tickets.tickets+%s
        """, (uid, tickets, tickets))
        c.execute("UPDATE lottery SET jackpot=jackpot+%s WHERE id=1", (cost,))
        c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
        row = c.fetchone()
    jackpot = row["jackpot"]
    draw_at = row["draw_at"]
    time_left = max(0, int(draw_at) - now())
    bot.send_message(uid,
        f"🎟️ Куплено <b>{tickets}</b> билет(ов) за {fmt(cost)} {CURRENCY}\n"
        f"💰 Джекпот: <b>{fmt(jackpot)} {CURRENCY}</b>\n"
        f"⏰ Розыгрыш через: {cd_str(time_left)}",
        parse_mode="HTML")

def run_lottery_draw():
    """Провести розыгрыш лотереи."""
    try:
        with db() as c:
            c.execute("SELECT jackpot FROM lottery WHERE id=1")
            lotto = c.fetchone()
            if not lotto or lotto["jackpot"] == 0:
                # Обновляем время следующего розыгрыша
                c.execute("UPDATE lottery SET draw_at=%s WHERE id=1", (now() + LOTTERY_INTERVAL,))
                return
            c.execute("SELECT user_id, tickets FROM lottery_tickets WHERE tickets > 0")
            participants = c.fetchall()
            if not participants:
                c.execute("UPDATE lottery SET draw_at=%s WHERE id=1", (now() + LOTTERY_INTERVAL,))
                return
            pool = []
            for p in participants:
                pool.extend([p["user_id"]] * p["tickets"])
            winner = random.choice(pool)
            jackpot = lotto["jackpot"]
            update_balance(winner, jackpot)
            c.execute("UPDATE lottery SET jackpot=0, draw_at=%s WHERE id=1", (now() + LOTTERY_INTERVAL,))
            c.execute("DELETE FROM lottery_tickets")
            try:
                w = get_user(winner)
                name = w["name"] if w else "Игрок"
            except:
                name = "Игрок"
            try:
                bot.send_message(winner,
                    f"🎉🎊 <b>ВЫ ВЫИГРАЛИ ЛОТЕРЕЮ!</b> 🎊🎉\n\n"
                    f"💰 Выигрыш: <b>{fmt(jackpot)} {CURRENCY}</b>\n"
                    f"🎟️ Участников было: {len(pool)}\n\n"
                    f"Удача на вашей стороне! 🍀",
                    parse_mode="HTML")
            except:
                pass
            print(f"[lottery] Победитель: {winner} ({name}), выигрыш: {jackpot}")
    except Exception as e:
        print(f"[lottery] ошибка: {e}")

def lottery_scheduler():
    while True:
        try:
            with db() as c:
                c.execute("SELECT draw_at FROM lottery WHERE id=1")
                row = c.fetchone()
            if row:
                draw_at = int(row["draw_at"])
                sleep_time = max(10, draw_at - now())
            else:
                sleep_time = LOTTERY_INTERVAL
            time.sleep(min(sleep_time, 3600))  # проверяем не реже чем раз в час
            with db() as c:
                c.execute("SELECT draw_at FROM lottery WHERE id=1")
                row2 = c.fetchone()
            if row2 and int(row2["draw_at"]) <= now():
                run_lottery_draw()
        except Exception as e:
            print(f"[lottery_scheduler] ошибка: {e}")
            time.sleep(60)

# ═══════════════════════════════════════════════════════════════
# 21. БАНК
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🏦 Банк")
def cmd_bank(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()
    loan_text = ""
    if loan and loan["amount"] > 0:
        due = datetime.fromtimestamp(loan["due_at"]).strftime("%d.%m")
        overdue = now() > loan["due_at"]
        loan_text = f"\n{'🔴' if overdue else '⚠️'} Долг: <b>{fmt(loan['amount'])}</b> до {due}{' (ПРОСРОЧЕН!)' if overdue else ''}"
    text = (
        f"<b>🏦 Банк</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"💰 Кошелёк: {fmt(u['balance'])} {CURRENCY}\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"📈 Ставка: {BANK_RATE*100:.2f}%/3ч{loan_text}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Команды:</b>\n"
        f"<code>вклад 5000</code> — положить на депозит\n"
        f"<code>снять 3000</code> — снять с депозита\n"
        f"<code>кредит 10000</code> — взять кредит\n"
        f"<code>погасить 5000</code> — погасить кредит"
    )
    bot.send_message(uid, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("вклад "))
def cmd_deposit(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>вклад 5000</code>", parse_mode="HTML")
        return
    amount = parse_bet(parts[1], u["balance"])
    if not amount or amount <= 0 or amount > int(u["balance"]):
        bot.send_message(uid, f"❌ Неверная сумма. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    update_balance(uid, -amount)
    with db() as c:
        c.execute("UPDATE users SET bank=bank+%s WHERE id=%s", (amount, uid))
    bot.send_message(uid, f"✅ Внесено на депозит: <b>{fmt(amount)} {CURRENCY}</b>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("снять "))
def cmd_withdraw(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>снять 3000</code>", parse_mode="HTML")
        return
    amount = parse_bet(parts[1], u["bank"])
    if not amount or amount <= 0 or amount > int(u["bank"]):
        bot.send_message(uid, f"❌ Неверная сумма. Депозит: {fmt(u['bank'])}", parse_mode="HTML")
        return
    update_balance(uid, amount)
    with db() as c:
        c.execute("UPDATE users SET bank=bank-%s WHERE id=%s", (amount, uid))
    bot.send_message(uid, f"✅ Снято с депозита: <b>{fmt(amount)} {CURRENCY}</b>", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 22. КРЕДИТ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("кредит "))
def cmd_loan(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, f"❌ Формат: <code>кредит 10000</code>\nМаксимум: {fmt(LOAN_MAX)}", parse_mode="HTML")
        return
    amount = parse_bet(parts[1], u["balance"])
    if not amount or amount <= 0:
        bot.send_message(uid, "❌ Неверная сумма", parse_mode="HTML")
        return
    if amount > LOAN_MAX:
        bot.send_message(uid, f"❌ Максимум кредита: {fmt(LOAN_MAX)}", parse_mode="HTML")
        return
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
        existing = c.fetchone()
        if existing and existing["amount"] > 0:
            bot.send_message(uid, "❌ У тебя уже есть кредит. Сначала погаси его.", parse_mode="HTML")
            return
    total_debt = int(amount * (1 + LOAN_RATE))
    due = now() + LOAN_TERM * 86400
    update_balance(uid, amount)
    with db() as c:
        c.execute("""
            INSERT INTO loans (user_id, amount, due_at) VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET amount=%s, due_at=%s
        """, (uid, total_debt, due, total_debt, due))
    bot.send_message(uid,
        f"✅ Кредит выдан: <b>{fmt(amount)} {CURRENCY}</b>\n"
        f"💳 Вернуть: <b>{fmt(total_debt)}</b> ({LOAN_RATE*100:.0f}% комиссия)\n"
        f"📅 Срок: {datetime.fromtimestamp(due).strftime('%d.%m.%Y')}",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("погасить "))
def cmd_repay(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>погасить 5000</code>", parse_mode="HTML")
        return
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=%s", (uid,))
        loan = c.fetchone()
    if not loan or loan["amount"] == 0:
        bot.send_message(uid, "❌ У тебя нет кредита", parse_mode="HTML")
        return
    debt = loan["amount"]
    amount = parse_bet(parts[1], u["balance"])
    if not amount or amount <= 0:
        bot.send_message(uid, "❌ Неверная сумма", parse_mode="HTML")
        return
    pay = min(amount, debt)
    if int(u["balance"]) < pay:
        bot.send_message(uid, f"❌ Не хватает {fmt(pay)} {CURRENCY}", parse_mode="HTML")
        return
    update_balance(uid, -pay)
    new_debt = debt - pay
    with db() as c:
        if new_debt <= 0:
            c.execute("DELETE FROM loans WHERE user_id=%s", (uid,))
            bot.send_message(uid, f"✅ <b>Кредит полностью погашен!</b> 🎉", parse_mode="HTML")
        else:
            c.execute("UPDATE loans SET amount=%s WHERE user_id=%s", (new_debt, uid))
            bot.send_message(uid, f"✅ Погашено: {fmt(pay)} {CURRENCY}\nОстаток долга: <b>{fmt(new_debt)}</b>", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 23. БИРЖА
# ═══════════════════════════════════════════════════════════════

def get_stock_price():
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=%s", (TICKER,))
        row = c.fetchone()
        return int(row["price"]) if row else STOCK_PRICE_START

def update_stock_price(impact: float = 0):
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=%s", (TICKER,))
        row = c.fetchone()
        price = int(row["price"])
        drift = (STOCK_PRICE_START - price) * 0.01
        new_price = max(100, int(price * (1 + drift + impact + random.uniform(-STOCK_VOLATILITY, STOCK_VOLATILITY))))
        c.execute(
            "UPDATE stocks SET prev_price=price, price=%s, updated_at=%s WHERE ticker=%s",
            (new_price, now(), TICKER)
        )
        c.execute(
            "INSERT INTO stock_history (ticker, price, ts) VALUES (%s, %s, %s)",
            (TICKER, new_price, now())
        )

@bot.message_handler(func=lambda m: m.text == "📈 Биржа")
def cmd_stock(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    price = get_stock_price()
    with db() as c:
        c.execute("SELECT shares, avg_buy, last_trade FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        port = c.fetchone()
        c.execute("SELECT prev_price FROM stocks WHERE ticker=%s", (TICKER,))
        stock_row = c.fetchone()
    prev = int(stock_row["prev_price"]) if stock_row else price
    chg = (price - prev) / prev * 100 if prev else 0
    icon = "🟢" if price >= prev else "🔴"
    cd_left = 0
    if port and port["last_trade"] > 0:
        cd_left = max(0, STOCK_COOLDOWN - (now() - int(port["last_trade"])))
    text = (
        f"<b>📈 Биржа — {TICKER}</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"Цена: <b>{fmt(price)} {CURRENCY}</b>  {icon} {chg:+.1f}%\n\n"
    )
    if port and port["shares"] > 0:
        pnl = (price - int(port["avg_buy"])) * int(port["shares"])
        pnl_icon = "📈" if pnl >= 0 else "📉"
        text += (
            f"📂 Портфель: <b>{port['shares']}</b> акций\n"
            f"📊 Средняя: {fmt(port['avg_buy'])}\n"
            f"{pnl_icon} P&L: {'+' if pnl>=0 else ''}{fmt(pnl)} {CURRENCY}\n"
        )
    if cd_left > 0:
        text += f"⏳ Кулдаун: {cd_str(cd_left)}\n"
    text += (
        f"\n<b>Команды:</b>\n"
        f"<code>купить 10</code> — купить акции\n"
        f"<code>продать 10</code> — продать акции\n"
        f"<code>история акций</code> — история цен"
    )
    bot.send_message(uid, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("купить ") and not m.text.lower().startswith("купить карту"))
def cmd_buy_stock(msg):
    uid = msg.from_user.id
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>купить 10</code>", parse_mode="HTML")
        return
    try:
        qty = int(parts[1])
        if qty <= 0:
            raise ValueError
    except:
        bot.send_message(uid, "❌ Укажи положительное целое число акций", parse_mode="HTML")
        return
    price = get_stock_price()
    total = price * qty
    u = get_user(uid)
    if not u:
        return
    with db() as c:
        c.execute("SELECT last_trade, shares FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        row = c.fetchone()
        if row and row["last_trade"] > 0:
            cd_left = STOCK_COOLDOWN - (now() - int(row["last_trade"]))
            if cd_left > 0:
                bot.send_message(uid, f"⏳ Кулдаун торговли: {cd_str(cd_left)}", parse_mode="HTML")
                return
        cur_shares = int(row["shares"]) if row else 0
        if cur_shares + qty > STOCK_MAX_PER_USER:
            bot.send_message(uid, f"❌ Максимум {STOCK_MAX_PER_USER} акций на руках", parse_mode="HTML")
            return
    if int(u["balance"]) < total:
        bot.send_message(uid, f"❌ Нужно {fmt(total)} {CURRENCY}, у тебя {fmt(u['balance'])}", parse_mode="HTML")
        return
    with db() as c:
        c.execute("SELECT shares, avg_buy FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        port = c.fetchone()
        old_shares = int(port["shares"]) if port else 0
        old_avg = int(port["avg_buy"]) if port else 0
        new_avg = (old_avg * old_shares + price * qty) // (old_shares + qty)
        c.execute("""
            INSERT INTO stock_portfolio (user_id, ticker, shares, avg_buy, last_trade)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id, ticker) DO UPDATE SET
                shares = stock_portfolio.shares + %s,
                avg_buy = %s,
                last_trade = %s
        """, (uid, TICKER, qty, new_avg, now(), qty, new_avg, now()))
        c.execute(
            "INSERT INTO stock_trades (user_id, ticker, action, amount, price, total, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (uid, TICKER, "buy", qty, price, total, now())
        )
    update_balance(uid, -total)
    update_stock_price(0.0005 * qty)
    bot.send_message(uid,
        f"✅ Куплено <b>{qty}</b> акций {TICKER}\n"
        f"💰 Потрачено: {fmt(total)} {CURRENCY}\n"
        f"📊 Цена за акцию: {fmt(price)}",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("продать "))
def cmd_sell_stock(msg):
    uid = msg.from_user.id
    parts = msg.text.split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>продать 10</code>", parse_mode="HTML")
        return
    try:
        qty = int(parts[1])
        if qty <= 0:
            raise ValueError
    except:
        bot.send_message(uid, "❌ Укажи положительное целое число акций", parse_mode="HTML")
        return
    price = get_stock_price()
    with db() as c:
        c.execute("SELECT shares, avg_buy, last_trade FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        port = c.fetchone()
    if not port or int(port["shares"]) < qty:
        cur = int(port["shares"]) if port else 0
        bot.send_message(uid, f"❌ У тебя только {cur} акций", parse_mode="HTML")
        return
    if port["last_trade"] and int(port["last_trade"]) > 0:
        cd_left = STOCK_COOLDOWN - (now() - int(port["last_trade"]))
        if cd_left > 0:
            bot.send_message(uid, f"⏳ Кулдаун торговли: {cd_str(cd_left)}", parse_mode="HTML")
            return
    total = price * qty
    fee = int(total * STOCK_SELL_FEE)
    net = total - fee
    pnl = net - int(port["avg_buy"]) * qty
    update_balance(uid, net)
    with db() as c:
        new_shares = int(port["shares"]) - qty
        if new_shares == 0:
            c.execute("DELETE FROM stock_portfolio WHERE user_id=%s AND ticker=%s", (uid, TICKER))
        else:
            c.execute(
                "UPDATE stock_portfolio SET shares=%s, last_trade=%s WHERE user_id=%s AND ticker=%s",
                (new_shares, now(), uid, TICKER)
            )
        c.execute(
            "INSERT INTO stock_trades (user_id, ticker, action, amount, price, fee, total, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (uid, TICKER, "sell", qty, price, fee, net, now())
        )
    update_stock_price(-0.0005 * qty)
    pnl_icon = "📈" if pnl >= 0 else "📉"
    bot.send_message(uid,
        f"✅ Продано <b>{qty}</b> акций {TICKER}\n"
        f"💰 Получено: {fmt(net)} (комиссия {fmt(fee)})\n"
        f"{pnl_icon} P&L: {'+' if pnl>=0 else ''}{fmt(pnl)} {CURRENCY}",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "история акций")
def cmd_stock_history(msg):
    uid = msg.from_user.id
    with db() as c:
        c.execute("SELECT price, ts FROM stock_history WHERE ticker=%s ORDER BY ts DESC LIMIT 10", (TICKER,))
        history = list(reversed(c.fetchall()))
    if not history:
        bot.send_message(uid, "📊 История пуста", parse_mode="HTML")
        return
    lines = []
    for i, row in enumerate(history):
        price = int(row["price"])
        ts = int(row["ts"])
        dt = datetime.fromtimestamp(ts).strftime("%d.%m %H:%M")
        if i > 0:
            prev = int(history[i-1]["price"])
            chg = (price - prev) / prev * 100
            icon = "🟢" if price >= prev else "🔴"
            lines.append(f"{icon} {dt}  <b>{fmt(price)}</b>  ({chg:+.1f}%)")
        else:
            lines.append(f"⬜ {dt}  <b>{fmt(price)}</b>")
    with db() as c:
        c.execute(
            "SELECT action, amount, price, created_at FROM stock_trades WHERE user_id=%s ORDER BY created_at DESC LIMIT 5",
            (uid,)
        )
        trades = c.fetchall()
    trade_lines = []
    for row in trades:
        dt = datetime.fromtimestamp(int(row["created_at"])).strftime("%d.%m %H:%M")
        icon = "🛒" if row["action"] == "buy" else "💰"
        trade_lines.append(f"{icon} {dt}  {row['amount']} шт. × {fmt(row['price'])}")
    text = f"<b>📊 История цен {TICKER}</b>\n\n" + "\n".join(lines)
    if trade_lines:
        text += f"\n\n<b>📋 Мои сделки:</b>\n" + "\n".join(trade_lines)
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 24. БОНУС
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🎁 Бонус")
def cmd_bonus(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    cd = CD_DAILY_PREMIUM if is_premium(uid) else CD_DAILY_NORMAL
    remaining = cd - (now() - int(u["last_daily"]))
    if remaining > 0:
        bot.send_message(uid, f"🎁 Следующий бонус через: <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return
    streak = int(u["daily_streak"])
    lvl = user_level(u["xp"])
    bonus = int(DAILY_BASE * (1 + min(streak * 0.1, 2) + lvl * 0.02))
    update_balance(uid, bonus)
    add_xp(uid, 50 + lvl)
    with db() as c:
        c.execute("UPDATE users SET last_daily=%s, daily_streak=daily_streak+1 WHERE id=%s", (now(), uid))
    streak_bonus = ""
    if streak > 0 and streak % 7 == 0:
        week_bonus = bonus
        update_balance(uid, week_bonus)
        streak_bonus = f"\n🎊 Бонус за {streak} дней! +{fmt(week_bonus)} дополнительно!"
    bot.send_message(uid,
        f"🎁 <b>Ежедневный бонус!</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Стрик: {streak + 1} дней\n"
        f"💰 Получено: <b>{fmt(bonus)} {CURRENCY}</b>\n"
        f"⭐ +{50 + lvl} опыта"
        f"{streak_bonus}",
        parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 25. ТОП
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🏆 Топ")
def cmd_top(msg):
    uid = msg.from_user.id
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("💰 По балансу", callback_data="top_balance"),
        InlineKeyboardButton("⭐ По опыту", callback_data="top_xp")
    )
    markup.row(
        InlineKeyboardButton("🎮 По победам", callback_data="top_wins"),
        InlineKeyboardButton("📈 По заработку", callback_data="top_earned")
    )
    bot.send_message(uid, "🏆 <b>Таблица лидеров</b>\nВыбери категорию:", reply_markup=markup, parse_mode="HTML")

def build_top_text(field: str, label: str, format_val=None):
    with db() as c:
        c.execute(f"SELECT name, {field} FROM users ORDER BY {field} DESC LIMIT 10")
        rows = c.fetchall()
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    text = f"<b>🏆 ТОП — {label}</b>\n━━━━━━━━━━━━━━━━━━\n"
    for i, row in enumerate(rows):
        val = format_val(row[field]) if format_val else str(row[field])
        text += f"{medals[i]} {row['name'] or 'Игрок'} — <b>{val}</b>\n"
    return text

@bot.callback_query_handler(func=lambda c: c.data.startswith("top_"))
def cb_top(call):
    uid = call.from_user.id
    category = call.data[4:]
    if category == "balance":
        text = build_top_text("balance", "Баланс", lambda v: f"{fmt(v)} {CURRENCY}")
        with db() as c:
            c.execute("SELECT COUNT(*)+1 FROM users WHERE balance > (SELECT balance FROM users WHERE id=%s)", (uid,))
            pos = c.fetchone()["count"]
        u = get_user(uid)
        text += f"\n👤 Ты: #{pos} | {fmt(u['balance'])} {CURRENCY}"
    elif category == "xp":
        text = build_top_text("xp", "Опыт", lambda v: f"{fmt(v)} XP (ур.{user_level(v)})")
        with db() as c:
            c.execute("SELECT COUNT(*)+1 FROM users WHERE xp > (SELECT xp FROM users WHERE id=%s)", (uid,))
            pos = c.fetchone()["count"]
        u = get_user(uid)
        text += f"\n👤 Ты: #{pos} | {fmt(u['xp'])} XP"
    elif category == "wins":
        text = build_top_text("games_won", "Победы", lambda v: f"{v} побед")
        with db() as c:
            c.execute("SELECT COUNT(*)+1 FROM users WHERE games_won > (SELECT games_won FROM users WHERE id=%s)", (uid,))
            pos = c.fetchone()["count"]
        u = get_user(uid)
        text += f"\n👤 Ты: #{pos} | {u['games_won']} побед"
    elif category == "earned":
        text = build_top_text("total_earned", "Заработок", lambda v: f"{fmt(v)} {CURRENCY}")
        with db() as c:
            c.execute("SELECT COUNT(*)+1 FROM users WHERE total_earned > (SELECT total_earned FROM users WHERE id=%s)", (uid,))
            pos = c.fetchone()["count"]
        u = get_user(uid)
        text += f"\n👤 Ты: #{pos} | {fmt(u['total_earned'])} {CURRENCY}"
    else:
        bot.answer_callback_query(call.id)
        return
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("💰 Баланс", callback_data="top_balance"),
        InlineKeyboardButton("⭐ Опыт", callback_data="top_xp")
    )
    markup.row(
        InlineKeyboardButton("🎮 Победы", callback_data="top_wins"),
        InlineKeyboardButton("📈 Заработок", callback_data="top_earned")
    )
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                              reply_markup=markup, parse_mode="HTML")
    except:
        bot.send_message(uid, text, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

# ═══════════════════════════════════════════════════════════════
# 26. ПЕРЕВОДЫ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("перевод"))
def cmd_transfer(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 3:
        bot.send_message(uid, "❌ Формат: <code>перевод @username 5000</code>", parse_mode="HTML")
        return
    target = parts[1].lstrip("@")
    amount = parse_bet(parts[2], u["balance"])
    if not amount or amount <= 0:
        bot.send_message(uid, "❌ Неверная сумма", parse_mode="HTML")
        return
    with db() as c:
        if target.isdigit():
            c.execute("SELECT id, name FROM users WHERE id=%s", (int(target),))
        else:
            c.execute("SELECT id, name FROM users WHERE name ILIKE %s", (f"%{target}%",))
        row = c.fetchone()
    if not row:
        bot.send_message(uid, "❌ Пользователь не найден", parse_mode="HTML")
        return
    to_uid = row["id"]
    if to_uid == uid:
        bot.send_message(uid, "❌ Нельзя переводить самому себе", parse_mode="HTML")
        return
    fee_rate = PREMIUM_TRANSFER_FEE if is_premium(uid) else TRANSFER_FEE
    fee = int(amount * fee_rate)
    total = amount + fee
    if int(u["balance"]) < total:
        bot.send_message(uid, f"❌ Нужно {fmt(total)} {CURRENCY} (включая комиссию {fmt(fee)})", parse_mode="HTML")
        return
    last_tr = int(u.get("last_transfer") or 0)
    if now() - last_tr < CD_TRANSFER:
        bot.send_message(uid, f"⏱ Подожди {cd_str(CD_TRANSFER - (now() - last_tr))} между переводами", parse_mode="HTML")
        return
    update_balance(uid, -total)
    update_balance(to_uid, amount)
    with db() as c:
        c.execute("UPDATE users SET last_transfer=%s WHERE id=%s", (now(), uid))
        c.execute(
            "INSERT INTO transfers (from_id, to_id, amount, fee, ts) VALUES (%s,%s,%s,%s,%s)",
            (uid, to_uid, amount, fee, now())
        )
    bot.send_message(uid,
        f"✅ Переведено <b>{fmt(amount)} {CURRENCY}</b> → {row['name'] or target}\n"
        f"💸 Комиссия: {fmt(fee)}",
        parse_mode="HTML")
    try:
        bot.send_message(to_uid, f"💰 Тебе перевели <b>{fmt(amount)} {CURRENCY}</b>!", parse_mode="HTML")
    except:
        pass

# ═══════════════════════════════════════════════════════════════
# 27. РЕФЕРАЛ
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
        referrals = c.fetchone()["count"]
    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={ref_code}"
    bot.send_message(uid,
        f"<b>🔗 Реферальная программа</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"👥 Приглашено: <b>{referrals}</b> игроков\n"
        f"🎁 Бонус за приглашение:\n"
        f"  • Тебе: +2 000 {CURRENCY}\n"
        f"  • Другу: +1 000 {CURRENCY}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔗 Твоя ссылка:\n<code>{link}</code>",
        parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 28. ДОНАТ
# ═══════════════════════════════════════════════════════════════

DONATE_PACKAGES = {
    "s1":   {"stars": 1,   "amount": 10000,   "label": "⭐ 10 000"},
    "s5":   {"stars": 5,   "amount": 66000,   "label": "⭐ 66 000"},
    "s15":  {"stars": 15,  "amount": 266000,  "label": "🔥 266 000"},
    "s50":  {"stars": 50,  "amount": 1000000, "label": "🔥 1 000 000"},
    "s150": {"stars": 150, "amount": 4000000, "label": "💎 4 000 000"},
    "s250": {"stars": 250, "amount": 8000000, "label": "💎 8 000 000"},
}

@bot.message_handler(func=lambda m: m.text == "💎 Донат")
def cmd_donate(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    text = "<b>💎 Пополнение баланса за Telegram Stars</b>\n━━━━━━━━━━━━━━━━━━\n"
    for key, pkg in DONATE_PACKAGES.items():
        text += f"{pkg['label']} → {pkg['stars']} ⭐\n"
    markup = InlineKeyboardMarkup()
    for key, pkg in DONATE_PACKAGES.items():
        markup.add(InlineKeyboardButton(f"{pkg['label']} — {pkg['stars']} ⭐", callback_data=f"donate_{key}"))
    bot.send_message(uid, text, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("donate_"))
def cb_donate(call):
    uid = call.from_user.id
    key = call.data[7:]
    pkg = DONATE_PACKAGES.get(key)
    if not pkg:
        bot.answer_callback_query(call.id, "❌ Пакет не найден")
        return
    bot.send_invoice(
        uid,
        title=f"Пополнение {fmt(pkg['amount'])} {CURRENCY}",
        description=f"Получи {fmt(pkg['amount'])} монет в FECTIZ BOT",
        payload=f"donate_{key}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(f"{pkg['stars']} ⭐", pkg['stars'])]
    )
    bot.answer_callback_query(call.id)

@bot.pre_checkout_query_handler(func=lambda q: True)
def pre_checkout(query):
    bot.answer_pre_checkout_query(query.id, ok=True)

@bot.message_handler(content_types=["successful_payment"])
def successful_payment(msg):
    uid = msg.from_user.id
    key = msg.successful_payment.invoice_payload[7:]
    pkg = DONATE_PACKAGES.get(key)
    if pkg:
        update_balance(uid, pkg["amount"])
        bot.send_message(uid,
            f"✅ <b>Пополнение успешно!</b>\n💰 +{fmt(pkg['amount'])} {CURRENCY}\nСпасибо за поддержку! 💎",
            parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 29. ПРОМОКОДЫ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("промо "))
def cmd_promo(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    code = msg.text.split(None, 1)[1].strip().upper()
    with db() as c:
        c.execute("SELECT * FROM promo_codes WHERE code=%s AND active=TRUE", (code,))
        promo = c.fetchone()
    if not promo:
        bot.send_message(uid, "❌ Промокод не найден или деактивирован", parse_mode="HTML")
        return
    if promo["expires"] and int(promo["expires"]) > 0 and int(promo["expires"]) < now():
        bot.send_message(uid, "❌ Промокод истёк", parse_mode="HTML")
        return
    if int(promo["uses"]) >= int(promo["max_uses"]):
        bot.send_message(uid, "❌ Промокод исчерпан", parse_mode="HTML")
        return
    with db() as c:
        try:
            c.execute("INSERT INTO promo_uses (user_id, code, ts) VALUES (%s,%s,%s)", (uid, code, now()))
        except Exception:
            bot.send_message(uid, "❌ Ты уже использовал этот промокод", parse_mode="HTML")
            return
        c.execute("UPDATE promo_codes SET uses=uses+1 WHERE code=%s", (code,))
    update_balance(uid, promo["reward"])
    bot.send_message(uid, f"✅ Промокод <b>{code}</b> активирован!\n💰 +{fmt(promo['reward'])} {CURRENCY}", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("создать промо ") and is_admin(m.from_user.id))
def cmd_create_promo(msg):
    parts = msg.text.split()
    if len(parts) < 4:
        bot.reply_to(msg, "❌ Формат: создать промо КОД СУММА [uses]\nПример: создать промо SALE23 50000 100")
        return
    code = parts[2].upper()
    try:
        reward = int(parts[3])
        uses = int(parts[4]) if len(parts) > 4 else 1
    except:
        bot.reply_to(msg, "❌ Неверные параметры")
        return
    with db() as c:
        try:
            c.execute("INSERT INTO promo_codes (code, reward, max_uses) VALUES (%s,%s,%s)", (code, reward, uses))
        except Exception:
            bot.reply_to(msg, "❌ Промокод уже существует")
            return
    bot.reply_to(msg, f"✅ Промокод <code>{code}</code> создан\n💰 +{fmt(reward)}, {uses} использований", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("удалить промо ") and is_admin(m.from_user.id))
def cmd_delete_promo(msg):
    code = msg.text.split(None, 2)[2].strip().upper()
    with db() as c:
        c.execute("UPDATE promo_codes SET active=FALSE WHERE code=%s", (code,))
    bot.reply_to(msg, f"✅ Промокод <code>{code}</code> деактивирован", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 30. ЧЕКИ
# ═══════════════════════════════════════════════════════════════

def _activate_check_by_code(uid: int, code: str, chat_id: int):
    """Вспомогательная функция активации чека."""
    with db() as c:
        c.execute("SELECT * FROM checks WHERE code=%s", (code,))
        check = c.fetchone()
    if not check:
        bot.send_message(chat_id, "❌ Чек не найден", parse_mode="HTML")
        return
    if int(check["current_activations"]) >= int(check["max_activations"]):
        bot.send_message(chat_id, "❌ Чек уже исчерпан", parse_mode="HTML")
        return
    if check.get("password"):
        # Запрашиваем пароль
        msg = bot.send_message(chat_id, "🔒 Введите пароль для активации чека:", parse_mode="HTML")
        bot.register_next_step_handler(msg, lambda m: _check_password_step(m, uid, code, check["password"]))
        return
    _do_activate_check(uid, code, check, chat_id)

def _check_password_step(msg, uid: int, code: str, expected_password: str):
    if msg.text and msg.text.strip() == expected_password:
        with db() as c:
            c.execute("SELECT * FROM checks WHERE code=%s", (code,))
            check = c.fetchone()
        if check:
            _do_activate_check(uid, code, check, msg.chat.id)
    else:
        bot.send_message(msg.chat.id, "❌ Неверный пароль", parse_mode="HTML")

def _do_activate_check(uid: int, code: str, check, chat_id: int):
    with db() as c:
        try:
            c.execute(
                "INSERT INTO check_activations (user_id, check_code, activated_at) VALUES (%s,%s,%s)",
                (uid, code, now())
            )
        except Exception:
            bot.send_message(chat_id, "❌ Ты уже активировал этот чек", parse_mode="HTML")
            return
        c.execute("UPDATE checks SET current_activations=current_activations+1 WHERE code=%s", (code,))
    update_balance(uid, check["amount"])
    remaining = int(check["max_activations"]) - int(check["current_activations"]) - 1
    bot.send_message(chat_id,
        f"✅ Чек активирован!\n"
        f"💰 +{fmt(check['amount'])} {CURRENCY}\n"
        f"🔄 Осталось активаций: {remaining}",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("чек "))
def cmd_check(msg):
    uid = msg.from_user.id
    u = ensure_user(uid, msg.from_user.first_name or "Игрок")
    parts = msg.text.split()
    if len(parts) < 3:
        bot.send_message(uid,
            "❌ Формат: <code>чек СУММА КОЛ-ВО [пароль]</code>\n"
            "Пример: <code>чек 5000 3</code>\n"
            "С паролем: <code>чек 5000 1 секрет</code>",
            parse_mode="HTML")
        return
    amount = parse_bet(parts[1], u["balance"])
    if not amount or amount <= 0:
        bot.send_message(uid, "❌ Неверная сумма", parse_mode="HTML")
        return
    try:
        uses = int(parts[2])
        if uses < 1 or uses > 100:
            raise ValueError
    except:
        bot.send_message(uid, "❌ Количество активаций от 1 до 100", parse_mode="HTML")
        return
    password = parts[3] if len(parts) > 3 else None
    total = amount * uses
    if int(u["balance"]) < total:
        bot.send_message(uid, f"❌ Нужно {fmt(total)} {CURRENCY} (на {uses} активаций)", parse_mode="HTML")
        return
    code = f"CHK{random.randint(100000, 999999)}"
    update_balance(uid, -total)
    with db() as c:
        c.execute(
            "INSERT INTO checks (code, amount, max_activations, password, created_by, created_at) VALUES (%s,%s,%s,%s,%s,%s)",
            (code, amount, uses, password, uid, now())
        )
    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={code}"
    pw_text = f"\n🔒 Пароль: <code>{password}</code>" if password else ""
    bot.send_message(uid,
        f"✅ Чек создан!\n"
        f"💵 {fmt(amount)} {CURRENCY} × {uses} активаций\n"
        f"💎 Всего заморожено: {fmt(total)}"
        f"{pw_text}\n\n"
        f"🔗 Ссылка:\n<code>{link}</code>",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("активировать "))
def cmd_activate_check(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "Игрок")
    code = msg.text.split(None, 1)[1].strip().upper()
    _activate_check_by_code(uid, code, msg.chat.id)

# ═══════════════════════════════════════════════════════════════
# 31. ПОМОЩЬ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "📞 Помощь")
def cmd_help(msg):
    text = (
        "<b>📞 ПОМОЩЬ ПО FECTIZ BOT</b>\n\n"
        "<b>👤 Профиль</b> — статистика, уровень, винрейт\n"
        "<b>💰 Баланс</b> — кошелёк + депозит + долг\n"
        "<b>🎁 Бонус</b> — ежедневная награда (стрик растёт!)\n"
        "<b>🔗 Реферал</b> — пригласи друга и получи бонус\n\n"
        "<b>⚒️ РАБОТА</b>\n"
        "• <b>👆 Кликер</b> — раз в 5 сек\n"
        "• <b>⛏️ Майнинг</b> — раз в час (видеокарты = больше монет)\n"
        "• <b>🚗 Такси</b> — случайный маршрут, нажми «Завершить»\n\n"
        "<b>🎰 ИГРЫ</b>\n"
        "<code>кости 1000 чет/нечет/малые/большие/1-6</code>\n"
        "<code>слоты 5000</code>\n"
        "<code>дартс 2000</code>\n"
        "<code>боулинг 1000</code>\n"
        "<code>баскетбол 1500</code>\n"
        "<code>футбол 800</code>\n"
        "<code>рулетка 1000 красное/черное/зеленое/0-36</code>\n"
        "<code>мины 5000 3</code> — открывай клетки, забирай прибыль\n"
        "<code>краш 10000 3.0</code> — множитель 1.1–10\n"
        "<code>лотерея 1000</code> — купить билеты\n\n"
        "<b>🏦 БАНК</b>\n"
        "<code>вклад 5000</code> / <code>снять 3000</code>\n"
        "<code>кредит 10000</code> / <code>погасить 5000</code>\n\n"
        "<b>📈 БИРЖА</b>\n"
        "<code>купить 10</code> / <code>продать 5</code>\n"
        "<code>история акций</code>\n\n"
        "<b>💸 ПЕРЕВОДЫ И ЧЕКИ</b>\n"
        "<code>перевод @username 5000</code>\n"
        "<code>чек 5000 3</code> — создать чек\n"
        "<code>активировать КОД</code> — активировать чек\n"
        "<code>промо КОД</code> — промокод\n\n"
        "💡 <i>Суммы: 100к, 1.5м, 2б, все</i>"
    )
    bot.send_message(msg.chat.id, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 32. АДМИН-КОМАНДЫ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("выдать ") and is_admin(m.from_user.id))
def cmd_give(msg):
    parts = msg.text.split()
    if len(parts) < 3:
        bot.reply_to(msg, "❌ Формат: выдать @username сумма")
        return
    target = parts[1].lstrip("@")
    try:
        amount = int(parts[2])
    except:
        bot.reply_to(msg, "❌ Неверная сумма")
        return
    with db() as c:
        if target.isdigit():
            c.execute("SELECT id, name FROM users WHERE id=%s", (int(target),))
        else:
            c.execute("SELECT id, name FROM users WHERE name ILIKE %s", (f"%{target}%",))
        row = c.fetchone()
    if not row:
        bot.reply_to(msg, "❌ Пользователь не найден")
        return
    update_balance(row["id"], amount)
    bot.reply_to(msg, f"✅ Выдано {fmt(amount)} {CURRENCY} → {row['name']}")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("забрать ") and is_admin(m.from_user.id))
def cmd_take(msg):
    parts = msg.text.split()
    if len(parts) < 3:
        bot.reply_to(msg, "❌ Формат: забрать @username сумма")
        return
    target = parts[1].lstrip("@")
    try:
        amount = int(parts[2])
    except:
        bot.reply_to(msg, "❌ Неверная сумма")
        return
    with db() as c:
        if target.isdigit():
            c.execute("SELECT id, name FROM users WHERE id=%s", (int(target),))
        else:
            c.execute("SELECT id, name FROM users WHERE name ILIKE %s", (f"%{target}%",))
        row = c.fetchone()
    if not row:
        bot.reply_to(msg, "❌ Пользователь не найден")
        return
    update_balance(row["id"], -amount)
    bot.reply_to(msg, f"✅ Забрано {fmt(amount)} {CURRENCY} у {row['name']}")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("установить ") and is_admin(m.from_user.id))
def cmd_set(msg):
    parts = msg.text.split()
    if len(parts) < 3:
        bot.reply_to(msg, "❌ Формат: установить @username сумма")
        return
    target = parts[1].lstrip("@")
    try:
        amount = int(parts[2])
    except:
        bot.reply_to(msg, "❌ Неверная сумма")
        return
    with db() as c:
        if target.isdigit():
            c.execute("SELECT id, name FROM users WHERE id=%s", (int(target),))
        else:
            c.execute("SELECT id, name FROM users WHERE name ILIKE %s", (f"%{target}%",))
        row = c.fetchone()
    if not row:
        bot.reply_to(msg, "❌ Пользователь не найден")
        return
    with db() as c:
        c.execute("UPDATE users SET balance=%s WHERE id=%s", (amount, row["id"]))
    bot.reply_to(msg, f"✅ Баланс {row['name']} → {fmt(amount)} {CURRENCY}")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("выдать_премиум ") and is_admin(m.from_user.id))
def cmd_give_premium(msg):
    parts = msg.text.split()
    if len(parts) < 3:
        bot.reply_to(msg, "❌ Формат: выдать_премиум @username ДНЕЙ")
        return
    target = parts[1].lstrip("@")
    try:
        days = int(parts[2])
    except:
        bot.reply_to(msg, "❌ Укажи количество дней")
        return
    with db() as c:
        if target.isdigit():
            c.execute("SELECT id, name FROM users WHERE id=%s", (int(target),))
        else:
            c.execute("SELECT id, name FROM users WHERE name ILIKE %s", (f"%{target}%",))
        row = c.fetchone()
    if not row:
        bot.reply_to(msg, "❌ Пользователь не найден")
        return
    with db() as c:
        c.execute(
            "UPDATE users SET premium_until=GREATEST(premium_until, %s) + %s WHERE id=%s",
            (now(), days * 86400, row["id"])
        )
    bot.reply_to(msg, f"✅ Премиум {row['name']} продлён на {days} дней")
    try:
        bot.send_message(row["id"], f"💎 Вам выдан Premium на {days} дней!", parse_mode="HTML")
    except:
        pass

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "стат" and is_admin(m.from_user.id))
def cmd_stat(msg):
    with db() as c:
        c.execute("SELECT COUNT(*) FROM users")
        users = c.fetchone()["count"]
        c.execute("SELECT COALESCE(SUM(balance),0) FROM users")
        total_bal = c.fetchone()["coalesce"]
        c.execute("SELECT COALESCE(SUM(bank),0) FROM users")
        total_bank = c.fetchone()["coalesce"]
        c.execute("SELECT COALESCE(SUM(total_earned),0) FROM users")
        total_earned = c.fetchone()["coalesce"]
        c.execute("SELECT price, prev_price FROM stocks WHERE ticker=%s", (TICKER,))
        stock = c.fetchone()
        price = stock["price"] if stock else 0
        prev = stock["prev_price"] if stock else 0
        c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
        lotto = c.fetchone()
        jackpot = lotto["jackpot"] if lotto else 0
        draw_at = lotto["draw_at"] if lotto else 0
        c.execute("SELECT COUNT(*) FROM lottery_tickets WHERE tickets>0")
        lotto_players = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) FROM users WHERE last_daily > %s", (now() - 86400,))
        daily_active = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) FROM game_history WHERE ts > %s", (now() - 86400,))
        games_today = c.fetchone()["count"]
    chg = (price - prev) / prev * 100 if prev else 0
    time_left = max(0, int(draw_at) - now())
    bot.send_message(msg.chat.id,
        f"<b>📊 Статистика бота</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"👥 Всего игроков: <b>{users}</b>\n"
        f"🟢 Активны сегодня: <b>{daily_active}</b> ({daily_active/users*100:.1f}% если {users}>0)\n"
        f"🎮 Игр сегодня: <b>{games_today}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Кошельки: <b>{fmt(total_bal)}</b>\n"
        f"🏦 Вклады: <b>{fmt(total_bank)}</b>\n"
        f"📈 Всего заработано: <b>{fmt(total_earned)}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 Акция {TICKER}: <b>{fmt(price)}</b> ({chg:+.1f}%)\n"
        f"🎟️ Джекпот: <b>{fmt(jackpot)}</b> ({lotto_players} участников)\n"
        f"⏰ Розыгрыш через: {cd_str(time_left)}",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "актив" and is_admin(m.from_user.id))
def cmd_active(msg):
    with db() as c:
        c.execute("SELECT COUNT(*) FROM users WHERE last_daily > %s", (now() - 86400,))
        daily = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) FROM users WHERE last_daily > %s", (now() - 604800,))
        weekly = c.fetchone()["count"]
        c.execute("SELECT COUNT(*) FROM users")
        total = c.fetchone()["count"]
    pct_d = daily / total * 100 if total else 0
    pct_w = weekly / total * 100 if total else 0
    bot.send_message(msg.chat.id,
        f"📊 <b>Активность</b>\n"
        f"За 24ч: {daily}/{total} ({pct_d:.1f}%)\n"
        f"За 7д: {weekly}/{total} ({pct_w:.1f}%)",
        parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "розыгрыш" and is_admin(m.from_user.id))
def cmd_force_lottery(msg):
    """Принудительный розыгрыш лотереи."""
    run_lottery_draw()
    bot.reply_to(msg, "✅ Розыгрыш лотереи запущен")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "список промо" and is_admin(m.from_user.id))
def cmd_list_promos(msg):
    with db() as c:
        c.execute("SELECT code, reward, uses, max_uses, active FROM promo_codes ORDER BY active DESC, code")
        rows = c.fetchall()
    if not rows:
        bot.reply_to(msg, "Промокодов нет")
        return
    lines = []
    for row in rows:
        status = "✅" if row["active"] else "❌"
        lines.append(f"{status} <code>{row['code']}</code> — {fmt(row['reward'])} ({row['uses']}/{row['max_uses']})")
    bot.send_message(msg.chat.id, "<b>Промокоды:</b>\n" + "\n".join(lines), parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 33. ПЛАНИРОВЩИКИ
# ═══════════════════════════════════════════════════════════════

def stock_scheduler():
    while True:
        time.sleep(STOCK_UPDATE_SEC)
        try:
            update_stock_price()
        except Exception as e:
            print(f"[stocks] ошибка: {e}")

def interest_scheduler():
    """Начисление процентов на депозит каждые 3 часа."""
    while True:
        time.sleep(10800)
        try:
            with db() as c:
                c.execute("UPDATE users SET bank=FLOOR(bank + bank * %s) WHERE bank > 0", (BANK_RATE,))
            print(f"[interest] проценты начислены в {datetime.now().strftime('%H:%M')}")
        except Exception as e:
            print(f"[interest] ошибка: {e}")

def loan_overdue_scheduler():
    """Штраф за просроченные кредиты каждые 12 часов."""
    while True:
        time.sleep(43200)
        try:
            with db() as c:
                c.execute(
                    "UPDATE loans SET amount=FLOOR(amount * 1.05) WHERE due_at < %s AND amount > 0",
                    (now(),)
                )
        except Exception as e:
            print(f"[loans] ошибка: {e}")

# ═══════════════════════════════════════════════════════════════
# 34. ЗАПУСК
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    init_db()
    threading.Thread(target=stock_scheduler, daemon=True).start()
    threading.Thread(target=interest_scheduler, daemon=True).start()
    threading.Thread(target=lottery_scheduler, daemon=True).start()
    threading.Thread(target=loan_overdue_scheduler, daemon=True).start()
    print("🚀 FECTIZ BOT v4.1 запущен (PostgreSQL/Supabase)")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)
