"""
╔══════════════════════════════════════╗
║   🌸  FECTIZ BOT  —  v3.0           ║
║   Minimalist · Все игры · Удобно    ║
╚══════════════════════════════════════╝
"""

import os, re, time, json, math, random, threading, sqlite3, string
from contextlib import contextmanager
from datetime import datetime, timedelta
import requests as _req
from dotenv import load_dotenv
from telebot import TeleBot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton,
    LabeledPrice, InlineQueryResultArticle, InputTextMessageContent
)
from http.server import HTTPServer, BaseHTTPRequestHandler

load_dotenv()

# ═══════════════════════════════════════════════════════════════
# 0. УДАЛЯЕМ WEBHOOK
# ═══════════════════════════════════════════════════════════════

TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

# Временный бот для удаления webhook
_temp_bot = TeleBot(TOKEN)
try:
    _temp_bot.delete_webhook()
    print("✅ Webhook удалён")
except Exception as e:
    print(f"⚠️ Ошибка удаления webhook: {e}")

# Основной бот
bot = TeleBot(TOKEN, threaded=True, num_threads=8)

# ═══════════════════════════════════════════════════════════════
# 1. HTTP-СЕРВЕР ДЛЯ RENDER (чтобы проходил health check)
# ═══════════════════════════════════════════════════════════════

PORT = int(os.environ.get("PORT", 8080))

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health" or self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Bot is running")
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Отключаем лишние логи
        pass

def run_http_server():
    """Запускает минимальный HTTP-сервер для health checks"""
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
        print(f"✅ HTTP-сервер запущен на порту {PORT}")
        server.serve_forever()
    except Exception as e:
        print(f"⚠️ Ошибка HTTP-сервера: {e}")

# Запускаем HTTP-сервер в отдельном потоке
http_thread = threading.Thread(target=run_http_server, daemon=True)
http_thread.start()

# ═══════════════════════════════════════════════════════════════
# 2. КОНФИГ
# ═══════════════════════════════════════════════════════════════

DB_FILE = "economy.db"
CURRENCY = "🌸"
TICKER = "FECTZ"

# Кулдауны
CD_CLICK = 5
CD_DAILY_NORMAL = 1200      # 20 минут
CD_DAILY_PREMIUM = 600       # 10 минут
CD_WORK = 14400              # 4 часа
CD_MINE = 3600               # 1 час
CD_TRANSFER = 60
GAME_COOLDOWN = 3

# Экономика
TRANSFER_FEE = 0.10
PREMIUM_TRANSFER_FEE = 0.05
CLICK_BASE = 100
MINE_BASE = 250
WORK_BASE = 1500
DAILY_BASE = 5000
BANK_RATE = 0.005            # 0.5% за 3 часа
LOAN_MAX = 100000
LOAN_RATE = 0.10
LOAN_TERM = 7                # дней

# Акции
STOCK_PRICE_START = 10000
STOCK_VOLATILITY = 0.04
STOCK_MAX_PER_USER = 5000
STOCK_SELL_FEE = 0.03
STOCK_COOLDOWN = 600
STOCK_UPDATE_SEC = 1800

# ═══════════════════════════════════════════════════════════════
# 3. БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════

class DatabasePool:
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
        return c

    def put(self, c):
        with self._lock:
            if len(self._pool) < self._size:
                self._pool.append(c)
            else:
                c.close()

_pool = DatabasePool(DB_FILE)

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
            name        TEXT DEFAULT '',
            balance     INTEGER DEFAULT 0,
            bank        INTEGER DEFAULT 0,
            xp          INTEGER DEFAULT 0,
            daily_streak INTEGER DEFAULT 0,
            last_click   INTEGER DEFAULT 0,
            last_daily   INTEGER DEFAULT 0,
            last_work    INTEGER DEFAULT 0,
            last_mine    INTEGER DEFAULT 0,
            last_transfer INTEGER DEFAULT 0,
            click_power  INTEGER DEFAULT 100,
            total_earned INTEGER DEFAULT 0,
            premium_until INTEGER DEFAULT 0,
            video_cards   INTEGER DEFAULT 0,
            games_won     INTEGER DEFAULT 0,
            games_lost    INTEGER DEFAULT 0,
            total_won     INTEGER DEFAULT 0,
            total_lost    INTEGER DEFAULT 0,
            created_at   INTEGER DEFAULT 0,
            ref_code     TEXT UNIQUE,
            ref_by       INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS loans (
            user_id     INTEGER PRIMARY KEY,
            amount      INTEGER DEFAULT 0,
            due_at      INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS stocks (
            ticker      TEXT PRIMARY KEY,
            price       INTEGER DEFAULT 10000,
            prev_price  INTEGER DEFAULT 10000,
            updated_at  INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS stock_portfolio (
            user_id INTEGER,
            ticker  TEXT,
            shares  INTEGER DEFAULT 0,
            avg_buy INTEGER DEFAULT 0,
            last_trade INTEGER DEFAULT 0,
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
        CREATE TABLE IF NOT EXISTS game_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER,
            game        TEXT,
            bet         INTEGER,
            win         INTEGER,
            result      TEXT,
            ts          INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS stock_trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER,
            ticker      TEXT,
            action      TEXT,
            amount      INTEGER,
            price       INTEGER,
            fee         INTEGER DEFAULT 0,
            total       INTEGER,
            created_at  INTEGER DEFAULT 0
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
        CREATE TABLE IF NOT EXISTS checks (
            code TEXT PRIMARY KEY,
            amount INTEGER,
            max_activations INTEGER,
            current_activations INTEGER DEFAULT 0,
            password TEXT,
            created_by INTEGER,
            created_at INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS check_activations (
            user_id INTEGER,
            check_code TEXT,
            activated_at INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, check_code)
        );
        CREATE TABLE IF NOT EXISTS donate_packages (
            key     TEXT PRIMARY KEY,
            stars   INTEGER,
            amount  INTEGER,
            label   TEXT
        );
        INSERT OR IGNORE INTO lottery (id, jackpot, draw_at) VALUES (1, 0, 0);
        INSERT OR IGNORE INTO stocks (ticker, price, prev_price, updated_at)
            VALUES ('FECTZ', 10000, 10000, 0);
        INSERT OR IGNORE INTO donate_packages VALUES
            ('s1',   1,    10000, '⭐ 10 000'),
            ('s5',   5,    66000, '⭐ 66 000'),
            ('s15',  15,  266000, '🔥 266 000'),
            ('s50',  50, 1000000, '🔥 1 000 000'),
            ('s150', 150, 4000000, '💎 4 000 000'),
            ('s250', 250, 8000000, '💎 8 000 000');
        """)
    print("✅ БД инициализирована")

# ═══════════════════════════════════════════════════════════════
# 4. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════

def fmt(n: int) -> str:
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
    h, r = divmod(seconds, 3600)
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
        c.execute("SELECT premium_until FROM users WHERE id=?", (uid,))
        row = c.fetchone()
        return row and row["premium_until"] > now()

def get_user(uid: int) -> dict | None:
    with db() as c:
        c.execute("SELECT * FROM users WHERE id=?", (uid,))
        row = c.fetchone()
        return dict(row) if row else None

def ensure_user(uid: int, name: str = "") -> dict:
    with db() as c:
        c.execute("INSERT OR IGNORE INTO users (id, name, created_at) VALUES (?,?,?)",
                  (uid, name, now()))
        c.execute("SELECT ref_code FROM users WHERE id=?", (uid,))
        row = c.fetchone()
        if row and not row["ref_code"]:
            c.execute("UPDATE users SET ref_code=? WHERE id=?", (f"REF{uid}", uid))
    return get_user(uid)

def update_balance(uid: int, amount: int):
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

def parse_bet(text: str, balance: int) -> int | None:
    text = text.lower().strip().replace(" ", "")
    if text in ["все", "all"]:
        return balance
    if text.endswith("к") and text[:-1].replace(".", "").isdigit():
        return int(float(text[:-1]) * 1000)
    if text.endswith("м") and text[:-1].replace(".", "").isdigit():
        return int(float(text[:-1]) * 1000000)
    if text.endswith("б") and text[:-1].replace(".", "").isdigit():
        return int(float(text[:-1]) * 1000000000)
    try:
        return int(text)
    except:
        return None

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
    buttons = ["👆 Кликер", "⛏️ Майнинг", "🚗 Такси", "🏠 Главное меню"]
    for btn in buttons:
        markup.add(KeyboardButton(btn))
    return markup

def games_menu() -> ReplyKeyboardMarkup:
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        "🎲 Кости", "🎰 Слоты", "🎯 Дартс", "🎳 Боулинг",
        "🏀 Баскетбол", "⚽ Футбол", "🚀 Краш", "🎡 Рулетка",
        "💣 Мины", "🎟️ Лотерея", "🏠 Главное меню"
    ]
    for btn in buttons:
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
        ref = parts[1]
        u = get_user(uid)
        if u and not u["ref_by"] and ref != str(uid):
            with db() as c:
                c.execute("SELECT id FROM users WHERE ref_code=?", (ref,))
                row = c.fetchone()
                if row:
                    ref_uid = row["id"]
                    update_balance(uid, 1000)
                    update_balance(ref_uid, 2000)
                    add_xp(ref_uid, 500)
                    c.execute("UPDATE users SET ref_by=? WHERE id=?", (ref_uid, uid))
                    try:
                        bot.send_message(ref_uid, f"🎉 По вашей ссылке пришёл новый игрок!\n+2000 {CURRENCY}, +500 опыта", parse_mode="HTML")
                    except:
                        pass
    
    text = f"🌸 <b>Добро пожаловать, {name}!</b>\n\nВыбери раздел в меню 👇"
    bot.send_message(uid, text, reply_markup=main_menu(), parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🏠 Главное меню")
def back_to_menu(msg):
    bot.send_message(msg.chat.id, "Главное меню:", reply_markup=main_menu(), parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "👤 Профиль")
def cmd_profile(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    if not u:
        ensure_user(uid, msg.from_user.first_name or "Игрок")
        u = get_user(uid)
    lvl = user_level(u["xp"])
    xp_cur = u["xp"] - level_xp(lvl)
    xp_need = level_xp(lvl + 1) - level_xp(lvl)
    bar_filled = int((xp_cur / max(1, xp_need)) * 10)
    bar = "▓" * bar_filled + "░" * (10 - bar_filled)
    prem = "💎 Премиум" if is_premium(uid) else "👤 Обычный"
    created = datetime.fromtimestamp(u["created_at"]).strftime("%d.%m.%Y") if u["created_at"] else "—"
    text = (
        f"<b>👤 Профиль — {u['name'] or 'Игрок'}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎖 Уровень: <b>{lvl}</b>  [{bar}]\n"
        f"⭐ Опыт: {fmt(u['xp'])} | До {lvl+1} ур.: {fmt(max(0, xp_need - xp_cur))}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Баланс: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"📈 Заработано: {fmt(u['total_earned'])} {CURRENCY}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎮 Побед: {u['games_won']} | Поражений: {u['games_lost']}\n"
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
    u = get_user(uid)
    if not u:
        ensure_user(uid, msg.from_user.first_name or "Игрок")
        u = get_user(uid)
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=?", (uid,))
        loan = c.fetchone()
    loan_text = f"\n⚠️ Кредит: <b>{fmt(loan['amount'])} {CURRENCY}</b>" if loan and loan["amount"] > 0 else ""
    text = (
        f"<b>💰 Баланс</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👛 Кошелёк: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"💎 Всего: {fmt(u['balance'] + u['bank'])} {CURRENCY}"
        f"{loan_text}"
    )
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 7. КЛИКЕР
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "⚒️ Работа")
def cmd_work_menu(msg):
    bot.send_message(msg.chat.id, "⚒️ <b>Выбери работу:</b>", reply_markup=work_menu(), parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎰 Игры")
def cmd_games_menu(msg):
    text = (
        "🎰 <b>ИГРЫ</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Выбери игру из меню ниже или введи команду:\n\n"
        "<code>кости 1000 чет</code> — кубики\n"
        "<code>слоты 5000</code> — автоматы\n"
        "<code>дартс 2000</code> — дартс\n"
        "<code>боулинг 1000</code> — боулинг\n"
        "<code>баскетбол 1500</code> — баскетбол\n"
        "<code>футбол 800</code> — футбол\n"
        "<code>краш 10000 3.0</code> — краш\n"
        "<code>рулетка 1000 красное</code> — рулетка\n"
        "<code>мины 5000 3</code> — игра мины\n"
        "<code>лотерея 1000</code> — лотерея"
    )
    bot.send_message(msg.chat.id, text, reply_markup=games_menu(), parse_mode="HTML")
def cmd_click(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    if not u:
        return
    
    remaining = CD_CLICK - (now() - u["last_click"])
    if remaining > 0:
        bot.send_message(uid, f"⏱ Клик через: <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return
    
    lvl = user_level(u["xp"])
    power = CLICK_BASE + lvl * 5
    earn = power + random.randint(-20, 50)
    earn = max(50, earn)
    
    update_balance(uid, earn)
    add_xp(uid, 5 + lvl // 10)
    
    with db() as c:
        c.execute("UPDATE users SET last_click=? WHERE id=?", (now(), uid))
    
    text = f"⚡ <b>Клик!</b> +<b>{fmt(earn)} {CURRENCY}</b>\n⭐ +{5 + lvl // 10} опыта"
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 8. МАЙНИНГ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "⛏️ Майнинг")
def cmd_mine(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    if not u:
        return
    
    remaining = CD_MINE - (now() - u["last_mine"])
    if remaining > 0:
        bot.send_message(uid, f"⛏️ <b>Майнинг</b>\n\n⏱ Следующий сбор через: <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return
    
    cards = u.get("video_cards", 0)
    earn = MINE_BASE + cards * 200 + random.randint(-50, 150)
    earn = max(100, earn)
    
    update_balance(uid, earn)
    add_xp(uid, 10 + cards // 5)
    
    with db() as c:
        c.execute("UPDATE users SET last_mine=? WHERE id=?", (now(), uid))
    
    text = (
        f"⛏️ <b>Майнинг</b>\n\n"
        f"💰 Намайнено: <b>+{fmt(earn)} {CURRENCY}</b>\n"
        f"🖥 Видеокарт: {cards}\n"
        f"⭐ +{10 + cards // 5} опыта"
    )
    bot.send_message(uid, text, parse_mode="HTML")
    
    card_price = 5000 * (2 ** cards)
    if u["balance"] >= card_price:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🖥 Купить карту", callback_data=f"buy_card_{uid}"))
        bot.send_message(uid, f"💡 Купить видеокарту за {fmt(card_price)}? ✨", reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda c: c.data.startswith("buy_card_"))
def cb_buy_card(call):
    uid = call.from_user.id
    if int(call.data.split("_")[2]) != uid:
        bot.answer_callback_query(call.id, "❌ Не твой запрос")
        return
    
    u = get_user(uid)
    cards = u.get("video_cards", 0)
    price = 5000 * (2 ** cards)
    
    if u["balance"] < price:
        bot.answer_callback_query(call.id, f"❌ Не хватает {fmt(price)}")
        return
    
    update_balance(uid, -price)
    with db() as c:
        c.execute("UPDATE users SET video_cards=video_cards+1 WHERE id=?", (uid,))
    
    bot.answer_callback_query(call.id, f"✅ Куплена видеокарта! Теперь у тебя {cards + 1} карт(ы)")

# ═══════════════════════════════════════════════════════════════
# 9. ТАКСИ
# ═══════════════════════════════════════════════════════════════

TAXI_ROUTES = [
    {"name": "📍 Центр → Аэропорт", "base": 1500, "time": 5},
    {"name": "🏠 Жилой р-н → Офис", "base": 1000, "time": 4},
    {"name": "🎓 Университет → ТЦ", "base": 800, "time": 3},
    {"name": "🏥 Больница → Вокзал", "base": 1200, "time": 4},
    {"name": "🏢 Бизнес-центр → Ресторан", "base": 600, "time": 3},
    {"name": "🛍️ ТЦ → Кинотеатр", "base": 500, "time": 3},
    {"name": "🌃 Ночной рейс", "base": 2000, "time": 6},
    {"name": "🚄 Вокзал → Гостиница", "base": 400, "time": 3},
]

active_rides = {}

@bot.message_handler(func=lambda m: m.text == "🚗 Такси")
def cmd_taxi(msg):
    uid = msg.from_user.id
    
    if uid in active_rides:
        data = active_rides[uid]
        elapsed = now() - data["start"]
        if elapsed < data["time"] * 60:
            left = data["time"] * 60 - elapsed
            bot.send_message(uid, f"⚠️ У тебя уже есть активная поездка!\n⏱ Осталось: {cd_str(left)}", parse_mode="HTML")
        return
    
    route = random.choice(TAXI_ROUTES)
    u = get_user(uid)
    lvl = user_level(u["xp"])
    earn = int(route["base"] * random.uniform(0.9, 1.2) * (1 + lvl * 0.01))
    
    active_rides[uid] = {
        "route": route,
        "earn": earn,
        "start": now(),
        "time": route["time"]
    }
    
    text = (
        f"🚕 <b>Новый заказ!</b>\n\n"
        f"Маршрут: {route['name']}\n"
        f"Время: {route['time']} мин\n"
        f"Оплата: <b>{fmt(earn)} {CURRENCY}</b>\n\n"
        f"✅ Поехали? Напиши <b>завершить</b> через {route['time']} мин"
    )
    bot.send_message(uid, text, parse_mode="HTML")
    
    def finish():
        time.sleep(route["time"] * 60)
        if uid in active_rides:
            data = active_rides.pop(uid)
            update_balance(uid, data["earn"])
            add_xp(uid, 30)
            try:
                bot.send_message(uid, f"🚕 <b>Поездка завершена!</b>\n💰 +{fmt(data['earn'])} {CURRENCY}\n⭐ +30 опыта", parse_mode="HTML")
            except:
                pass
    
    threading.Thread(target=finish, daemon=True).start()

@bot.message_handler(func=lambda m: m.text and m.text.lower() in ["завершить", "закончить", "готово"])
def cmd_finish_ride(msg):
    uid = msg.from_user.id
    if uid in active_rides:
        data = active_rides.pop(uid)
        elapsed = now() - data["start"]
        time_spent = elapsed / 60
        
        if time_spent < data["time"]:
            bonus = int(data["earn"] * (1 - time_spent / data["time"]) * 0.3)
            total = data["earn"] + bonus
            text = f"🚕 <b>Поездка завершена досрочно!</b>\n💰 +{fmt(total)} {CURRENCY} (бонус {fmt(bonus)})"
        else:
            total = data["earn"]
            text = f"🚕 <b>Поездка завершена!</b>\n💰 +{fmt(total)} {CURRENCY}"
        
        update_balance(uid, total)
        add_xp(uid, 30)
        bot.send_message(uid, text + "\n⭐ +30 опыта", parse_mode="HTML")
    else:
        bot.send_message(uid, "❌ У тебя нет активной поездки", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 10. ИГРЫ — КОСТИ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🎲 Кости")
def btn_dice(msg):
    bot.send_message(msg.chat.id, "🎲 <b>Кости</b>\nФормат: <code>кости СТАВКА ТИП</code>\nТипы: чет, нечет, малые, большие, или число 1-6\nПример: <code>кости 1000 чет</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎰 Слоты")
def btn_slots(msg):
    bot.send_message(msg.chat.id, "🎰 <b>Слоты</b>\nФормат: <code>слоты СТАВКА</code>\nПример: <code>слоты 5000</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎯 Дартс")
def btn_darts(msg):
    bot.send_message(msg.chat.id, "🎯 <b>Дартс</b>\nФормат: <code>дартс СТАВКА</code>\nПример: <code>дартс 2000</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎳 Боулинг")
def btn_bowling(msg):
    bot.send_message(msg.chat.id, "🎳 <b>Боулинг</b>\nФормат: <code>боулинг СТАВКА</code>\nПример: <code>боулинг 1000</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🏀 Баскетбол")
def btn_basketball(msg):
    bot.send_message(msg.chat.id, "🏀 <b>Баскетбол</b>\nФормат: <code>баскетбол СТАВКА</code>\nПример: <code>баскетбол 1500</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "⚽ Футбол")
def btn_football(msg):
    bot.send_message(msg.chat.id, "⚽ <b>Футбол</b>\nФормат: <code>футбол СТАВКА</code>\nПример: <code>футбол 800</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🚀 Краш")
def btn_crash(msg):
    bot.send_message(msg.chat.id, "🚀 <b>Краш</b>\nФормат: <code>краш СТАВКА МНОЖИТЕЛЬ</code>\nМножитель от 1.1 до 10\nПример: <code>краш 10000 3.0</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎡 Рулетка")
def btn_roulette(msg):
    bot.send_message(msg.chat.id, "🎡 <b>Рулетка</b>\nФормат: <code>рулетка СТАВКА ЦВЕТ</code>\nЦвета: красное, черное, зеленое, или число 0-36\nПример: <code>рулетка 1000 красное</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "💣 Мины")
def btn_mines(msg):
    bot.send_message(msg.chat.id, "💣 <b>Мины</b>\nФормат: <code>мины СТАВКА КОЛ-ВО_МИН</code>\nМин от 1 до 10\nПример: <code>мины 5000 3</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "🎟️ Лотерея")
def btn_lottery(msg):
    uid = msg.from_user.id
    with db() as c:
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        lotto = c.fetchone()
        c.execute("SELECT tickets FROM lottery_tickets WHERE user_id=?", (uid,))
        my_tickets = c.fetchone()
    jackpot = lotto["jackpot"] if lotto else 0
    my = my_tickets["tickets"] if my_tickets else 0
    bot.send_message(msg.chat.id,
        f"🎟️ <b>Лотерея</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Джекпот: <b>{fmt(jackpot)} {CURRENCY}</b>\n"
        f"🎫 Твоих билетов: <b>{my}</b>\n"
        f"💵 Цена билета: {fmt(LOTTERY_TICKET_PRICE)}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Формат: <code>лотерея СУММА</code>\nПример: <code>лотерея 1000</code>",
        parse_mode="HTML")


def cmd_dice(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>кости 1000 чет</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    bet_type = parts[2].lower() if len(parts) > 2 else "чет"
    result = random.randint(1, 6)
    
    win = False
    multiplier = 1
    
    if bet_type in ["чет", "even", "ч"]:
        win = result % 2 == 0
        multiplier = 2
    elif bet_type in ["нечет", "odd", "н"]:
        win = result % 2 == 1
        multiplier = 2
    elif bet_type in ["малые", "мал", "small"]:
        win = result in [1, 2, 3]
        multiplier = 2
    elif bet_type in ["большие", "бол", "big"]:
        win = result in [4, 5, 6]
        multiplier = 2
    elif bet_type.isdigit() and 1 <= int(bet_type) <= 6:
        win = result == int(bet_type)
        multiplier = 6
    else:
        bot.send_message(uid, "❌ Тип ставки: чет, нечет, малые, большие, или число 1-6", parse_mode="HTML")
        return
    
    if win:
        win_amount = bet * multiplier
        update_balance(uid, win_amount - bet)
        text = f"🎲 <b>Выпало {result}!</b> Победа! +{fmt(win_amount - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        text = f"🎲 <b>Выпало {result}!</b> Проигрыш. -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 11. СЛОТЫ
# ═══════════════════════════════════════════════════════════════

SLOT_SYMBOLS = ["🍒", "🍋", "🍊", "🍇", "⭐", "💎", "🔔", "7️⃣"]
SLOT_PAYOUTS = {
    "💎💎💎": 10, "⭐⭐⭐": 7, "7️⃣7️⃣7️⃣": 5, "🍇🍇🍇": 4,
    "🍊🍊🍊": 3, "🍋🍋🍋": 2, "🍒🍒🍒": 1.5,
}

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("слоты"))
def cmd_slots(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>слоты 1000</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    reels = [random.choice(SLOT_SYMBOLS) for _ in range(3)]
    combo = "".join(reels)
    mult = SLOT_PAYOUTS.get(combo, 0)
    
    if mult:
        win = int(bet * mult)
        update_balance(uid, win - bet)
        text = f"🎰 {combo}\n🎉 Джекпот x{mult}! +{fmt(win - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        text = f"🎰 {combo}\n😔 Нет совпадений. -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 12. ДАРТС
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("дартс"))
def cmd_darts(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>дартс 1000</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    result = random.randint(1, 6)
    
    if result == 6:
        win = bet * 5
        update_balance(uid, win - bet)
        text = f"🎯 <b>ЯБЛОЧКО!</b> +{fmt(win - bet)} {CURRENCY}"
    elif result in [4, 5]:
        text = f"🎯 Попадание в кольцо! Ставка возвращена."
    else:
        update_balance(uid, -bet * 2)
        text = f"💥 ПРОМАХ! -{fmt(bet * 2)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 13. БОУЛИНГ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("боулинг"))
def cmd_bowling(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>боулинг 1000</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    result = random.randint(1, 6)
    
    if result == 6:
        win = bet * 3
        update_balance(uid, win - bet)
        text = f"🎳 <b>СТРАЙК!</b> x3! +{fmt(win - bet)} {CURRENCY}"
    elif result == 5:
        win = int(bet * 1.5)
        update_balance(uid, win - bet)
        text = f"🎳 Спэр! x1.5! +{fmt(win - bet)} {CURRENCY}"
    elif result >= 3:
        text = f"🎳 9 кеглей! Ставка возвращена."
    else:
        update_balance(uid, -bet)
        text = f"🎳 Промах! -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 14. БАСКЕТБОЛ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("баскетбол"))
def cmd_basketball(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>баскетбол 1000</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    result = random.randint(1, 6)
    
    if result in [4, 5]:
        win = int(bet * 2.5)
        update_balance(uid, win - bet)
        text = f"🏀 <b>ПОПАДАНИЕ!</b> x2.5! +{fmt(win - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        text = f"🏀 Промах! -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 15. ФУТБОЛ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("футбол"))
def cmd_football(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>футбол 1000</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    result = random.randint(1, 6)
    
    if result in [3, 4]:
        win = bet * 2
        update_balance(uid, win - bet)
        text = f"⚽ <b>ГОЛ!</b> x2! +{fmt(win - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        text = f"⚽ Мимо! -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 16. КРАШ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("краш"))
def cmd_crash(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 3:
        bot.send_message(uid, "❌ Формат: <code>краш 1000 2.5</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    try:
        target_mult = float(parts[2])
    except:
        bot.send_message(uid, "❌ Неверный множитель. Пример: 2.5", parse_mode="HTML")
        return
    
    if target_mult < 1.1 or target_mult > 10:
        bot.send_message(uid, "❌ Множитель от 1.1 до 10", parse_mode="HTML")
        return
    
    crash_at = round(random.expovariate(0.8) + 1.0, 2)
    crash_at = max(1.01, min(20, crash_at))
    
    update_balance(uid, -bet)
    
    if crash_at > target_mult:
        win = int(bet * target_mult)
        update_balance(uid, win)
        text = f"🎲 <b>КРАШ НА {crash_at:.2f}x!</b>\n💰 Твой множитель: {target_mult}x\n✅ Выигрыш: +{fmt(win)} {CURRENCY}"
    else:
        text = f"💥 <b>КРАШ НА {crash_at:.2f}x!</b>\n📉 Твой множитель: {target_mult}x\n❌ Проигрыш: -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 17. РУЛЕТКА
# ═══════════════════════════════════════════════════════════════

RED_NUMBERS = [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("рулетка"))
def cmd_roulette(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 3:
        bot.send_message(uid, "❌ Формат: <code>рулетка 1000 красное</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    bet_type = parts[2].lower()
    result = random.randint(0, 36)
    
    win = False
    multiplier = 1
    
    if bet_type in ["красное", "крас", "к", "red"]:
        win = result in RED_NUMBERS
        multiplier = 2
    elif bet_type in ["черное", "черн", "ч", "black"]:
        win = result != 0 and result not in RED_NUMBERS
        multiplier = 2
    elif bet_type in ["зеленое", "зел", "з", "green"]:
        win = result == 0
        multiplier = 36
    elif bet_type.isdigit() and 0 <= int(bet_type) <= 36:
        win = result == int(bet_type)
        multiplier = 36
    else:
        bot.send_message(uid, "❌ Ставка: красное, черное, зеленое, или число 0-36", parse_mode="HTML")
        return
    
    color = "🔴" if result in RED_NUMBERS else "⚫" if result != 0 else "🟢"
    
    if win:
        win_amount = bet * multiplier
        update_balance(uid, win_amount - bet)
        text = f"🎡 <b>Выпало {color}{result}</b>\n🎉 Победа! +{fmt(win_amount - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        text = f"🎡 <b>Выпало {color}{result}</b>\n😔 Проигрыш. -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 18. МИНЫ
# ═══════════════════════════════════════════════════════════════

mines_games = {}

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("мины"))
def cmd_mines(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 3:
        bot.send_message(uid, "❌ Формат: <code>мины 1000 3</code>", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная ставка. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    try:
        mines_count = int(parts[2])
        if mines_count < 1 or mines_count > 10:
            bot.send_message(uid, "❌ Мин от 1 до 10", parse_mode="HTML")
            return
    except:
        bot.send_message(uid, "❌ Укажи количество мин", parse_mode="HTML")
        return
    
    total_cells = 25
    mine_positions = random.sample(range(total_cells), mines_count)
    
    mines_games[uid] = {
        "bet": bet,
        "mines": mine_positions,
        "opened": [],
    }
    
    update_balance(uid, -bet)
    show_mines_board(uid, msg.chat.id)

def show_mines_board(uid: int, chat_id: int, message_id: int = None):
    game = mines_games.get(uid)
    if not game:
        return

    mult = 1.0 + len(game["opened"]) * 0.2
    potential = int(game["bet"] * mult)

    text = f"💣 <b>Мины</b> | Ставка: {fmt(game['bet'])} | Открыто: {len(game['opened'])}/25\n💰 Потенциал: {fmt(potential)}"

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
        InlineKeyboardButton("💰 Забрать", callback_data=f"mines_cashout_{uid}"),
        InlineKeyboardButton("🏠 Выход", callback_data=f"mines_exit_{uid}")
    )

    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="HTML")
            return
        except Exception:
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
        bot.answer_callback_query(call.id, "❌ Уже открыто")
        return
    
    if cell in game["mines"]:
        bot.edit_message_text(f"💥 <b>БУМ!</b> Ты наступил на мину!\nПотеряно: {fmt(game['bet'])} {CURRENCY}",
                              call.message.chat.id, call.message.message_id, parse_mode="HTML")
        del mines_games[uid]
        bot.answer_callback_query(call.id, "💥 Мина!")
        return
    
    game["opened"].append(cell)

    if len(game["opened"]) >= 20:
        mult = 1.0 + len(game["opened"]) * 0.2
        win = int(game["bet"] * mult)
        update_balance(uid, win)
        bot.edit_message_text(f"🎉 <b>ПОБЕДА!</b>\nТы открыл {len(game['opened'])} клеток!\n+{fmt(win)} {CURRENCY}",
                              call.message.chat.id, call.message.message_id, parse_mode="HTML")
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
    
    mult = 1.0 + len(game["opened"]) * 0.2
    win = int(game["bet"] * mult)
    update_balance(uid, win)
    
    bot.edit_message_text(f"💰 <b>Выигрыш!</b>\n+{fmt(win)} {CURRENCY} (x{mult:.1f})",
                          call.message.chat.id, call.message.message_id, parse_mode="HTML")
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
    
    update_balance(uid, game["bet"])
    bot.edit_message_text(f"🏃‍♂️ Выход из игры\nВозвращено: {fmt(game['bet'])} {CURRENCY}",
                          call.message.chat.id, call.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(call.id, "✅ Возвращено")

@bot.callback_query_handler(func=lambda c: c.data.startswith("mines_no_"))
def cb_mines_no(call):
    bot.answer_callback_query(call.id)

# ═══════════════════════════════════════════════════════════════
# 19. ЛОТЕРЕЯ
# ═══════════════════════════════════════════════════════════════

LOTTERY_TICKET_PRICE = 500

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("лотерея"))
def cmd_lottery(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>лотерея 500</code> (купить билет)", parse_mode="HTML")
        return
    
    bet = parse_bet(parts[1], u["balance"])
    if not bet or bet <= 0 or bet > u["balance"]:
        bot.send_message(uid, f"❌ Неверная сумма. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    tickets = bet // LOTTERY_TICKET_PRICE
    if tickets == 0:
        bot.send_message(uid, f"❌ Минимум {fmt(LOTTERY_TICKET_PRICE)} за билет", parse_mode="HTML")
        return
    
    cost = tickets * LOTTERY_TICKET_PRICE
    update_balance(uid, -cost)
    
    with db() as c:
        c.execute("INSERT INTO lottery_tickets (user_id, tickets) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET tickets=tickets+?",
                  (uid, tickets, tickets))
        c.execute("UPDATE lottery SET jackpot=jackpot+? WHERE id=1", (cost,))
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        jackpot = c.fetchone()["jackpot"]
    
    bot.send_message(uid, f"🎟️ Куплено <b>{tickets}</b> билетов!\n💰 Джекпот: {fmt(jackpot)} {CURRENCY}", parse_mode="HTML")

def lottery_scheduler():
    while True:
        time.sleep(86400)
        try:
            with db() as c:
                c.execute("SELECT jackpot FROM lottery WHERE id=1")
                lotto = c.fetchone()
                if not lotto or lotto["jackpot"] == 0:
                    continue
                
                c.execute("SELECT user_id, tickets FROM lottery_tickets WHERE tickets > 0")
                participants = c.fetchall()
                
                if not participants:
                    continue
                
                pool = []
                for p in participants:
                    pool.extend([p["user_id"]] * p["tickets"])
                
                winner = random.choice(pool)
                jackpot = lotto["jackpot"]
                update_balance(winner, jackpot)
                
                c.execute("UPDATE lottery SET jackpot=0 WHERE id=1")
                c.execute("DELETE FROM lottery_tickets")
                
                try:
                    bot.send_message(winner, f"🎉 <b>ВЫ ВЫИГРАЛИ ЛОТЕРЕЮ!</b>\n+{fmt(jackpot)} {CURRENCY}", parse_mode="HTML")
                except:
                    pass
        except Exception as e:
            print(f"[lottery] ошибка: {e}")

# ═══════════════════════════════════════════════════════════════
# 20. БАНК
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🏦 Банк")
def cmd_bank(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    
    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=?", (uid,))
        loan = c.fetchone()
    
    loan_text = ""
    if loan and loan["amount"] > 0:
        due = datetime.fromtimestamp(loan["due_at"]).strftime("%d.%m")
        loan_text = f"\n⚠️ Долг: <b>{fmt(loan['amount'])}</b> до {due}"
    
    text = (
        f"<b>🏦 Банк</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Кошелёк: {fmt(u['balance'])} {CURRENCY}\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"📈 Ставка: {BANK_RATE*100:.2f}%/3ч\n"
        f"{loan_text}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Команды:</b>\n"
        f"<code>вклад 5000</code> — положить\n"
        f"<code>снять 3000</code> — снять\n"
        f"<code>кредит 10000</code> — взять кредит\n"
        f"<code>погасить 5000</code> — погасить"
    )
    bot.send_message(uid, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("вклад "))
def cmd_deposit(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>вклад 5000</code>", parse_mode="HTML")
        return
    
    amount = parse_bet(parts[1], u["balance"])
    if not amount or amount <= 0 or amount > u["balance"]:
        bot.send_message(uid, f"❌ Неверная сумма. Баланс: {fmt(u['balance'])}", parse_mode="HTML")
        return
    
    update_balance(uid, -amount)
    with db() as c:
        c.execute("UPDATE users SET bank=bank+? WHERE id=?", (amount, uid))
    
    bot.send_message(uid, f"✅ Внесено на депозит: <b>{fmt(amount)} {CURRENCY}</b>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("снять "))
def cmd_withdraw(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>снять 3000</code>", parse_mode="HTML")
        return
    
    amount = parse_bet(parts[1], u["bank"])
    if not amount or amount <= 0 or amount > u["bank"]:
        bot.send_message(uid, f"❌ Неверная сумма. Депозит: {fmt(u['bank'])}", parse_mode="HTML")
        return
    
    update_balance(uid, amount)
    with db() as c:
        c.execute("UPDATE users SET bank=bank-? WHERE id=?", (amount, uid))
    
    bot.send_message(uid, f"✅ Снято с депозита: <b>{fmt(amount)} {CURRENCY}</b>", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 21. КРЕДИТ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("кредит "))
def cmd_loan(msg):
    uid = msg.from_user.id
    u = get_user(uid)
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
        c.execute("SELECT amount FROM loans WHERE user_id=?", (uid,))
        existing = c.fetchone()
        if existing and existing["amount"] > 0:
            bot.send_message(uid, "❌ У тебя уже есть кредит. Сначала погаси его.", parse_mode="HTML")
            return
    
    total_debt = int(amount * (1 + LOAN_RATE))
    due = now() + LOAN_TERM * 86400
    
    update_balance(uid, amount)
    with db() as c:
        c.execute("INSERT OR REPLACE INTO loans (user_id, amount, due_at) VALUES (?, ?, ?)",
                  (uid, total_debt, due))
    
    bot.send_message(uid, f"✅ Кредит выдан: <b>{fmt(amount)} {CURRENCY}</b>\nВернуть: <b>{fmt(total_debt)}</b> до {datetime.fromtimestamp(due).strftime('%d.%m.%Y')}", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("погасить "))
def cmd_repay(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>погасить 5000</code>", parse_mode="HTML")
        return
    
    with db() as c:
        c.execute("SELECT amount FROM loans WHERE user_id=?", (uid,))
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
    if u["balance"] < pay:
        bot.send_message(uid, f"❌ Не хватает {fmt(pay)} {CURRENCY}", parse_mode="HTML")
        return
    
    update_balance(uid, -pay)
    new_debt = debt - pay
    
    with db() as c:
        if new_debt <= 0:
            c.execute("DELETE FROM loans WHERE user_id=?", (uid,))
            bot.send_message(uid, f"✅ Кредит погашен полностью! +{fmt(pay)}", parse_mode="HTML")
        else:
            c.execute("UPDATE loans SET amount=? WHERE user_id=?", (new_debt, uid))
            bot.send_message(uid, f"✅ Погашено: {fmt(pay)} {CURRENCY}\nОстаток: {fmt(new_debt)}", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 22. БИРЖА
# ═══════════════════════════════════════════════════════════════

def get_stock_price():
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
        row = c.fetchone()
        return row["price"] if row else STOCK_PRICE_START

def update_stock_price(impact: float = 0):
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
        row = c.fetchone()
        price = row["price"]
        
        drift = (STOCK_PRICE_START - price) * 0.01
        new_price = price * (1 + drift + impact + random.uniform(-STOCK_VOLATILITY, STOCK_VOLATILITY))
        new_price = max(100, int(new_price))
        
        c.execute("UPDATE stocks SET prev_price=price, price=?, updated_at=? WHERE ticker=?", 
                  (new_price, now(), TICKER))
        c.execute("INSERT INTO stock_history (ticker, price, ts) VALUES (?,?,?)",
                  (TICKER, new_price, now()))

@bot.message_handler(func=lambda m: m.text == "📈 Биржа")
def cmd_stock(msg):
    uid = msg.from_user.id
    price = get_stock_price()
    
    with db() as c:
        c.execute("SELECT shares, avg_buy, last_trade FROM stock_portfolio WHERE user_id=? AND ticker=?", (uid, TICKER))
        port = c.fetchone()
    
    cd_left = 0
    if port and port["last_trade"] > 0:
        cd_left = max(0, STOCK_COOLDOWN - (now() - port["last_trade"]))
    
    text = f"<b>📈 Биржа — {TICKER}</b>\n━━━━━━━━━━━━━━━━━━\nЦена: <b>{fmt(price)} {CURRENCY}</b>\n\n"
    
    if port and port["shares"] > 0:
        pnl = (price - port["avg_buy"]) * port["shares"]
        text += f"📂 Портфель: {port['shares']} акций\n📊 P&L: {fmt(pnl)} {CURRENCY}\n"
    
    if cd_left > 0:
        text += f"⏳ Кулдаун: {cd_str(cd_left)}\n\n"
    
    text += "<b>Команды:</b>\n<code>купить 10</code>\n<code>продать 10</code>\n<code>история акций</code>"
    bot.send_message(uid, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("купить "))
def cmd_buy_stock(msg):
    uid = msg.from_user.id
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>купить 10</code>", parse_mode="HTML")
        return
    
    try:
        qty = int(parts[1])
    except:
        bot.send_message(uid, "❌ Укажи число", parse_mode="HTML")
        return
    
    price = get_stock_price()
    total = price * qty
    u = get_user(uid)
    
    with db() as c:
        c.execute("SELECT last_trade FROM stock_portfolio WHERE user_id=? AND ticker=?", (uid, TICKER))
        row = c.fetchone()
        if row and row["last_trade"] > 0:
            cd_left = STOCK_COOLDOWN - (now() - row["last_trade"])
            if cd_left > 0:
                bot.send_message(uid, f"⏳ Кулдаун: {cd_str(cd_left)}", parse_mode="HTML")
                return
    
    if u["balance"] < total:
        bot.send_message(uid, f"❌ Нужно {fmt(total)} {CURRENCY}", parse_mode="HTML")
        return
    
    with db() as c:
        c.execute("SELECT shares, avg_buy FROM stock_portfolio WHERE user_id=? AND ticker=?", (uid, TICKER))
        port = c.fetchone()
        
        if port and port["shares"] + qty > STOCK_MAX_PER_USER:
            bot.send_message(uid, f"❌ Максимум {STOCK_MAX_PER_USER} акций", parse_mode="HTML")
            return
        
        new_avg = ((port["avg_buy"] * port["shares"] if port else 0) + price * qty) // (qty + (port["shares"] if port else 0))
        
        c.execute("""
            INSERT INTO stock_portfolio (user_id, ticker, shares, avg_buy, last_trade)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, ticker) DO UPDATE SET
                shares = shares + ?,
                avg_buy = ?,
                last_trade = ?
        """, (uid, TICKER, qty, new_avg, now(), qty, new_avg, now()))
        
        c.execute("INSERT INTO stock_trades (user_id, ticker, action, amount, price, total, created_at) VALUES (?,?,?,?,?,?,?)",
                  (uid, TICKER, "buy", qty, price, total, now()))
    
    update_balance(uid, -total)
    update_stock_price(0.0005 * qty)
    
    bot.send_message(uid, f"✅ Куплено <b>{qty} акций</b> по {fmt(price)}", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("продать "))
def cmd_sell_stock(msg):
    uid = msg.from_user.id
    parts = msg.text.split()
    
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: <code>продать 10</code>", parse_mode="HTML")
        return
    
    try:
        qty = int(parts[1])
    except:
        bot.send_message(uid, "❌ Укажи число", parse_mode="HTML")
        return
    
    price = get_stock_price()
    
    with db() as c:
        c.execute("SELECT shares, avg_buy, last_trade FROM stock_portfolio WHERE user_id=? AND ticker=?", (uid, TICKER))
        port = c.fetchone()
        
        if not port or port["shares"] < qty:
            bot.send_message(uid, f"❌ У тебя {port['shares'] if port else 0} акций", parse_mode="HTML")
            return
        
        if port["last_trade"] > 0:
            cd_left = STOCK_COOLDOWN - (now() - port["last_trade"])
            if cd_left > 0:
                bot.send_message(uid, f"⏳ Кулдаун: {cd_str(cd_left)}", parse_mode="HTML")
                return
        
        total = price * qty
        fee = int(total * STOCK_SELL_FEE)
        net = total - fee
        
        update_balance(uid, net)
        
        new_shares = port["shares"] - qty
        if new_shares == 0:
            c.execute("DELETE FROM stock_portfolio WHERE user_id=? AND ticker=?", (uid, TICKER))
        else:
            c.execute("UPDATE stock_portfolio SET shares=?, last_trade=? WHERE user_id=? AND ticker=?", 
                      (new_shares, now(), uid, TICKER))
        
        c.execute("INSERT INTO stock_trades (user_id, ticker, action, amount, price, fee, total, created_at) VALUES (?,?,?,?,?,?,?,?)",
                  (uid, TICKER, "sell", qty, price, fee, net, now()))
    
    update_stock_price(-0.0005 * qty)
    
    pnl = net - port["avg_buy"] * qty
    bot.send_message(uid, f"✅ Продано <b>{qty} акций</b>\n💰 Получено: {fmt(net)} (комиссия {fmt(fee)})\n📈 P&L: {fmt(pnl)}", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "история акций")
def cmd_stock_history(msg):
    uid = msg.from_user.id
    
    with db() as c:
        c.execute("SELECT price, ts FROM stock_history WHERE ticker=? ORDER BY ts DESC LIMIT 10", (TICKER,))
        history = list(reversed(c.fetchall()))
    
    if not history:
        bot.send_message(uid, "📊 История пуста", parse_mode="HTML")
        return
    
    lines = []
    for i, row in enumerate(history):
        price = row["price"]
        ts = row["ts"]
        dt = datetime.fromtimestamp(ts).strftime("%d.%m %H:%M")
        if i > 0:
            prev = history[i-1]["price"]
            chg = (price - prev) / prev * 100
            icon = "🟢" if price >= prev else "🔴"
            lines.append(f"{icon} {dt}  <b>{fmt(price)}</b>  ({chg:+.1f}%)")
        else:
            lines.append(f"⬜ {dt}  <b>{fmt(price)}</b>")
    
    with db() as c:
        c.execute("SELECT action, amount, price, created_at FROM stock_trades WHERE user_id=? ORDER BY created_at DESC LIMIT 5", (uid,))
        trades = c.fetchall()
    
    trade_lines = []
    for row in trades:
        action = row["action"]
        amt = row["amount"]
        p = row["price"]
        ts = row["created_at"]
        dt = datetime.fromtimestamp(ts).strftime("%d.%m %H:%M")
        icon = "🛒" if action == "buy" else "💰"
        trade_lines.append(f"{icon} {dt}  {amt} шт. × {fmt(p)}")
    
    text = f"<b>📊 История цен {TICKER}</b>\n\n" + "\n".join(lines)
    if trade_lines:
        text += f"\n\n<b>📋 Мои сделки:</b>\n" + "\n".join(trade_lines)
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 23. БОНУС
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🎁 Бонус")
def cmd_bonus(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    
    cd = CD_DAILY_PREMIUM if is_premium(uid) else CD_DAILY_NORMAL
    remaining = cd - (now() - u["last_daily"])
    
    if remaining > 0:
        bot.send_message(uid, f"🎁 Бонус через: <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return
    
    streak = u["daily_streak"]
    lvl = user_level(u["xp"])
    mult = 1 + min(streak * 0.1, 2) + (lvl * 0.02)
    bonus = int(DAILY_BASE * mult)
    
    update_balance(uid, bonus)
    add_xp(uid, 50 + lvl)
    
    with db() as c:
        c.execute("UPDATE users SET last_daily=?, daily_streak=daily_streak+1 WHERE id=?", (now(), uid))
    
    text = (
        f"🎁 <b>Ежедневный бонус!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Стрик: {streak} дней\n"
        f"💰 Получено: <b>{fmt(bonus)} {CURRENCY}</b>\n"
        f"⭐ +{50 + lvl} опыта"
    )
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 24. ТОП
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🏆 Топ")
def cmd_top(msg):
    uid = msg.from_user.id
    
    with db() as c:
        c.execute("SELECT name, balance FROM users ORDER BY balance DESC LIMIT 10")
        rows = c.fetchall()
    
    text = "<b>🏆 ТОП ПО БАЛАНСУ</b>\n━━━━━━━━━━━━━━━━━━\n"
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    
    for i, row in enumerate(rows):
        name = row["name"] or f"Игрок"
        text += f"{medals[i]} {name} — <b>{fmt(row['balance'])}</b>\n"
    
    with db() as c:
        c.execute("SELECT COUNT(*) + 1 FROM users WHERE balance > (SELECT balance FROM users WHERE id=?)", (uid,))
        pos = c.fetchone()[0]
        u = get_user(uid)
        text += f"\n👤 Твоя позиция: #{pos} | {fmt(u['balance'])} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 25. ПЕРЕВОДЫ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("перевод"))
def cmd_transfer(msg):
    uid = msg.from_user.id
    u = get_user(uid)
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
            c.execute("SELECT id, name FROM users WHERE id=?", (int(target),))
        else:
            c.execute("SELECT id, name FROM users WHERE name LIKE ?", (f"%{target}%",))
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
    
    if u["balance"] < total:
        bot.send_message(uid, f"❌ Нужно {fmt(total)} (включая комиссию {fmt(fee)})", parse_mode="HTML")
        return
    
    if now() - u.get("last_transfer", 0) < CD_TRANSFER:
        bot.send_message(uid, f"⏱ Подожди {CD_TRANSFER} сек между переводами", parse_mode="HTML")
        return
    
    update_balance(uid, -total)
    update_balance(to_uid, amount)
    
    with db() as c:
        c.execute("UPDATE users SET last_transfer=? WHERE id=?", (now(), uid))
        c.execute("INSERT INTO transfers (from_id, to_id, amount, fee, ts) VALUES (?,?,?,?,?)",
                  (uid, to_uid, amount, fee, now()))
    
    bot.send_message(uid, f"✅ Переведено {fmt(amount)} {CURRENCY}\nКомиссия: {fmt(fee)}", parse_mode="HTML")
    
    try:
        bot.send_message(to_uid, f"💰 Вам перевели {fmt(amount)} {CURRENCY}", parse_mode="HTML")
    except:
        pass

# ═══════════════════════════════════════════════════════════════
# 26. РЕФЕРАЛ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🔗 Реферал")
def cmd_referral(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    
    with db() as c:
        c.execute("SELECT ref_code FROM users WHERE id=?", (uid,))
        row = c.fetchone()
        ref_code = row["ref_code"] if row else f"REF{uid}"
        
        c.execute("SELECT COUNT(*) FROM users WHERE ref_by=?", (uid,))
        referrals = c.fetchone()[0]
    
    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={ref_code}"
    
    text = (
        f"<b>🔗 Реферальная программа</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 Приглашено: {referrals} игроков\n"
        f"🎁 Бонус: +1000 тебе, +2000 другу\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Твоя ссылка:\n{link}"
    )
    bot.send_message(uid, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 27. ДОНАТ
# ═══════════════════════════════════════════════════════════════

DONATE_PACKAGES = {
    "s1": {"stars": 1, "amount": 10000, "label": "⭐ 10 000"},
    "s5": {"stars": 5, "amount": 66000, "label": "⭐ 66 000"},
    "s15": {"stars": 15, "amount": 266000, "label": "🔥 266 000"},
    "s50": {"stars": 50, "amount": 1000000, "label": "🔥 1 000 000"},
    "s150": {"stars": 150, "amount": 4000000, "label": "💎 4 000 000"},
    "s250": {"stars": 250, "amount": 8000000, "label": "💎 8 000 000"},
}

@bot.message_handler(func=lambda m: m.text == "💎 Донат")
def cmd_donate(msg):
    uid = msg.from_user.id
    
    text = "<b>💎 Пополнение баланса</b>\n━━━━━━━━━━━━━━━━━━\n"
    for key, pkg in DONATE_PACKAGES.items():
        text += f"{pkg['label']} → {fmt(pkg['amount'])} {CURRENCY}\n"
    
    markup = InlineKeyboardMarkup()
    for key, pkg in DONATE_PACKAGES.items():
        markup.add(InlineKeyboardButton(pkg['label'], callback_data=f"donate_{key}"))
    
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
        description=f"{pkg['label']}",
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
    payload = msg.successful_payment.invoice_payload
    key = payload[7:]
    pkg = DONATE_PACKAGES.get(key)
    
    if pkg:
        update_balance(uid, pkg["amount"])
        bot.send_message(uid, f"✅ Пополнение на <b>{fmt(pkg['amount'])} {CURRENCY}</b>", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 28. ПРОМОКОДЫ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("промо "))
def cmd_promo(msg):
    uid = msg.from_user.id
    code = msg.text.split()[1].upper()
    
    with db() as c:
        c.execute("SELECT * FROM promo_codes WHERE code=? AND active=1", (code,))
        promo = c.fetchone()
    
    if not promo:
        bot.send_message(uid, "❌ Промокод не найден или истёк", parse_mode="HTML")
        return
    
    if promo["expires"] and promo["expires"] < now():
        bot.send_message(uid, "❌ Промокод истёк", parse_mode="HTML")
        return
    
    if promo["uses"] >= promo["max_uses"]:
        bot.send_message(uid, "❌ Промокод исчерпан", parse_mode="HTML")
        return
    
    with db() as c:
        try:
            c.execute("INSERT INTO promo_uses (user_id, code, ts) VALUES (?,?,?)", (uid, code, now()))
        except:
            bot.send_message(uid, "❌ Ты уже использовал этот промокод", parse_mode="HTML")
            return
        
        c.execute("UPDATE promo_codes SET uses=uses+1 WHERE code=?", (code,))
    
    update_balance(uid, promo["reward"])
    bot.send_message(uid, f"✅ Промокод активирован!\n+{fmt(promo['reward'])} {CURRENCY}", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("создать промо ") and is_admin(m.from_user.id))
def cmd_create_promo(msg):
    parts = msg.text.split()
    if len(parts) < 4:
        bot.reply_to(msg, "❌ Формат: создать промо КОД СУММА [uses]")
        return
    
    code = parts[2].upper()
    reward = int(parts[3])
    uses = int(parts[4]) if len(parts) > 4 else 1
    
    with db() as c:
        try:
            c.execute("INSERT INTO promo_codes (code, reward, max_uses) VALUES (?,?,?)", (code, reward, uses))
        except:
            bot.reply_to(msg, "❌ Код уже существует")
            return
    
    bot.reply_to(msg, f"✅ Промокод <code>{code}</code> создан\n+{fmt(reward)}, {uses} использований", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 29. ЧЕКИ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("чек "))
def cmd_check(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    parts = msg.text.split()
    
    if len(parts) < 3:
        bot.send_message(uid, "❌ Формат: <code>чек 5000 1</code> (сумма, кол-во активаций)", parse_mode="HTML")
        return
    
    amount = parse_bet(parts[1], u["balance"])
    if not amount or amount <= 0:
        bot.send_message(uid, "❌ Неверная сумма", parse_mode="HTML")
        return
    
    try:
        uses = int(parts[2])
    except:
        bot.send_message(uid, "❌ Укажи количество активаций", parse_mode="HTML")
        return
    
    total = amount * uses
    if u["balance"] < total:
        bot.send_message(uid, f"❌ Нужно {fmt(total)} {CURRENCY}", parse_mode="HTML")
        return
    
    code = f"CHK{random.randint(100000, 999999)}"
    update_balance(uid, -total)
    
    with db() as c:
        c.execute("INSERT INTO checks (code, amount, max_activations, created_by, created_at) VALUES (?,?,?,?,?)",
                  (code, amount, uses, uid, now()))
    
    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={code}"
    
    bot.send_message(uid, f"✅ Чек создан!\n💵 {fmt(amount)} × {uses}\n🔗 {link}", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("активировать "))
def cmd_activate_check(msg):
    uid = msg.from_user.id
    code = msg.text.split()[1].upper()
    
    with db() as c:
        c.execute("SELECT * FROM checks WHERE code=?", (code,))
        check = c.fetchone()
    
    if not check:
        bot.send_message(uid, "❌ Чек не найден", parse_mode="HTML")
        return
    
    if check["current_activations"] >= check["max_activations"]:
        bot.send_message(uid, "❌ Чек исчерпан", parse_mode="HTML")
        return
    
    with db() as c:
        try:
            c.execute("INSERT INTO check_activations (user_id, check_code, activated_at) VALUES (?,?,?)",
                      (uid, code, now()))
        except:
            bot.send_message(uid, "❌ Ты уже активировал этот чек", parse_mode="HTML")
            return
        
        c.execute("UPDATE checks SET current_activations=current_activations+1 WHERE code=?", (code,))
    
    update_balance(uid, check["amount"])
    bot.send_message(uid, f"✅ Чек активирован!\n+{fmt(check['amount'])} {CURRENCY}", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 30. ПОМОЩЬ
# ═══════════════════════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "📞 Помощь")
def cmd_help(msg):
    text = """
<b>📞 ПОМОЩЬ ПО БОТУ</b>

<b>👤 Профиль</b> — статистика игрока
<b>💰 Баланс</b> — текущий баланс
<b>⚒️ Работа</b> — кликер, майнинг, такси

<b>🎰 ИГРЫ</b>
<code>кости 1000 чет</code> — кубики
<code>слоты 5000</code> — автоматы
<code>дартс 2000</code> — дартс
<code>боулинг 1000</code> — боулинг
<code>баскетбол 1500</code> — баскетбол
<code>футбол 800</code> — футбол
<code>рулетка 1000 красное</code> — рулетка
<code>мины 5000 3</code> — игра мины
<code>краш 10000 3.0</code> — краш
<code>лотерея 1000</code> — купить билеты

<b>🏦 БАНК</b>
<code>вклад 5000</code> — положить
<code>снять 3000</code> — снять
<code>кредит 10000</code> — взять кредит
<code>погасить 5000</code> — погасить

<b>📈 БИРЖА</b>
<code>купить 10</code> — купить акции
<code>продать 5</code> — продать
<code>история акций</code> — история

<b>💸 ПЕРЕВОДЫ</b>
<code>перевод @username 5000</code>

<b>🔗 РЕФЕРАЛ</b> — реферальная ссылка
<b>🎁 БОНУС</b> — ежедневная награда
<b>🏆 ТОП</b> — рейтинг
<b>💎 ДОНАТ</b> — пополнить баланс

💡 Суммы: 100к, 1м, 1б, все
    """
    bot.send_message(msg.chat.id, text, parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 31. АДМИН-КОМАНДЫ
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
            c.execute("SELECT id FROM users WHERE id=?", (int(target),))
        else:
            c.execute("SELECT id FROM users WHERE name LIKE ?", (f"%{target}%",))
        row = c.fetchone()
        
        if not row:
            bot.reply_to(msg, "❌ Пользователь не найден")
            return
        
        update_balance(row["id"], amount)
    
    bot.reply_to(msg, f"✅ Выдано {fmt(amount)} {CURRENCY}")

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
            c.execute("SELECT id FROM users WHERE id=?", (int(target),))
        else:
            c.execute("SELECT id FROM users WHERE name LIKE ?", (f"%{target}%",))
        row = c.fetchone()
        
        if not row:
            bot.reply_to(msg, "❌ Пользователь не найден")
            return
        
        update_balance(row["id"], -amount)
    
    bot.reply_to(msg, f"✅ Забрано {fmt(amount)} {CURRENCY}")

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
            c.execute("SELECT id FROM users WHERE id=?", (int(target),))
        else:
            c.execute("SELECT id FROM users WHERE name LIKE ?", (f"%{target}%",))
        row = c.fetchone()
        
        if not row:
            bot.reply_to(msg, "❌ Пользователь не найден")
            return
        
        c.execute("UPDATE users SET balance=? WHERE id=?", (amount, row["id"]))
    
    bot.reply_to(msg, f"✅ Баланс установлен: {fmt(amount)} {CURRENCY}")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "стат" and is_admin(m.from_user.id))
def cmd_stat(msg):
    with db() as c:
        c.execute("SELECT COUNT(*) FROM users")
        users = c.fetchone()[0]
        
        c.execute("SELECT SUM(balance) FROM users")
        total_bal = c.fetchone()[0] or 0
        
        c.execute("SELECT SUM(bank) FROM users")
        total_bank = c.fetchone()[0] or 0
        
        c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
        stock = c.fetchone()
        price = stock["price"] if stock else 0
        
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        lotto = c.fetchone()
        jackpot = lotto["jackpot"] if lotto else 0
    
    text = (
        f"<b>📊 Статистика</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 Игроков: <b>{users}</b>\n"
        f"💰 Кошельки: <b>{fmt(total_bal)}</b>\n"
        f"🏦 Вклады: <b>{fmt(total_bank)}</b>\n"
        f"📈 Акция {TICKER}: <b>{fmt(price)}</b>\n"
        f"🎟️ Джекпот: <b>{fmt(jackpot)}</b>"
    )
    bot.send_message(msg.chat.id, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "актив" and is_admin(m.from_user.id))
def cmd_active(msg):
    with db() as c:
        c.execute("SELECT COUNT(*) FROM users WHERE last_daily > ?", (now() - 86400,))
        daily = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users")
        total = c.fetchone()[0]
    
    bot.send_message(msg.chat.id, f"📊 Активность за сутки: {daily}/{total} ({daily/total*100:.1f}%)", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════
# 32. ЗАПУСК
# ═══════════════════════════════════════════════════════════════

def stock_scheduler():
    while True:
        time.sleep(STOCK_UPDATE_SEC)
        try:
            update_stock_price()
        except Exception as e:
            print(f"[stocks] ошибка: {e}")

def interest_scheduler():
    while True:
        time.sleep(10800)
        try:
            with db() as c:
                c.execute("UPDATE users SET bank=bank+bank*? WHERE bank>0", (BANK_RATE,))
        except Exception as e:
            print(f"[interest] ошибка: {e}")

if __name__ == "__main__":
    init_db()
    
    threading.Thread(target=stock_scheduler, daemon=True).start()
    threading.Thread(target=interest_scheduler, daemon=True).start()
    threading.Thread(target=lottery_scheduler, daemon=True).start()
    
    print("🚀 Бот запущен")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)
