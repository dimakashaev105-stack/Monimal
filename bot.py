"""
╔══════════════════════════════════════╗
║   🌸  FECTIZ BOT  —  v3.0           ║
║   Minimalist · Все игры · Удобно    ║
╚══════════════════════════════════════╝
"""

import os, re, time, json, math, random, threading, sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
import requests as _req
from dotenv import load_dotenv
from telebot import TeleBot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton,
    LabeledPrice, InlineQueryResultArticle, InputTextMessageContent
)

load_dotenv()

# ══════════════════════════════════════════════
# 1. КОНФИГ
# ══════════════════════════════════════════════

TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DB_FILE = "economy.db"
CURRENCY = "🌸"
TICKER = "FECTZ"

# Кулдауны (секунды)
CD_CLICK = 5
CD_DAILY = 600  # 10 минут (для премиум) / 20 минут для обычных
CD_WORK = 14400
CD_MINE = 3600
CD_LOTTERY = 86400
GAME_COOLDOWN = 3

# Экономика
TRANSFER_FEE = 0.10
CLICK_BASE = 100
MINE_BASE = 250
WORK_BASE = 1500
DAILY_BASE = 5000
DAILY_PREMIUM_CD = 600
DAILY_NORMAL_CD = 1200

# Акции
STOCK_PRICE_START = 10000
STOCK_VOLATILITY = 0.04
STOCK_MAX_PER_USER = 5000
STOCK_SELL_FEE = 0.03
STOCK_COOLDOWN = 600

# ══════════════════════════════════════════════
# 2. БАЗА ДАННЫХ
# ══════════════════════════════════════════════

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
            click_power  INTEGER DEFAULT 100,
            total_earned INTEGER DEFAULT 0,
            premium_until INTEGER DEFAULT 0,
            video_cards   INTEGER DEFAULT 0,
            games_won     INTEGER DEFAULT 0,
            games_lost    INTEGER DEFAULT 0,
            total_won     INTEGER DEFAULT 0,
            total_lost    INTEGER DEFAULT 0,
            created_at   INTEGER DEFAULT 0
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
        INSERT OR IGNORE INTO lottery (id, jackpot, draw_at) VALUES (1, 0, 0);
        INSERT OR IGNORE INTO stocks (ticker, price, prev_price, updated_at)
            VALUES ('FECTZ', 10000, 10000, 0);
        """)
    print("✅ БД инициализирована")

# ══════════════════════════════════════════════
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
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

def update_game_stats(uid: int, game: str, bet: int, win: int, result: str):
    with db() as c:
        c.execute("""
            INSERT INTO game_history (user_id, game, bet, win, result, ts)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (uid, game, bet, win, result, now()))
        if win > 0:
            c.execute("UPDATE users SET games_won=games_won+1, total_won=total_won+? WHERE id=?", (win, uid))
        else:
            c.execute("UPDATE users SET games_lost=games_lost+1, total_lost=total_lost+? WHERE id=?", (bet, uid))

# ══════════════════════════════════════════════
# 4. КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def main_menu() -> ReplyKeyboardMarkup:
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
    buttons = [
        "👤 Профиль", "💰 Баланс", "⚒️ Работа",
        "🎰 Игры", "🏦 Банк", "📈 Биржа",
        "🎁 Бонус", "🏆 Топ", "💎 Донат"
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
    buttons = ["🎲 Кости", "🎰 Слоты", "🎯 Дартс", "🎳 Боулинг", 
               "🏀 Баскетбол", "⚽ Футбол", "🎲 Краш", "🏠 Главное меню"]
    for btn in buttons:
        markup.add(KeyboardButton(btn))
    return markup

def games_help():
    return """<b>🎲 ДОСТУПНЫЕ ИГРЫ</b>

<b>🎲 Кости</b> — <code>кости [сумма] [тип]</code>
Типы: 1-6, малые, большие, чет, нечет

<b>🎰 Слоты</b> — <code>слоты [сумма]</code>
Выигрыш до x100!

<b>🎯 Дартс</b> — <code>дартс [сумма]</code>
Яблочко x5, кольцо возврат ставки

<b>🎳 Боулинг</b> — <code>боулинг [сумма]</code>
Страйк x3, спэр x1.5

<b>🏀 Баскетбол</b> — <code>баскетбол [сумма]</code>
Попадание x2.5

<b>⚽ Футбол</b> — <code>футбол [сумма]</code>
Гол x2

<b>🎲 Краш</b> — <code>краш [сумма] [множитель]</code>
Пример: краш 1000 2.5

💡 Примеры:
<code>кости 1000 чет</code>
<code>слоты 5000</code>
<code>дартс 2000</code>
<code>краш 10000 3.0</code>"""

# ══════════════════════════════════════════════
# 5. ОСНОВНЫЕ КОМАНДЫ
# ══════════════════════════════════════════════

bot = TeleBot(TOKEN, threaded=True, num_threads=6)

@bot.message_handler(commands=["start", "меню"])
def cmd_start(msg):
    uid = msg.from_user.id
    name = msg.from_user.first_name or "Игрок"
    ensure_user(uid, name)
    
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
        return
    
    lvl = user_level(u["xp"])
    prem = is_premium(uid)
    prem_tag = "⭐" if prem else ""
    
    wins = u.get("games_won", 0)
    losses = u.get("games_lost", 0)
    total_games = wins + losses
    
    text = (
        f"<b>👤 Профиль</b> {prem_tag}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Имя: <b>{u['name'] or 'Игрок'}</b>\n"
        f"ID: <code>{uid}</code>\n"
        f"Уровень: <b>{lvl}</b>\n"
        f"Опыт: <b>{fmt(u['xp'])}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Баланс: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"🏦 В банке: <b>{fmt(u['bank'])} {CURRENCY}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎮 Игры: {wins} побед / {losses} поражений\n"
        f"📈 Всего выиграно: <b>{fmt(u.get('total_won', 0))}</b>\n"
        f"📉 Всего проиграно: <b>{fmt(u.get('total_lost', 0))}</b>"
    )
    bot.send_message(uid, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "💰 Баланс")
def cmd_balance(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    if not u:
        return
    
    text = (
        f"<b>💰 Баланс</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"В кошельке: <b>{fmt(u['balance'])} {CURRENCY}</b>\n"
        f"В банке: <b>{fmt(u['bank'])} {CURRENCY}</b>\n"
        f"Всего заработано: <b>{fmt(u['total_earned'])}</b>"
    )
    bot.send_message(uid, text, parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text == "⚒️ Работа")
def cmd_work(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")
    
    text = "<b>⚒️ Работа</b>\n\nВыбери способ заработка:"
    bot.send_message(uid, text, reply_markup=work_menu(), parse_mode="HTML")

# ══════════════════════════════════════════════
# 6. КЛИКЕР
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "👆 Кликер")
def cmd_click(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    if not u:
        return
    
    remaining = CD_CLICK - (now() - u["last_click"])
    if remaining > 0:
        bot.send_message(uid, f"⏱ Клик через: <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return
    
    earn = CLICK_BASE + random.randint(-20, 50)
    update_balance(uid, earn)
    add_xp(uid, 5)
    
    with db() as c:
        c.execute("UPDATE users SET last_click=? WHERE id=?", (now(), uid))
    
    text = f"⚡ <b>Клик!</b> +<b>{fmt(earn)} {CURRENCY}</b>\n⭐ +5 опыта"
    bot.send_message(uid, text, parse_mode="HTML")

# ══════════════════════════════════════════════
# 7. МАЙНИНГ
# ══════════════════════════════════════════════

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
    earn = MINE_BASE + cards * 200 + random.randint(-50, 100)
    earn = max(100, earn)
    update_balance(uid, earn)
    add_xp(uid, 10)
    
    with db() as c:
        c.execute("UPDATE users SET last_mine=? WHERE id=?", (now(), uid))
    
    text = (
        f"⛏️ <b>Майнинг</b>\n\n"
        f"💰 Намайнено: <b>+{fmt(earn)} {CURRENCY}</b>\n"
        f"🖥 Видеокарт: {cards}\n"
        f"⭐ +10 опыта"
    )
    bot.send_message(uid, text, parse_mode="HTML")
    
    # Предложение купить карту
    card_price = 5000 * (2 ** cards)
    if u["balance"] >= card_price:
        bot.send_message(uid, f"💡 Купить видеокарту за {fmt(card_price)}? Напиши: <code>купить карту</code>", parse_mode="HTML")

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "купить карту")
def cmd_buy_card(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    cards = u.get("video_cards", 0)
    price = 5000 * (2 ** cards)
    
    if u["balance"] < price:
        bot.send_message(uid, f"❌ Не хватает {fmt(price)} {CURRENCY}", parse_mode="HTML")
        return
    
    update_balance(uid, -price)
    with db() as c:
        c.execute("UPDATE users SET video_cards=video_cards+1 WHERE id=?", (uid,))
    
    bot.send_message(uid, f"✅ Куплена видеокарта! Теперь у тебя {cards + 1} карт(ы)", parse_mode="HTML")

# ══════════════════════════════════════════════
# 8. ТАКСИ
# ══════════════════════════════════════════════

TAXI_ROUTES = [
    {"name": "📍 Центр → Аэропорт", "base": 1500, "time": 5},
    {"name": "🏠 Жилой р-н → Офис", "base": 1000, "time": 4},
    {"name": "🎓 Университет → ТЦ", "base": 800, "time": 3},
    {"name": "🏥 Больница → Вокзал", "base": 1200, "time": 4},
    {"name": "🏢 Бизнес-центр → Ресторан", "base": 600, "time": 3},
    {"name": "🛍️ ТЦ → Кинотеатр", "base": 500, "time": 3},
    {"name": "🌃 Ночной рейс", "base": 2000, "time": 6},
]

active_rides = {}

@bot.message_handler(func=lambda m: m.text == "🚗 Такси")
def cmd_taxi(msg):
    uid = msg.from_user.id
    
    if uid in active_rides:
        bot.send_message(uid, "⚠️ У тебя уже есть активная поездка! Заверши её.", parse_mode="HTML")
        return
    
    route = random.choice(TAXI_ROUTES)
    earn = int(route["base"] * random.uniform(0.9, 1.2))
    
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
        f"✅ Поехали? Напиши <b>завершить поездку</b> через {route['time']} мин"
    )
    bot.send_message(uid, text, parse_mode="HTML")
    
    # Авто-завершение
    def finish():
        time.sleep(route["time"] * 60)
        if uid in active_rides:
            data = active_rides.pop(uid)
            update_balance(uid, data["earn"])
            add_xp(uid, 50)
            bot.send_message(uid, f"🚕 <b>Поездка завершена!</b>\n💰 +{fmt(data['earn'])} {CURRENCY}\n⭐ +50 опыта", parse_mode="HTML")
    
    threading.Thread(target=finish, daemon=True).start()

@bot.message_handler(func=lambda m: m.text and m.text.lower() == "завершить поездку")
def cmd_finish_ride(msg):
    uid = msg.from_user.id
    if uid in active_rides:
        data = active_rides.pop(uid)
        # Бонус за досрочное завершение
        time_left = max(0, data["time"] * 60 - (now() - data["start"]))
        bonus = int(data["earn"] * (1 - time_left / (data["time"] * 60)) * 0.3)
        total = data["earn"] + bonus
        update_balance(uid, total)
        add_xp(uid, 50)
        bot.send_message(uid, f"🚕 <b>Поездка завершена досрочно!</b>\n💰 +{fmt(total)} {CURRENCY}\n⭐ +50 опыта", parse_mode="HTML")
    else:
        bot.send_message(uid, "❌ У тебя нет активной поездки", parse_mode="HTML")

# ══════════════════════════════════════════════
# 9. ИГРЫ
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🎰 Игры")
def cmd_games(msg):
    bot.send_message(msg.chat.id, games_help(), reply_markup=games_menu(), parse_mode="HTML")

def parse_bet(text: str, balance: int) -> int | None:
    text = text.lower().strip()
    if text in ["все", "all"]:
        return balance
    if text.endswith("к"):
        try:
            return int(float(text[:-1]) * 1000)
        except:
            return None
    if text.endswith("м"):
        try:
            return int(float(text[:-1]) * 1000000)
        except:
            return None
    try:
        return int(text)
    except:
        return None

def get_display_name(uid: int) -> str:
    u = get_user(uid)
    if u and u.get("name"):
        return u["name"]
    return str(uid)

# ── Кости ──────────────────────────────────────────────────

@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("кости"))
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
        update_game_stats(uid, "кости", bet, win_amount, "win")
        text = f"🎲 <b>Выпало {result}!</b> Победа! +{fmt(win_amount - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        update_game_stats(uid, "кости", bet, 0, "lose")
        text = f"🎲 <b>Выпало {result}!</b> Проигрыш. -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ── Слоты ──────────────────────────────────────────────────

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
        update_game_stats(uid, "слоты", bet, win, "win")
        text = f"🎰 {combo}\n🎉 Джекпот x{mult}! +{fmt(win - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        update_game_stats(uid, "слоты", bet, 0, "lose")
        text = f"🎰 {combo}\n😔 Нет совпадений. -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ── Дартс ──────────────────────────────────────────────────

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
        update_game_stats(uid, "дартс", bet, win, "win")
        text = f"🎯 <b>ЯБЛОЧКО!</b> +{fmt(win - bet)} {CURRENCY}"
    elif result in [4, 5]:
        update_balance(uid, 0)
        update_game_stats(uid, "дартс", bet, 0, "draw")
        text = f"🎯 Попадание в кольцо! Ставка возвращена."
    else:
        update_balance(uid, -bet * 2)
        update_game_stats(uid, "дартс", bet, 0, "lose")
        text = f"💥 ПРОМАХ! -{fmt(bet * 2)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ── Боулинг ──────────────────────────────────────────────────

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
        update_game_stats(uid, "боулинг", bet, win, "win")
        text = f"🎳 <b>СТРАЙК!</b> x3! +{fmt(win - bet)} {CURRENCY}"
    elif result == 5:
        win = int(bet * 1.5)
        update_balance(uid, win - bet)
        update_game_stats(uid, "боулинг", bet, win, "win")
        text = f"🎳 Спэр! x1.5! +{fmt(win - bet)} {CURRENCY}"
    elif result >= 3:
        update_balance(uid, 0)
        update_game_stats(uid, "боулинг", bet, 0, "draw")
        text = f"🎳 9 кеглей! Ставка возвращена."
    else:
        update_balance(uid, -bet)
        update_game_stats(uid, "боулинг", bet, 0, "lose")
        text = f"🎳 Промах! -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ── Баскетбол ──────────────────────────────────────────────────

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
        update_game_stats(uid, "баскетбол", bet, win, "win")
        text = f"🏀 <b>ПОПАДАНИЕ!</b> x2.5! +{fmt(win - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        update_game_stats(uid, "баскетбол", bet, 0, "lose")
        text = f"🏀 Промах! -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ── Футбол ──────────────────────────────────────────────────

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
        update_game_stats(uid, "футбол", bet, win, "win")
        text = f"⚽ <b>ГОЛ!</b> x2! +{fmt(win - bet)} {CURRENCY}"
    else:
        update_balance(uid, -bet)
        update_game_stats(uid, "футбол", bet, 0, "lose")
        text = f"⚽ Мимо! -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ── Краш ──────────────────────────────────────────────────

crash_games = {}

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
        update_game_stats(uid, "краш", bet, win, "win")
        text = f"🎲 <b>КРАШ НА {crash_at:.2f}x!</b>\n💰 Твой множитель: {target_mult}x\n✅ Выигрыш: +{fmt(win)} {CURRENCY}"
    else:
        update_game_stats(uid, "краш", bet, 0, "lose")
        text = f"💥 <b>КРАШ НА {crash_at:.2f}x!</b>\n📉 Твой множитель: {target_mult}x\n❌ Проигрыш: -{fmt(bet)} {CURRENCY}"
    
    bot.send_message(uid, text, parse_mode="HTML")

# ══════════════════════════════════════════════
# 10. БАНК
# ══════════════════════════════════════════════

BANK_RATE = 0.005  # 0.5% в 3 часа

@bot.message_handler(func=lambda m: m.text == "🏦 Банк")
def cmd_bank(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    
    text = (
        f"<b>🏦 Банк</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Баланс: {fmt(u['balance'])} {CURRENCY}\n"
        f"🏦 Депозит: {fmt(u['bank'])} {CURRENCY}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📈 Ставка: {BANK_RATE*100:.2f}%/3ч\n\n"
        f"<b>Команды:</b>\n"
        f"<code>вклад 5000</code> — положить на депозит\n"
        f"<code>снять 5000</code> — снять с депозита"
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
        bot.send_message(uid, "❌ Формат: <code>снять 5000</code>", parse_mode="HTML")
        return
    
    amount = parse_bet(parts[1], u["bank"])
    if not amount or amount <= 0 or amount > u["bank"]:
        bot.send_message(uid, f"❌ Неверная сумма. На депозите: {fmt(u['bank'])}", parse_mode="HTML")
        return
    
    update_balance(uid, amount)
    with db() as c:
        c.execute("UPDATE users SET bank=bank-? WHERE id=?", (amount, uid))
    
    bot.send_message(uid, f"✅ Снято с депозита: <b>{fmt(amount)} {CURRENCY}</b>", parse_mode="HTML")

# ══════════════════════════════════════════════
# 11. БИРЖА
# ══════════════════════════════════════════════

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
        
        new_price = price * (1 + impact + random.uniform(-STOCK_VOLATILITY, STOCK_VOLATILITY))
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
        c.execute("SELECT shares, avg_buy FROM stock_portfolio WHERE user_id=? AND ticker=?", (uid, TICKER))
        port = c.fetchone()
    
    text = f"<b>📈 Биржа — {TICKER}</b>\n━━━━━━━━━━━━━━━━━━\nЦена: <b>{fmt(price)} {CURRENCY}</b>\n\n"
    
    if port and port["shares"] > 0:
        pnl = (price - port["avg_buy"]) * port["shares"]
        text += f"📂 Портфель: {port['shares']} акций\nP&L: {fmt(pnl)} {CURRENCY}\n\n"
    
    text += "<b>Команды:</b>\n<code>купить 10</code>\n<code>продать 10</code>"
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
    
    update_balance(uid, -total)
    update_stock_price(0.001 * qty)
    
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
        c.execute("SELECT shares, avg_buy FROM stock_portfolio WHERE user_id=? AND ticker=?", (uid, TICKER))
        port = c.fetchone()
        
        if not port or port["shares"] < qty:
            bot.send_message(uid, f"❌ У тебя {port['shares'] if port else 0} акций", parse_mode="HTML")
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
    
    update_stock_price(-0.001 * qty)
    
    pnl = net - port["avg_buy"] * qty
    bot.send_message(uid, f"✅ Продано <b>{qty} акций</b>\n💰 Получено: {fmt(net)} (комиссия {fmt(fee)})\n📈 P&L: {fmt(pnl)}", parse_mode="HTML")

# ══════════════════════════════════════════════
# 12. БОНУС
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "🎁 Бонус")
def cmd_bonus(msg):
    uid = msg.from_user.id
    u = get_user(uid)
    
    cd = DAILY_PREMIUM_CD if is_premium(uid) else DAILY_NORMAL_CD
    remaining = cd - (now() - u["last_daily"])
    
    if remaining > 0:
        bot.send_message(uid, f"🎁 Бонус через: <b>{cd_str(remaining)}</b>", parse_mode="HTML")
        return
    
    streak = u["daily_streak"]
    mult = 1 + min(streak * 0.1, 2)
    base = DAILY_BASE
    bonus = int(base * mult)
    
    update_balance(uid, bonus)
    add_xp(uid, 50)
    
    with db() as c:
        c.execute("UPDATE users SET last_daily=?, daily_streak=daily_streak+1 WHERE id=?", (now(), uid))
    
    text = (
        f"🎁 <b>Ежедневный бонус!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Стрик: {streak} дней\n"
        f"💰 Получено: <b>{fmt(bonus)} {CURRENCY}</b>\n"
        f"⭐ +50 опыта"
    )
    bot.send_message(uid, text, parse_mode="HTML")

# ══════════════════════════════════════════════
# 13. ТОП
# ══════════════════════════════════════════════

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
    
    bot.send_message(uid, text, parse_mode="HTML")

# ══════════════════════════════════════════════
# 14. ДОНАТ
# ══════════════════════════════════════════════

DONATE_PACKAGES = {
    "stars_1": {"stars": 1, "amount": 10000, "emoji": "⭐"},
    "stars_5": {"stars": 5, "amount": 66000, "emoji": "⭐"},
    "stars_15": {"stars": 15, "amount": 266000, "emoji": "🔥"},
    "stars_50": {"stars": 50, "amount": 1000000, "emoji": "🔥"},
    "stars_150": {"stars": 150, "amount": 4000000, "emoji": "💎"},
    "stars_250": {"stars": 250, "amount": 8000000, "emoji": "💎"},
}

@bot.message_handler(func=lambda m: m.text == "💎 Донат")
def cmd_donate(msg):
    uid = msg.from_user.id
    
    text = "<b>💎 Пополнение баланса</b>\n━━━━━━━━━━━━━━━━━━\n"
    for key, pkg in DONATE_PACKAGES.items():
        text += f"{pkg['emoji']} {pkg['stars']} ⭐ → {fmt(pkg['amount'])} {CURRENCY}\n"
    
    markup = InlineKeyboardMarkup()
    for key, pkg in DONATE_PACKAGES.items():
        markup.add(InlineKeyboardButton(
            f"{pkg['emoji']} {pkg['stars']} ⭐ — {fmt(pkg['amount'])}",
            callback_data=f"donate_{key}"
        ))
    
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
        title=f"Пополнение на {fmt(pkg['amount'])} {CURRENCY}",
        description=f"{pkg['stars']} ⭐ → {fmt(pkg['amount'])} {CURRENCY}",
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

# ══════════════════════════════════════════════
# 15. ПОМОЩЬ
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text == "📞 Помощь")
def cmd_help(msg):
    text = """
<b>📞 Помощь по боту</b>

<b>👤 Профиль</b> — твоя статистика
<b>💰 Баланс</b> — сколько денег
<b>⚒️ Работа</b> — кликер, майнинг, такси

<b>🎰 Игры</b>
кости [сумма] [чет/нечет/число]
слоты [сумма]
дартс [сумма]
боулинг [сумма]
баскетбол [сумма]
футбол [сумма]
краш [сумма] [множитель]

<b>🏦 Банк</b>
вклад [сумма]
снять [сумма]

<b>📈 Биржа</b>
купить [количество]
продать [количество]

<b>🎁 Бонус</b> — ежедневная награда
<b>🏆 Топ</b> — рейтинг богачей
<b>💎 Донат</b> — пополнить баланс

💡 Все суммы можно писать с к/м: 100к, 1м, все
    """
    bot.send_message(msg.chat.id, text, parse_mode="HTML")

# ══════════════════════════════════════════════
# 16. АДМИН-КОМАНДЫ
# ══════════════════════════════════════════════

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

# ══════════════════════════════════════════════
# 17. ЗАПУСК
# ══════════════════════════════════════════════

def stock_scheduler():
    """Обновление цены акций каждые 30 минут"""
    while True:
        time.sleep(1800)
        try:
            update_stock_price()
        except Exception as e:
            print(f"[stocks] ошибка: {e}")

def interest_scheduler():
    """Начисление процентов по вкладам каждые 3 часа"""
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
    
    print("🚀 Бот запущен")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)
