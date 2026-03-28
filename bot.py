"""
╔══════════════════════════════════════════════╗
║   💎  ECON BOT  —  v3.0  ULTRA CLEAN       ║
║   Minimalist · Smart · Feature-Rich         ║
╚══════════════════════════════════════════════╝

Архитектура:
  • Один файл, 30 чётких блоков
  • SQLite + WAL + connection pool (потокобезопасно)
  • Только inline-кнопки — 0 reply-клавиатур
  • Единая точка входа главного меню (home)
  • Все деньги — целые числа
  • Flood protection in-memory
  • Групповые игры + ЛС-функционал

Фичи:
  КОШЕЛЁК    Баланс · Банк · Вклад · Кредит · Перевод · Чек
  РАБОТА     5 профессий · кулдаун · случайные события
  КЛИКЕР     Прокачка клика · бустеры
  МАЙНИНГ    Фермы · авто-сбор · GPU-апгрейды
  БИРЖА      1 акция · история · дивиденды · рыночный импакт
  ИГРЫ (ЛС)  Кубик · Слоты · Рулетка · Мины · Краш · Башня · Лотерея
  ИГРЫ (гр.) Рулетка · Кости · Слоты · Дуэль · КНБ · Чек · Краш
  ТОП        Баланс · XP · Акции
  КЛАН       Создать · Казна · Роли · Участники
  МАГАЗИН    Товары · Инвентарь · Апгрейды
  БОНУСЫ     Ежедневный стрик · Реферал · Промокод
  ПРОФИЛЬ    Уровень · XP · Статистика · Ачивки
  ДОНАТ      Telegram Stars → монеты
  СОБЫТИЯ    Случайные метеоры · Налог · Bio-drop
  АДМИН      Выдача · Ban · Промо · Рассылка · Стат
"""

# ══════════════════════════════════════════════
# 0. ЗАВИСИМОСТИ
# ══════════════════════════════════════════════

import os, re, time, json, math, random, threading
from contextlib import contextmanager
from datetime import datetime, timedelta
import sqlite3
from dotenv import load_dotenv
import telebot
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, LabeledPrice
)

load_dotenv()

# ══════════════════════════════════════════════
# 1. КОНФИГ
# ══════════════════════════════════════════════

TOKEN       = os.getenv("BOT_TOKEN", "")
ADMIN_IDS   = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
ALERT_CHAT  = int(os.getenv("ALERT_CHAT", "0"))
DB_FILE     = "econ_v3.db"
CUR         = "💎"          # валюта
TICKER      = "ECO"

# ── Кулдауны (секунды) ──
CD_CLICK    = 8
CD_DAILY    = 86400
CD_WORK     = 14_400      # 4 часа
CD_MINE     = 3_600       # 1 час
CD_LOTTERY  = 86400

# ── Экономика ──
TRANSFER_FEE   = 0.05
BANK_RATE      = 0.01     # 1%/сутки — обычный вклад
TERM_RATE      = 0.04     # 4%/сутки — срочный вклад (7 дней lock)
LOAN_RATE      = 0.10
LOAN_MAX       = 100_000
CLICK_BASE     = 150
MINE_BASE      = 600
WORK_BASE      = 2_000
TICKET_PRICE   = 500
CLAN_COST      = 5_000

# ── Акция ──
STOCK_TICK      = 1_800   # обновление каждые 30 мин
STOCK_START     = 10_000
STOCK_VOL       = 0.04    # ±4%

# ── Флуд-защита ──
FLOOD_N, FLOOD_W, FLOOD_BAN = 12, 5, 300

# ── Башня ──
TOWER_LEVELS    = 10
TOWER_MULT_BASE = 1.4     # множитель за уровень

# ── Ачивки ──
ACHIEVEMENTS = {
    "first_click":  ("⚡ Первый клик",   "Сделай первый клик"),
    "rich":         ("💰 Миллионер",     "Накопи 1 000 000"),
    "gambler":      ("🎲 Игрок",         "Сыграй 100 игр"),
    "investor":     ("📈 Инвестор",      "Купи акции впервые"),
    "social":       ("👥 Социальный",    "Пригласи 5 игроков"),
    "worker":       ("⚒️ Трудяга",       "Выйди на работу 30 раз"),
    "miner":        ("⛏️ Шахтёр",        "Собери руду 50 раз"),
    "streak_7":     ("🔥 Недельный",     "7 дней подряд бонус"),
    "streak_30":    ("🔥🔥 Месячный",    "30 дней подряд бонус"),
    "clan_owner":   ("🏰 Основатель",    "Создай клан"),
}

# ══════════════════════════════════════════════
# 2. БАЗА ДАННЫХ
# ══════════════════════════════════════════════

class _Pool:
    def __init__(self, path, size=10):
        self._path = path
        self._size = size
        self._pool: list[sqlite3.Connection] = []
        self._lock = threading.Lock()

    def get(self) -> sqlite3.Connection:
        with self._lock:
            if self._pool:
                return self._pool.pop()
        c = sqlite3.connect(self._path, timeout=30, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    def put(self, c: sqlite3.Connection):
        with self._lock:
            if len(self._pool) < self._size:
                self._pool.append(c)
            else:
                c.close()

_pool = _Pool(DB_FILE)

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
            id              INTEGER PRIMARY KEY,
            name            TEXT    DEFAULT '',
            balance         INTEGER DEFAULT 0,
            bank            INTEGER DEFAULT 0,
            xp              INTEGER DEFAULT 0,
            daily_streak    INTEGER DEFAULT 0,
            last_click      INTEGER DEFAULT 0,
            last_daily      INTEGER DEFAULT 0,
            last_work       INTEGER DEFAULT 0,
            last_mine       INTEGER DEFAULT 0,
            click_power     INTEGER DEFAULT 150,
            total_earned    INTEGER DEFAULT 0,
            total_games     INTEGER DEFAULT 0,
            total_clicks    INTEGER DEFAULT 0,
            total_works     INTEGER DEFAULT 0,
            total_mines     INTEGER DEFAULT 0,
            ref_by          INTEGER DEFAULT 0,
            ref_code        TEXT    UNIQUE,
            ref_count       INTEGER DEFAULT 0,
            premium_until   INTEGER DEFAULT 0,
            achievements    TEXT    DEFAULT '[]',
            bio_drop_claimed INTEGER DEFAULT 0,
            last_interest   INTEGER DEFAULT 0,
            created_at      INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS term_deposits (
            user_id     INTEGER PRIMARY KEY,
            amount      INTEGER DEFAULT 0,
            rate        REAL    DEFAULT 0.04,
            locked_until INTEGER DEFAULT 0,
            started_at  INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS loans (
            user_id     INTEGER PRIMARY KEY,
            amount      INTEGER DEFAULT 0,
            due_at      INTEGER DEFAULT 0,
            taken_at    INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS stocks (
            ticker      TEXT PRIMARY KEY,
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
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker  TEXT,
            price   INTEGER,
            ts      INTEGER DEFAULT 0
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
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            from_id INTEGER,
            to_id   INTEGER,
            amount  INTEGER,
            fee     INTEGER,
            ts      INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS items (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT,
            emoji   TEXT DEFAULT '📦',
            desc    TEXT DEFAULT '',
            price   INTEGER,
            effect  TEXT DEFAULT '',
            supply  INTEGER DEFAULT -1,
            sold    INTEGER DEFAULT 0,
            active  INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS inventory (
            user_id INTEGER,
            item_id INTEGER,
            qty     INTEGER DEFAULT 1,
            PRIMARY KEY (user_id, item_id)
        );
        CREATE TABLE IF NOT EXISTS clans (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT UNIQUE,
            tag         TEXT UNIQUE,
            owner       INTEGER,
            balance     INTEGER DEFAULT 0,
            level       INTEGER DEFAULT 1,
            xp          INTEGER DEFAULT 0,
            created_at  INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS clan_members (
            user_id     INTEGER PRIMARY KEY,
            clan_id     INTEGER,
            role        TEXT DEFAULT 'member',
            joined_at   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY,
            reason  TEXT,
            by      INTEGER,
            ts      INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS promo_codes (
            code        TEXT PRIMARY KEY,
            reward      INTEGER,
            max_uses    INTEGER DEFAULT 1,
            uses        INTEGER DEFAULT 0,
            expires     INTEGER DEFAULT 0,
            active      INTEGER DEFAULT 1
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
        CREATE TABLE IF NOT EXISTS events (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            type    TEXT,
            payload TEXT,
            active  INTEGER DEFAULT 1,
            created_at INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS game_log (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            game    TEXT,
            bet     INTEGER,
            result  INTEGER,
            ts      INTEGER DEFAULT 0
        );

        INSERT OR IGNORE INTO lottery (id,jackpot,draw_at) VALUES (1,0,0);
        INSERT OR IGNORE INTO stocks  (ticker,price,prev_price,updated_at)
            VALUES ('ECO',10000,10000,0);
        INSERT OR IGNORE INTO donate_packages VALUES
            ('s1',   1,    15000,  '⭐ 15 000'),
            ('s5',   5,    80000,  '⭐ 80 000'),
            ('s15',  15,   300000, '🔥 300 000'),
            ('s50',  50,  1100000, '🔥 1 100 000'),
            ('s150', 150, 3500000, '💎 3 500 000'),
            ('s250', 250, 6000000, '💎 6 000 000');

        INSERT OR IGNORE INTO items (emoji,name,desc,price,effect) VALUES
            ('🖥','GPU x1',    'Майнинг +200/час',  8_000,  'mine+200'),
            ('🖥','GPU x3',    'Майнинг +600/час', 22_000,  'mine+600'),
            ('⚡','Турбо',     'Клик x2 на 1 час',  5_000,  'click_boost_3600'),
            ('🎰','VIP-слоты', 'Слоты x1.5 выигрыш',12_000, 'slots_vip'),
            ('🛡','Страховка', 'Защита от краша x1', 3_000,  'crash_shield');

        CREATE INDEX IF NOT EXISTS ix_users_bal  ON users(balance DESC);
        CREATE INDEX IF NOT EXISTS ix_users_xp   ON users(xp DESC);
        CREATE INDEX IF NOT EXISTS ix_port_user  ON portfolios(user_id);
        CREATE INDEX IF NOT EXISTS ix_hist_tick  ON stock_history(ticker,ts DESC);
        CREATE INDEX IF NOT EXISTS ix_gamelog    ON game_log(user_id,ts DESC);
        """)
    print("✅ БД инициализирована")


# ══════════════════════════════════════════════
# 3. ФЛУД-ЗАЩИТА
# ══════════════════════════════════════════════

_flood: dict[int, list] = {}
_banned_flood: dict[int, float] = {}
_flood_lock = threading.Lock()

def is_flooding(uid: int) -> bool:
    t = time.time()
    with _flood_lock:
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
# 4. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════

def now() -> int:
    return int(time.time())

def fmt(n: int) -> str:
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if abs(n) >= 1_000:
        return f"{n:,}".replace(",", " ")
    return str(n)

def cd_str(s: int) -> str:
    if s <= 0: return "готово"
    h, r = divmod(max(0, s), 3600)
    m, s = divmod(r, 60)
    if h: return f"{h}ч {m}м"
    if m: return f"{m}м {s}с"
    return f"{s}с"

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

def user_level(xp: int) -> int:
    return max(1, int(math.sqrt(xp / 150)) + 1)

def level_xp(lvl: int) -> int:
    return (lvl - 1) ** 2 * 150

def xp_bar(xp: int, bars: int = 10) -> str:
    lvl = user_level(xp)
    lo  = level_xp(lvl)
    hi  = level_xp(lvl + 1)
    pct = min(bars, int((xp - lo) / max(1, hi - lo) * bars))
    return "█" * pct + "░" * (bars - pct)

def send_alert(text: str):
    if ALERT_CHAT:
        try:
            bot.send_message(ALERT_CHAT, text, parse_mode="HTML")
        except Exception:
            pass

def mention(uid: int, name: str) -> str:
    return f'<a href="tg://user?id={uid}">{name}</a>'


# ── DB helpers ──

def get_user(uid: int) -> dict | None:
    with db() as c:
        c.execute("SELECT * FROM users WHERE id=?", (uid,))
        row = c.fetchone()
        return dict(row) if row else None

def ensure_user(uid: int, name: str = "") -> dict:
    ref_code = f"R{uid}"
    with db() as c:
        c.execute(
            "INSERT OR IGNORE INTO users (id,name,ref_code,created_at) VALUES (?,?,?,?)",
            (uid, name, ref_code, now())
        )
    return get_user(uid)

def add_balance(uid: int, amount: int):
    with db() as c:
        c.execute(
            "UPDATE users SET balance=balance+?, total_earned=total_earned+MAX(0,?) WHERE id=?",
            (amount, amount, uid)
        )

def add_xp(uid: int, xp: int):
    with db() as c:
        c.execute("UPDATE users SET xp=xp+? WHERE id=?", (xp, uid))

def is_banned_db(uid: int) -> bool:
    with db() as c:
        c.execute("SELECT 1 FROM bans WHERE user_id=?", (uid,))
        return bool(c.fetchone())

def get_achievements(uid: int) -> set:
    u = get_user(uid)
    if not u: return set()
    try:
        return set(json.loads(u.get("achievements") or "[]"))
    except Exception:
        return set()

def grant_achievement(uid: int, key: str):
    achs = get_achievements(uid)
    if key in achs: return False
    achs.add(key)
    with db() as c:
        c.execute("UPDATE users SET achievements=? WHERE id=?", (json.dumps(list(achs)), uid))
    if key in ACHIEVEMENTS:
        name, _ = ACHIEVEMENTS[key]
        try:
            bot.send_message(uid, f"🏅 <b>Ачивка разблокирована!</b>\n{name}", parse_mode="HTML")
        except Exception:
            pass
    return True

def log_game(uid: int, game: str, bet: int, result: int):
    with db() as c:
        c.execute("INSERT INTO game_log (user_id,game,bet,result,ts) VALUES (?,?,?,?,?)",
                  (uid, game, bet, result, now()))
        c.execute("UPDATE users SET total_games=total_games+1 WHERE id=?", (uid,))

def check_milestones(uid: int):
    u = get_user(uid)
    if not u: return
    if u["balance"] >= 1_000_000:
        grant_achievement(uid, "rich")
    if u["total_games"] >= 100:
        grant_achievement(uid, "gambler")
    if u["total_works"] >= 30:
        grant_achievement(uid, "worker")
    if u["total_mines"] >= 50:
        grant_achievement(uid, "miner")
    if u["total_clicks"] >= 1:
        grant_achievement(uid, "first_click")
    if u["ref_count"] >= 5:
        grant_achievement(uid, "social")
    if u["daily_streak"] >= 7:
        grant_achievement(uid, "streak_7")
    if u["daily_streak"] >= 30:
        grant_achievement(uid, "streak_30")

def has_item(uid: int, item_name: str) -> int:
    """Возвращает кол-во предметов по эффекту в инвентаре."""
    with db() as c:
        c.execute("""SELECT SUM(inv.qty) FROM inventory inv
                     JOIN items it ON it.id=inv.item_id
                     WHERE inv.user_id=? AND it.effect LIKE ?""", (uid, f"%{item_name}%"))
        row = c.fetchone()
        return int(row[0] or 0)

def mine_power(uid: int) -> int:
    """Вычисляет мощность майнинга с учётом GPU."""
    base = MINE_BASE
    with db() as c:
        c.execute("""SELECT it.effect, inv.qty FROM inventory inv
                     JOIN items it ON it.id=inv.item_id
                     WHERE inv.user_id=? AND it.effect LIKE 'mine+%'""", (uid,))
        for row in c.fetchall():
            try:
                bonus = int(row["effect"].split("+")[1])
                base += bonus * row["qty"]
            except Exception:
                pass
    return base


# ══════════════════════════════════════════════
# 5. БОТ
# ══════════════════════════════════════════════

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=8)

_orig_pnm = bot.process_new_messages
def _safe_pnm(msgs):
    ok = []
    for m in msgs:
        try:
            uid = m.from_user.id if m.from_user else None
            if uid and is_flooding(uid): continue
        except Exception:
            pass
        ok.append(m)
    if ok: _orig_pnm(ok)
bot.process_new_messages = _safe_pnm


# ══════════════════════════════════════════════
# 6. UI — КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def kb(*rows) -> InlineKeyboardMarkup:
    """
    Короткий конструктор клавиатуры.
    Каждый row — список tuples: (text, callback) или ("url", text, url).
    """
    m = InlineKeyboardMarkup()
    for row in rows:
        btns = []
        for item in row:
            if item[0] == "url":
                btns.append(InlineKeyboardButton(item[1], url=item[2]))
            else:
                btns.append(InlineKeyboardButton(item[0], callback_data=item[1]))
        m.row(*btns)
    return m

def _edit(call, text: str, markup: InlineKeyboardMarkup):
    uid = call.from_user.id
    try:
        bot.edit_message_text(text, uid, call.message.message_id,
                              reply_markup=markup, parse_mode="HTML")
    except Exception:
        bot.send_message(uid, text, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)


def main_menu_msg(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    u = get_user(uid)
    bal  = u["balance"] if u else 0
    bank = u["bank"] if u else 0
    lvl  = user_level(u["xp"]) if u else 1
    prem = u and u["premium_until"] > now()
    star = " ⭐" if prem else ""

    text = (
        f"<b>💎 Главная{star}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👛 {fmt(bal)} {CUR}   🏦 {fmt(bank)}\n"
        f"⚡ Уровень <b>{lvl}</b>\n"
    )
    buttons = kb(
        [("👛 Кошелёк", "menu_wallet"),   ("📈 Биржа",   "menu_stock")],
        [("⚒️ Работа",  "menu_work"),     ("🎰 Игры",    "menu_games")],
        [("🏦 Банк",    "menu_bank"),     ("🛍️ Магазин", "menu_shop")],
        [("🏆 Топ",     "menu_top"),      ("🎁 Бонус",   "menu_bonus")],
        [("👤 Профиль", "menu_profile"),  ("🏰 Клан",    "menu_clan")],
        [("💎 Донат",   "menu_donate"),   ("❓ Помощь",  "menu_help")],
    )
    return text, buttons


# ══════════════════════════════════════════════
# 7. СТАРТ / МЕНЮ
# ══════════════════════════════════════════════

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    uid  = msg.from_user.id
    name = msg.from_user.first_name or "Игрок"
    if is_banned_db(uid):
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
                    ruid = row["id"]
                    bonus = 3_000
                    add_balance(uid, bonus)
                    add_balance(ruid, bonus)
                    c.execute("UPDATE users SET ref_by=? WHERE id=?", (ruid, uid))
                    c.execute("UPDATE users SET ref_count=ref_count+1 WHERE id=?", (ruid,))
                    try:
                        bot.send_message(ruid,
                            f"🎉 По вашей ссылке пришёл новый игрок!\n"
                            f"+<b>{fmt(bonus)} {CUR}</b>", parse_mode="HTML")
                    except Exception:
                        pass

    text, btns = main_menu_msg(uid)
    bot.send_message(uid, text, reply_markup=btns, parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and m.text.lower().strip() in
    ["меню", "/меню", "menu", "/menu", "🏠", "назад"])
def cmd_menu(msg):
    uid = msg.from_user.id
    if is_banned_db(uid): return
    ensure_user(uid, msg.from_user.first_name or "")
    text, btns = main_menu_msg(uid)
    bot.send_message(uid, text, reply_markup=btns, parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: c.data == "home")
def cb_home(call):
    uid = call.from_user.id
    text, btns = main_menu_msg(uid)
    _edit(call, text, btns)


# ══════════════════════════════════════════════
# 8. КОШЕЛЁК / ПРОФИЛЬ
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_wallet")
def cb_wallet(call):
    uid = call.from_user.id
    u = get_user(uid)
    if not u:
        bot.answer_callback_query(call.id, "Используй /start")
        return

    lvl = user_level(u["xp"])
    bar = xp_bar(u["xp"])

    # Долг
    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=?", (uid,))
        loan = c.fetchone()
    loan_str = ""
    if loan and loan["amount"] > 0:
        due = datetime.fromtimestamp(loan["due_at"]).strftime("%d.%m")
        loan_str = f"\n⚠️ Долг: <b>{fmt(loan['amount'])}</b> до {due}"

    text = (
        f"<b>👛 Кошелёк</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Наличные: <b>{fmt(u['balance'])} {CUR}</b>\n"
        f"Банк:     <b>{fmt(u['bank'])} {CUR}</b>\n"
        f"Заработано: {fmt(u['total_earned'])}\n"
        f"{loan_str}\n"
        f"⚡ Ур. {lvl}  [{bar}]\n"
        f"XP: {fmt(u['xp'])}"
    )
    buttons = kb(
        [("💸 Перевод", "act_transfer"), ("🔄 Обновить", "menu_wallet")],
        [("📜 История", "act_tx_hist"),  ("🏠 Меню",      "home")],
    )
    _edit(call, text, buttons)


@bot.callback_query_handler(func=lambda c: c.data == "menu_profile")
def cb_profile(call):
    uid = call.from_user.id
    u = get_user(uid)
    if not u:
        bot.answer_callback_query(call.id, "Используй /start")
        return

    lvl  = user_level(u["xp"])
    prem = u["premium_until"] > now()
    prem_str = f"⭐ до {datetime.fromtimestamp(u['premium_until']).strftime('%d.%m.%y')}" if prem else "—"

    with db() as c:
        c.execute("SELECT cl.name, cm.role FROM clan_members cm "
                  "JOIN clans cl ON cl.id=cm.clan_id WHERE cm.user_id=?", (uid,))
        clan = c.fetchone()

    clan_str = f"[{clan['name']}] {clan['role']}" if clan else "—"

    achs = get_achievements(uid)
    ach_str = " ".join(ACHIEVEMENTS[k][0].split()[0] for k in achs) if achs else "нет"

    reg = datetime.fromtimestamp(u["created_at"]).strftime("%d.%m.%Y") if u["created_at"] else "?"

    text = (
        f"<b>👤 Профиль</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>{u['name'] or 'Игрок'}</b>  <code>{uid}</code>\n"
        f"Уровень: <b>{lvl}</b>  XP: {fmt(u['xp'])}\n"
        f"🔥 Стрик: <b>{u['daily_streak']}</b> дней\n"
        f"Premium: {prem_str}\n"
        f"Клан: {clan_str}\n"
        f"Приглашено: {u['ref_count']} игроков\n"
        f"Игр: {u['total_games']}  Работ: {u['total_works']}  Кликов: {u['total_clicks']}\n"
        f"Рег: {reg}\n"
        f"🏅 {ach_str}"
    )
    buttons = kb(
        [("✏️ Имя",         "act_rename"),  ("🏅 Ачивки",   "menu_achiev")],
        [("🔗 Реферал",     "act_reflink"),  ("🏠 Меню",     "home")],
    )
    _edit(call, text, buttons)


@bot.callback_query_handler(func=lambda c: c.data == "menu_achiev")
def cb_achiev(call):
    uid = call.from_user.id
    achs = get_achievements(uid)
    lines = []
    for key, (name, desc) in ACHIEVEMENTS.items():
        done = "✅" if key in achs else "🔒"
        lines.append(f"{done} <b>{name}</b> — {desc}")
    text = "<b>🏅 Ачивки</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines)
    _edit(call, text, kb([("◀️ Назад", "menu_profile"), ("🏠 Меню", "home")]))


# ══════════════════════════════════════════════
# 9. ВВОД ТЕКСТА — FSM-словарь
# ══════════════════════════════════════════════

_waiting: dict[int, str] = {}

PROMPTS = {
    "act_transfer":   "💸 Введи: @username сумма\nПример: @ivan 5000",
    "act_rename":     "✏️ Введи новое имя (до 20 символов):",
    "bank_deposit":   "🏦 Введи сумму для вклада:",
    "bank_withdraw":  "🏦 Введи сумму для снятия:",
    "bank_loan":      f"💳 Введи сумму кредита (макс {fmt(LOAN_MAX)}):",
    "bank_repay":     "💳 Введи сумму для погашения:",
    "term_deposit":   f"⏳ Введи сумму срочного вклада (lock 7 дней, +{TERM_RATE*100:.0f}%/сут):",
    "term_withdraw":  "⏳ Введи сумму для снятия срочного вклада (штраф 50%):",
    "stock_buy":      "📈 Введи количество акций для покупки:",
    "stock_sell":     "📉 Введи количество акций для продажи:",
    "game_dice":      "🎲 Введи ставку:",
    "game_slots":     "🎰 Введи ставку:",
    "game_roul":      "🎡 Введи: цвет сумма\nПример: красное 1000",
    "game_crash":     "⚡ Введи ставку:",
    "game_mines":     "💣 Введи ставку:",
    "game_tower":     "🏯 Введи ставку:",
    "clan_create":    "🏰 Введи: Название ТЕГ\nПример: Легион LEG\nСтоимость: 5 000",
    "clan_donate":    f"💰 Введи сумму взноса в казну клана:",
    "promo_use":      "🎁 Введи промокод:",
    "act_tx_hist":    None,  # безвводный
}

@bot.callback_query_handler(func=lambda c: c.data in PROMPTS and PROMPTS[c.data] is not None)
def cb_ask_input(call):
    uid = call.from_user.id
    action = call.data

    # Спец-обработка stock_buy/sell — нужны доп. данные
    if action == "stock_buy":
        st = _get_stock()
        u  = get_user(uid)
        max_sh = u["balance"] // st["price"] if u else 0
        extra = f"\nЦена: <b>{fmt(st['price'])}</b>  Можешь купить: <b>{max_sh}</b>"
    elif action == "stock_sell":
        with db() as c:
            c.execute("SELECT shares FROM portfolios WHERE user_id=? AND ticker=?", (uid, TICKER))
            port = c.fetchone()
        extra = f"\nУ тебя: <b>{port['shares'] if port else 0}</b> акций"
    else:
        extra = ""

    _waiting[uid] = action
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        PROMPTS[action] + extra,
        parse_mode="HTML",
        reply_markup=kb([("❌ Отмена", "cancel_input")]))


@bot.callback_query_handler(func=lambda c: c.data == "cancel_input")
def cb_cancel(call):
    uid = call.from_user.id
    _waiting.pop(uid, None)
    bot.answer_callback_query(call.id, "Отменено")
    text, btns = main_menu_msg(uid)
    bot.send_message(uid, text, reply_markup=btns, parse_mode="HTML")


@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in _waiting
                                    and m.chat.type == "private")
def handle_input(msg):
    uid    = msg.from_user.id
    action = _waiting.pop(uid, None)
    if not action: return
    text   = msg.text.strip() if msg.text else ""
    u      = get_user(uid)

    home_btn = kb([("🏠 Меню", "home")])

    # ── Rename ──
    if action == "act_rename":
        name = text[:20]
        with db() as c:
            c.execute("UPDATE users SET name=? WHERE id=?", (name, uid))
        bot.send_message(uid, f"✅ Имя изменено на <b>{name}</b>",
                         parse_mode="HTML", reply_markup=home_btn)
        return

    # ── Transfer ──
    if action == "act_transfer":
        parts = text.split()
        if len(parts) < 2:
            bot.send_message(uid, "❌ Формат: @username сумма"); return
        uname = parts[0].lstrip("@")
        try: amount = int(parts[1].replace(" ", ""))
        except ValueError:
            bot.send_message(uid, "❌ Неверная сумма"); return
        if amount <= 0:
            bot.send_message(uid, "❌ Сумма > 0"); return
        fee   = int(amount * TRANSFER_FEE)
        total = amount + fee
        if u["balance"] < total:
            bot.send_message(uid, f"❌ Нужно {fmt(total)} (комиссия {fmt(fee)})"); return
        with db() as c:
            c.execute("SELECT id,name FROM users WHERE name LIKE ? OR id=?",
                      (f"%{uname}%", 0))
            c.execute("SELECT id,name FROM users WHERE name LIKE ?", (f"%{uname}%",))
            row = c.fetchone()
        if not row:
            bot.send_message(uid, "❌ Игрок не найден"); return
        to_uid = row["id"]
        if to_uid == uid:
            bot.send_message(uid, "❌ Нельзя себе"); return
        add_balance(uid,   -total)
        add_balance(to_uid, amount)
        with db() as c:
            c.execute("INSERT INTO transfers (from_id,to_id,amount,fee,ts) VALUES (?,?,?,?,?)",
                      (uid, to_uid, amount, fee, now()))
        try:
            bot.send_message(to_uid,
                f"💸 Вам перевели <b>{fmt(amount)} {CUR}</b>",
                parse_mode="HTML")
        except Exception: pass
        bot.send_message(uid,
            f"✅ Переведено <b>{fmt(amount)}</b> + комиссия <b>{fmt(fee)}</b>",
            parse_mode="HTML", reply_markup=home_btn)
        return

    # ── Bank ops ──
    if action in ("bank_deposit", "bank_withdraw", "bank_loan", "bank_repay",
                  "term_deposit", "term_withdraw"):
        try: amount = int(text.replace(" ", "").replace(",", ""))
        except ValueError:
            bot.send_message(uid, "❌ Введи число"); return
        if amount <= 0:
            bot.send_message(uid, "❌ > 0"); return
        _do_bank_op(uid, action, amount)
        return

    # ── Stock ──
    if action in ("stock_buy", "stock_sell"):
        try: qty = int(text)
        except ValueError:
            bot.send_message(uid, "❌ Введи число"); return
        if qty <= 0:
            bot.send_message(uid, "❌ > 0"); return
        _do_stock_op(uid, action, qty)
        return

    # ── Games ──
    game_map = {
        "game_dice":  _game_dice,
        "game_slots": _game_slots,
        "game_crash": _game_crash_start,
        "game_mines": _game_mines_start,
        "game_tower": _game_tower_start,
    }
    if action in game_map:
        try: bet = int(text.replace(" ", ""))
        except ValueError:
            bot.send_message(uid, "❌ Введи ставку"); return
        if bet <= 0:
            bot.send_message(uid, "❌ > 0"); return
        if u["balance"] < bet:
            bot.send_message(uid, f"❌ Баланс: {fmt(u['balance'])}"); return
        game_map[action](uid, bet)
        return

    if action == "game_roul":
        parts = text.lower().split()
        if len(parts) < 2:
            bot.send_message(uid, "❌ Формат: красное 1000"); return
        try: bet = int(parts[1])
        except ValueError:
            bot.send_message(uid, "❌ Неверная ставка"); return
        if u["balance"] < bet:
            bot.send_message(uid, f"❌ Баланс: {fmt(u['balance'])}"); return
        _game_roulette(uid, parts[0], bet)
        return

    # ── Clan ──
    if action == "clan_create":
        _do_clan_create(uid, text)
        return
    if action == "clan_donate":
        try: amount = int(text.replace(" ", ""))
        except ValueError:
            bot.send_message(uid, "❌ Введи число"); return
        _do_clan_donate(uid, amount)
        return

    # ── Promo ──
    if action == "promo_use":
        _use_promo(uid, text.upper())
        return


# ══════════════════════════════════════════════
# 10. ЕЖЕДНЕВНЫЙ БОНУС
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_bonus")
def cb_bonus(call):
    uid = call.from_user.id
    u   = get_user(uid)
    rem = CD_DAILY - (now() - u["last_daily"])

    if rem > 0:
        text = (
            f"<b>🎁 Ежедневный бонус</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⏱ Через: <b>{cd_str(rem)}</b>\n"
            f"🔥 Стрик: <b>{u['daily_streak']}</b> дней"
        )
        _edit(call, text, kb([("🎁 Промокод", "promo_use"), ("🏠 Меню", "home")]))
        return

    streak = u["daily_streak"]
    bonus  = 1_500 + streak * 150
    if u.get("premium_until", 0) > now():
        bonus = int(bonus * 1.5)

    text = (
        f"<b>🎁 Ежедневный бонус</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔥 Стрик: <b>{streak}</b> дней\n"
        f"💰 Получишь: <b>+{fmt(bonus)} {CUR}</b>"
        + (" ⭐×1.5" if u.get("premium_until", 0) > now() else "")
    )
    _edit(call, text, kb([("✅ Получить", "claim_bonus"), ("🏠 Меню", "home")]))


@bot.callback_query_handler(func=lambda c: c.data == "claim_bonus")
def cb_claim_bonus(call):
    uid = call.from_user.id
    u   = get_user(uid)
    rem = CD_DAILY - (now() - u["last_daily"])
    if rem > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(rem)}", show_alert=True)
        return

    consec  = (now() - u["last_daily"]) < CD_DAILY + 7200
    streak  = (u["daily_streak"] + 1) if consec else 1
    bonus   = 1_500 + streak * 150
    if u.get("premium_until", 0) > now():
        bonus = int(bonus * 1.5)

    add_balance(uid, bonus)
    add_xp(uid, 30)
    with db() as c:
        c.execute("UPDATE users SET last_daily=?, daily_streak=? WHERE id=?",
                  (now(), streak, uid))
    check_milestones(uid)

    text = (
        f"<b>🎁 Получено!</b>\n"
        f"+<b>{fmt(bonus)} {CUR}</b>\n"
        f"🔥 Стрик: <b>{streak}</b> дней"
    )
    _edit(call, text, kb([("🎁 Промокод", "promo_use"), ("🏠 Меню", "home")]))
    bot.answer_callback_query(call.id, "✅ Получено!")


@bot.callback_query_handler(func=lambda c: c.data == "promo_use")
def cb_promo_menu(call):
    uid = call.from_user.id
    _waiting[uid] = "promo_use"
    bot.answer_callback_query(call.id)
    bot.send_message(uid, "🎁 Введи промокод:",
                     reply_markup=kb([("❌ Отмена", "cancel_input")]))


def _use_promo(uid: int, code: str):
    with db() as c:
        c.execute("SELECT * FROM promo_codes WHERE code=? AND active=1", (code,))
        promo = c.fetchone()
    home_btn = kb([("🏠 Меню", "home")])
    if not promo:
        bot.send_message(uid, "❌ Промокод не найден", reply_markup=home_btn); return
    if promo["expires"] and promo["expires"] < now():
        bot.send_message(uid, "❌ Истёк", reply_markup=home_btn); return
    if promo["max_uses"] > 0 and promo["uses"] >= promo["max_uses"]:
        bot.send_message(uid, "❌ Исчерпан", reply_markup=home_btn); return
    with db() as c:
        try:
            c.execute("INSERT INTO promo_uses (user_id,code,ts) VALUES (?,?,?)",
                      (uid, code, now()))
        except Exception:
            bot.send_message(uid, "❌ Уже использован", reply_markup=home_btn); return
        c.execute("UPDATE promo_codes SET uses=uses+1 WHERE code=?", (code,))
    add_balance(uid, promo["reward"])
    bot.send_message(uid,
        f"✅ <b>Промокод активирован!</b>\n+{fmt(promo['reward'])} {CUR}",
        parse_mode="HTML", reply_markup=home_btn)


# ══════════════════════════════════════════════
# 11. КЛИКЕР
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().strip()
                     in ["клик", "/клик", "click", "/click", "⚡"])
def cmd_click(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")
    u = get_user(uid)

    rem = CD_CLICK - (now() - u["last_click"])
    if rem > 0:
        bot.send_message(uid, f"⏱ <b>{cd_str(rem)}</b>", parse_mode="HTML")
        return

    power = u["click_power"]
    # Стрик бонус
    sb = 1.0
    if u["daily_streak"] >= 30: sb = 2.0
    elif u["daily_streak"] >= 7: sb = 1.5
    elif u["daily_streak"] >= 3: sb = 1.2
    # Premium бонус
    if u.get("premium_until", 0) > now(): sb *= 1.3

    earn = int(power * sb * random.uniform(0.85, 1.15))
    add_balance(uid, earn)
    add_xp(uid, 3)
    with db() as c:
        c.execute("UPDATE users SET last_click=?, total_clicks=total_clicks+1 WHERE id=?",
                  (now(), uid))
    check_milestones(uid)

    sb_str = f" 🔥×{sb:.1f}" if sb > 1 else ""
    bot.send_message(uid,
        f"⚡ +<b>{fmt(earn)} {CUR}</b>{sb_str}",
        parse_mode="HTML",
        reply_markup=kb([("⚡ Ещё", "act_click"), ("🏠 Меню", "home")]))


@bot.callback_query_handler(func=lambda c: c.data == "act_click")
def cb_click(call):
    uid = call.from_user.id
    u = get_user(uid)
    if not u:
        bot.answer_callback_query(call.id, "Используй /start"); return
    rem = CD_CLICK - (now() - u["last_click"])
    if rem > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(rem)}", show_alert=False); return
    earn = int(u["click_power"] * random.uniform(0.85, 1.15))
    add_balance(uid, earn)
    add_xp(uid, 3)
    with db() as c:
        c.execute("UPDATE users SET last_click=?, total_clicks=total_clicks+1 WHERE id=?",
                  (now(), uid))
    bot.answer_callback_query(call.id, f"⚡ +{fmt(earn)} {CUR}")


# ══════════════════════════════════════════════
# 12. РАБОТА
# ══════════════════════════════════════════════

JOBS = {
    "taxi":    {"name": "🚕 Такси",       "earn": (1_000, 2_000), "xp": 25},
    "cargo":   {"name": "🚚 Курьер",      "earn": (1_500, 3_000), "xp": 35},
    "trade":   {"name": "📊 Трейдер",     "earn": (800,  4_000),  "xp": 30, "risk": True},
    "code":    {"name": "💻 Программист", "earn": (2_500, 5_000), "xp": 50},
    "streamer":{"name": "🎮 Стример",     "earn": (500,  6_000),  "xp": 40, "wildcard": True},
}

WORK_EVENTS = [
    "🎊 Удачный день! Бонус x1.5!",
    "💼 Обычная смена.",
    "🌧 Плохой день, -30% доход.",
    "🎁 Клиент дал чаевые! +500",
    "🚨 Штраф инспекции. -300",
]

@bot.callback_query_handler(func=lambda c: c.data == "menu_work")
def cb_work_menu(call):
    uid = call.from_user.id
    u = get_user(uid)
    rem = CD_WORK - (now() - u["last_work"])

    if rem > 0:
        text = f"<b>⚒️ Работа</b>\n\n⏱ Доступна через: <b>{cd_str(rem)}</b>"
        _edit(call, text, kb([("🏠 Меню", "home")]))
        return

    lines = []
    for k, v in JOBS.items():
        lo, hi = v["earn"]
        tag = " 🎲" if v.get("wildcard") else (" ⚠️" if v.get("risk") else "")
        lines.append(f"{v['name']}{tag}  {fmt(lo)}–{fmt(hi)}")

    text = "<b>⚒️ Выбери профессию:</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines)
    rows = [[(f"{v['name']}", f"do_work_{k}")] for k, v in JOBS.items()]
    rows.append([("🏠 Меню", "home")])
    _edit(call, text, kb(*rows))


@bot.callback_query_handler(func=lambda c: c.data.startswith("do_work_"))
def cb_do_work(call):
    uid = call.from_user.id
    key = call.data[8:]
    job = JOBS.get(key)
    if not job:
        bot.answer_callback_query(call.id); return

    u = get_user(uid)
    rem = CD_WORK - (now() - u["last_work"])
    if rem > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(rem)}", show_alert=True); return

    lo, hi = job["earn"]
    earn = random.randint(lo, hi)

    # Wildcard — огромный разброс
    if job.get("wildcard"):
        earn = random.choice([random.randint(100, 1_000), random.randint(5_000, 10_000)])

    # Risk — потеря 30%
    event_text = ""
    if job.get("risk") and random.random() < 0.25:
        earn = -random.randint(lo // 3, hi // 3)
    else:
        ev_idx = random.randint(0, len(WORK_EVENTS) - 1)
        event = WORK_EVENTS[ev_idx]
        if "x1.5" in event:
            earn = int(earn * 1.5)
        elif "-30%" in event:
            earn = int(earn * 0.7)
        elif "+500" in event:
            earn += 500
        elif "-300" in event:
            earn -= 300
        event_text = f"\n<i>{event}</i>"

    add_balance(uid, earn)
    add_xp(uid, job["xp"])
    with db() as c:
        c.execute("UPDATE users SET last_work=?, total_works=total_works+1 WHERE id=?",
                  (now(), uid))
    check_milestones(uid)

    sign = "+" if earn >= 0 else ""
    icon = "✅" if earn >= 0 else "📉"
    text = (
        f"<b>{job['name']}</b>\n"
        f"{icon} <b>{sign}{fmt(earn)} {CUR}</b>{event_text}\n"
        f"⭐ +{job['xp']} XP\n"
        f"⏱ Следующая через <b>{cd_str(CD_WORK)}</b>"
    )
    _edit(call, text, kb([("⚒️ Работа", "menu_work"), ("🏠 Меню", "home")]))


# ══════════════════════════════════════════════
# 13. МАЙНИНГ
# ══════════════════════════════════════════════

@bot.message_handler(func=lambda m: m.text and m.text.lower().strip()
                     in ["майнинг", "/майнинг", "mine", "⛏"])
def cmd_mine(msg):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.first_name or "")
    u = get_user(uid)

    rem = CD_MINE - (now() - u["last_mine"])
    if rem > 0:
        bot.send_message(uid,
            f"<b>⛏️ Майнинг</b>\n\n⏱ Сбор через: <b>{cd_str(rem)}</b>",
            parse_mode="HTML")
        return

    power = mine_power(uid)
    earn  = int(power * random.uniform(0.9, 1.1))
    add_balance(uid, earn)
    add_xp(uid, 15)
    with db() as c:
        c.execute("UPDATE users SET last_mine=?, total_mines=total_mines+1 WHERE id=?",
                  (now(), uid))
    check_milestones(uid)

    # GPU кол-во
    with db() as c:
        c.execute("""SELECT SUM(inv.qty) FROM inventory inv
                     JOIN items it ON it.id=inv.item_id
                     WHERE inv.user_id=? AND it.effect LIKE 'mine+%'""", (uid,))
        gpus = int(c.fetchone()[0] or 0)

    bot.send_message(uid,
        f"<b>⛏️ Майнинг</b>\n\n"
        f"💰 +<b>{fmt(earn)} {CUR}</b>\n"
        f"🖥 GPU: {gpus}  Мощность: {power}/час\n"
        f"⏱ Сбор через <b>{cd_str(CD_MINE)}</b>",
        parse_mode="HTML",
        reply_markup=kb([("⛏️ Ещё раз", "act_mine"), ("🏠 Меню", "home")]))


@bot.callback_query_handler(func=lambda c: c.data == "act_mine")
def cb_mine(call):
    uid = call.from_user.id
    u = get_user(uid)
    rem = CD_MINE - (now() - u["last_mine"])
    if rem > 0:
        bot.answer_callback_query(call.id, f"⏱ {cd_str(rem)}"); return
    cmd_mine(call.message)
    call.message.from_user = call.from_user
    bot.answer_callback_query(call.id)


# ══════════════════════════════════════════════
# 14. БАНК
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_bank")
def cb_bank(call):
    uid = call.from_user.id
    u   = get_user(uid)

    # Начисляем проценты
    if u["bank"] > 0 and u["last_interest"] > 0:
        days = (now() - u["last_interest"]) / 86400
        if days >= 1:
            interest = int(u["bank"] * BANK_RATE * days)
            with db() as c:
                c.execute("UPDATE users SET bank=bank+?, last_interest=? WHERE id=?",
                          (interest, now(), uid))
            u = get_user(uid)
    elif u["bank"] > 0 and u["last_interest"] == 0:
        with db() as c:
            c.execute("UPDATE users SET last_interest=? WHERE id=?", (now(), uid))

    # Срочный вклад
    with db() as c:
        c.execute("SELECT * FROM term_deposits WHERE user_id=?", (uid,))
        term = c.fetchone()

    # Кредит
    with db() as c:
        c.execute("SELECT amount, due_at FROM loans WHERE user_id=?", (uid,))
        loan = c.fetchone()

    loan_str = "нет"
    if loan and loan["amount"] > 0:
        due = datetime.fromtimestamp(loan["due_at"]).strftime("%d.%m")
        loan_str = f"{fmt(loan['amount'])} до {due}"

    term_str = "нет"
    if term and term["amount"] > 0:
        locked = term["locked_until"] > now()
        unlock = datetime.fromtimestamp(term["locked_until"]).strftime("%d.%m")
        term_str = f"{fmt(term['amount'])} (до {unlock}{'🔒' if locked else '✅'})"

    text = (
        f"<b>🏦 Банк</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Вклад: <b>{fmt(u['bank'])} {CUR}</b>  {BANK_RATE*100:.0f}%/сут\n"
        f"Срочный: {term_str}  {TERM_RATE*100:.0f}%/сут\n"
        f"——\n"
        f"Кредит: {loan_str}\n"
        f"Макс. займ: {fmt(LOAN_MAX)}"
    )
    buttons = kb(
        [("📥 Внести",   "bank_deposit"),  ("📤 Снять",    "bank_withdraw")],
        [("⏳ Срочный",  "term_deposit"),   ("⏳ Забрать",  "term_withdraw")],
        [("💳 Кредит",  "bank_loan"),      ("💰 Погасить", "bank_repay")],
        [("🏠 Меню",    "home")],
    )
    _edit(call, text, buttons)


def _do_bank_op(uid: int, action: str, amount: int):
    u = get_user(uid)
    home_btn = kb([("🏦 Банк", "menu_bank"), ("🏠 Меню", "home")])

    if action == "bank_deposit":
        if u["balance"] < amount:
            bot.send_message(uid, "❌ Недостаточно наличных"); return
        with db() as c:
            c.execute("UPDATE users SET balance=balance-?, bank=bank+?, last_interest=? WHERE id=?",
                      (amount, amount, now(), uid))
        bot.send_message(uid, f"✅ Внесено: <b>{fmt(amount)} {CUR}</b>",
                         parse_mode="HTML", reply_markup=home_btn)

    elif action == "bank_withdraw":
        if u["bank"] < amount:
            bot.send_message(uid, "❌ Недостаточно в банке"); return
        with db() as c:
            c.execute("UPDATE users SET balance=balance+?, bank=bank-? WHERE id=?",
                      (amount, amount, uid))
        bot.send_message(uid, f"✅ Снято: <b>{fmt(amount)} {CUR}</b>",
                         parse_mode="HTML", reply_markup=home_btn)

    elif action == "bank_loan":
        with db() as c:
            c.execute("SELECT amount FROM loans WHERE user_id=?", (uid,))
            ex = c.fetchone()
        if ex and ex["amount"] > 0:
            bot.send_message(uid, "❌ Уже есть кредит"); return
        if amount > LOAN_MAX:
            bot.send_message(uid, f"❌ Макс: {fmt(LOAN_MAX)}"); return
        if u["bank"] < 1_000:
            bot.send_message(uid, "❌ Нужно ≥1 000 на вкладе для кредита"); return
        debt = int(amount * (1 + LOAN_RATE))
        due  = now() + 7 * 86400
        add_balance(uid, amount)
        with db() as c:
            c.execute("INSERT OR REPLACE INTO loans (user_id,amount,due_at,taken_at) VALUES (?,?,?,?)",
                      (uid, debt, due, now()))
        bot.send_message(uid,
            f"✅ Кредит: <b>{fmt(amount)} {CUR}</b>\n"
            f"Вернуть <b>{fmt(debt)}</b> до {datetime.fromtimestamp(due).strftime('%d.%m.%Y')}",
            parse_mode="HTML", reply_markup=home_btn)

    elif action == "bank_repay":
        with db() as c:
            c.execute("SELECT amount FROM loans WHERE user_id=?", (uid,))
            loan = c.fetchone()
        if not loan or loan["amount"] == 0:
            bot.send_message(uid, "❌ Нет кредита"); return
        pay = min(amount, loan["amount"])
        if u["balance"] < pay:
            bot.send_message(uid, "❌ Недостаточно"); return
        add_balance(uid, -pay)
        new_debt = loan["amount"] - pay
        with db() as c:
            if new_debt <= 0:
                c.execute("DELETE FROM loans WHERE user_id=?", (uid,))
                extra = "\n🎉 Кредит погашен!"
            else:
                c.execute("UPDATE loans SET amount=? WHERE user_id=?", (new_debt, uid))
                extra = f"\nОстаток: {fmt(new_debt)}"
        bot.send_message(uid, f"✅ Погашено: <b>{fmt(pay)} {CUR}</b>{extra}",
                         parse_mode="HTML", reply_markup=home_btn)

    elif action == "term_deposit":
        if u["balance"] < amount:
            bot.send_message(uid, "❌ Недостаточно наличных"); return
        with db() as c:
            c.execute("SELECT amount FROM term_deposits WHERE user_id=?", (uid,))
            ex = c.fetchone()
        if ex and ex["amount"] > 0:
            bot.send_message(uid, "❌ Срочный вклад уже есть. Сначала заберите."); return
        add_balance(uid, -amount)
        locked = now() + 7 * 86400
        with db() as c:
            c.execute("INSERT OR REPLACE INTO term_deposits (user_id,amount,rate,locked_until,started_at) "
                      "VALUES (?,?,?,?,?)", (uid, amount, TERM_RATE, locked, now()))
        bot.send_message(uid,
            f"✅ Срочный вклад: <b>{fmt(amount)} {CUR}</b>\n"
            f"Доступно: {datetime.fromtimestamp(locked).strftime('%d.%m.%Y')}",
            parse_mode="HTML", reply_markup=home_btn)

    elif action == "term_withdraw":
        with db() as c:
            c.execute("SELECT * FROM term_deposits WHERE user_id=?", (uid,))
            td = c.fetchone()
        if not td or td["amount"] == 0:
            bot.send_message(uid, "❌ Нет срочного вклада"); return
        days = (now() - td["started_at"]) / 86400
        earned = int(td["amount"] * td["rate"] * days)
        total  = td["amount"] + earned
        penalty = 0
        if td["locked_until"] > now():
            penalty = total // 2
            total   = total - penalty
        with db() as c:
            c.execute("DELETE FROM term_deposits WHERE user_id=?", (uid,))
        add_balance(uid, total)
        pen_str = f"\n⚠️ Штраф: -{fmt(penalty)}" if penalty else ""
        bot.send_message(uid,
            f"✅ Снято: <b>{fmt(total)} {CUR}</b>\n"
            f"Проценты: +{fmt(earned)}{pen_str}",
            parse_mode="HTML", reply_markup=home_btn)


# ══════════════════════════════════════════════
# 15. БИРЖА
# ══════════════════════════════════════════════

def _get_stock() -> dict:
    with db() as c:
        c.execute("SELECT * FROM stocks WHERE ticker=?", (TICKER,))
        return dict(c.fetchone())

def _stock_history(n=10) -> list:
    with db() as c:
        c.execute("SELECT price, ts FROM stock_history WHERE ticker=? ORDER BY ts DESC LIMIT ?",
                  (TICKER, n))
        return list(reversed(c.fetchall()))

def _market_impact(qty: int, direction: str):
    with db() as c:
        c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
        row = c.fetchone()
        if not row: return
        p = row["price"]
        delta = p * 0.001 * qty
        new_p = max(100, int(p + (delta if direction == "buy" else -delta)))
        c.execute("UPDATE stocks SET prev_price=price, price=?, updated_at=? WHERE ticker=?",
                  (new_p, now(), TICKER))
        c.execute("INSERT INTO stock_history (ticker,price,ts) VALUES (?,?,?)",
                  (TICKER, new_p, now()))


@bot.callback_query_handler(func=lambda c: c.data == "menu_stock")
def cb_stock(call):
    uid = call.from_user.id
    st  = _get_stock()
    chg = (st["price"] - st["prev_price"]) / st["prev_price"] * 100

    with db() as c:
        c.execute("SELECT shares, avg_buy FROM portfolios WHERE user_id=? AND ticker=?",
                  (uid, TICKER))
        port = c.fetchone()

    hist = _stock_history(8)
    mini = ""
    if len(hist) >= 2:
        prices = [r["price"] for r in hist]
        lo, hi = min(prices), max(prices)
        for p in prices:
            if hi == lo:       mini += "━"
            elif p >= hi*0.85: mini += "▲"
            elif p <= lo*1.15: mini += "▼"
            else:              mini += "─"

    arrow = "📈" if chg >= 0 else "📉"
    port_str = ""
    if port and port["shares"] > 0:
        pnl = (st["price"] - port["avg_buy"]) * port["shares"]
        pnl_str = f"+{fmt(pnl)}" if pnl >= 0 else fmt(pnl)
        port_str = (
            f"\n——\n"
            f"📂 {port['shares']} акций  avg {fmt(port['avg_buy'])}\n"
            f"P&L: <b>{pnl_str} {CUR}</b>"
        )

    text = (
        f"<b>📈 Биржа — {TICKER}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Цена: <b>{fmt(st['price'])} {CUR}</b>  {arrow} {chg:+.1f}%\n"
        f"<code>{mini}</code>"
        f"{port_str}"
    )
    _edit(call, text, kb(
        [("🛒 Купить", "stock_buy"), ("💰 Продать", "stock_sell")],
        [("📊 История", "stock_hist"), ("🔄 Обновить", "menu_stock")],
        [("🏠 Меню", "home")],
    ))


@bot.callback_query_handler(func=lambda c: c.data == "stock_hist")
def cb_stock_hist(call):
    uid = call.from_user.id
    hist = _stock_history(10)
    if not hist:
        bot.answer_callback_query(call.id, "История пуста"); return
    lines = []
    for i, row in enumerate(hist):
        dt = datetime.fromtimestamp(row["ts"]).strftime("%d.%m %H:%M")
        p  = row["price"]
        if i > 0:
            prev = hist[i-1]["price"]
            chg  = (p - prev) / prev * 100
            icon = "🟢" if p >= prev else "🔴"
            lines.append(f"{icon} {dt}  <b>{fmt(p)}</b>  {chg:+.1f}%")
        else:
            lines.append(f"⬜ {dt}  <b>{fmt(p)}</b>")
    text = f"<b>📊 История {TICKER}</b>\n\n" + "\n".join(lines)
    _edit(call, text, kb([("📈 Биржа", "menu_stock"), ("🏠 Меню", "home")]))


def _do_stock_op(uid: int, action: str, qty: int):
    st = _get_stock()
    u  = get_user(uid)
    home_btn = kb([("📈 Биржа", "menu_stock"), ("🏠 Меню", "home")])

    if action == "stock_buy":
        fee   = int(st["price"] * qty * 0.02)
        cost  = st["price"] * qty + fee
        if u["balance"] < cost:
            bot.send_message(uid, f"❌ Нужно {fmt(cost)} (комиссия {fmt(fee)})"); return
        add_balance(uid, -cost)
        with db() as c:
            c.execute("""INSERT INTO portfolios (user_id,ticker,shares,avg_buy) VALUES (?,?,?,?)
                         ON CONFLICT(user_id,ticker) DO UPDATE SET
                         avg_buy=(avg_buy*shares+?*?)/(shares+?),
                         shares=shares+?""",
                      (uid, TICKER, qty, st["price"], st["price"], qty, qty, qty))
        _market_impact(qty, "buy")
        grant_achievement(uid, "investor")
        bot.send_message(uid,
            f"✅ Куплено <b>{qty} акций {TICKER}</b>  @{fmt(st['price'])}\n"
            f"Комиссия: {fmt(fee)}  Итого: {fmt(cost)} {CUR}",
            parse_mode="HTML", reply_markup=home_btn)

    elif action == "stock_sell":
        with db() as c:
            c.execute("SELECT shares, avg_buy FROM portfolios WHERE user_id=? AND ticker=?",
                      (uid, TICKER))
            port = c.fetchone()
        if not port or port["shares"] < qty:
            bot.send_message(uid, "❌ Недостаточно акций"); return
        fee  = int(st["price"] * qty * 0.02)
        gain = st["price"] * qty - fee
        pnl  = (st["price"] - port["avg_buy"]) * qty
        add_balance(uid, gain)
        with db() as c:
            new_sh = port["shares"] - qty
            if new_sh == 0:
                c.execute("DELETE FROM portfolios WHERE user_id=? AND ticker=?", (uid, TICKER))
            else:
                c.execute("UPDATE portfolios SET shares=? WHERE user_id=? AND ticker=?",
                          (new_sh, uid, TICKER))
        _market_impact(qty, "sell")
        pnl_str = f"+{fmt(pnl)}" if pnl >= 0 else fmt(pnl)
        bot.send_message(uid,
            f"✅ Продано <b>{qty} акций</b>\n"
            f"Получено: <b>{fmt(gain)} {CUR}</b>  P&L: <b>{pnl_str}</b>",
            parse_mode="HTML", reply_markup=home_btn)


def _stock_scheduler():
    print(f"[stocks] старт ({STOCK_TICK//60}мин)")
    while True:
        time.sleep(STOCK_TICK)
        try:
            with db() as c:
                c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
                row = c.fetchone()
                if not row: continue
                old = row["price"]
                drift = (STOCK_START - old) * 0.01
                vol   = old * STOCK_VOL
                new   = max(100, int(old + drift + random.gauss(0, vol)))
                c.execute("UPDATE stocks SET prev_price=price, price=?, updated_at=? WHERE ticker=?",
                          (new, now(), TICKER))
                c.execute("INSERT INTO stock_history (ticker,price,ts) VALUES (?,?,?)",
                          (TICKER, new, now()))
            chg = (new - old) / old * 100
            if abs(chg) >= 3:
                arrow = "📈" if new > old else "📉"
                send_alert(f"{arrow} {TICKER}: {fmt(old)} → <b>{fmt(new)}</b> ({chg:+.1f}%)")
        except Exception as e:
            print(f"[stocks] err: {e}")


# ══════════════════════════════════════════════
# 16. ИГРЫ — МЕНЮ
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_games")
def cb_games_menu(call):
    uid = call.from_user.id
    text = (
        "<b>🎰 Игры</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Выбери игру:"
    )
    _edit(call, text, kb(
        [("🎲 Кубик",   "game_dice"),   ("🎰 Слоты",    "game_slots")],
        [("🎡 Рулетка", "game_roul"),   ("⚡ Краш",     "game_crash")],
        [("💣 Мины",    "game_mines"),  ("🏯 Башня",    "game_tower")],
        [("🏆 Лотерея", "game_lotto"),  ("📜 Статы",    "game_stats")],
        [("🏠 Меню", "home")],
    ))


@bot.callback_query_handler(func=lambda c: c.data == "game_stats")
def cb_game_stats(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("SELECT game, COUNT(*) as cnt, SUM(result) as profit "
                  "FROM game_log WHERE user_id=? GROUP BY game", (uid,))
        rows = c.fetchall()
    if not rows:
        _edit(call, "📜 Нет игр.", kb([("🎰 Игры", "menu_games"), ("🏠 Меню", "home")])); return
    lines = []
    for r in rows:
        profit = r["profit"] or 0
        sign   = "+" if profit >= 0 else ""
        lines.append(f"• {r['game']}: {r['cnt']} игр  {sign}{fmt(profit)}")
    _edit(call, "<b>📜 Статистика игр</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
          kb([("🎰 Игры", "menu_games"), ("🏠 Меню", "home")]))


# ── Кубик ──

def _game_dice(uid: int, bet: int):
    add_balance(uid, -bet)
    sent = bot.send_dice(uid, emoji="🎲")
    time.sleep(2.8)
    val = sent.dice.value
    if val >= 4:
        win = int(bet * 2)
        add_balance(uid, win)
        result = win - bet
        txt = f"🎲 Выпало <b>{val}</b> — победа! +{fmt(result)} {CUR}"
    else:
        result = -bet
        txt = f"🎲 Выпало <b>{val}</b> — проигрыш. -{fmt(bet)} {CUR}"
    log_game(uid, "Кубик", bet, result)
    check_milestones(uid)
    bot.send_message(uid, txt, parse_mode="HTML",
                     reply_markup=kb([("🎲 Ещё", "game_dice"),
                                      ("🎰 Игры", "menu_games"), ("🏠 Меню", "home")]))


# ── Слоты ──

SLOT_SYM  = ["🍒","🍋","🍊","🍇","⭐","💎"]
SLOT_PAY  = {"💎💎💎": 12, "⭐⭐⭐": 8, "🍇🍇🍇": 5,
             "🍊🍊🍊": 4,  "🍋🍋🍋": 3, "🍒🍒🍒": 2}

def _game_slots(uid: int, bet: int):
    vip   = has_item(uid, "slots_vip") > 0
    reels = [random.choice(SLOT_SYM) for _ in range(3)]
    combo = "".join(reels)
    mult  = SLOT_PAY.get(combo, 0)
    if vip and mult > 0:
        mult = int(mult * 1.5)

    add_balance(uid, -bet)
    if mult:
        win = bet * mult
        add_balance(uid, win)
        result = win - bet
        txt = f"🎰 {combo}\n🎉 x{mult}!  +{fmt(result)} {CUR}"
    else:
        result = -bet
        txt = f"🎰 {combo}\n😔  -{fmt(bet)} {CUR}"
    log_game(uid, "Слоты", bet, result)
    check_milestones(uid)
    bot.send_message(uid, txt, parse_mode="HTML",
                     reply_markup=kb([("🎰 Ещё", "game_slots"),
                                      ("🎰 Игры", "menu_games"), ("🏠 Меню", "home")]))


# ── Рулетка ──

COLOR_MAP = {
    "красное": "red","красный":"red","red":"red","r":"red",
    "чёрное":"black","черное":"black","black":"black","b":"black",
    "зеро":"zero","0":"zero","zero":"zero",
}
COLOR_MULT = {"red": 2, "black": 2, "zero": 14}
COLOR_ICON = {"red": "🔴","black": "⬛","zero": "🟢"}

def _game_roulette(uid: int, color_input: str, bet: int):
    color = COLOR_MAP.get(color_input.lower())
    if not color:
        bot.send_message(uid, "❌ Цвет: красное / чёрное / зеро"); return

    num    = random.randint(0, 36)
    actual = "zero" if num == 0 else ("red" if num % 2 == 1 else "black")
    add_balance(uid, -bet)

    if actual == color:
        win    = bet * COLOR_MULT[color]
        result = win - bet
        add_balance(uid, win)
        txt = f"{COLOR_ICON[actual]} Выпало {num} — победа! +{fmt(result)} {CUR}"
    else:
        result = -bet
        txt = f"{COLOR_ICON[actual]} Выпало {num} — проигрыш. -{fmt(bet)} {CUR}"

    log_game(uid, "Рулетка", bet, result)
    check_milestones(uid)
    bot.send_message(uid, txt, parse_mode="HTML",
                     reply_markup=kb([("🎡 Ещё", "game_roul"),
                                      ("🎰 Игры", "menu_games"), ("🏠 Меню", "home")]))


# ── Краш ──

_crashes: dict[int, dict] = {}

def _game_crash_start(uid: int, bet: int):
    crash_at = max(1.01, round(random.expovariate(0.9) + 1.0, 2))
    shield   = has_item(uid, "crash_shield")

    add_balance(uid, -bet)
    _crashes[uid] = {"bet": bet, "crash": crash_at, "shield": shield, "ts": now()}

    # Убрать shield из инвентаря при использовании
    if shield:
        with db() as c:
            c.execute("""UPDATE inventory SET qty=qty-1
                         WHERE user_id=? AND item_id=(
                         SELECT id FROM items WHERE effect='crash_shield' LIMIT 1)""", (uid,))

    bot.send_message(uid,
        f"⚡ <b>Краш</b>  Ставка: {fmt(bet)}\n"
        f"{'🛡 Страховка активна!' if shield else ''}\n"
        f"Забери до краша:",
        parse_mode="HTML",
        reply_markup=kb(
            [(f"💰 x1.5", f"crash_co_1.5_{uid}"),
             (f"💰 x2.0", f"crash_co_2.0_{uid}"),
             (f"💰 x3.0", f"crash_co_3.0_{uid}"),
             (f"💰 x5.0", f"crash_co_5.0_{uid}")],
        ))


@bot.callback_query_handler(func=lambda c: c.data.startswith("crash_co_"))
def cb_crash_cashout(call):
    uid   = call.from_user.id
    parts = call.data.split("_")
    mult  = float(parts[2])
    g_uid = int(parts[3])

    if g_uid != uid:
        bot.answer_callback_query(call.id, "Не ваша игра"); return

    game = _crashes.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "Игра завершена"); return

    bet = game["bet"]
    if mult <= game["crash"]:
        win    = int(bet * mult)
        result = win - bet
        add_balance(uid, win)
        txt = f"✅ Забрал x{mult}!  +{fmt(result)} {CUR}"
    else:
        if game.get("shield"):
            # Страховка — возвращаем ставку
            add_balance(uid, bet)
            result = 0
            txt = f"🛡 Краш x{game['crash']}! Страховка спасла ставку."
        else:
            result = -bet
            txt = f"💥 Краш на x{game['crash']}!  -{fmt(bet)} {CUR}"

    log_game(uid, "Краш", bet, result)
    check_milestones(uid)
    try:
        bot.edit_message_text(txt, uid, call.message.message_id,
                              reply_markup=kb([("⚡ Ещё", "game_crash"),
                                               ("🎰 Игры", "menu_games"), ("🏠 Меню", "home")]),
                              parse_mode="HTML")
    except Exception:
        bot.send_message(uid, txt, parse_mode="HTML")
    bot.answer_callback_query(call.id)


# ── Мины ──

_mines: dict[int, dict] = {}

def _game_mines_start(uid: int, bet: int):
    mines = random.sample(range(9), 3)
    _mines[uid] = {"bet": bet, "mines": mines, "opened": [], "active": True}
    add_balance(uid, -bet)
    _send_mines_board(uid, uid)

def _send_mines_board(uid: int, chat_id: int, msg_id: int = None):
    game   = _mines.get(uid)
    if not game: return
    opened = game["opened"]
    mult   = 1.0 + len(opened) * 0.6

    board_rows = []
    row = []
    for i in range(9):
        if i in opened:
            row.append(("💎", f"mn_nop"))
        else:
            row.append(("⬜", f"mn_{uid}_{i}"))
        if len(row) == 3:
            board_rows.append(row); row = []
    board_rows.append([("💰 Забрать", f"mn_co_{uid}"), ("🚪 Выйти", f"mn_exit_{uid}")])

    text = (
        f"<b>💣 Мины</b>  Ставка: {fmt(game['bet'])}\n"
        f"Открыто: {len(opened)}/6   x{mult:.1f}\n"
        f"Потенциал: <b>{fmt(int(game['bet']*mult))}</b>"
    )
    if msg_id:
        try:
            bot.edit_message_text(text, chat_id, msg_id,
                                  reply_markup=kb(*board_rows), parse_mode="HTML"); return
        except Exception: pass
    bot.send_message(chat_id, text, reply_markup=kb(*board_rows), parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: re.match(r"^mn_\d+_\d+$", c.data))
def cb_mine_open(call):
    uid   = call.from_user.id
    parts = call.data.split("_")
    g_uid = int(parts[1])
    cell  = int(parts[2])

    if g_uid != uid:
        bot.answer_callback_query(call.id); return

    game = _mines.get(uid)
    if not game or not game["active"]:
        bot.answer_callback_query(call.id, "Игра завершена"); return

    if cell in game["mines"]:
        _mines.pop(uid, None)
        log_game(uid, "Мины", game["bet"], -game["bet"])
        check_milestones(uid)
        bot.answer_callback_query(call.id, "💥 Мина!", show_alert=False)
        try:
            bot.edit_message_text(
                f"💥 <b>БУМ!</b>  -{fmt(game['bet'])} {CUR}",
                uid, call.message.message_id,
                reply_markup=kb([("💣 Ещё", "game_mines"), ("🎰 Игры", "menu_games")]),
                parse_mode="HTML")
        except Exception: pass
        return

    game["opened"].append(cell)
    if len(game["opened"]) >= 6:
        mult = 1.0 + 6 * 0.6
        win  = int(game["bet"] * mult)
        add_balance(uid, win)
        _mines.pop(uid, None)
        log_game(uid, "Мины", game["bet"], win - game["bet"])
        bot.answer_callback_query(call.id, f"🎉 +{fmt(win)}")
        bot.send_message(uid, f"🎉 <b>Все клетки!</b> +{fmt(win)} {CUR}",
                         parse_mode="HTML",
                         reply_markup=kb([("💣 Ещё", "game_mines"), ("🎰 Игры", "menu_games")]))
        return

    bot.answer_callback_query(call.id, "💎")
    _send_mines_board(uid, call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda c: re.match(r"^mn_co_\d+$", c.data))
def cb_mine_cashout(call):
    uid   = call.from_user.id
    g_uid = int(call.data[6:])
    if g_uid != uid:
        bot.answer_callback_query(call.id); return
    game = _mines.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "Игра завершена"); return
    mult = 1.0 + len(game["opened"]) * 0.6
    win  = int(game["bet"] * mult)
    add_balance(uid, win)
    result = win - game["bet"]
    log_game(uid, "Мины", game["bet"], result)
    check_milestones(uid)
    try:
        bot.edit_message_text(
            f"💰 Забрал x{mult:.1f}  +{fmt(result)} {CUR}",
            uid, call.message.message_id,
            reply_markup=kb([("💣 Ещё", "game_mines"), ("🎰 Игры", "menu_games"), ("🏠 Меню", "home")]),
            parse_mode="HTML")
    except Exception: pass
    bot.answer_callback_query(call.id, f"✅ +{fmt(result)}")


@bot.callback_query_handler(func=lambda c: re.match(r"^mn_exit_\d+$", c.data))
def cb_mine_exit(call):
    uid   = call.from_user.id
    g_uid = int(call.data[8:])
    if g_uid != uid:
        bot.answer_callback_query(call.id); return
    game = _mines.pop(uid, None)
    if game:
        add_balance(uid, game["bet"])  # возврат ставки при выходе без открытий
        log_game(uid, "Мины", game["bet"], 0)
    bot.answer_callback_query(call.id, "Выход")
    bot.send_message(uid, "🚪 Вы вышли из игры. Ставка возвращена.",
                     reply_markup=kb([("💣 Мины", "game_mines"), ("🏠 Меню", "home")]))


@bot.callback_query_handler(func=lambda c: c.data == "mn_nop")
def cb_mine_nop(call):
    bot.answer_callback_query(call.id)


# ── Башня ──

_towers: dict[int, dict] = {}

def _game_tower_start(uid: int, bet: int):
    add_balance(uid, -bet)
    _towers[uid] = {"bet": bet, "level": 0, "active": True}
    _send_tower(uid, uid)

def _tower_mult(level: int) -> float:
    return round(TOWER_MULT_BASE ** level, 2)

def _send_tower(uid: int, chat_id: int, msg_id: int = None):
    game = _towers.get(uid)
    if not game: return
    lvl  = game["level"]
    mult = _tower_mult(lvl)
    win  = int(game["bet"] * mult)

    # На каждом уровне 3 ячейки, 1 — бомба
    rows = [
        [(f"⬜ {i+1}", f"tw_{uid}_{lvl}_{i}") for i in range(3)],
        [("💰 Забрать", f"tw_co_{uid}"), ("🚪 Выйти", f"tw_exit_{uid}")],
    ]
    text = (
        f"<b>🏯 Башня</b>  Ставка: {fmt(game['bet'])}\n"
        f"Уровень: <b>{lvl+1}/{TOWER_LEVELS}</b>  x{mult}\n"
        f"Потенциал: <b>{fmt(win)}</b>"
    )
    if msg_id:
        try:
            bot.edit_message_text(text, chat_id, msg_id,
                                  reply_markup=kb(*rows), parse_mode="HTML"); return
        except Exception: pass
    bot.send_message(chat_id, text, reply_markup=kb(*rows), parse_mode="HTML")


@bot.callback_query_handler(func=lambda c: re.match(r"^tw_\d+_\d+_\d+$", c.data))
def cb_tower_cell(call):
    uid   = call.from_user.id
    parts = call.data.split("_")
    g_uid = int(parts[1])
    lvl   = int(parts[2])
    cell  = int(parts[3])

    if g_uid != uid:
        bot.answer_callback_query(call.id); return

    game = _towers.get(uid)
    if not game or game["level"] != lvl:
        bot.answer_callback_query(call.id, "Устарело"); return

    # Вероятность мины растёт с уровнем
    bomb_prob = 0.2 + lvl * 0.05
    is_bomb   = random.random() < bomb_prob

    if is_bomb:
        _towers.pop(uid, None)
        log_game(uid, "Башня", game["bet"], -game["bet"])
        check_milestones(uid)
        bot.answer_callback_query(call.id, "💥", show_alert=False)
        try:
            bot.edit_message_text(
                f"💥 <b>ВЗРЫВ</b> на уровне {lvl+1}!  -{fmt(game['bet'])} {CUR}",
                uid, call.message.message_id,
                reply_markup=kb([("🏯 Ещё", "game_tower"), ("🎰 Игры", "menu_games")]),
                parse_mode="HTML")
        except Exception: pass
        return

    game["level"] += 1
    if game["level"] >= TOWER_LEVELS:
        # Победа — вершина
        mult = _tower_mult(TOWER_LEVELS)
        win  = int(game["bet"] * mult)
        add_balance(uid, win)
        _towers.pop(uid, None)
        log_game(uid, "Башня", game["bet"], win - game["bet"])
        bot.answer_callback_query(call.id, f"🎉 Вершина! +{fmt(win)}")
        bot.send_message(uid,
            f"🏆 <b>Вершина!</b>  x{mult}  +{fmt(win-game['bet'])} {CUR}",
            parse_mode="HTML",
            reply_markup=kb([("🏯 Ещё", "game_tower"), ("🎰 Игры", "menu_games")]))
        return

    bot.answer_callback_query(call.id, f"✅ Уровень {game['level']}")
    _send_tower(uid, call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda c: re.match(r"^tw_co_\d+$", c.data))
def cb_tower_cashout(call):
    uid   = call.from_user.id
    g_uid = int(call.data[6:])
    if g_uid != uid:
        bot.answer_callback_query(call.id); return
    game = _towers.pop(uid, None)
    if not game:
        bot.answer_callback_query(call.id, "Завершено"); return
    mult = _tower_mult(game["level"])
    win  = int(game["bet"] * mult)
    add_balance(uid, win)
    result = win - game["bet"]
    log_game(uid, "Башня", game["bet"], result)
    check_milestones(uid)
    try:
        bot.edit_message_text(
            f"💰 Забрал с уровня {game['level']}  x{mult}  +{fmt(result)} {CUR}",
            uid, call.message.message_id,
            reply_markup=kb([("🏯 Ещё", "game_tower"), ("🎰 Игры", "menu_games"), ("🏠 Меню", "home")]),
            parse_mode="HTML")
    except Exception: pass
    bot.answer_callback_query(call.id, f"✅ +{fmt(result)}")


@bot.callback_query_handler(func=lambda c: re.match(r"^tw_exit_\d+$", c.data))
def cb_tower_exit(call):
    uid   = call.from_user.id
    g_uid = int(call.data[8:])
    if g_uid != uid:
        bot.answer_callback_query(call.id); return
    game = _towers.pop(uid, None)
    if game:
        add_balance(uid, game["bet"])
    bot.answer_callback_query(call.id, "Выход")
    bot.send_message(uid, "🚪 Вышли. Ставка возвращена.",
                     reply_markup=kb([("🏯 Башня", "game_tower"), ("🏠 Меню", "home")]))


# ── Лотерея ──

@bot.callback_query_handler(func=lambda c: c.data == "game_lotto")
def cb_lotto(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
        lotto = c.fetchone()
        c.execute("SELECT tickets FROM lottery_tickets WHERE user_id=?", (uid,))
        my = c.fetchone()

    jackpot   = lotto["jackpot"] if lotto else 0
    draw_at   = lotto["draw_at"] if lotto else 0
    my_tickets = my["tickets"] if my else 0
    draw_str   = datetime.fromtimestamp(draw_at).strftime("%d.%m %H:%M") if draw_at > now() else "скоро"

    text = (
        f"<b>🏆 Лотерея</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Джекпот: <b>{fmt(jackpot)} {CUR}</b>\n"
        f"Розыгрыш: <b>{draw_str}</b>\n"
        f"Билет: {fmt(TICKET_PRICE)}\n"
        f"Ваши билеты: <b>{my_tickets}</b>"
    )
    _edit(call, text, kb(
        [("🎟 x1 билет", "lotto_1"), ("🎟 x5 билетов", "lotto_5")],
        [("🎟 x10 билетов", "lotto_10"), ("🔄 Обновить", "game_lotto")],
        [("🏠 Меню", "home")],
    ))


@bot.callback_query_handler(func=lambda c: c.data in ["lotto_1","lotto_5","lotto_10"])
def cb_lotto_buy(call):
    uid = call.from_user.id
    qty_map = {"lotto_1": 1, "lotto_5": 5, "lotto_10": 10}
    qty  = qty_map[call.data]
    cost = TICKET_PRICE * qty
    u    = get_user(uid)
    if u["balance"] < cost:
        bot.answer_callback_query(call.id, f"❌ Нужно {fmt(cost)}", show_alert=True); return
    add_balance(uid, -cost)
    with db() as c:
        c.execute("INSERT INTO lottery_tickets (user_id,tickets) VALUES (?,?) "
                  "ON CONFLICT(user_id) DO UPDATE SET tickets=tickets+?", (uid, qty, qty))
        c.execute("UPDATE lottery SET jackpot=jackpot+? WHERE id=1", (cost,))
    bot.answer_callback_query(call.id, f"✅ Куплено {qty} билетов!")
    cb_lotto(call)


def _lottery_scheduler():
    while True:
        time.sleep(3600)
        try:
            with db() as c:
                c.execute("SELECT jackpot, draw_at FROM lottery WHERE id=1")
                lotto = c.fetchone()
            if not lotto or lotto["draw_at"] > now(): continue
            with db() as c:
                c.execute("SELECT user_id, tickets FROM lottery_tickets WHERE tickets > 0")
                participants = c.fetchall()
            if not participants:
                with db() as c:
                    c.execute("UPDATE lottery SET draw_at=? WHERE id=1", (now() + 86400,))
                continue
            pool   = [p["user_id"] for p in participants for _ in range(p["tickets"])]
            winner = random.choice(pool)
            jackpot = lotto["jackpot"]
            add_balance(winner, jackpot)
            with db() as c:
                c.execute("UPDATE lottery SET jackpot=0, draw_at=? WHERE id=1",
                          (now() + 86400,))
                c.execute("DELETE FROM lottery_tickets")
                c.execute("SELECT name FROM users WHERE id=?", (winner,))
                name = (c.fetchone() or {}).get("name", f"#{winner}")
            send_alert(f"🏆 <b>Лотерея!</b> {name} выиграл <b>{fmt(jackpot)} {CUR}</b>")
            try:
                bot.send_message(winner,
                    f"🎉 <b>Вы выиграли лотерею!</b>\n+{fmt(jackpot)} {CUR}",
                    parse_mode="HTML")
            except Exception: pass
        except Exception as e:
            print(f"[lottery] err: {e}")


# ══════════════════════════════════════════════
# 17. МАГАЗИН
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_shop")
def cb_shop(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("SELECT id,emoji,name,desc,price,supply,sold FROM items WHERE active=1 ORDER BY price")
        items = c.fetchall()

    if not items:
        _edit(call, "🛍️ <b>Магазин пуст</b>", kb([("🏠 Меню","home")])); return

    lines = []
    rows  = []
    for it in items:
        left = f" ({it['supply']-it['sold']}шт.)" if it["supply"] != -1 else ""
        lines.append(f"{it['emoji']} <b>{it['name']}</b> — {fmt(it['price'])} {CUR}{left}\n"
                     f"   <i>{it['desc']}</i>")
        rows.append([(f"{it['emoji']} Купить", f"buy_item_{it['id']}")])

    rows.append([("🎒 Инвентарь", "menu_inv"), ("🏠 Меню", "home")])
    _edit(call, "🛍️ <b>Магазин</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
          kb(*rows))


@bot.callback_query_handler(func=lambda c: c.data.startswith("buy_item_"))
def cb_buy_item(call):
    uid     = call.from_user.id
    item_id = int(call.data[9:])
    with db() as c:
        c.execute("SELECT * FROM items WHERE id=? AND active=1", (item_id,))
        it = c.fetchone()
    if not it:
        bot.answer_callback_query(call.id, "❌ Недоступно", show_alert=True); return
    if it["supply"] != -1 and it["sold"] >= it["supply"]:
        bot.answer_callback_query(call.id, "❌ Распродано", show_alert=True); return
    u = get_user(uid)
    if u["balance"] < it["price"]:
        bot.answer_callback_query(call.id,
            f"❌ Нужно {fmt(it['price'])}, есть {fmt(u['balance'])}", show_alert=True); return
    add_balance(uid, -it["price"])
    with db() as c:
        c.execute("INSERT INTO inventory (user_id,item_id,qty) VALUES (?,?,1) "
                  "ON CONFLICT(user_id,item_id) DO UPDATE SET qty=qty+1", (uid, item_id))
        if it["supply"] != -1:
            c.execute("UPDATE items SET sold=sold+1 WHERE id=?", (item_id,))

    # Применяем бустер клика мгновенно
    if "click_boost" in (it["effect"] or ""):
        try:
            duration = int(it["effect"].split("_")[-1])
            with db() as c:
                c.execute("UPDATE users SET click_power=click_power*2 WHERE id=?", (uid,))
            def _reset():
                time.sleep(duration)
                with db() as c:
                    c.execute("UPDATE users SET click_power=click_power//2 WHERE id=?", (uid,))
            threading.Thread(target=_reset, daemon=True).start()
        except Exception: pass

    bot.answer_callback_query(call.id, f"✅ {it['emoji']} {it['name']} куплено!")


@bot.callback_query_handler(func=lambda c: c.data == "menu_inv")
def cb_inventory(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("""SELECT it.emoji, it.name, it.desc, inv.qty
                     FROM inventory inv JOIN items it ON it.id=inv.item_id
                     WHERE inv.user_id=? AND inv.qty>0""", (uid,))
        inv = c.fetchall()
    if not inv:
        _edit(call, "🎒 <b>Инвентарь пуст</b>",
              kb([("🛍️ Магазин", "menu_shop"), ("🏠 Меню", "home")])); return
    lines = [f"{r['emoji']} <b>{r['name']}</b> ×{r['qty']}\n   <i>{r['desc']}</i>" for r in inv]
    _edit(call, "🎒 <b>Инвентарь</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
          kb([("🛍️ Магазин", "menu_shop"), ("🏠 Меню", "home")]))


# ══════════════════════════════════════════════
# 18. ТОП
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_top")
def cb_top(call):
    _edit(call, "<b>🏆 Рейтинги</b>", kb(
        [("💎 Баланс", "top_balance"), ("⭐ XP",    "top_xp")],
        [("📈 Акции",  "top_stocks"),  ("🏠 Меню", "home")],
    ))


@bot.callback_query_handler(func=lambda c: c.data.startswith("top_"))
def cb_top_cat(call):
    uid     = call.from_user.id
    cat     = call.data[4:]
    medals  = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

    with db() as c:
        if cat == "balance":
            c.execute("SELECT name, balance FROM users ORDER BY balance DESC LIMIT 10")
            rows = c.fetchall()
            title, vf = "💎 Топ по балансу", lambda r: fmt(r["balance"])
        elif cat == "xp":
            c.execute("SELECT name, xp FROM users ORDER BY xp DESC LIMIT 10")
            rows = c.fetchall()
            title, vf = "⭐ Топ по XP", lambda r: fmt(r["xp"])
        elif cat == "stocks":
            c.execute("""SELECT u.name, SUM(p.shares) as s FROM portfolios p
                         JOIN users u ON u.id=p.user_id WHERE p.ticker=?
                         GROUP BY p.user_id ORDER BY s DESC LIMIT 10""", (TICKER,))
            rows = c.fetchall()
            title, vf = "📈 Акционеры", lambda r: f"{r['s']} акций"
        else:
            bot.answer_callback_query(call.id); return

    lines = [f"{medals[i]} <b>{r['name'] or 'Игрок'}</b> — {vf(r)}" for i, r in enumerate(rows)]
    text  = f"<b>{title}</b>\n━━━━━━━━━━━━━━━━━━\n" + ("\n".join(lines) if lines else "Пусто")
    _edit(call, text, kb([("🏆 Топ", "menu_top"), ("🏠 Меню", "home")]))


# ══════════════════════════════════════════════
# 19. КЛАН
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_clan")
def cb_clan(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("""SELECT cl.*, cm.role FROM clan_members cm
                     JOIN clans cl ON cl.id=cm.clan_id WHERE cm.user_id=?""", (uid,))
        clan = c.fetchone()

    if not clan:
        _edit(call,
            "<b>🏰 Клан</b>\n━━━━━━━━━━━━━━━━━━\nТы не в клане.",
            kb([("⚔️ Создать", "clan_create_act"), ("🏠 Меню", "home")]))
        return

    with db() as c:
        c.execute("SELECT COUNT(*) as cnt FROM clan_members WHERE clan_id=?", (clan["id"],))
        members = c.fetchone()["cnt"]

    text = (
        f"<b>🏰 {clan['name']}</b>  [{clan['tag']}]\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Уровень: <b>{clan['level']}</b>  XP: {fmt(clan['xp'])}\n"
        f"Казна: <b>{fmt(clan['balance'])} {CUR}</b>\n"
        f"Участников: <b>{members}</b>\n"
        f"Ваша роль: <b>{clan['role']}</b>"
    )
    rows = [[("👥 Участники", f"clan_mem_{clan['id']}"),
             ("💰 Взнос",    "clan_donate")]]
    if clan["role"] in ("owner", "admin"):
        rows.append([("⚙️ Управление", f"clan_manage_{clan['id']}")])
    rows.append([("🚪 Покинуть", f"clan_leave_{clan['id']}"), ("🏠 Меню", "home")])
    _edit(call, text, kb(*rows))


@bot.callback_query_handler(func=lambda c: c.data == "clan_create_act")
def cb_clan_create_act(call):
    uid = call.from_user.id
    _waiting[uid] = "clan_create"
    bot.answer_callback_query(call.id)
    bot.send_message(uid,
        f"⚔️ Введи: <b>Название ТЕГ</b>\nПример: Легион LEG\n"
        f"Стоимость: <b>{fmt(CLAN_COST)} {CUR}</b>",
        parse_mode="HTML",
        reply_markup=kb([("❌ Отмена", "cancel_input")]))


def _do_clan_create(uid: int, text: str):
    parts = text.strip().split()
    if len(parts) < 2:
        bot.send_message(uid, "❌ Формат: Название ТЕГ"); return
    tag  = parts[-1].upper()[:5]
    name = " ".join(parts[:-1])[:30]
    u    = get_user(uid)
    if u["balance"] < CLAN_COST:
        bot.send_message(uid, f"❌ Нужно {fmt(CLAN_COST)} {CUR}"); return
    # Проверяем — уже в клане?
    with db() as c:
        c.execute("SELECT 1 FROM clan_members WHERE user_id=?", (uid,))
        if c.fetchone():
            bot.send_message(uid, "❌ Вы уже в клане"); return
    try:
        add_balance(uid, -CLAN_COST)
        with db() as c:
            c.execute("INSERT INTO clans (name,tag,owner,created_at) VALUES (?,?,?,?)",
                      (name, tag, uid, now()))
            cid = c.lastrowid
            c.execute("INSERT INTO clan_members (user_id,clan_id,role,joined_at) VALUES (?,?,?,?)",
                      (uid, cid, "owner", now()))
        grant_achievement(uid, "clan_owner")
        bot.send_message(uid, f"✅ Клан <b>{name}</b> [{tag}] создан!",
                         parse_mode="HTML",
                         reply_markup=kb([("🏰 Клан", "menu_clan"), ("🏠 Меню", "home")]))
    except Exception:
        add_balance(uid, CLAN_COST)
        bot.send_message(uid, "❌ Название или тег уже заняты")


@bot.callback_query_handler(func=lambda c: c.data == "clan_donate")
def cb_clan_donate_menu(call):
    uid = call.from_user.id
    _waiting[uid] = "clan_donate"
    bot.answer_callback_query(call.id)
    bot.send_message(uid, f"💰 Введи сумму взноса в казну:",
                     reply_markup=kb([("❌ Отмена", "cancel_input")]))


def _do_clan_donate(uid: int, amount: int):
    u = get_user(uid)
    if u["balance"] < amount:
        bot.send_message(uid, "❌ Недостаточно"); return
    with db() as c:
        c.execute("SELECT clan_id FROM clan_members WHERE user_id=?", (uid,))
        row = c.fetchone()
    if not row:
        bot.send_message(uid, "❌ Вы не в клане"); return
    cid = row["clan_id"]
    add_balance(uid, -amount)
    with db() as c:
        c.execute("UPDATE clans SET balance=balance+?, xp=xp+? WHERE id=?",
                  (amount, amount // 100, cid))
    bot.send_message(uid, f"✅ Внесено в казну: <b>{fmt(amount)} {CUR}</b>",
                     parse_mode="HTML",
                     reply_markup=kb([("🏰 Клан", "menu_clan"), ("🏠 Меню", "home")]))


@bot.callback_query_handler(func=lambda c: re.match(r"^clan_mem_\d+$", c.data))
def cb_clan_members(call):
    cid = int(call.data[9:])
    with db() as c:
        c.execute("""SELECT u.name, cm.role FROM clan_members cm
                     JOIN users u ON u.id=cm.user_id WHERE cm.clan_id=?""", (cid,))
        members = c.fetchall()
    lines = [f"{'👑' if r['role']=='owner' else '⭐' if r['role']=='admin' else '•'} "
             f"<b>{r['name'] or 'Игрок'}</b>  {r['role']}" for r in members]
    _edit(call, f"<b>👥 Участники</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
          kb([("🏰 Клан", "menu_clan"), ("🏠 Меню", "home")]))


@bot.callback_query_handler(func=lambda c: re.match(r"^clan_leave_\d+$", c.data))
def cb_clan_leave(call):
    uid = call.from_user.id
    cid = int(call.data[11:])
    with db() as c:
        c.execute("SELECT role FROM clan_members WHERE user_id=? AND clan_id=?", (uid, cid))
        row = c.fetchone()
    if row and row["role"] == "owner":
        bot.answer_callback_query(call.id, "❌ Передай права прежде чем покинуть", show_alert=True); return
    with db() as c:
        c.execute("DELETE FROM clan_members WHERE user_id=?", (uid,))
    bot.answer_callback_query(call.id, "✅ Вышли из клана")
    cb_clan(call)


# ══════════════════════════════════════════════
# 20. ДОНАТ
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_donate")
def cb_donate_menu(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("SELECT key,stars,amount,label FROM donate_packages ORDER BY stars")
        pkgs = c.fetchall()
    rows = [[(p["label"], f"donate_{p['key']}")] for p in pkgs]
    rows.append([("🏠 Меню", "home")])
    _edit(call,
        f"<b>💎 Донат</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"Пополни баланс через Telegram Stars:",
        kb(*rows))


@bot.callback_query_handler(func=lambda c: c.data.startswith("donate_"))
def cb_donate(call):
    uid = call.from_user.id
    key = call.data[7:]
    with db() as c:
        c.execute("SELECT * FROM donate_packages WHERE key=?", (key,))
        pkg = c.fetchone()
    if not pkg:
        bot.answer_callback_query(call.id, "❌ Не найдено"); return
    bot.answer_callback_query(call.id)
    bot.send_invoice(uid,
        title=f"Пополнение {pkg['label']}",
        description=f"+{fmt(pkg['amount'])} {CUR}",
        payload=f"donate_{key}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(pkg["label"], pkg["stars"])])


@bot.pre_checkout_query_handler(func=lambda q: True)
def pre_checkout(query):
    bot.answer_pre_checkout_query(query.id, ok=True)


@bot.message_handler(content_types=["successful_payment"])
def on_payment(msg):
    uid     = msg.from_user.id
    payload = msg.successful_payment.invoice_payload
    key     = payload[7:]
    with db() as c:
        c.execute("SELECT amount FROM donate_packages WHERE key=?", (key,))
        pkg = c.fetchone()
    if pkg:
        add_balance(uid, pkg["amount"])
        # premium 30 дней за покупку ≥50⭐
        with db() as c:
            c.execute("SELECT stars FROM donate_packages WHERE key=?", (key,))
            stars = (c.fetchone() or {}).get("stars", 0)
        if stars >= 50:
            until = now() + 30 * 86400
            with db() as c:
                c.execute("UPDATE users SET premium_until=MAX(premium_until,?) WHERE id=?",
                          (until, uid))
        bot.send_message(uid,
            f"✅ <b>Оплачено!</b> +{fmt(pkg['amount'])} {CUR}",
            parse_mode="HTML",
            reply_markup=kb([("🏠 Меню", "home")]))
        send_alert(f"💎 Донат uid={uid} key={key} +{fmt(pkg['amount'])}")


# ══════════════════════════════════════════════
# 21. РЕФЕРАЛЬНАЯ ССЫЛКА
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "act_reflink")
def cb_reflink(call):
    uid = call.from_user.id
    u   = get_user(uid)
    ref_code = u.get("ref_code") or f"R{uid}"
    bi = bot.get_me()
    link = f"https://t.me/{bi.username}?start={ref_code}"
    text = (
        f"<b>🔗 Реферальная программа</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Приглашено: <b>{u['ref_count']}</b> игроков\n"
        f"Бонус: <b>3 000 {CUR}</b> тебе и другу\n\n"
        f"{link}"
    )
    _edit(call, text, kb([("🏠 Меню", "home")]))


# ══════════════════════════════════════════════
# 22. ПОМОЩЬ
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "menu_help")
def cb_help(call):
    text = (
        "<b>❓ Помощь</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<b>ЛС-команды:</b>\n"
        "клик — кликер\n"
        "майнинг — сбор руды\n"
        "меню — главное меню\n\n"
        "<b>Группа:</b>\n"
        "баланс · топ · акции\n"
        "рул красное 1000 · кости 500\n"
        "слот 1000 · краш 500\n"
        "дуэль 1000 · кнб 500 (reply)\n"
        "дать @user 500 · чек 5000\n"
        "лот 3 · промо КОД\n\n"
        "<b>Меню → Игры:</b>\n"
        "Кубик · Слоты · Рулетка\n"
        "Краш · Мины · Башня · Лотерея"
    )
    _edit(call, text, kb([("🏠 Меню", "home")]))


# ══════════════════════════════════════════════
# 23. ГРУППОВЫЕ КОМАНДЫ
# ══════════════════════════════════════════════

GROUP_TYPES = {"group","supergroup"}

def is_group(msg) -> bool:
    return msg.chat.type in GROUP_TYPES

def gname(msg) -> str:
    u = msg.from_user
    return u.first_name or u.username or str(u.id)


# Баланс

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["баланс", "б", "/б", "/баланс"])
def group_balance(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    u = get_user(uid)
    lvl = user_level(u["xp"])
    bot.reply_to(msg,
        f"👤 {mention(uid, name)}\n"
        f"💎 <b>{fmt(u['balance'])}</b>  🏦 {fmt(u['bank'])}\n"
        f"⚡ Уровень <b>{lvl}</b>",
        parse_mode="HTML")


# Топ

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r"^(топ|top)(\s+\S+)?$", m.text.lower().strip()))
def group_top(msg):
    parts = msg.text.lower().strip().split()
    cat   = parts[1] if len(parts) > 1 else "баланс"
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

    with db() as c:
        if cat in ("xp","опыт","уровень"):
            c.execute("SELECT name, xp FROM users ORDER BY xp DESC LIMIT 10")
            rows  = c.fetchall()
            title = "⭐ Топ по XP"
            vf    = lambda r: f"{fmt(r['xp'])} XP"
        else:
            c.execute("SELECT name, balance FROM users ORDER BY balance DESC LIMIT 10")
            rows  = c.fetchall()
            title = "💎 Топ по балансу"
            vf    = lambda r: fmt(r["balance"])

    lines = [f"{medals[i]} <b>{r['name'] or 'Игрок'}</b> — {vf(r)}" for i, r in enumerate(rows)]
    bot.send_message(msg.chat.id,
        f"<b>{title}</b>\n━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
        parse_mode="HTML")


# Акции в группе

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["акции","биржа","акция"])
def group_stocks(msg):
    st  = _get_stock()
    chg = (st["price"] - st["prev_price"]) / st["prev_price"] * 100
    arrow = "📈" if chg >= 0 else "📉"
    bot.send_message(msg.chat.id,
        f"<b>📈 {TICKER}</b>  {fmt(st['price'])} {CUR}  {arrow} {chg:+.1f}%\n"
        f"Купить/продать: в <b>личку</b> → /start → Биржа",
        parse_mode="HTML")


# Рулетка в группе

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r"^(рул|рулетка)\s+\S+\s+\d+", m.text.lower().strip()))
def group_roulette(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    parts = msg.text.strip().lower().split()
    color_in = parts[1]
    try: bet = int(parts[2])
    except ValueError:
        bot.reply_to(msg, "❌ рул красное 1000"); return
    if bet <= 0: return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}"); return

    color  = COLOR_MAP.get(color_in)
    if not color:
        bot.reply_to(msg, "❌ Цвет: красное / чёрное / зеро"); return

    num    = random.randint(0, 36)
    actual = "zero" if num == 0 else ("red" if num % 2 == 1 else "black")
    add_balance(uid, -bet)

    if actual == color:
        win = bet * COLOR_MULT[color]
        add_balance(uid, win)
        res = f"✅ {mention(uid,name)} поставил {COLOR_ICON[color]} выпало {num} {COLOR_ICON[actual]} — победа! +{fmt(win-bet)}"
    else:
        res = f"😔 {mention(uid,name)} поставил {COLOR_ICON[color]} выпало {num} {COLOR_ICON[actual]} — мимо. -{fmt(bet)}"
    log_game(uid, "Рулетка(гр)", bet, win-bet if actual==color else -bet)
    bot.send_message(msg.chat.id, res + f" {CUR}", parse_mode="HTML")


# Кости в группе

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r"^(кости|куб|кубик|dice)\s+\d+", m.text.lower().strip()))
def group_dice(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    try: bet = int(msg.text.strip().split()[1])
    except ValueError: return
    if bet <= 0: return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}"); return
    add_balance(uid, -bet)
    sent = bot.send_dice(msg.chat.id, emoji="🎲", reply_to_message_id=msg.message_id)
    time.sleep(3)
    val = sent.dice.value
    if val >= 4:
        win = bet * 2
        add_balance(uid, win)
        bot.send_message(msg.chat.id,
            f"🎲 {mention(uid,name)} бросил <b>{val}</b> — победа! +{fmt(win-bet)} {CUR}",
            parse_mode="HTML")
        log_game(uid, "Кубик(гр)", bet, win-bet)
    else:
        bot.send_message(msg.chat.id,
            f"🎲 {mention(uid,name)} бросил <b>{val}</b> — мимо. -{fmt(bet)} {CUR}",
            parse_mode="HTML")
        log_game(uid, "Кубик(гр)", bet, -bet)


# Слоты в группе

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r"^(слот|слоты|slots?)\s+\d+", m.text.lower().strip()))
def group_slots(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    try: bet = int(msg.text.strip().split()[1])
    except ValueError: return
    if bet <= 0: return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}"); return
    reels = [random.choice(SLOT_SYM) for _ in range(3)]
    combo = "".join(reels)
    mult  = SLOT_PAY.get(combo, 0)
    add_balance(uid, -bet)
    if mult:
        win = bet * mult
        add_balance(uid, win)
        bot.send_message(msg.chat.id,
            f"🎰 {mention(uid,name)}: {combo} 🎉 x{mult} +{fmt(win-bet)} {CUR}",
            parse_mode="HTML")
        log_game(uid,"Слоты(гр)",bet,win-bet)
    else:
        bot.send_message(msg.chat.id,
            f"🎰 {mention(uid,name)}: {combo} 😔 -{fmt(bet)} {CUR}",
            parse_mode="HTML")
        log_game(uid,"Слоты(гр)",bet,-bet)


# Краш в группе

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r"^(краш|crash)\s+\d+", m.text.lower().strip()))
def group_crash(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    try: bet = int(msg.text.strip().split()[1])
    except ValueError: return
    if bet <= 0: return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}"); return
    crash_at = max(1.01, round(random.expovariate(0.9)+1.0, 2))
    add_balance(uid, -bet)
    kb_crash = InlineKeyboardMarkup(row_width=4)
    for m_ in ["1.5","2.0","3.0","5.0"]:
        kb_crash.insert(InlineKeyboardButton(
            f"x{m_}", callback_data=f"gcrash_{uid}_{m_}_{bet}_{crash_at}"))
    bot.send_message(msg.chat.id,
        f"⚡ {mention(uid,name)}  Ставка: {fmt(bet)}\nЗабери до краша!",
        parse_mode="HTML", reply_markup=kb_crash)


@bot.callback_query_handler(func=lambda c: c.data.startswith("gcrash_"))
def cb_group_crash(call):
    parts    = call.data.split("_")
    o_uid    = int(parts[1])
    mult     = float(parts[2])
    bet      = int(parts[3])
    crash_at = float(parts[4])
    uid      = call.from_user.id
    if uid != o_uid:
        bot.answer_callback_query(call.id, "Не ваша игра"); return
    name = call.from_user.first_name or str(uid)
    if mult <= crash_at:
        win = int(bet * mult)
        add_balance(uid, win)
        result = win - bet
        txt = f"✅ {mention(uid,name)} забрал x{mult}! +{fmt(result)} {CUR}"
        log_game(uid,"Краш(гр)",bet,result)
    else:
        result = -bet
        txt = f"💥 Краш x{crash_at}! {mention(uid,name)} -{fmt(bet)} {CUR}"
        log_game(uid,"Краш(гр)",bet,result)
    try:
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id,
                              parse_mode="HTML")
    except Exception: pass
    bot.answer_callback_query(call.id)


# Дуэль в группе

_duels: dict[int, dict] = {}

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r"^(дуэль|duel)\s+\d+", m.text.lower().strip()))
def group_duel(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    try: bet = int(msg.text.strip().split()[1])
    except ValueError: return
    if bet <= 0: return
    u = get_user(uid)
    if u["balance"] < bet:
        bot.reply_to(msg, f"❌ Баланс: {fmt(u['balance'])}"); return
    if uid in _duels:
        bot.reply_to(msg, "❌ У тебя уже активная дуэль"); return

    # Определяем соперника (reply)
    opp_uid, opp_name = None, "любой"
    if msg.reply_to_message and msg.reply_to_message.from_user:
        ru = msg.reply_to_message.from_user
        if ru.id != uid and not ru.is_bot:
            opp_uid  = ru.id
            opp_name = ru.first_name or str(ru.id)
            ensure_user(opp_uid, opp_name)

    _duels[uid] = {"bet": bet, "opp": opp_uid, "cname": name,
                   "chat": msg.chat.id, "expires": now()+60}

    opp_str = mention(opp_uid, opp_name) if opp_uid else "<b>любой игрок</b>"
    sent = bot.send_message(msg.chat.id,
        f"⚔️ {mention(uid,name)} вызывает {opp_str} на дуэль!\n"
        f"Ставка: <b>{fmt(bet)} {CUR}</b>  ⏱60с",
        parse_mode="HTML",
        reply_markup=kb([(f"⚔️ Принять", f"duel_acc_{uid}")]))
    _duels[uid]["msg_id"] = sent.message_id

    def _cancel():
        time.sleep(62)
        if uid in _duels:
            _duels.pop(uid, None)
            try:
                bot.edit_message_text(
                    f"⚔️ Дуэль {mention(uid,name)} отменена.",
                    msg.chat.id, sent.message_id, parse_mode="HTML")
            except Exception: pass
    threading.Thread(target=_cancel, daemon=True).start()


@bot.callback_query_handler(func=lambda c: c.data.startswith("duel_acc_"))
def cb_duel_accept(call):
    c_uid = int(call.data[9:])
    o_uid = call.from_user.id
    oname = call.from_user.first_name or str(o_uid)
    duel  = _duels.get(c_uid)
    if not duel:
        bot.answer_callback_query(call.id, "Дуэль завершена"); return
    if duel["expires"] < now():
        _duels.pop(c_uid, None)
        bot.answer_callback_query(call.id, "Время вышло"); return
    if c_uid == o_uid:
        bot.answer_callback_query(call.id, "❌ Своя дуэль"); return
    if duel["opp"] and duel["opp"] != o_uid:
        bot.answer_callback_query(call.id, "❌ Не для тебя"); return

    ensure_user(o_uid, oname)
    bet   = duel["bet"]
    cname = duel["cname"]
    chat  = duel["chat"]
    cu    = get_user(c_uid)
    ou    = get_user(o_uid)
    if ou["balance"] < bet:
        bot.answer_callback_query(call.id, f"❌ Нужно {fmt(bet)}", show_alert=True); return
    if cu["balance"] < bet:
        bot.answer_callback_query(call.id, "❌ У организатора не хватает", show_alert=True)
        _duels.pop(c_uid, None); return

    _duels.pop(c_uid, None)
    bot.answer_callback_query(call.id)

    cr = random.randint(1, 6)
    or_ = random.randint(1, 6)
    tries = 0
    while cr == or_ and tries < 5:
        cr = random.randint(1, 6); or_ = random.randint(1, 6); tries += 1

    if cr > or_:
        w_uid, w_name = c_uid, cname
        l_uid = o_uid
    else:
        w_uid, w_name = o_uid, oname
        l_uid = c_uid

    add_balance(l_uid, -bet)
    add_balance(w_uid, bet)
    log_game(w_uid, "Дуэль", bet, bet)
    log_game(l_uid, "Дуэль", bet, -bet)

    try:
        bot.edit_message_text(
            f"⚔️ <b>Дуэль!</b>\n"
            f"{mention(c_uid,cname)}: 🎲<b>{cr}</b>  vs  {mention(o_uid,oname)}: 🎲<b>{or_}</b>\n"
            f"🏆 {mention(w_uid,w_name)} +{fmt(bet)} {CUR}",
            chat, call.message.message_id, parse_mode="HTML")
    except Exception: pass


# КНБ в группе

_knb: dict[int, dict] = {}
KNB_BEATS = {"камень":"ножницы","ножницы":"бумага","бумага":"камень"}
KNB_EMOJI = {"камень":"🪨","ножницы":"✂️","бумага":"📄"}

@bot.message_handler(func=lambda m: is_group(m) and m.reply_to_message and
    m.text and re.match(r"^(кнб|knb|рпс)\s+\d+", m.text.lower().strip()))
def group_knb(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    ru = msg.reply_to_message.from_user
    if not ru or ru.id == uid or ru.is_bot: return
    try: bet = int(msg.text.strip().split()[1])
    except ValueError: return
    if bet <= 0: return
    u = get_user(uid)
    if u["balance"] < bet: return
    o_uid  = ru.id
    oname  = ru.first_name or str(o_uid)
    ensure_user(o_uid, oname)

    gid = uid
    _knb[gid] = {"bet":bet,"chat":msg.chat.id,
                 "c_uid":uid,"c_name":name,"c_ch":None,
                 "o_uid":o_uid,"o_name":oname,"o_ch":None,
                 "expires":now()+120}
    kb_knb = InlineKeyboardMarkup(row_width=3)
    for k in ["камень","ножницы","бумага"]:
        kb_knb.add(InlineKeyboardButton(KNB_EMOJI[k], callback_data=f"knb_{gid}_{k}"))

    bot.send_message(msg.chat.id,
        f"🪨✂️📄 {mention(uid,name)} vs {mention(o_uid,oname)}\n"
        f"Ставка: <b>{fmt(bet)} {CUR}</b>  Выбирайте👇",
        parse_mode="HTML", reply_markup=kb_knb)


@bot.callback_query_handler(func=lambda c: re.match(r"^knb_\d+_\S+$", c.data))
def cb_knb(call):
    parts  = call.data.split("_")
    gid    = int(parts[1])
    choice = parts[2]
    uid    = call.from_user.id
    game   = _knb.get(gid)
    if not game or game["expires"] < now():
        bot.answer_callback_query(call.id, "Игра завершена"); return

    if uid == game["c_uid"]:
        if game["c_ch"]:
            bot.answer_callback_query(call.id, "Уже выбрал"); return
        game["c_ch"] = choice
    elif uid == game["o_uid"]:
        if game["o_ch"]:
            bot.answer_callback_query(call.id, "Уже выбрал"); return
        game["o_ch"] = choice
    else:
        bot.answer_callback_query(call.id, "Не участник"); return

    bot.answer_callback_query(call.id, f"Ты выбрал {KNB_EMOJI[choice]}")
    if not game["c_ch"] or not game["o_ch"]: return

    _knb.pop(gid, None)
    cc, oc = game["c_ch"], game["o_ch"]
    bet    = game["bet"]

    if cc == oc:
        result = f"🤝 Ничья! Ставки возвращены."
    elif KNB_BEATS[cc] == oc:
        add_balance(game["o_uid"], -bet)
        add_balance(game["c_uid"], bet)
        log_game(game["c_uid"],"КНБ",bet,bet)
        log_game(game["o_uid"],"КНБ",bet,-bet)
        result = f"🏆 {mention(game['c_uid'],game['c_name'])} {KNB_EMOJI[cc]} +{fmt(bet)} {CUR}"
    else:
        add_balance(game["c_uid"], -bet)
        add_balance(game["o_uid"], bet)
        log_game(game["o_uid"],"КНБ",bet,bet)
        log_game(game["c_uid"],"КНБ",bet,-bet)
        result = f"🏆 {mention(game['o_uid'],game['o_name'])} {KNB_EMOJI[oc]} +{fmt(bet)} {CUR}"

    try:
        bot.edit_message_text(
            f"🪨✂️📄  {mention(game['c_uid'],game['c_name'])}: {KNB_EMOJI[cc]}\n"
            f"        {mention(game['o_uid'],game['o_name'])}: {KNB_EMOJI[oc]}\n"
            f"{result}",
            game["chat"], call.message.message_id, parse_mode="HTML")
    except Exception: pass


# Чек (групповой перевод кодом)

_checks: dict[str, dict] = {}

@bot.message_handler(func=lambda m: m.text and
    re.match(r"^чек\s+\d+(\s+\d+)?$", m.text.lower().strip()))
def cmd_check_create(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    parts = msg.text.strip().split()
    try:
        amount = int(parts[1])
        uses   = int(parts[2]) if len(parts) > 2 else 1
    except ValueError: return
    if amount <= 0 or uses <= 0: return
    total = amount * uses
    u = get_user(uid)
    if u["balance"] < total:
        bot.reply_to(msg, f"❌ Нужно {fmt(total)}, баланс: {fmt(u['balance'])}"); return
    add_balance(uid, -total)
    code = "C" + "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=7))
    _checks[code] = {"amount": amount, "left": uses, "uid": uid}
    bot.send_message(msg.chat.id,
        f"💸 {mention(uid,name)} создал чек!\n"
        f"{fmt(amount)} {CUR} × {uses} активаций\n"
        f"Код: <code>{code}</code>\n"
        f"Активировать: <code>активировать {code}</code>",
        parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and
    re.match(r"^активировать\s+\S+", m.text.lower().strip()))
def cmd_check_use(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    code = msg.text.strip().split()[1].upper()
    chk  = _checks.get(code)
    if not chk:
        bot.reply_to(msg, "❌ Чек не найден"); return
    if chk["uid"] == uid:
        bot.reply_to(msg, "❌ Нельзя свой чек"); return
    add_balance(uid, chk["amount"])
    chk["left"] -= 1
    extra = "" if chk["left"] > 0 else " — исчерпан"
    if chk["left"] <= 0:
        _checks.pop(code, None)
    bot.reply_to(msg,
        f"✅ {mention(uid,name)} +{fmt(chk['amount'])} {CUR}{extra}",
        parse_mode="HTML")


# Перевод в группе

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r"^(дать|pay)\s+", m.text.lower().strip()))
def group_transfer(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    parts  = msg.text.strip().split()
    to_uid, to_name, amount = None, None, None

    if msg.reply_to_message and msg.reply_to_message.from_user:
        ru = msg.reply_to_message.from_user
        if ru.id == uid or ru.is_bot: return
        to_uid  = ru.id
        to_name = ru.first_name or str(ru.id)
        ensure_user(to_uid, to_name)
        try: amount = int(parts[1])
        except ValueError: return
    else:
        if len(parts) < 3: return
        uname = parts[1].lstrip("@")
        with db() as c:
            c.execute("SELECT id,name FROM users WHERE name LIKE ?", (f"%{uname}%",))
            row = c.fetchone()
        if not row: return
        to_uid  = row["id"]
        to_name = row["name"]
        if to_uid == uid: return
        try: amount = int(parts[2])
        except ValueError: return

    if not amount or amount <= 0: return
    fee   = int(amount * TRANSFER_FEE)
    total = amount + fee
    u = get_user(uid)
    if u["balance"] < total:
        bot.reply_to(msg, f"❌ Нужно {fmt(total)}"); return
    add_balance(uid, -total)
    add_balance(to_uid, amount)
    with db() as c:
        c.execute("INSERT INTO transfers (from_id,to_id,amount,fee,ts) VALUES (?,?,?,?,?)",
                  (uid, to_uid, amount, fee, now()))
    bot.send_message(msg.chat.id,
        f"💸 {mention(uid,name)} → {mention(to_uid,to_name)}\n"
        f"<b>{fmt(amount)} {CUR}</b>  (комиссия {fmt(fee)})",
        parse_mode="HTML")


# Лотерея в группе

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    re.match(r"^(лот|лотерея)\s+\d+", m.text.lower().strip()))
def group_lotto(msg):
    uid  = msg.from_user.id
    name = gname(msg)
    ensure_user(uid, name)
    try: qty = min(100, int(msg.text.strip().split()[1]))
    except ValueError: return
    cost = TICKET_PRICE * qty
    u = get_user(uid)
    if u["balance"] < cost:
        bot.reply_to(msg, f"❌ Нужно {fmt(cost)}"); return
    add_balance(uid, -cost)
    with db() as c:
        c.execute("INSERT INTO lottery_tickets (user_id,tickets) VALUES (?,?) "
                  "ON CONFLICT(user_id) DO UPDATE SET tickets=tickets+?", (uid, qty, qty))
        c.execute("UPDATE lottery SET jackpot=jackpot+? WHERE id=1", (cost,))
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        jackpot = c.fetchone()["jackpot"]
    bot.reply_to(msg,
        f"🎟 {mention(uid,name)} купил <b>{qty}</b> билетов!\n"
        f"Джекпот: <b>{fmt(jackpot)} {CUR}</b>",
        parse_mode="HTML")


# Промокод в группе / ЛС

@bot.message_handler(func=lambda m: m.text and
    re.match(r"^промо\s+\S+", m.text.lower().strip()))
def cmd_promo(msg):
    uid  = msg.from_user.id
    code = msg.text.strip().split(None, 1)[1].strip().upper()
    ensure_user(uid, msg.from_user.first_name or "")
    _use_promo(uid, code)


# Помощь в группе

@bot.message_handler(func=lambda m: is_group(m) and m.text and
    m.text.lower().strip() in ["/help", "помощь", "/помощь"])
def group_help(msg):
    bot.send_message(msg.chat.id,
        "<b>📋 Команды</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        "<b>Инфо</b>\nбаланс · топ · топ xp · акции\n\n"
        "<b>Игры</b>\nрул красное 1000\nкости 500 · слот 1000\n"
        "краш 500 · дуэль 1000\nкнб 500 (reply) · лот 3\n\n"
        "<b>Переводы</b>\nдать @user 500\nдать 500 (reply)\n"
        "чек 5000 [N] · активировать КОД\n\n"
        "<b>Другое</b>\nпромо КОД\n\n"
        "Банк · Биржа · Магазин · Клан — в <b>личку</b> → /start",
        parse_mode="HTML")


# ══════════════════════════════════════════════
# 24. АДМИН-КОМАНДЫ
# ══════════════════════════════════════════════

def adm(fn):
    def w(msg):
        if not is_admin(msg.from_user.id): return
        fn(msg)
    return w


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("выдать ") and is_admin(m.from_user.id))
@adm
def adm_give(msg):
    p = msg.text.split()
    try: uid_, amt = int(p[1]), int(p[2])
    except: bot.reply_to(msg, "выдать <uid> <сумма>"); return
    add_balance(uid_, amt)
    bot.reply_to(msg, f"✅ +{fmt(amt)} → {uid_}")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("забрать ") and is_admin(m.from_user.id))
@adm
def adm_take(msg):
    p = msg.text.split()
    try: uid_, amt = int(p[1]), int(p[2])
    except: bot.reply_to(msg, "забрать <uid> <сумма>"); return
    add_balance(uid_, -amt)
    bot.reply_to(msg, f"✅ -{fmt(amt)} у {uid_}")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("бан ") and is_admin(m.from_user.id))
@adm
def adm_ban(msg):
    p = msg.text.split(None, 2)
    try: uid_ = int(p[1])
    except: bot.reply_to(msg, "бан <uid> [причина]"); return
    reason = p[2] if len(p) > 2 else "Нарушение"
    with db() as c:
        c.execute("INSERT OR REPLACE INTO bans (user_id,reason,by,ts) VALUES (?,?,?,?)",
                  (uid_, reason, msg.from_user.id, now()))
    bot.reply_to(msg, f"🚫 {uid_} заблокирован")
    try: bot.send_message(uid_, f"🚫 Заблокирован. Причина: {reason}")
    except: pass


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("разбан ") and is_admin(m.from_user.id))
@adm
def adm_unban(msg):
    try: uid_ = int(msg.text.split()[1])
    except: bot.reply_to(msg, "разбан <uid>"); return
    with db() as c:
        c.execute("DELETE FROM bans WHERE user_id=?", (uid_,))
    bot.reply_to(msg, f"✅ {uid_} разблокирован")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("промо создать ") and is_admin(m.from_user.id))
@adm
def adm_promo(msg):
    p = msg.text.split()
    try: code, amt, uses = p[2].upper(), int(p[3]), int(p[4]) if len(p)>4 else 100
    except: bot.reply_to(msg, "промо создать КОД СУММА [uses]"); return
    with db() as c:
        try:
            c.execute("INSERT INTO promo_codes (code,reward,max_uses) VALUES (?,?,?)", (code,amt,uses))
        except:
            bot.reply_to(msg, "❌ Код уже есть"); return
    bot.reply_to(msg, f"✅ <code>{code}</code> +{fmt(amt)}  uses={uses}", parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and m.text.lower() == "стат" and is_admin(m.from_user.id))
@adm
def adm_stat(msg):
    with db() as c:
        c.execute("SELECT COUNT(*) FROM users"); users = c.fetchone()[0]
        c.execute("SELECT COALESCE(SUM(balance),0) FROM users"); bal = c.fetchone()[0]
        c.execute("SELECT COALESCE(SUM(bank),0) FROM users"); bank = c.fetchone()[0]
        c.execute("SELECT price FROM stocks WHERE ticker=?", (TICKER,))
        row = c.fetchone(); price = row["price"] if row else 0
        c.execute("SELECT jackpot FROM lottery WHERE id=1")
        lotto = c.fetchone(); jackpot = lotto["jackpot"] if lotto else 0
        c.execute("SELECT COUNT(*) FROM game_log WHERE ts>?", (now()-86400,)); games24 = c.fetchone()[0]
    bot.reply_to(msg,
        f"<b>📊 Статистика</b>\n"
        f"Игроков: <b>{users}</b>\n"
        f"В кошельках: <b>{fmt(bal)}</b>\n"
        f"В банках: <b>{fmt(bank)}</b>\n"
        f"{TICKER}: <b>{fmt(price)}</b>\n"
        f"Лото: <b>{fmt(jackpot)}</b>\n"
        f"Игр за 24ч: <b>{games24}</b>",
        parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("рассылка") and is_admin(m.from_user.id))
@adm
def adm_broadcast(msg):
    text = msg.text[8:].strip()
    if not text: return
    with db() as c:
        c.execute("SELECT id FROM users")
        uids = [r["id"] for r in c.fetchall()]
    sent = fail = 0
    for uid_ in uids:
        try:
            bot.send_message(uid_, text, parse_mode="HTML"); sent += 1
        except: fail += 1
        time.sleep(0.04)
    bot.reply_to(msg, f"✅ {sent} отправлено, {fail} ошибок")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("дивиденды ") and is_admin(m.from_user.id))
@adm
def adm_dividends(msg):
    try: per_sh = int(msg.text.split()[1])
    except: bot.reply_to(msg, "дивиденды СУММА_ЗА_АКЦИЮ"); return
    with db() as c:
        c.execute("SELECT user_id, shares FROM portfolios WHERE ticker=?", (TICKER,))
        holders = c.fetchall()
    total = 0
    for h in holders:
        pay = h["shares"] * per_sh
        add_balance(h["user_id"], pay)
        total += pay
        try:
            bot.send_message(h["user_id"],
                f"💰 Дивиденды {TICKER}: +{fmt(pay)} {CUR}", parse_mode="HTML")
        except: pass
    bot.reply_to(msg, f"✅ Выплачено {fmt(total)} → {len(holders)} держателей")
    send_alert(f"💰 Дивиденды {TICKER}: {fmt(per_sh)}/акц  итого {fmt(total)}")


@bot.message_handler(func=lambda m: m.text and m.text.lower().startswith("добавить товар ") and is_admin(m.from_user.id))
@adm
def adm_add_item(msg):
    # добавить товар 💻 Ноутбук Описание 10000
    p = msg.text.split(None, 5)
    try:
        emoji, name, desc, price = p[2], p[3], p[4], int(p[5].split()[0])
    except:
        bot.reply_to(msg, "добавить товар EMOJI ИМЯ ОПИСАНИЕ ЦЕНА"); return
    with db() as c:
        c.execute("INSERT INTO items (emoji,name,desc,price) VALUES (?,?,?,?)",
                  (emoji, name, desc, price))
    bot.reply_to(msg, f"✅ {emoji} {name} добавлен за {fmt(price)}")


# ══════════════════════════════════════════════
# 25. СОБЫТИЯ — МЕТЕОРЫ / НАЛОГ
# ══════════════════════════════════════════════

def _events_scheduler():
    """Случайные события каждые 2-6 часов."""
    while True:
        sleep_sec = random.randint(7200, 21600)
        time.sleep(sleep_sec)
        try:
            ev = random.choice(["meteor", "tax"])
            if ev == "meteor":
                _do_meteor()
            elif ev == "tax":
                _do_tax()
        except Exception as e:
            print(f"[events] err: {e}")


def _do_meteor():
    """Пуш метеора — первый забирает монеты."""
    reward = random.randint(5_000, 30_000)
    if not ALERT_CHAT: return
    msg = bot.send_message(ALERT_CHAT,
        f"☄️ <b>Метеор!</b> Первый поймает <b>{fmt(reward)} {CUR}</b>!\n"
        f"Нажми кнопку:",
        parse_mode="HTML",
        reply_markup=kb([("☄️ Поймать!", f"meteor_{reward}_{now()}")]))
    # Через 60 сек — истекает
    def _expire():
        time.sleep(62)
        try:
            bot.edit_message_reply_markup(ALERT_CHAT, msg.message_id, reply_markup=None)
        except: pass
    threading.Thread(target=_expire, daemon=True).start()


@bot.callback_query_handler(func=lambda c: c.data.startswith("meteor_"))
def cb_meteor(call):
    uid    = call.from_user.id
    parts  = call.data.split("_")
    reward = int(parts[1])
    ts     = int(parts[2])
    if now() - ts > 60:
        bot.answer_callback_query(call.id, "☄️ Метеор уже упал!"); return
    ensure_user(uid, call.from_user.first_name or "")
    add_balance(uid, reward)
    name = call.from_user.first_name or str(uid)
    try:
        bot.edit_message_text(
            f"☄️ Метеор поймал {mention(uid,name)}! +{fmt(reward)} {CUR}",
            call.message.chat.id, call.message.message_id, parse_mode="HTML")
    except: pass
    bot.answer_callback_query(call.id, f"☄️ +{fmt(reward)} {CUR}!")


def _do_tax():
    """Налог на богатейших игроков."""
    with db() as c:
        c.execute("SELECT id, name, balance FROM users WHERE balance>50000 ORDER BY balance DESC LIMIT 20")
        rich = c.fetchall()
    if not rich: return
    total = 0
    for u in rich:
        tax = int(u["balance"] * 0.03)
        if tax > 0:
            add_balance(u["id"], -tax)
            total += tax
            try:
                bot.send_message(u["id"],
                    f"💸 Налог на богатство: -{fmt(tax)} {CUR} (3%)", parse_mode="HTML")
            except: pass
    if ALERT_CHAT and total > 0:
        send_alert(f"💸 <b>Налог</b> собран: <b>{fmt(total)} {CUR}</b> с {len(rich)} игроков")


# ══════════════════════════════════════════════
# 26. HISTORY (заглушка из waiting)
# ══════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "act_tx_hist")
def cb_tx_hist(call):
    uid = call.from_user.id
    with db() as c:
        c.execute("""SELECT from_id, to_id, amount, fee, ts FROM transfers
                     WHERE from_id=? OR to_id=? ORDER BY ts DESC LIMIT 10""", (uid, uid))
        rows = c.fetchall()
    if not rows:
        _edit(call, "📜 История пуста.",
              kb([("👛 Кошелёк", "menu_wallet"), ("🏠 Меню", "home")])); return
    lines = []
    for r in rows:
        dt   = datetime.fromtimestamp(r["ts"]).strftime("%d.%m %H:%M")
        if r["from_id"] == uid:
            lines.append(f"🔴 {dt}  -{fmt(r['amount']+r['fee'])}")
        else:
            lines.append(f"🟢 {dt}  +{fmt(r['amount'])}")
    _edit(call, "<b>📜 Переводы</b>\n━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines),
          kb([("👛 Кошелёк", "menu_wallet"), ("🏠 Меню", "home")]))


# ══════════════════════════════════════════════
# 27. ЗАПУСК
# ══════════════════════════════════════════════

if __name__ == "__main__":
    init_db()
    threading.Thread(target=_stock_scheduler,   daemon=True).start()
    threading.Thread(target=_lottery_scheduler, daemon=True).start()
    threading.Thread(target=_events_scheduler,  daemon=True).start()
    print("🚀 Бот запущен — v3.0 Ultra Clean")
    bot.infinity_polling(timeout=30, long_polling_timeout=30)
