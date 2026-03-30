import telebot
import threading
from flask import Flask
from telebot import types
import sqlite3
import random
import time
import os
from datetime import datetime

# ─────────────────────────────────────────
#  КОНФИГ
# ─────────────────────────────────────────
BOT_TOKEN   = "8761494197:AAEr9t6hqogFCKGAm4COrrVVCQ9TyoK3aj8"          # @BotFather
ADMIN_ID    = 8139807344            # твой Telegram ID
DB_PATH     = "casino.db"

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ─────────────────────────────────────────
#  БД
# ─────────────────────────────────────────
def db():
    return sqlite3.connect(DB_PATH)

def init_db():
    with db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id   INTEGER PRIMARY KEY,
            username  TEXT,
            balance   INTEGER DEFAULT 0,
            wins      INTEGER DEFAULT 0,
            losses    INTEGER DEFAULT 0,
            total_bet INTEGER DEFAULT 0
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER,
            amount    INTEGER,
            status    TEXT DEFAULT 'pending',
            created   TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS bets (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER,
            game      TEXT,
            bet       INTEGER,
            result    TEXT,
            win       INTEGER,
            created   TEXT
        )""")

init_db()

# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────
def get_user(user_id, username=None):
    with db() as c:
        row = c.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            c.execute("INSERT INTO users (user_id, username) VALUES (?,?)", (user_id, username or ""))
            return {"user_id": user_id, "username": username, "balance": 0, "wins": 0, "losses": 0, "total_bet": 0}
        return dict(zip(["user_id","username","balance","wins","losses","total_bet"], row))

def update_balance(user_id, delta):
    with db() as c:
        c.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (delta, user_id))

def add_bet_record(user_id, game, bet, result, win):
    with db() as c:
        c.execute("INSERT INTO bets (user_id,game,bet,result,win,created) VALUES (?,?,?,?,?,?)",
                  (user_id, game, bet, result, win, datetime.now().strftime("%Y-%m-%d %H:%M")))
        if win > 0:
            c.execute("UPDATE users SET wins=wins+1, total_bet=total_bet+? WHERE user_id=?", (bet, user_id))
        else:
            c.execute("UPDATE users SET losses=losses+1, total_bet=total_bet+? WHERE user_id=?", (bet, user_id))

def parse_bet(text, user_id):
    """Вернуть (ставка, ошибка)"""
    parts = text.strip().split()
    if len(parts) < 2:
        return None, "Укажи ставку. Пример: <code>бск 10</code>"
    try:
        bet = int(parts[1])
    except ValueError:
        return None, "Ставка должна быть числом."
    if bet <= 0:
        return None, "Ставка должна быть > 0."
    u = get_user(user_id)
    if u["balance"] < bet:
        return None, f"Недостаточно звёзд. Баланс: <b>{u['balance']} ⭐</b>"
    return bet, None

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M")

# ─────────────────────────────────────────
#  ГЛАВНОЕ МЕНЮ
# ─────────────────────────────────────────
def main_menu(user_id):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton("💳 Пополнить", callback_data="deposit"))
    kb.add(types.InlineKeyboardButton("💸 Вывести",   callback_data="withdraw"))
    kb.add(types.InlineKeyboardButton("🏆 Топ",       callback_data="top"),
           types.InlineKeyboardButton("📊 Профиль",   callback_data="profile"))
    kb.add(types.InlineKeyboardButton("🆘 Поддержка", url="https://t.me/Cary_python"))
    return kb

MENU_TEXT = (
    "🎰 <b>CASINO</b>\n\n"
    "Баланс: <b>{bal} ⭐</b>\n\n"
    "Игры — пиши в чат:\n"
    "<code>бск 50</code>   — 🏀 Баскетбол\n"
    "<code>фтб 50</code>   — ⚽ Футбол\n"
    "<code>дартс 50</code> — 🎯 Дартс\n"
    "<code>боул 50</code>  — 🎳 Боулинг\n"
    "<code>куб 50</code>   — 🎲 Кубик\n\n"
    "Пополнение: <code>пополнить 200</code>"
)

# ─────────────────────────────────────────
#  /start
# ─────────────────────────────────────────
# ─────────────────────────────────────────
#  KEYWORDS — срабатывают БЕЗ слеша, в любом чате
# ─────────────────────────────────────────
GAME_KEYS = {
    "бск":   "bsk",
    "bsk":   "bsk",
    "боул":  "bowl",
    "bowl":  "bowl",
    "дартс": "darts",
    "darts": "darts",
    "куб":   "cube",
    "cube":  "cube",
    "фтб":   "ftb",
    "ftb":   "ftb",
}

def get_game_key(text):
    """Вернуть ключ игры если сообщение начинается с нужного слова (с или без /)."""
    if not text:
        return None
    word = text.strip().lstrip("/").lower().split()[0]
    return GAME_KEYS.get(word)

def is_deposit_text(text):
    """пополнить 200  /  пополнить200  /  deposit 100"""
    if not text:
        return False
    word = text.strip().lstrip("/").lower().split()[0]
    return word in ("пополнить", "пополни", "deposit", "pay")

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    u = get_user(msg.from_user.id, msg.from_user.username)
    bot.send_message(
        msg.chat.id,
        MENU_TEXT.format(bal=u["balance"]),
        reply_markup=main_menu(msg.from_user.id)
    )

# ─────────────────────────────────────────
#  УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК ИГР (без / и с /)
# ─────────────────────────────────────────
@bot.message_handler(func=lambda m: get_game_key(m.text) is not None)
def handle_game(msg):
    key = get_game_key(msg.text)
    parts = msg.text.strip().split()
    normalized = key + (" " + parts[1] if len(parts) > 1 else "")
    bet, err = parse_bet(normalized, msg.from_user.id)
    if err:
        return bot.reply_to(msg, err)
    if key == "bsk":
        _play_bsk(msg, bet)
    elif key == "bowl":
        _play_bowl(msg, bet)
    elif key == "darts":
        _play_darts(msg, bet)
    elif key == "cube":
        _play_cube(msg, bet)
    elif key == "ftb":
        _play_ftb(msg, bet)

# ─────────────────────────────────────────
#  ТЕКСТОВОЕ ПОПОЛНЕНИЕ: "пополнить 200"
# ─────────────────────────────────────────
@bot.message_handler(func=lambda m: is_deposit_text(m.text))
def handle_deposit_text(msg):
    parts = msg.text.strip().split()
    if len(parts) < 2:
        return bot.reply_to(msg, "Укажи сумму. Пример: <code>пополнить 200</code>")
    try:
        amount = int(parts[1])
    except ValueError:
        return bot.reply_to(msg, "Сумма должна быть числом. Пример: <code>пополнить 200</code>")
    if amount < 1:
        return bot.reply_to(msg, "Минимум 1 ⭐")
    prices = [types.LabeledPrice(label=f"Пополнение {amount} ⭐", amount=amount)]
    bot.send_invoice(
        chat_id=msg.chat.id,
        title=f"Пополнение {amount} ⭐",
        description="Баланс в Casino боте",
        invoice_payload=f"deposit_{amount}",
        provider_token="",
        currency="XTR",
        prices=prices
    )

# ── БАСКЕТБОЛ ───────────────────────────
def _play_bsk(msg, bet):
    update_balance(msg.from_user.id, -bet)
    sent = bot.send_dice(msg.chat.id, emoji="🏀")
    time.sleep(3)
    val   = sent.dice.value
    win   = val in (4, 5)
    prize = bet * 2 if win else 0
    if win:
        update_balance(msg.from_user.id, prize)
        result_text = f"🏀 Гол! +{prize} ⭐"
    else:
        result_text = f"💨 Мимо. -{bet} ⭐"
    add_bet_record(msg.from_user.id, "basketball", bet, str(val), prize)
    u = get_user(msg.from_user.id)
    bot.reply_to(msg, f"{result_text}\nБаланс: <b>{u['balance']} ⭐</b>")

# ── БОУЛИНГ ─────────────────────────────
def _play_bowl(msg, bet):
    update_balance(msg.from_user.id, -bet)
    sent = bot.send_dice(msg.chat.id, emoji="🎳")
    time.sleep(3)
    val   = sent.dice.value
    prize = bet * 3 if val == 6 else (bet if val >= 4 else 0)
    if val == 6:
        result_text = f"🎳 Страйк! x3 +{prize} ⭐"
    elif val >= 4:
        result_text = f"🎳 Неплохо x1 +{prize} ⭐"
    else:
        result_text = f"😔 Промах. -{bet} ⭐"
    if prize > 0:
        update_balance(msg.from_user.id, prize)
    add_bet_record(msg.from_user.id, "bowling", bet, str(val), prize)
    u = get_user(msg.from_user.id)
    bot.reply_to(msg, f"{result_text}\nБаланс: <b>{u['balance']} ⭐</b>")

# ── ДАРТС ───────────────────────────────
def _play_darts(msg, bet):
    update_balance(msg.from_user.id, -bet)
    sent = bot.send_dice(msg.chat.id, emoji="🎯")
    time.sleep(3)
    val   = sent.dice.value
    prize = {6: bet*5, 5: bet*3, 4: bet*2, 3: bet, 2: 0, 1: 0}.get(val, 0)
    labels = {6:"🎯 Bullseye! x5", 5:"🔥 Отлично! x3", 4:"👍 Хорошо x2",
              3:"😐 Попал x1", 2:"😕 Почти...", 1:"💨 Мимо"}
    result_text = labels[val]
    if prize > 0:
        update_balance(msg.from_user.id, prize)
        result_text += f" +{prize} ⭐"
    else:
        result_text += f" -{bet} ⭐"
    add_bet_record(msg.from_user.id, "darts", bet, str(val), prize)
    u = get_user(msg.from_user.id)
    bot.reply_to(msg, f"{result_text}\nБаланс: <b>{u['balance']} ⭐</b>")

# ── КУБИК ───────────────────────────────
def _play_cube(msg, bet):
    update_balance(msg.from_user.id, -bet)
    sent     = bot.send_dice(msg.chat.id, emoji="🎲")
    time.sleep(2)
    bot_dice = bot.send_dice(msg.chat.id, emoji="🎲")
    time.sleep(2)
    pval  = sent.dice.value
    bval  = bot_dice.dice.value
    prize = 0
    if pval > bval:
        prize = bet * 2
        update_balance(msg.from_user.id, prize)
        result_text = f"🎲 Ты {pval} vs Бот {bval} — <b>Победа!</b> +{prize} ⭐"
    elif pval == bval:
        prize = bet
        update_balance(msg.from_user.id, prize)
        result_text = f"🎲 {pval} = {bval} — <b>Ничья</b>, возврат"
    else:
        result_text = f"🎲 Ты {pval} vs Бот {bval} — <b>Поражение</b> -{bet} ⭐"
    add_bet_record(msg.from_user.id, "dice", bet, f"{pval}vs{bval}", prize)
    u = get_user(msg.from_user.id)
    bot.reply_to(msg, f"{result_text}\nБаланс: <b>{u['balance']} ⭐</b>")

# ── ФУТБОЛ ──────────────────────────────
def _play_ftb(msg, bet):
    update_balance(msg.from_user.id, -bet)
    sent  = bot.send_dice(msg.chat.id, emoji="⚽")
    time.sleep(3)
    val   = sent.dice.value
    win   = val >= 3
    prize = bet * 2 if win else 0
    if win:
        update_balance(msg.from_user.id, prize)
        result_text = f"⚽ Гол! +{prize} ⭐"
    else:
        result_text = f"🧤 Вратарь взял. -{bet} ⭐"
    add_bet_record(msg.from_user.id, "football", bet, str(val), prize)
    u = get_user(msg.from_user.id)
    bot.reply_to(msg, f"{result_text}\nБаланс: <b>{u['balance']} ⭐</b>")

# ─────────────────────────────────────────
#  ПОПОЛНЕНИЕ (Telegram Stars)
# ─────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data == "deposit")
def cb_deposit(call):
    bot.answer_callback_query(call.id)
    sent = bot.send_message(call.message.chat.id,
        "💳 <b>Пополнение</b>\n\nВведи сумму в Stars:")
    bot.register_next_step_handler(sent, process_deposit_amount)

def process_deposit_amount(msg):
    try:
        amount = int(msg.text.strip())
    except:
        return bot.reply_to(msg, "Введи число. Пример: <code>200</code>")
    if amount < 1:
        return bot.reply_to(msg, "Минимум 1 ⭐")
    prices = [types.LabeledPrice(label=f"Пополнение {amount} ⭐", amount=amount)]
    bot.send_invoice(
        chat_id=msg.chat.id,
        title=f"Пополнение {amount} ⭐",
        description="Баланс в Casino боте",
        invoice_payload=f"deposit_{amount}",
        provider_token="",
        currency="XTR",
        prices=prices
    )

@bot.callback_query_handler(func=lambda c: c.data.startswith("pay_"))
def cb_pay(call):
    amount = int(call.data.split("_")[1])
    bot.answer_callback_query(call.id)
    prices = [types.LabeledPrice(label=f"Пополнение {amount} ⭐", amount=amount)]
    bot.send_invoice(
        chat_id=call.message.chat.id,
        title=f"Пополнение {amount} ⭐",
        description="Баланс в Casino боте",
        invoice_payload=f"deposit_{amount}",
        provider_token="",
        currency="XTR",
        prices=prices
    )

@bot.pre_checkout_query_handler(func=lambda q: True)
def pre_checkout(query):
    bot.answer_pre_checkout_query(query.id, ok=True)

@bot.message_handler(content_types=["successful_payment"])
def payment_done(msg):
    amount = msg.successful_payment.total_amount
    update_balance(msg.from_user.id, amount)
    u = get_user(msg.from_user.id)
    bot.send_message(msg.chat.id,
        f"✅ Пополнено <b>{amount} ⭐</b>\nБаланс: <b>{u['balance']} ⭐</b>")

# ─────────────────────────────────────────
#  ВЫВОД
# ─────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data == "withdraw")
def cb_withdraw(call):
    bot.answer_callback_query(call.id)
    u = get_user(call.from_user.id)
    if u["balance"] < 50:
        return bot.send_message(call.message.chat.id,
            "❌ Минимальный вывод — 50 ⭐\nТвой баланс: <b>{}</b> ⭐".format(u["balance"]))
    msg = bot.send_message(call.message.chat.id,
        f"💸 <b>Заявка на вывод</b>\n\nБаланс: <b>{u['balance']} ⭐</b>\n"
        "Введи сумму для вывода (мин. 50):")
    bot.register_next_step_handler(msg, process_withdraw_amount)

def process_withdraw_amount(msg):
    try:
        amount = int(msg.text.strip())
    except:
        return bot.send_message(msg.chat.id, "❌ Введи число.")
    if amount < 50:
        return bot.send_message(msg.chat.id, "❌ Минимальный вывод — 50 ⭐")
    u = get_user(msg.from_user.id)
    if u["balance"] < amount:
        return bot.send_message(msg.chat.id,
            f"❌ Недостаточно. Баланс: <b>{u['balance']} ⭐</b>")

    update_balance(msg.from_user.id, -amount)
    with db() as c:
        c.execute("INSERT INTO withdrawals (user_id,amount,created) VALUES (?,?,?)",
                  (msg.from_user.id, amount, now()))
        wid = c.lastrowid

    u = get_user(msg.from_user.id)
    username = f"@{u['username']}" if u['username'] else f"id{msg.from_user.id}"

    # уведомление игроку
    bot.send_message(msg.chat.id,
        f"✅ Заявка #{wid} принята.\nСумма: <b>{amount} ⭐</b>\nОжидай обработки.")

    # уведомление админу
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton("✅ Одобрить", callback_data=f"wd_ok_{wid}_{msg.from_user.id}_{amount}"),
        types.InlineKeyboardButton("❌ Отказать", callback_data=f"wd_no_{wid}_{msg.from_user.id}_{amount}")
    )
    try:
        bot.send_message(ADMIN_ID,
            f"💸 <b>Заявка на вывод #{wid}</b>\n"
            f"Игрок: {username} (id: <code>{msg.from_user.id}</code>)\n"
            f"Сумма: <b>{amount} ⭐</b>\n"
            f"Баланс после: <b>{u['balance']} ⭐</b>",
            reply_markup=kb)
    except Exception as e:
        print("Ошибка уведомления админа:", e)

@bot.callback_query_handler(func=lambda c: c.data.startswith("wd_"))
def cb_wd(call):
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "Нет доступа")
    parts   = call.data.split("_")
    action  = parts[1]          # ok / no
    wid     = int(parts[2])
    user_id = int(parts[3])
    amount  = int(parts[4])

    if action == "ok":
        with db() as c:
            c.execute("UPDATE withdrawals SET status='done' WHERE id=?", (wid,))
        bot.send_message(user_id,
            f"✅ Вывод #{wid} одобрен!\nСумма: <b>{amount} ⭐</b> будет отправлена.")
        bot.edit_message_text(
            f"✅ Вывод #{wid} — <b>ОДОБРЕН</b>\nСумма: {amount} ⭐",
            call.message.chat.id, call.message.message_id)
    else:
        # возврат средств
        update_balance(user_id, amount)
        with db() as c:
            c.execute("UPDATE withdrawals SET status='rejected' WHERE id=?", (wid,))
        bot.send_message(user_id,
            f"❌ Вывод #{wid} отклонён.\n{amount} ⭐ возвращены на баланс.")
        bot.edit_message_text(
            f"❌ Вывод #{wid} — <b>ОТКЛОНЁН</b>",
            call.message.chat.id, call.message.message_id)

    bot.answer_callback_query(call.id)

# ─────────────────────────────────────────
#  ТОП ИГРОКОВ
# ─────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data == "top")
def cb_top(call):
    bot.answer_callback_query(call.id)
    with db() as c:
        rows = c.execute(
            "SELECT username, total_bet, wins FROM users ORDER BY total_bet DESC LIMIT 10"
        ).fetchall()

    lines = ["🏆 <b>Топ по ставкам</b>\n"]
    medals = ["🥇","🥈","🥉"] + ["▪️"]*7
    for i, (uname, total, wins) in enumerate(rows):
        name = f"@{uname}" if uname else "Аноним"
        lines.append(f"{medals[i]} {name} — <b>{total} ⭐</b> ставок, {wins} побед")

    bot.send_message(call.message.chat.id, "\n".join(lines) if rows else "Пока нет данных.")

# ─────────────────────────────────────────
#  ПРОФИЛЬ
# ─────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data == "profile")
def cb_profile(call):
    bot.answer_callback_query(call.id)
    u  = get_user(call.from_user.id)
    total = u["wins"] + u["losses"]
    wr = round(u["wins"] / total * 100) if total else 0
    text = (
        f"📊 <b>Профиль</b>\n\n"
        f"Баланс: <b>{u['balance']} ⭐</b>\n"
        f"Всего ставок: <b>{u['total_bet']} ⭐</b>\n"
        f"Побед: <b>{u['wins']}</b>  |  Поражений: <b>{u['losses']}</b>\n"
        f"Винрейт: <b>{wr}%</b>"
    )
    bot.send_message(call.message.chat.id, text)

# ─────────────────────────────────────────
#  АДМИН: ручное начисление /add <id> <sum>
# ─────────────────────────────────────────
@bot.message_handler(commands=["add"])
def cmd_add(msg):
    if msg.from_user.id != ADMIN_ID:
        return
    parts = msg.text.split()
    if len(parts) < 3:
        return bot.reply_to(msg, "Формат: /add <user_id> <сумма>")
    uid, amount = int(parts[1]), int(parts[2])
    update_balance(uid, amount)
    bot.reply_to(msg, f"✅ Начислено {amount} ⭐ пользователю {uid}")
    try:
        bot.send_message(uid, f"🎁 Тебе начислено <b>{amount} ⭐</b> администратором!")
    except:
        pass

# ─────────────────────────────────────────
#  АДМИН: выгрузка и загрузка базы
#  /база         — скачать файл БД
#  /база загрузить — ответить на это сообщение файлом .db
# ─────────────────────────────────────────
@bot.message_handler(commands=["база", "db"])
def cmd_db(msg):
    if msg.from_user.id != ADMIN_ID:
        return

    parts = msg.text.strip().split()
    # /база загрузить — ждём файл
    if len(parts) > 1 and parts[1].lower() in ("загрузить", "load"):
        sent = bot.reply_to(msg,
            "📥 Отправь файл <b>casino.db</b> следующим сообщением.\n"
            "<b>⚠️ Текущая база будет перезаписана!</b>")
        bot.register_next_step_handler(sent, receive_db_file)
        return

    # /база — выгрузить
    if not os.path.exists(DB_PATH):
        return bot.reply_to(msg, "❌ Файл базы не найден.")

    with open(DB_PATH, "rb") as f:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        bot.send_document(
            msg.chat.id,
            f,
            caption=f"📦 База данных casino.db\n🕐 {ts}",
            visible_file_name=f"casino_{ts}.db"
        )

def receive_db_file(msg):
    if msg.from_user.id != ADMIN_ID:
        return
    if not msg.document:
        return bot.reply_to(msg, "❌ Это не файл. Отправь .db файл.")

    fname = msg.document.file_name or ""
    if not fname.endswith(".db"):
        return bot.reply_to(msg, "❌ Нужен файл с расширением .db")

    # Скачиваем
    file_info = bot.get_file(msg.document.file_id)
    downloaded = bot.download_file(file_info.file_path)

    # Бэкап текущей
    if os.path.exists(DB_PATH):
        backup = DB_PATH + ".bak"
        with open(backup, "wb") as f:
            with open(DB_PATH, "rb") as src:
                f.write(src.read())

    # Записываем новую
    with open(DB_PATH, "wb") as f:
        f.write(downloaded)

    bot.reply_to(msg,
        "✅ База успешно загружена!\n"
        "Старая сохранена как <code>casino.db.bak</code>")

# ─────────────────────────────────────────
#  АДМИН: SQL-консоль
#  /sql SELECT * FROM users
#  /sql UPDATE users SET balance=100 WHERE user_id=123
# ─────────────────────────────────────────
@bot.message_handler(commands=["sql"])
def cmd_sql(msg):
    if msg.from_user.id != ADMIN_ID:
        return

    query = msg.text.strip()[4:].strip()  # убираем "/sql"
    if not query:
        return bot.reply_to(msg,
            "Формат: <code>/sql ЗАПРОС</code>\n\n"
            "Примеры:\n"
            "<code>/sql SELECT * FROM users</code>\n"
            "<code>/sql SELECT * FROM withdrawals WHERE status='pending'</code>\n"
            "<code>/sql UPDATE users SET balance=500 WHERE user_id=123</code>\n"
            "<code>/sql DELETE FROM bets WHERE user_id=123</code>")

    try:
        with db() as c:
            cur = c.execute(query)
            # SELECT — возвращаем строки
            if cur.description:
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]

                if not rows:
                    return bot.reply_to(msg, "✅ Запрос выполнен. Строк не найдено.")

                # Форматируем таблицу
                lines = [" | ".join(cols)]
                lines.append("─" * len(lines[0]))
                for row in rows[:50]:  # лимит 50 строк
                    lines.append(" | ".join(str(v) if v is not None else "NULL" for v in row))

                result = "\n".join(lines)
                if len(rows) > 50:
                    result += f"\n\n⚠️ Показано 50 из {len(rows)} строк"

                # Если текст длинный — шлём файлом
                if len(result) > 3500:
                    import io
                    buf = io.BytesIO(result.encode())
                    buf.name = "result.txt"
                    bot.send_document(msg.chat.id, buf, caption=f"📊 {len(rows)} строк")
                else:
                    bot.reply_to(msg, f"<pre>{result}</pre>")

            # INSERT/UPDATE/DELETE — сообщаем сколько строк затронуто
            else:
                c.connection.commit()
                bot.reply_to(msg,
                    f"✅ Выполнено. Затронуто строк: <b>{cur.rowcount}</b>")

    except Exception as e:
        bot.reply_to(msg, f"❌ Ошибка:\n<code>{e}</code>")

# ─────────────────────────────────────────
#  KEEP-ALIVE (Render free tier)
# ─────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def ping():
    return "OK", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

# ─────────────────────────────────────────
#  ЗАПУСК
# ─────────────────────────────────────────
print("🎰 Casino bot запущен...")
threading.Thread(target=run_flask, daemon=True).start()

# Сбрасываем очередь чтобы не было 409
try:
    bot.delete_webhook(drop_pending_updates=True)
except Exception:
    bot.delete_webhook()

bot.infinity_polling()
