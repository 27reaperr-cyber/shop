"""
Telegram Shop Bot — aiogram v3
Full-featured shop: catalog (товары/услуги), payments, referrals, admin panel
"""

import asyncio
import json
import logging
import sqlite3
import os
import sys
import shutil
import aiohttp
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
BOT_TOKEN       = os.getenv("BOT_TOKEN", "")
ADMIN_ID        = int(os.getenv("ADMIN_ID", "0"))
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "")
BANK_CARD       = os.getenv("BANK_CARD", "79000000000")
BANK_NAME       = os.getenv("BANK_NAME", "Т-Банк")
BANK_RECEIVER   = os.getenv("BANK_RECEIVER", "Имя Ф.")
BOT_USERNAME    = os.getenv("BOT_USERNAME", "myshopbot")
REFERRAL_PCT    = float(os.getenv("REFERRAL_PCT", "5"))

DB_PATH = "shop.db"

# ── Константы главного меню ───────────────────
MENU_BUY     = "✦ Купить"
MENU_PROFILE = "◈ Профиль"
MENU_ABOUT   = "✹ О шопе"
MENU_SUPPORT = "♱ Поддержка"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            username    TEXT,
            full_name   TEXT,
            balance     REAL DEFAULT 0.0,
            total_spent REAL DEFAULT 0.0,
            purchases   INTEGER DEFAULT 0,
            reg_date    TEXT NOT NULL,
            referrer_id INTEGER DEFAULT NULL
        );

        CREATE TABLE IF NOT EXISTS categories (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            name  TEXT NOT NULL,
            emoji TEXT DEFAULT '✦'
        );

        CREATE TABLE IF NOT EXISTS products (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id    INTEGER NOT NULL,
            name           TEXT NOT NULL,
            description    TEXT,
            price          REAL NOT NULL,
            stock          INTEGER DEFAULT -1,
            is_active      INTEGER DEFAULT 1,
            type           TEXT DEFAULT 'product',
            prod_file      TEXT,
            form_questions TEXT,
            allow_repurchase INTEGER DEFAULT 0,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );

        CREATE TABLE IF NOT EXISTS purchases (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            price      REAL NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS payments (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            amount     REAL NOT NULL,
            method     TEXT NOT NULL,
            status     TEXT DEFAULT 'pending',
            invoice_id TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS referrals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER NOT NULL,
            user_id     INTEGER NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS service_orders (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            status     TEXT DEFAULT 'pending',
            answers    TEXT,
            created_at TEXT NOT NULL
        );
    """)
    conn.commit()

    # Migrate existing DB: add new columns if they don't exist
    for col, definition in [
        ("type",           "TEXT DEFAULT 'product'"),
        ("prod_file",      "TEXT"),
        ("form_questions",    "TEXT"),
        ("allow_repurchase", "INTEGER DEFAULT 0"),
    ]:
        try:
            c.execute(f"ALTER TABLE products ADD COLUMN {col} {definition}")
            conn.commit()
        except Exception:
            pass  # column already exists

    # Seed demo data if empty
    if not c.execute("SELECT id FROM categories LIMIT 1").fetchone():
        c.executemany("INSERT INTO categories (name, emoji) VALUES (?,?)", [
            ("Аккаунты", "☽"),
            ("VPN",      "✦"),
            ("Игры",     "⬡"),
            ("Услуги",   "♱"),
        ])
        conn.commit()
        c.executemany(
            "INSERT INTO products (category_id, name, description, price, type) VALUES (?,?,?,?,?)",
            [
                (1, "Instagram аккаунт", "Аккаунт с подтверждённой почтой, возраст 1+ год", 299.0, "product"),
                (2, "VPN на 1 месяц",    "Быстрый VPN, 50+ серверов, без логов",            149.0, "product"),
                (4, "Настройка ПК",      "Удалённая настройка компьютера под ваши задачи",  499.0, "service"),
            ]
        )
        conn.commit()
    conn.close()

# ─── DB helpers ───────────────────────────────
def db_get_user(telegram_id: int):
    with get_db() as conn:
        return conn.execute("SELECT * FROM users WHERE telegram_id=?", (telegram_id,)).fetchone()

def db_create_user(telegram_id: int, username: str, full_name: str, referrer_id=None):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (telegram_id, username, full_name, reg_date, referrer_id) "
            "VALUES (?,?,?,?,?)",
            (telegram_id, username, full_name, now, referrer_id)
        )
        conn.commit()
    return db_get_user(telegram_id)

def db_get_or_create_user(telegram_id: int, username: str, full_name: str, referrer_id=None):
    u = db_get_user(telegram_id)
    return u if u else db_create_user(telegram_id, username, full_name, referrer_id)

def db_update_balance(telegram_id: int, delta: float):
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET balance = balance + ? WHERE telegram_id=?", (delta, telegram_id)
        )
        conn.commit()

def db_add_referral(referrer_db_id: int, user_db_id: int):
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO referrals (referrer_id, user_id) VALUES (?,?)",
            (referrer_db_id, user_db_id)
        )
        conn.commit()

def db_referral_stats(telegram_id: int):
    with get_db() as conn:
        u = conn.execute("SELECT id FROM users WHERE telegram_id=?", (telegram_id,)).fetchone()
        if not u:
            return 0, 0.0
        count = conn.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (u['id'],)
        ).fetchone()[0]
        earned = conn.execute(
            """SELECT COALESCE(SUM(p.amount * ? / 100), 0)
               FROM payments p
               JOIN referrals r ON r.user_id = p.user_id
               WHERE r.referrer_id = ? AND p.status='confirmed'""",
            (REFERRAL_PCT, u['id'])
        ).fetchone()[0]
        return count, earned

def db_get_setting(key: str):
    with get_db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row['value'] if row else None

def db_set_setting(key: str, value: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value)
        )
        conn.commit()

def db_del_setting(key: str):
    with get_db() as conn:
        conn.execute("DELETE FROM settings WHERE key=?", (key,))
        conn.commit()

def db_already_purchased(user_db_id: int, product_id: int) -> bool:
    """True если пользователь уже покупал этот товар/услугу."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM purchases WHERE user_id=? AND product_id=? LIMIT 1",
            (user_db_id, product_id)
        ).fetchone()
    return row is not None

def db_has_active_service(user_db_id: int) -> bool:
    """Проверяет, есть ли у пользователя активная/ожидающая услуга."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM service_orders WHERE user_id=? AND status IN ('pending','active')",
            (user_db_id,)
        ).fetchone()
    return row is not None

def db_get_active_service(user_db_id: int):
    with get_db() as conn:
        return conn.execute(
            "SELECT so.*, p.name AS pname FROM service_orders so "
            "JOIN products p ON p.id=so.product_id "
            "WHERE so.user_id=? AND so.status IN ('pending','active') LIMIT 1",
            (user_db_id,)
        ).fetchone()

# ── Утилиты для файлов ────────────────────────
def encode_file(file_id: str, file_type: str) -> str:
    return f"{file_type}:{file_id}"

def decode_file(raw: str):
    """Возвращает (file_type, file_id) или (None, None)."""
    if not raw or ":" not in raw:
        return None, None
    parts = raw.split(":", 1)
    return parts[0], parts[1]

async def send_product_file(bot: Bot, chat_id: int, raw_file: str, caption: str = ""):
    """Отправляет файл товара пользователю."""
    file_type, file_id = decode_file(raw_file)
    if not file_type:
        return
    try:
        if file_type == "photo":
            await bot.send_photo(chat_id, file_id, caption=caption, parse_mode=ParseMode.HTML)
        elif file_type == "video":
            await bot.send_video(chat_id, file_id, caption=caption, parse_mode=ParseMode.HTML)
        elif file_type == "audio":
            await bot.send_audio(chat_id, file_id, caption=caption, parse_mode=ParseMode.HTML)
        elif file_type == "animation":
            await bot.send_animation(chat_id, file_id, caption=caption, parse_mode=ParseMode.HTML)
        else:
            await bot.send_document(chat_id, file_id, caption=caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        log.error(f"send_product_file error: {e}")

def extract_file_from_msg(msg: Message):
    """Извлекает file_id и тип из сообщения."""
    if msg.photo:
        return msg.photo[-1].file_id, "photo"
    if msg.video:
        return msg.video.file_id, "video"
    if msg.audio:
        return msg.audio.file_id, "audio"
    if msg.animation:
        return msg.animation.file_id, "animation"
    if msg.document:
        return msg.document.file_id, "document"
    return None, None

# ─────────────────────────────────────────────
#  KEYBOARDS
# ─────────────────────────────────────────────
def kb_main():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=MENU_BUY),     KeyboardButton(text=MENU_PROFILE)],
        [KeyboardButton(text=MENU_ABOUT),   KeyboardButton(text=MENU_SUPPORT)],
    ], resize_keyboard=True)

def kb_profile():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✦ Пополнить баланс", callback_data="topup")],
        [InlineKeyboardButton(text="☽ Рефералка",        callback_data="referral"),
         InlineKeyboardButton(text="⇄ Передать баланс",  callback_data="transfer")],
        [InlineKeyboardButton(text="◈ Мои покупки",      callback_data="my_purchases")],
    ])

def kb_topup():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="♱ По реквизитам", callback_data="topup_bank")],
        [InlineKeyboardButton(text="✦ Crypto Bot",     callback_data="topup_crypto")],
        [InlineKeyboardButton(text="← Назад",          callback_data="profile_back")],
    ])

def kb_bank_paid(payment_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔ Я оплатил — прислать чек", callback_data=f"bank_paid:{payment_id}")],
        [InlineKeyboardButton(text="← Отмена",                   callback_data="topup")],
    ])

def kb_crypto(invoice_url: str, payment_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✦ Оплатить",         url=invoice_url)],
        [InlineKeyboardButton(text="⟳ Проверить оплату", callback_data=f"check_crypto:{payment_id}")],
        [InlineKeyboardButton(text="← Отмена",           callback_data="topup")],
    ])

def kb_categories(cats):
    rows = [[InlineKeyboardButton(
        text=f"{cat['emoji']} {cat['name']}",
        callback_data=f"cat:{cat['id']}"
    )] for cat in cats]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_products(prods, cat_id: int):
    rows = []
    for p in prods:
        icon = "◈" if p['type'] == 'service' else "◦"
        rows.append([InlineKeyboardButton(
            text=f"{icon} {p['name']} — {p['price']:.2f}₽",
            callback_data=f"product:{p['id']}"
        )])
    rows.append([InlineKeyboardButton(text="← Категории", callback_data="catalog")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_buy_product(product_id: int, cat_id: int, ptype: str = "product"):
    label = "✔ Заказать услугу" if ptype == "service" else "✔ Купить"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label,    callback_data=f"buy:{product_id}")],
        [InlineKeyboardButton(text="← Назад", callback_data=f"cat:{cat_id}")],
    ])

def kb_service_locked(order_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◦ Статус услуги", callback_data=f"svc_status:{order_id}")],
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◈ Товары",          callback_data="adm_products"),
         InlineKeyboardButton(text="⬡ Категории",       callback_data="adm_categories")],
        [InlineKeyboardButton(text="☽ Пользователи",    callback_data="adm_users"),
         InlineKeyboardButton(text="✦ Статистика",      callback_data="adm_stats")],
        [InlineKeyboardButton(text="♱ Заявки оплат",    callback_data="adm_payments")],
        [InlineKeyboardButton(text="✹ Заявки услуг",    callback_data="adm_svc_orders")],
        [InlineKeyboardButton(text="⇢ Рассылка",        callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="☁︎ GIF при старте",  callback_data="adm_start_gif")],
        [InlineKeyboardButton(text="◈ База данных",      callback_data="adm_database")],
    ])

def kb_admin_products():
    with get_db() as conn:
        prods = conn.execute(
            "SELECT p.*, c.name AS cat_name FROM products p "
            "JOIN categories c ON c.id=p.category_id WHERE p.is_active=1"
        ).fetchall()
    rows = [[InlineKeyboardButton(text="✦ Добавить", callback_data="adm_add_product")]]
    for p in prods:
        icon = "♱" if p['type'] == 'service' else "◦"
        rows.append([InlineKeyboardButton(
            text=f"{icon} {p['name']} ({p['price']:.0f}₽)",
            callback_data=f"adm_edit_prod:{p['id']}"
        )])
    rows.append([InlineKeyboardButton(text="← Назад", callback_data="admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_admin_categories():
    with get_db() as conn:
        cats = conn.execute("SELECT * FROM categories").fetchall()
    rows = [[InlineKeyboardButton(text="✦ Добавить категорию", callback_data="adm_add_category")]]
    for cat in cats:
        rows.append([InlineKeyboardButton(
            text=f"◦ {cat['emoji']} {cat['name']}",
            callback_data=f"adm_edit_cat:{cat['id']}"
        )])
    rows.append([InlineKeyboardButton(text="← Назад", callback_data="admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_edit_product(product_id: int, ptype: str = "product", allow_repurchase: int = 0):
    repurchase_label = "◦ Повт. покупка: ✔ Вкл" if allow_repurchase else "◦ Повт. покупка: ✕ Выкл"
    rows = [
        [InlineKeyboardButton(text="◦ Название",  callback_data=f"adm_pname:{product_id}"),
         InlineKeyboardButton(text="◦ Описание",  callback_data=f"adm_pdesc:{product_id}")],
        [InlineKeyboardButton(text="◦ Цена",      callback_data=f"adm_pprice:{product_id}"),
         InlineKeyboardButton(text="✕ Удалить",   callback_data=f"adm_pdel:{product_id}")],
        [InlineKeyboardButton(text=repurchase_label, callback_data=f"adm_toggle_repurchase:{product_id}")],
    ]
    if ptype == "product":
        rows.append([InlineKeyboardButton(text="◦ Файл товара", callback_data=f"adm_pfile:{product_id}")])
    else:
        rows.append([InlineKeyboardButton(text="◦ Форма заявки", callback_data=f"adm_pform:{product_id}")])
    rows.append([InlineKeyboardButton(text="← Назад", callback_data="adm_products")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_edit_category(cat_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◦ Название", callback_data=f"adm_cname:{cat_id}"),
         InlineKeyboardButton(text="◦ Символ",   callback_data=f"adm_cemoji:{cat_id}")],
        [InlineKeyboardButton(text="✕ Удалить",  callback_data=f"adm_cdel:{cat_id}")],
        [InlineKeyboardButton(text="← Назад",    callback_data="adm_categories")],
    ])

def kb_confirm_payment(payment_id: int, user_db_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✔ Подтвердить", callback_data=f"adm_confirm:{payment_id}:{user_db_id}"),
        InlineKeyboardButton(text="✕ Отклонить",   callback_data=f"adm_reject:{payment_id}:{user_db_id}"),
    ]])

def kb_service_order_admin(order_id: int, user_tg_id: int, status: str):
    rows = []
    if status == "pending":
        rows.append([InlineKeyboardButton(
            text="⇢ Взять в работу", callback_data=f"adm_svc_active:{order_id}"
        )])
    if status in ("pending", "active"):
        rows.append([InlineKeyboardButton(
            text="✔ Завершить услугу", callback_data=f"adm_svc_done:{order_id}"
        )])
        rows.append([InlineKeyboardButton(
            text="✕ Отменить услугу (возврат)", callback_data=f"adm_svc_cancel:{order_id}"
        )])
    rows.append([InlineKeyboardButton(
        text="☛ Написать клиенту", url=f"tg://user?id={user_tg_id}"
    )])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ─────────────────────────────────────────────
#  FSM STATES
# ─────────────────────────────────────────────
class TopupStates(StatesGroup):
    amount_bank   = State()
    bank_receipt  = State()
    amount_crypto = State()

class TransferStates(StatesGroup):
    target_id = State()
    amount    = State()

class ServiceStates(StatesGroup):
    answering = State()  # пользователь отвечает на вопросы формы

class ConfirmStates(StatesGroup):
    confirm_buy = State()   # ожидание подтверждения покупки

class AdminStates(StatesGroup):
    give_balance_id      = State()
    give_balance_amount  = State()
    add_category_name    = State()
    add_category_emoji   = State()
    edit_cat_name        = State()
    edit_cat_emoji       = State()
    add_product_cat      = State()
    add_product_name     = State()
    add_product_desc     = State()
    add_product_price    = State()
    add_product_type     = State()
    edit_prod_name       = State()
    edit_prod_desc       = State()
    edit_prod_price      = State()
    edit_prod_file       = State()
    edit_prod_form       = State()
    broadcast_text       = State()
    set_start_gif        = State()
    upload_db            = State()

# ─────────────────────────────────────────────
#  CRYPTO BOT API
# ─────────────────────────────────────────────
CRYPTOBOT_API = "https://pay.crypt.bot/api"

async def get_usdt_rate() -> float:
    """
    Получаем курс USDT → RUB через CryptoBot API.
    CryptoBot возвращает пары вида {source, target, rate} где
    rate = сколько target за 1 source.
    Нам нужна пара USDT/RUB (сколько рублей за 1 USDT).
    Фолбэк: 90.0.
    """
    if CRYPTOBOT_TOKEN:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{CRYPTOBOT_API}/getExchangeRates",
                    headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    data = await r.json()
                    items = data.get("result", [])
                    for item in items:
                        src = item.get("source", "").upper()
                        tgt = item.get("target", "").upper()
                        raw = item.get("rate", "0")
                        rate = float(raw)
                        # Прямая пара: USDT → RUB
                        if src == "USDT" and tgt == "RUB" and rate > 1:
                            log.info(f"USDT rate from CryptoBot (USDT→RUB): {rate}")
                            return rate
                    # Обратная пара: RUB → USDT (1 RUB = X USDT → 1 USDT = 1/X RUB)
                    for item in items:
                        src = item.get("source", "").upper()
                        tgt = item.get("target", "").upper()
                        raw = item.get("rate", "0")
                        rate = float(raw)
                        if src == "RUB" and tgt == "USDT" and rate > 0:
                            inverted = round(1.0 / rate, 4)
                            log.info(f"USDT rate from CryptoBot (RUB→USDT inverted): {inverted}")
                            return inverted
        except Exception as e:
            log.warning(f"CryptoBot rate error: {e}")
    log.warning("Using hardcoded USDT rate: 90.0")
    return 90.0

async def create_crypto_invoice(amount_rub: float):
    if not CRYPTOBOT_TOKEN:
        return None
    try:
        rate        = await get_usdt_rate()
        amount_usdt = round(amount_rub / rate, 2)
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{CRYPTOBOT_API}/createInvoice",
                headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                json={"asset": "USDT", "amount": str(amount_usdt),
                      "description": f"Пополнение баланса {amount_rub:.2f}₽", "expires_in": 3600},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
                if data.get("ok"):
                    inv = data["result"]
                    return {"pay_url": inv["pay_url"], "invoice_id": inv["invoice_id"],
                            "rate": rate, "usdt": amount_usdt}
    except Exception as e:
        log.error(f"Crypto invoice error: {e}")
    return None

async def check_crypto_invoice(invoice_id: str) -> bool:
    if not CRYPTOBOT_TOKEN:
        return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{CRYPTOBOT_API}/getInvoices",
                headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                params={"invoice_ids": invoice_id},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data  = await r.json()
                items = data.get("result", {}).get("items", [])
                return bool(items) and items[0].get("status") == "paid"
    except Exception as e:
        log.error(f"Crypto check error: {e}")
    return False

# ─────────────────────────────────────────────
#  ROUTER
# ─────────────────────────────────────────────
router = Router()

# ── /start ────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(msg: Message):
    args      = msg.text.split()
    ref_tg_id = None
    if len(args) > 1:
        try:
            ref_tg_id = int(args[1])
            if ref_tg_id == msg.from_user.id:
                ref_tg_id = None
        except ValueError:
            pass

    ref_db_id = None
    if ref_tg_id:
        ref_user = db_get_user(ref_tg_id)
        if ref_user:
            ref_db_id = ref_user['id']

    user = db_get_or_create_user(
        msg.from_user.id,
        msg.from_user.username or "",
        msg.from_user.full_name or "",
        referrer_id=ref_db_id
    )
    if ref_db_id:
        db_add_referral(ref_db_id, user['id'])

    text = (
        f"☁︎ Добро пожаловать, <b>{msg.from_user.first_name}</b> :D\n\n"
        f"❝dreinn.shop❞\n\n"
        f"◦ используй меню ниже"
    )
    gif_file_id = db_get_setting("start_gif")
    if gif_file_id:
        await msg.answer_animation(animation=gif_file_id, caption=text,
                                   parse_mode=ParseMode.HTML, reply_markup=kb_main())
    else:
        await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb_main())

# ── Главное меню ──────────────────────────────
@router.message(F.text == MENU_ABOUT)
async def about(msg: Message):
    await msg.answer(
        "✹ <b>О нашем шопе</b>\n\n"
        "❝dreinn.shop❞\n\n"
        "◦ ✔ Все товары проверены вручную\n"
        "◦ ♱ Поддержка 24/7\n"
        "◦ ♡ Гарантия на все покупки\n",
        parse_mode=ParseMode.HTML
    )

@router.message(F.text == MENU_SUPPORT)
async def support(msg: Message):
    await msg.answer(
        "♱ <b>Поддержка</b>\n\n"
        "Если возникли вопросы:\n\n"
        "→ @ke9ab\n\n"
        "◦ время ответа до 2 часов",
        parse_mode=ParseMode.HTML
    )

# ── Профиль ───────────────────────────────────
async def _profile_text(telegram_id: int) -> str:
    user = db_get_user(telegram_id)
    reg  = user['reg_date'][:10].replace("-", "·")
    return (
        f"© <b>Профиль</b>\n"
        f"{'─' * 22}\n"
        f"☛ ID: <code>{telegram_id}</code>\n"
        f"✯ Баланс: <b>{user['balance']:.2f}₽</b>\n"
        f"⬈ Покупок: <b>{user['purchases']}</b>\n"
        f"⬊ Потрачено: <b>{user['total_spent']:.2f}₽</b>\n"
        f"༄ Рега: <b>{reg}</b>"
    )

@router.message(F.text == MENU_PROFILE)
async def show_profile(msg: Message):
    db_get_or_create_user(msg.from_user.id, msg.from_user.username or "", msg.from_user.full_name or "")
    await msg.answer(
        await _profile_text(msg.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_profile()
    )

@router.callback_query(F.data == "profile_back")
async def profile_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text(
        await _profile_text(call.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_profile()
    )

# ── Пополнение ────────────────────────────────
@router.callback_query(F.data == "topup")
async def topup_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text(
        "✦ <b>Пополнение баланса</b>\n\n◦ выбери способ оплаты",
        parse_mode=ParseMode.HTML, reply_markup=kb_topup()
    )

@router.callback_query(F.data == "topup_bank")
async def topup_bank_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(TopupStates.amount_bank)
    await call.message.edit_text(
        "♱ <b>Оплата по реквизитам</b>\n\n◦ введи сумму пополнения (минимум 10₽)",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="topup")]
        ])
    )

@router.message(TopupStates.amount_bank)
async def topup_bank_amount(msg: Message, state: FSMContext):
    try:
        amount = float(msg.text.replace(",", ".").replace(" ", ""))
        if amount < 10:
            await msg.answer("◦ минимальная сумма — 10₽"); return
    except ValueError:
        await msg.answer("◦ введи корректную сумму"); return

    user = db_get_user(msg.from_user.id)
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO payments (user_id, amount, method, status, created_at) VALUES (?,?,?,?,?)",
            (user['id'], amount, "bank", "pending", now)
        )
        conn.commit()
        payment_id = cur.lastrowid

    await state.update_data(bank_payment_id=payment_id)
    await state.set_state(TopupStates.bank_receipt)
    await msg.answer(
        f"♱ <b>Реквизиты для оплаты</b>\n{'─'*22}\n"
        f"◦ Банк: <b>{BANK_NAME}</b>\n"
        f"◦ Реквизиты: <code>{BANK_CARD}</code>\n"
        f"◦ Получатель: <b>{BANK_RECEIVER}</b>\n"
        f"◦ Сумма: <b>{amount:.2f}₽</b>\n\n"
        f"☛ переведи точную сумму, затем нажми кнопку ниже",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_bank_paid(payment_id)
    )

@router.callback_query(F.data.startswith("bank_paid:"))
async def bank_paid(call: CallbackQuery, state: FSMContext):
    payment_id = int(call.data.split(":")[1])
    with get_db() as conn:
        pay = conn.execute("SELECT * FROM payments WHERE id=?", (payment_id,)).fetchone()
    if not pay or pay['status'] != 'pending':
        await call.answer("◦ заявка уже отправлена или обработана", show_alert=True); return

    await state.update_data(bank_payment_id=payment_id)
    await state.set_state(TopupStates.bank_receipt)
    await call.message.edit_text(
        "♱ <b>Подтверждение оплаты</b>\n\n"
        "◦ пришли скриншот или фото чека оплаты\n"
        "◦ поддерживаются фото и документы\n\n"
        "☛ просто отправь файл в этот чат",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="topup")]
        ])
    )
    await call.answer()

@router.message(TopupStates.bank_receipt, F.photo | F.document)
async def topup_bank_receipt(msg: Message, state: FSMContext, bot: Bot):
    data       = await state.get_data()
    payment_id = data.get("bank_payment_id")
    if not payment_id:
        await state.clear(); await msg.answer("◦ что-то пошло не так, начни заново"); return

    with get_db() as conn:
        updated = conn.execute(
            "UPDATE payments SET status='sent' WHERE id=? AND status='pending'", (payment_id,)
        ).rowcount
        conn.commit()
    if not updated:
        await state.clear(); await msg.answer("◦ заявка уже была отправлена ранее"); return

    await state.clear()
    user    = db_get_user(msg.from_user.id)
    with get_db() as conn:
        pay = conn.execute("SELECT * FROM payments WHERE id=?", (payment_id,)).fetchone()

    await msg.answer(
        "✔ <b>Чек получен, заявка отправлена</b>\n\n"
        "◦ проверим платёж и зачислим средства в течение нескольких минут",
        parse_mode=ParseMode.HTML
    )
    caption = (
        f"✦ <b>Новая заявка на пополнение</b>\n{'─'*22}\n"
        f"☛ <a href='tg://user?id={msg.from_user.id}'>{msg.from_user.full_name}</a>\n"
        f"◦ ID: <code>{msg.from_user.id}</code>\n"
        f"◦ Сумма: <b>{pay['amount']:.2f}₽</b>\n◦ Метод: реквизиты"
    )
    try:
        await bot.send_message(ADMIN_ID, caption, parse_mode=ParseMode.HTML,
                               reply_markup=kb_confirm_payment(payment_id, user['id']))
        if msg.photo:
            await bot.send_photo(ADMIN_ID, msg.photo[-1].file_id)
        elif msg.document:
            await bot.send_document(ADMIN_ID, msg.document.file_id)
    except Exception as e:
        log.error(f"Cannot notify admin: {e}")

@router.message(TopupStates.bank_receipt)
async def topup_bank_receipt_wrong(msg: Message):
    await msg.answer(
        "◦ нужно прислать <b>фото</b> или <b>документ</b>\n☛ просто отправь файл в чат",
        parse_mode=ParseMode.HTML
    )

# — Crypto —
@router.callback_query(F.data == "topup_crypto")
async def topup_crypto_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(TopupStates.amount_crypto)
    await call.message.edit_text(
        "✦ <b>Crypto Bot</b>\n\n◦ введи сумму пополнения в рублях",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="topup")]
        ])
    )

@router.message(TopupStates.amount_crypto)
async def topup_crypto_amount(msg: Message, state: FSMContext):
    try:
        amount = float(msg.text.replace(",", ".").replace(" ", ""))
        if amount < 10:
            await msg.answer("◦ минимальная сумма — 10₽"); return
    except ValueError:
        await msg.answer("◦ введи корректную сумму"); return

    await state.clear()
    inv = await create_crypto_invoice(amount)
    if not inv:
        await msg.answer("◦ CryptoBot не настроен. Обратитесь к администратору."); return

    user = db_get_user(msg.from_user.id)
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO payments (user_id, amount, method, status, invoice_id, created_at) VALUES (?,?,?,?,?,?)",
            (user['id'], amount, "crypto", "pending", str(inv['invoice_id']), now)
        )
        conn.commit()
        payment_id = cur.lastrowid

    await msg.answer(
        f"✦ <b>Оплата через Crypto Bot</b>\n{'─'*22}\n"
        f"◦ Пополнение: <b>{amount:.2f}₽</b> (~<b>{inv['usdt']} USDT</b>)\n"
        f"◦ Курс: <b>{inv['rate']:.2f}₽</b>\n\n"
        f"☛ нажми оплатить → переведи USDT → проверь оплату",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_crypto(inv['pay_url'], payment_id)
    )

@router.callback_query(F.data.startswith("check_crypto:"))
async def check_crypto(call: CallbackQuery, bot: Bot):
    payment_id = int(call.data.split(":")[1])
    with get_db() as conn:
        pay = conn.execute("SELECT * FROM payments WHERE id=?", (payment_id,)).fetchone()
    if not pay:
        await call.answer("платёж не найден", show_alert=True); return
    if pay['status'] == 'confirmed':
        await call.answer("✔ уже зачислено!", show_alert=True); return

    paid = await check_crypto_invoice(str(pay['invoice_id']))
    if paid:
        with get_db() as conn:
            conn.execute("UPDATE payments SET status='confirmed' WHERE id=?", (payment_id,))
            conn.commit()
        db_update_balance(call.from_user.id, pay['amount'])
        user = db_get_user(call.from_user.id)
        if user['referrer_id']:
            ref_bonus = pay['amount'] * REFERRAL_PCT / 100
            with get_db() as conn:
                ref_tg = conn.execute(
                    "SELECT telegram_id FROM users WHERE id=?", (user['referrer_id'],)
                ).fetchone()
            if ref_tg:
                db_update_balance(ref_tg['telegram_id'], ref_bonus)
                try:
                    await bot.send_message(ref_tg['telegram_id'],
                        f"✦ реферальный бонус +<b>{ref_bonus:.2f}₽</b>", parse_mode=ParseMode.HTML)
                except Exception:
                    pass
        await call.message.edit_text(
            f"✔ <b>Оплата подтверждена</b>\n\n◦ баланс пополнен на <b>{pay['amount']:.2f}₽</b>",
            parse_mode=ParseMode.HTML
        )
    else:
        await call.answer("◦ оплата ещё не найдена. попробуй через минуту", show_alert=True)

# ── Рефералка ─────────────────────────────────
@router.callback_query(F.data == "referral")
async def referral_info(call: CallbackQuery):
    count, earned = db_referral_stats(call.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start={call.from_user.id}"
    await call.message.edit_text(
        f"☽ <b>Реферальная программа</b>\n{'─'*22}\n"
        f"◦ за каждое пополнение реферала — <b>{REFERRAL_PCT:.0f}%</b> тебе\n\n"
        f"☛ твоя ссылка:\n<code>{ref_link}</code>\n\n"
        f"✹ статистика\n◦ рефералов: <b>{count}</b>\n◦ заработано: <b>{earned:.2f}₽</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="profile_back")]
        ])
    )

# ── Передача баланса ──────────────────────────
@router.callback_query(F.data == "transfer")
async def transfer_start(call: CallbackQuery, state: FSMContext):
    user = db_get_user(call.from_user.id)
    if db_has_active_service(user['id']):
        await call.answer("◦ передача недоступна — у вас активная услуга", show_alert=True); return
    await state.set_state(TransferStates.target_id)
    await call.message.edit_text(
        "⇄ <b>Передача баланса</b>\n\n◦ введи Telegram ID получателя",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="profile_back")]
        ])
    )

@router.message(TransferStates.target_id)
async def transfer_target(msg: Message, state: FSMContext):
    try:
        target_id = int(msg.text.strip())
    except ValueError:
        await msg.answer("◦ введи числовой ID"); return
    if target_id == msg.from_user.id:
        await msg.answer("◦ нельзя перевести самому себе"); return
    if not db_get_user(target_id):
        await msg.answer("◦ пользователь не найден"); return
    await state.update_data(target_id=target_id)
    await state.set_state(TransferStates.amount)
    await msg.answer(
        f"◦ переводим пользователю <code>{target_id}</code>\n\n☛ введи сумму:",
        parse_mode=ParseMode.HTML
    )

@router.message(TransferStates.amount)
async def transfer_amount(msg: Message, state: FSMContext, bot: Bot):
    try:
        amount = float(msg.text.replace(",", ".").replace(" ", ""))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await msg.answer("◦ введи корректную сумму"); return

    data      = await state.get_data()
    target_id = data['target_id']
    sender    = db_get_user(msg.from_user.id)
    if sender['balance'] < amount:
        await msg.answer(f"◦ недостаточно средств. баланс: {sender['balance']:.2f}₽")
        await state.clear(); return

    db_update_balance(msg.from_user.id, -amount)
    db_update_balance(target_id, amount)
    await state.clear()
    await msg.answer(f"✔ переведено <b>{amount:.2f}₽</b> → <code>{target_id}</code>",
                     parse_mode=ParseMode.HTML)
    try:
        await bot.send_message(target_id,
            f"✦ вам переведено <b>{amount:.2f}₽</b> от <code>{msg.from_user.id}</code>",
            parse_mode=ParseMode.HTML)
    except Exception:
        pass

# ── Мои покупки ───────────────────────────────
@router.callback_query(F.data == "my_purchases")
async def my_purchases(call: CallbackQuery):
    user = db_get_user(call.from_user.id)
    with get_db() as conn:
        purchases = conn.execute(
            "SELECT pu.*, pr.name AS pname, pr.type AS ptype, pr.prod_file "
            "FROM purchases pu "
            "JOIN products pr ON pr.id=pu.product_id "
            "WHERE pu.user_id=? ORDER BY pu.created_at DESC LIMIT 20",
            (user['id'],)
        ).fetchall()

    if not purchases:
        text = "◈ <b>Мои покупки</b>\n\n◦ покупок пока нет"
    else:
        lines = ["◈ <b>Мои покупки</b>\n"]
        for p in purchases:
            date = p['created_at'][:10]
            icon = "♱" if p['ptype'] == 'service' else "◦"
            lines.append(f"{icon} {p['pname']} — <b>{p['price']:.2f}₽</b> <i>({date})</i>")
        text = "\n".join(lines)

    kb_rows = []
    # Если есть активная услуга — показать статус
    active = db_get_active_service(user['id'])
    if active:
        status_map = {"pending": "⏳ ожидает", "active": "⇢ в работе"}
        kb_rows.append([InlineKeyboardButton(
            text=f"♱ {active['pname']} — {status_map.get(active['status'], active['status'])}",
            callback_data=f"svc_status:{active['id']}"
        )])
    # Кнопки «Получить файл» для товаров с загруженным файлом (уникальные product_id)
    seen_products = set()
    for p in purchases:
        if p['ptype'] == 'product' and p['prod_file'] and p['product_id'] not in seen_products:
            seen_products.add(p['product_id'])
            kb_rows.append([InlineKeyboardButton(
                text=f"◦ Получить файл: {p['pname'][:28]}",
                callback_data=f"resend_file:{p['product_id']}"
            )])
    kb_rows.append([InlineKeyboardButton(text="← Назад", callback_data="profile_back")])
    await call.message.edit_text(text, parse_mode=ParseMode.HTML,
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("resend_file:"))
async def resend_file(call: CallbackQuery, bot: Bot):
    """Повторно отправляет файл товара пользователю из раздела Мои Покупки."""
    product_id = int(call.data.split(":")[1])
    user = db_get_user(call.from_user.id)
    # Проверяем что пользователь реально покупал этот товар
    if not db_already_purchased(user['id'], product_id):
        await call.answer("◦ этот товар не найден в ваших покупках", show_alert=True); return
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not p or not p['prod_file']:
        await call.answer("◦ файл для этого товара пока не загружен", show_alert=True); return
    await call.answer("◦ отправляем файл…")
    await send_product_file(
        bot, call.from_user.id, p['prod_file'],
        caption=f"◦ файл к товару <b>{p['name']}</b>"
    )

@router.callback_query(F.data.startswith("svc_status:"))
async def svc_status(call: CallbackQuery):
    order_id = int(call.data.split(":")[1])
    with get_db() as conn:
        order = conn.execute(
            "SELECT so.*, p.name AS pname FROM service_orders so "
            "JOIN products p ON p.id=so.product_id WHERE so.id=?", (order_id,)
        ).fetchone()
    if not order:
        await call.answer("◦ заказ не найден", show_alert=True); return
    status_map = {"pending": "⏳ ожидает рассмотрения", "active": "⇢ выполняется", "done": "✔ завершена", "cancelled": "✕ отменена"}
    kb_rows = []
    # Пользователь может отменить только pending-заявку (ещё не взята в работу)
    if order['status'] == 'pending':
        kb_rows.append([InlineKeyboardButton(
            text="✕ Отменить услугу (возврат средств)",
            callback_data=f"user_svc_cancel:{order_id}"
        )])
    kb_rows.append([InlineKeyboardButton(text="← Назад", callback_data="my_purchases")])
    await call.message.edit_text(
        f"♱ <b>{order['pname']}</b>\n{'─'*22}\n"
        f"◦ статус: <b>{status_map.get(order['status'], order['status'])}</b>\n\n"
        f"◦ администратор свяжется с вами для уточнения деталей",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows)
    )

@router.callback_query(F.data.startswith("user_svc_cancel:"))
async def user_svc_cancel(call: CallbackQuery, state: FSMContext, bot: Bot):
    """Пользователь отменяет услугу (только pending)."""
    order_id = int(call.data.split(":")[1])
    await state.clear()
    with get_db() as conn:
        order = conn.execute("SELECT * FROM service_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order['status'] != 'pending':
            await call.answer("◦ отмена недоступна — услуга уже взята в работу", show_alert=True); return
        p = conn.execute("SELECT * FROM products WHERE id=?", (order['product_id'],)).fetchone()
        # Возвращаем деньги
        conn.execute("UPDATE service_orders SET status='cancelled' WHERE id=?", (order_id,))
        conn.execute(
            "UPDATE users SET balance=balance+?, purchases=purchases-1, total_spent=total_spent-? WHERE id=?",
            (p['price'], p['price'], order['user_id'])
        )
        conn.execute(
            "DELETE FROM purchases WHERE user_id=? AND product_id=? AND id=("
            "SELECT id FROM purchases WHERE user_id=? AND product_id=? ORDER BY id DESC LIMIT 1)",
            (order['user_id'], order['product_id'], order['user_id'], order['product_id'])
        )
        conn.commit()
        u = conn.execute("SELECT telegram_id FROM users WHERE id=?", (order['user_id'],)).fetchone()
    await call.message.edit_text(
        f"✕ <b>Услуга отменена</b>\n\n"
        f"◦ <b>{p['price']:.2f}₽</b> возвращены на ваш баланс",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← в каталог", callback_data="catalog")]
        ])
    )
    # Уведомляем администратора
    try:
        await bot.send_message(
            ADMIN_ID,
            f"✕ <b>Услуга отменена пользователем</b>\n{'─'*22}\n"
            f"◦ Заявка #{order_id}: <b>{p['name']}</b>\n"
            f"◦ Пользователь: <a href='tg://user?id={u['telegram_id']}'>{u['telegram_id']}</a>\n"
            f"◦ Возврат: <b>{p['price']:.2f}₽</b>",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

# ─────────────────────────────────────────────
#  КАТАЛОГ
# ─────────────────────────────────────────────
async def _check_service_lock(user_db_id: int) -> str | None:
    """Возвращает сообщение о блокировке или None."""
    order = db_get_active_service(user_db_id)
    if order:
        status_map = {"pending": "ожидает рассмотрения", "active": "выполняется"}
        return (
            f"♱ у вас есть активная услуга: <b>{order['pname']}</b>\n"
            f"◦ статус: {status_map.get(order['status'], order['status'])}\n\n"
            f"☛ каталог доступен после завершения услуги"
        )
    return None

@router.message(F.text == MENU_BUY)
async def show_catalog(msg: Message):
    user = db_get_or_create_user(msg.from_user.id, msg.from_user.username or "",
                                  msg.from_user.full_name or "")
    lock = await _check_service_lock(user['id'])
    if lock:
        await msg.answer(lock, parse_mode=ParseMode.HTML,
                         reply_markup=kb_service_locked(
                             db_get_active_service(user['id'])['id'])); return
    with get_db() as conn:
        cats = conn.execute("SELECT * FROM categories").fetchall()
    if not cats:
        await msg.answer("◦ каталог пока пуст"); return
    await msg.answer("✹ <b>Каталог</b>\n\n◦ выбери категорию",
                     parse_mode=ParseMode.HTML, reply_markup=kb_categories(cats))

@router.callback_query(F.data == "catalog")
async def catalog_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    with get_db() as conn:
        cats = conn.execute("SELECT * FROM categories").fetchall()
    await call.message.edit_text("✹ <b>Каталог</b>\n\n◦ выбери категорию",
                                  parse_mode=ParseMode.HTML, reply_markup=kb_categories(cats))

@router.callback_query(F.data.startswith("cat:"))
async def show_category(call: CallbackQuery):
    cat_id = int(call.data.split(":")[1])
    with get_db() as conn:
        cat   = conn.execute("SELECT * FROM categories WHERE id=?", (cat_id,)).fetchone()
        prods = conn.execute(
            "SELECT * FROM products WHERE category_id=? AND is_active=1", (cat_id,)
        ).fetchall()
    if not prods:
        await call.answer("в этой категории нет товаров", show_alert=True); return
    await call.message.edit_text(
        f"{cat['emoji']} <b>{cat['name']}</b>\n\n"
        f"◦ Товар / ◈ Услуга\n\n◦ выбери позицию",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_products(prods, cat_id)
    )

@router.callback_query(F.data.startswith("product:"))
async def show_product(call: CallbackQuery):
    product_id = int(call.data.split(":")[1])
    user = db_get_user(call.from_user.id)
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    ptype   = p['type'] or 'product'
    type_badge     = "♱ Услуга" if ptype == 'service' else "◦ Товар"
    allow_repurchase = p['allow_repurchase'] or 0
    already = db_already_purchased(user['id'], product_id) if user else False
    # Блокируем повторную покупку только если allow_repurchase выключен
    if already and not allow_repurchase:
        label = "✔ Уже куплено"
        if ptype == 'service':
            label = "✔ Услуга уже заказана"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data="noop")],
            [InlineKeyboardButton(text="← Назад", callback_data=f"cat:{p['category_id']}")],
        ])
        badge_note = "\n◦ <i>вы уже приобрели этот товар</i>"
    else:
        kb = kb_buy_product(product_id, p['category_id'], ptype)
        badge_note = ("\n◦ <i>повторная покупка разрешена</i>" if already and allow_repurchase else "")
    await call.message.edit_text(
        f"{'♱' if ptype=='service' else '◈'} <b>{p['name']}</b>\n"
        f"{'─'*22}\n"
        f"{p['description']}\n\n"
        f"◦ Тип: {type_badge}\n"
        f"✯ Цена: <b>{p['price']:.2f}₽</b>{badge_note}",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )

# ── Заглушка для неактивных кнопок ───────────
@router.callback_query(F.data == "noop")
async def noop(call: CallbackQuery):
    await call.answer()

# ── Покупка: шаг 1 — показываем подтверждение ──
@router.callback_query(F.data.startswith("buy:"))
async def buy_product(call: CallbackQuery, state: FSMContext):
    product_id = int(call.data.split(":")[1])
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not p or not p['is_active']:
        await call.answer("◦ товар недоступен", show_alert=True); return

    user  = db_get_user(call.from_user.id)
    ptype = p['type'] or 'product'
    allow_repurchase = p['allow_repurchase'] or 0

    # Проверка: уже куплено (только если повторная покупка выключена)
    if db_already_purchased(user['id'], product_id) and not allow_repurchase:
        await call.answer("◦ вы уже приобрели этот товар", show_alert=True); return

    # Блокировка при активной услуге
    if db_has_active_service(user['id']):
        await call.answer("◦ у вас есть активная услуга — дождитесь её завершения",
                          show_alert=True); return

    if user['balance'] < p['price']:
        await call.answer(
            f"◦ недостаточно средств\nнужно: {p['price']:.2f}₽\nбаланс: {user['balance']:.2f}₽",
            show_alert=True); return

    # Сохраняем данные и показываем подтверждение
    await state.update_data(confirm_product_id=product_id)
    await state.set_state(ConfirmStates.confirm_buy)
    type_word  = "услуги" if ptype == 'service' else "товара"
    action_btn = "✔ Заказать услугу" if ptype == 'service' else "✔ Подтвердить покупку"
    await call.message.edit_text(
        f"◦ <b>Подтверждение {type_word}</b>\n{'─'*22}\n"
        f"{'♱' if ptype=='service' else '◈'} <b>{p['name']}</b>\n\n"
        f"✯ Цена: <b>{p['price']:.2f}₽</b>\n"
        f"◦ Баланс после: <b>{user['balance'] - p['price']:.2f}₽</b>\n\n"
        f"☛ подтвердите покупку",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=action_btn,   callback_data=f"buy_confirm:{product_id}")],
            [InlineKeyboardButton(text="✕ Отмена",   callback_data=f"product:{product_id}")],
        ])
    )

# ── Покупка: шаг 2 — подтверждение, реальная транзакция ──
@router.callback_query(F.data.startswith("buy_confirm:"), ConfirmStates.confirm_buy)
async def buy_confirm(call: CallbackQuery, state: FSMContext, bot: Bot):
    product_id = int(call.data.split(":")[1])
    await state.clear()

    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not p or not p['is_active']:
        await call.answer("◦ товар недоступен", show_alert=True); return

    user  = db_get_user(call.from_user.id)
    ptype = p['type'] or 'product'
    allow_repurchase = p['allow_repurchase'] or 0

    if db_already_purchased(user['id'], product_id) and not allow_repurchase:
        await call.answer("◦ вы уже приобрели этот товар", show_alert=True); return
    if db_has_active_service(user['id']):
        await call.answer("◦ у вас есть активная услуга", show_alert=True); return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Атомарное списание
    with get_db() as conn:
        fresh = conn.execute(
            "SELECT balance FROM users WHERE telegram_id=?", (call.from_user.id,)
        ).fetchone()
        if not fresh or fresh['balance'] < p['price']:
            await call.answer("◦ недостаточно средств", show_alert=True); return
        conn.execute(
            "UPDATE users SET balance=balance-?, purchases=purchases+1, total_spent=total_spent+? "
            "WHERE telegram_id=?",
            (p['price'], p['price'], call.from_user.id)
        )
        conn.execute(
            "INSERT INTO purchases (user_id, product_id, price, created_at) VALUES (?,?,?,?)",
            (user['id'], product_id, p['price'], now)
        )
        conn.commit()

    if ptype == 'product':
        await call.message.edit_text(
            f"✔ <b>Покупка совершена</b>\n\n"
            f"◦ товар: <b>{p['name']}</b>\n"
            f"◦ списано: <b>{p['price']:.2f}₽</b>\n\n"
            f"♡ спасибо за покупку",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← в каталог", callback_data="catalog")]
            ])
        )
        if p['prod_file']:
            await send_product_file(
                bot, call.from_user.id, p['prod_file'],
                caption=f"◦ файл к товару <b>{p['name']}</b>"
            )

    else:
        # УСЛУГА — создаём заявку, запускаем форму
        with get_db() as conn:
            cur = conn.execute(
                "INSERT INTO service_orders (user_id, product_id, status, created_at) VALUES (?,?,?,?)",
                (user['id'], product_id, "pending", now)
            )
            conn.commit()
            order_id = cur.lastrowid

        questions = []
        if p['form_questions']:
            try:
                questions = json.loads(p['form_questions'])
            except Exception:
                pass

        if not questions:
            await call.message.edit_text(
                f"✔ <b>Услуга оформлена</b>\n\n"
                f"◦ услуга: <b>{p['name']}</b>\n"
                f"◦ списано: <b>{p['price']:.2f}₽</b>\n\n"
                f"♱ администратор свяжется с вами в ближайшее время",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="◦ Статус услуги", callback_data=f"svc_status:{order_id}")]
                ])
            )
            await _notify_admin_service(bot, call.from_user, p, order_id, {})
        else:
            await state.update_data(
                svc_order_id=order_id,
                svc_product_name=p['name'],
                svc_questions=questions,
                svc_current_q=0,
                svc_answers=[]
            )
            await state.set_state(ServiceStates.answering)
            await call.message.edit_text(
                f"♱ <b>Оформление услуги — {p['name']}</b>\n\n"
                f"◦ ответь на несколько вопросов\n"
                f"◦ вопрос 1 из {len(questions)}\n\n"
                f"<b>{questions[0]}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✕ Отмена (услуга возвращена)", callback_data=f"svc_cancel:{order_id}")]
                ])
            )

# ── Обработка формы услуги ────────────────────
@router.message(ServiceStates.answering)
async def service_form_answer(msg: Message, state: FSMContext, bot: Bot):
    data      = await state.get_data()
    questions = data['svc_questions']
    current   = data['svc_current_q']
    answers   = data['svc_answers'] + [msg.text.strip()]
    order_id  = data['svc_order_id']
    pname     = data['svc_product_name']

    if current + 1 < len(questions):
        # Следующий вопрос
        next_q = current + 1
        await state.update_data(svc_current_q=next_q, svc_answers=answers)
        await msg.answer(
            f"◦ вопрос {next_q + 1} из {len(questions)}\n\n"
            f"<b>{questions[next_q]}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✕ Отмена (услуга возвращена)", callback_data=f"svc_cancel:{order_id}")]
            ])
        )
    else:
        # Форма завершена
        await state.clear()
        qa = {questions[i]: answers[i] for i in range(len(questions))}
        with get_db() as conn:
            conn.execute(
                "UPDATE service_orders SET answers=? WHERE id=?",
                (json.dumps(qa, ensure_ascii=False), order_id)
            )
            conn.commit()

        await msg.answer(
            f"✔ <b>Форма заполнена!</b>\n\n"
            f"♱ услуга <b>{pname}</b> оформлена\n\n"
            f"◦ администратор рассмотрит заявку и свяжется с вами\n"
            f"◦ каталог будет доступен после завершения услуги",
            parse_mode=ParseMode.HTML
        )
        with get_db() as conn:
            p = conn.execute("SELECT * FROM products WHERE id=?",
                             (conn.execute("SELECT product_id FROM service_orders WHERE id=?",
                              (order_id,)).fetchone()['product_id'],)).fetchone()
        await _notify_admin_service(bot, msg.from_user, p, order_id, qa)

@router.callback_query(F.data.startswith("svc_cancel:"))
async def svc_cancel(call: CallbackQuery, state: FSMContext, bot: Bot):
    order_id = int(call.data.split(":")[1])
    await state.clear()
    with get_db() as conn:
        order = conn.execute("SELECT * FROM service_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order['status'] != 'pending':
            await call.answer("◦ отмена невозможна", show_alert=True); return
        # Возвращаем деньги
        p = conn.execute("SELECT * FROM products WHERE id=?", (order['product_id'],)).fetchone()
        conn.execute("DELETE FROM service_orders WHERE id=?", (order_id,))
        conn.execute(
            "UPDATE users SET balance=balance+?, purchases=purchases-1, total_spent=total_spent-? "
            "WHERE id=?",
            (p['price'], p['price'], order['user_id'])
        )
        conn.execute(
            "DELETE FROM purchases WHERE user_id=? AND product_id=? ORDER BY id DESC LIMIT 1",
            (order['user_id'], order['product_id'])
        )
        conn.commit()
    await call.message.edit_text(
        f"✕ <b>Услуга отменена</b>\n\n◦ <b>{p['price']:.2f}₽</b> возвращены на баланс",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← в каталог", callback_data="catalog")]
        ])
    )

async def _notify_admin_service(bot: Bot, from_user, product, order_id: int, qa: dict):
    """Уведомляет администратора о новой заявке на услугу."""
    lines = [
        f"✹ <b>Новая заявка на услугу</b>\n{'─'*22}",
        f"☛ <a href='tg://user?id={from_user.id}'>{from_user.full_name}</a>",
        f"◦ ID: <code>{from_user.id}</code>",
        f"◦ Услуга: <b>{product['name']}</b>",
        f"◦ Сумма: <b>{product['price']:.2f}₽</b>",
    ]
    if qa:
        lines.append("\n◦ <b>Ответы на форму:</b>")
        for q, a in qa.items():
            lines.append(f"  ❝{q}❞\n  → {a}")
    try:
        with get_db() as conn:
            tg_row = conn.execute(
                "SELECT u.telegram_id FROM service_orders so JOIN users u ON u.id=so.user_id WHERE so.id=?",
                (order_id,)
            ).fetchone()
        user_tg = tg_row['telegram_id'] if tg_row else from_user.id
        await bot.send_message(
            ADMIN_ID, "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=kb_service_order_admin(order_id, user_tg, "pending")
        )
    except Exception as e:
        log.error(f"Cannot notify admin about service: {e}")

# ─────────────────────────────────────────────
#  ADMIN PANEL
# ─────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if not is_admin(msg.from_user.id):
        await msg.answer("✕ нет доступа"); return
    await msg.answer("✹ <b>Админ панель</b>\n\n◦ что делаем?",
                     parse_mode=ParseMode.HTML, reply_markup=kb_admin())

@router.callback_query(F.data == "admin_back")
async def admin_back(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.clear()
    await call.message.edit_text("✹ <b>Админ панель</b>",
                                  parse_mode=ParseMode.HTML, reply_markup=kb_admin())

# ── Статистика ────────────────────────────────
@router.callback_query(F.data == "adm_stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    with get_db() as conn:
        users_n  = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        revenue  = conn.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='confirmed'").fetchone()[0]
        purch_n  = conn.execute("SELECT COUNT(*) FROM purchases").fetchone()[0]
        pending  = conn.execute("SELECT COUNT(*) FROM payments WHERE status IN ('pending','sent')").fetchone()[0]
        svc_pend = conn.execute("SELECT COUNT(*) FROM service_orders WHERE status='pending'").fetchone()[0]
        svc_act  = conn.execute("SELECT COUNT(*) FROM service_orders WHERE status='active'").fetchone()[0]
    await call.message.edit_text(
        f"✦ <b>Статистика</b>\n{'─'*22}\n"
        f"◦ пользователей: <b>{users_n}</b>\n"
        f"◦ выручка: <b>{revenue:.2f}₽</b>\n"
        f"◦ покупок: <b>{purch_n}</b>\n"
        f"◦ ждут оплаты: <b>{pending}</b>\n"
        f"◦ услуги (ожидают): <b>{svc_pend}</b>\n"
        f"◦ услуги (в работе): <b>{svc_act}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="admin_back")]
        ])
    )

# ── Пользователи ──────────────────────────────
@router.callback_query(F.data == "adm_users")
async def adm_users(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.clear()
    with get_db() as conn:
        users = conn.execute("SELECT * FROM users ORDER BY id DESC LIMIT 15").fetchall()
    lines = ["☽ <b>Пользователи</b>\n"]
    for u in users:
        name = (u['full_name'] or "—")[:20]
        lines.append(f"◦ <code>{u['telegram_id']}</code>  {name}  {u['balance']:.0f}₽")
    await call.message.edit_text(
        "\n".join(lines), parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✦ Выдать баланс", callback_data="adm_give_balance")],
            [InlineKeyboardButton(text="← Назад",         callback_data="admin_back")],
        ])
    )

@router.callback_query(F.data == "adm_give_balance")
async def adm_give_balance_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.set_state(AdminStates.give_balance_id)
    await call.message.edit_text(
        "☛ введи Telegram ID пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="adm_users")]
        ])
    )

@router.message(AdminStates.give_balance_id)
async def adm_give_balance_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        await msg.answer("◦ введи числовой ID"); return
    if not db_get_user(uid):
        await msg.answer("◦ пользователь не найден"); return
    await state.update_data(target_uid=uid)
    await state.set_state(AdminStates.give_balance_amount)
    await msg.answer(f"◦ введи сумму для <code>{uid}</code>:", parse_mode=ParseMode.HTML)

@router.message(AdminStates.give_balance_amount)
async def adm_give_balance_amount(msg: Message, state: FSMContext, bot: Bot):
    if not is_admin(msg.from_user.id): return
    try:
        amount = float(msg.text.replace(",", "."))
    except ValueError:
        await msg.answer("◦ введи сумму числом"); return
    data = await state.get_data()
    uid  = data['target_uid']
    db_update_balance(uid, amount)
    await state.clear()
    await msg.answer(f"✔ выдано <b>{amount:.2f}₽</b> → <code>{uid}</code>",
                     parse_mode=ParseMode.HTML)
    try:
        await bot.send_message(uid, f"✦ вам начислено <b>{amount:.2f}₽</b> от администратора",
                               parse_mode=ParseMode.HTML)
    except Exception:
        pass

# ── Заявки оплат ──────────────────────────────
@router.callback_query(F.data == "adm_payments")
async def adm_payments(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    with get_db() as conn:
        pays = conn.execute(
            "SELECT p.*, u.telegram_id FROM payments p JOIN users u ON u.id=p.user_id "
            "WHERE p.status IN ('pending','sent') ORDER BY p.created_at DESC"
        ).fetchall()
    if not pays:
        await call.message.edit_text(
            "✔ нет ожидающих заявок",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data="admin_back")]
            ])
        ); return
    await call.answer()
    for pay in pays:
        await call.message.answer(
            f"♱ <b>Заявка #{pay['id']}</b>\n"
            f"◦ TG: <code>{pay['telegram_id']}</code>\n"
            f"◦ сумма: <b>{pay['amount']:.2f}₽</b>\n"
            f"◦ метод: {pay['method']}\n◦ дата: {pay['created_at'][:16]}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_confirm_payment(pay['id'], pay['user_id'])
        )

@router.callback_query(F.data.startswith("adm_confirm:"))
async def adm_confirm_payment(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return
    parts      = call.data.split(":")
    payment_id = int(parts[1])
    user_db_id = int(parts[2])

    with get_db() as conn:
        pay = conn.execute("SELECT * FROM payments WHERE id=?", (payment_id,)).fetchone()
        u   = conn.execute("SELECT telegram_id FROM users WHERE id=?", (user_db_id,)).fetchone()
        if pay and pay['status'] in ('pending', 'sent'):
            conn.execute("UPDATE payments SET status='confirmed' WHERE id=?", (payment_id,))
            conn.commit()

    if pay and u:
        db_update_balance(u['telegram_id'], pay['amount'])
        with get_db() as conn:
            user_row = conn.execute("SELECT * FROM users WHERE id=?", (user_db_id,)).fetchone()
        if user_row and user_row['referrer_id']:
            ref_bonus = pay['amount'] * REFERRAL_PCT / 100
            with get_db() as conn:
                ref_tg = conn.execute(
                    "SELECT telegram_id FROM users WHERE id=?", (user_row['referrer_id'],)
                ).fetchone()
            if ref_tg:
                db_update_balance(ref_tg['telegram_id'], ref_bonus)
                try:
                    await bot.send_message(ref_tg['telegram_id'],
                        f"✦ реферальный бонус +<b>{ref_bonus:.2f}₽</b>", parse_mode=ParseMode.HTML)
                except Exception: pass
        try:
            await bot.send_message(u['telegram_id'],
                f"✔ платёж подтверждён. баланс пополнен на <b>{pay['amount']:.2f}₽</b>",
                parse_mode=ParseMode.HTML)
        except Exception: pass
        try:
            await call.message.delete()
        except Exception:
            await call.message.edit_text(f"✔ платёж #{payment_id} подтверждён")
        await bot.send_message(ADMIN_ID, f"✔ платёж #{payment_id} подтверждён — {pay['amount']:.2f}₽")
    await call.answer("✔ подтверждено")

@router.callback_query(F.data.startswith("adm_reject:"))
async def adm_reject_payment(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return
    parts      = call.data.split(":")
    payment_id = int(parts[1])
    user_db_id = int(parts[2])
    with get_db() as conn:
        u = conn.execute("SELECT telegram_id FROM users WHERE id=?", (user_db_id,)).fetchone()
        conn.execute("UPDATE payments SET status='rejected' WHERE id=?", (payment_id,))
        conn.commit()
    if u:
        try:
            await bot.send_message(u['telegram_id'],
                "✕ ваш платёж отклонён. обратитесь в поддержку.")
        except Exception: pass
    try:
        await call.message.delete()
    except Exception:
        await call.message.edit_text(f"✕ платёж #{payment_id} отклонён")
    await bot.send_message(ADMIN_ID, f"✕ платёж #{payment_id} отклонён")
    await call.answer("✕ отклонено")

# ── Заявки услуг (admin) ──────────────────────
@router.callback_query(F.data == "adm_svc_orders")
async def adm_svc_orders(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.clear()
    with get_db() as conn:
        orders = conn.execute(
            "SELECT so.*, p.name AS pname, u.telegram_id, u.full_name AS uname "
            "FROM service_orders so "
            "JOIN products p ON p.id=so.product_id "
            "JOIN users u ON u.id=so.user_id "
            "WHERE so.status IN ('pending','active') ORDER BY so.created_at DESC"
        ).fetchall()
    if not orders:
        await call.message.edit_text(
            "✔ нет активных заявок на услуги",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data="admin_back")]
            ])
        ); return
    await call.answer()
    for o in orders:
        status_map = {"pending": "⏳ ожидает", "active": "⇢ в работе"}
        answers_text = ""
        if o['answers']:
            try:
                qa = json.loads(o['answers'])
                answers_text = "\n" + "\n".join(f"  ❝{q}❞\n  → {a}" for q, a in qa.items())
            except Exception:
                pass
        await call.message.answer(
            f"♱ <b>Заявка #{o['id']}</b> — {status_map.get(o['status'], o['status'])}\n"
            f"◦ клиент: <a href='tg://user?id={o['telegram_id']}'>{o['uname']}</a>\n"
            f"◦ ID: <code>{o['telegram_id']}</code>\n"
            f"◦ услуга: <b>{o['pname']}</b>\n"
            f"◦ дата: {o['created_at'][:16]}"
            f"{answers_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_service_order_admin(o['id'], o['telegram_id'], o['status'])
        )

@router.callback_query(F.data.startswith("adm_svc_active:"))
async def adm_svc_active(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return
    order_id = int(call.data.split(":")[1])
    with get_db() as conn:
        order = conn.execute("SELECT * FROM service_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order['status'] != 'pending':
            await call.answer("◦ статус уже изменён", show_alert=True); return
        conn.execute("UPDATE service_orders SET status='active' WHERE id=?", (order_id,))
        conn.commit()
        u = conn.execute("SELECT telegram_id FROM users WHERE id=?", (order['user_id'],)).fetchone()
        p = conn.execute("SELECT name FROM products WHERE id=?", (order['product_id'],)).fetchone()
    if u:
        try:
            await bot.send_message(u['telegram_id'],
                f"⇢ <b>Ваша услуга взята в работу!</b>\n\n"
                f"♱ <b>{p['name']}</b>\n\n"
                f"◦ администратор свяжется с вами для уточнения деталей",
                parse_mode=ParseMode.HTML)
        except Exception: pass
    await call.message.edit_text(
        f"✔ заявка #{order_id} взята в работу",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✔ Завершить услугу",
                                  callback_data=f"adm_svc_done:{order_id}")],
            [InlineKeyboardButton(text="☛ Написать клиенту",
                                  url=f"tg://user?id={u['telegram_id'] if u else 0}")]
        ])
    )
    await call.answer()

@router.callback_query(F.data.startswith("adm_svc_done:"))
async def adm_svc_done(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return
    order_id = int(call.data.split(":")[1])
    with get_db() as conn:
        order = conn.execute("SELECT * FROM service_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order['status'] == 'done':
            await call.answer("◦ уже завершено", show_alert=True); return
        conn.execute("UPDATE service_orders SET status='done' WHERE id=?", (order_id,))
        conn.commit()
        u = conn.execute("SELECT telegram_id FROM users WHERE id=?", (order['user_id'],)).fetchone()
        p = conn.execute("SELECT name FROM products WHERE id=?", (order['product_id'],)).fetchone()
    if u:
        try:
            await bot.send_message(u['telegram_id'],
                f"✔ <b>Услуга выполнена!</b>\n\n"
                f"♱ <b>{p['name']}</b>\n\n"
                f"◦ каталог снова доступен\n♡ спасибо что выбрали нас!",
                parse_mode=ParseMode.HTML)
        except Exception: pass
    await call.message.edit_text(f"✔ услуга #{order_id} завершена. пользователь разблокирован.")
    await call.answer()

# ── Отмена услуги администратором ────────────
@router.callback_query(F.data.startswith("adm_svc_cancel:"))
async def adm_svc_cancel(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return
    order_id = int(call.data.split(":")[1])
    with get_db() as conn:
        order = conn.execute("SELECT * FROM service_orders WHERE id=?", (order_id,)).fetchone()
        if not order or order['status'] in ('done', 'cancelled'):
            await call.answer("◦ уже завершено или отменено", show_alert=True); return
        p = conn.execute("SELECT * FROM products WHERE id=?", (order['product_id'],)).fetchone()
        u = conn.execute("SELECT * FROM users WHERE id=?", (order['user_id'],)).fetchone()
        # Возвращаем деньги пользователю
        conn.execute("UPDATE service_orders SET status='cancelled' WHERE id=?", (order_id,))
        conn.execute(
            "UPDATE users SET balance=balance+?, purchases=purchases-1, total_spent=total_spent-? WHERE id=?",
            (p['price'], p['price'], order['user_id'])
        )
        conn.execute(
            "DELETE FROM purchases WHERE user_id=? AND product_id=? AND id=("
            "SELECT id FROM purchases WHERE user_id=? AND product_id=? ORDER BY id DESC LIMIT 1)",
            (order['user_id'], order['product_id'], order['user_id'], order['product_id'])
        )
        conn.commit()
    # Уведомляем пользователя
    if u:
        try:
            await bot.send_message(
                u['telegram_id'],
                f"✕ <b>Ваша услуга отменена администратором</b>\n\n"
                f"♱ <b>{p['name']}</b>\n\n"
                f"◦ <b>{p['price']:.2f}₽</b> возвращены на ваш баланс\n"
                f"◦ каталог снова доступен",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    await call.message.edit_text(
        f"✕ услуга #{order_id} отменена. <b>{p['price']:.2f}₽</b> возвращены пользователю.",
        parse_mode=ParseMode.HTML
    )
    await call.answer()

# ── Категории (admin) ─────────────────────────
@router.callback_query(F.data == "adm_categories")
async def adm_categories(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.clear()
    await call.message.edit_text("⬡ <b>Категории</b>", parse_mode=ParseMode.HTML,
                                  reply_markup=kb_admin_categories())

@router.callback_query(F.data == "adm_add_category")
async def adm_add_category(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.set_state(AdminStates.add_category_name)
    await call.message.edit_text(
        "◦ введи название новой категории:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="adm_categories")]
        ])
    )

@router.message(AdminStates.add_category_name)
async def adm_add_category_name(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    await state.update_data(cat_name=msg.text.strip())
    await state.set_state(AdminStates.add_category_emoji)
    await msg.answer("◦ введи символ для категории (например ✦ ☽ ◈):")

@router.message(AdminStates.add_category_emoji)
async def adm_add_category_emoji(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute("INSERT INTO categories (name, emoji) VALUES (?,?)",
                     (data['cat_name'], msg.text.strip()))
        conn.commit()
    await state.clear()
    await msg.answer(f"✔ категория ❝{data['cat_name']}❞ добавлена")

@router.callback_query(F.data.startswith("adm_edit_cat:"))
async def adm_edit_cat(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    cat_id = int(call.data.split(":")[1])
    with get_db() as conn:
        cat = conn.execute("SELECT * FROM categories WHERE id=?", (cat_id,)).fetchone()
    await call.message.edit_text(
        f"◦ категория: {cat['emoji']} <b>{cat['name']}</b>",
        parse_mode=ParseMode.HTML, reply_markup=kb_edit_category(cat_id)
    )

@router.callback_query(F.data.startswith("adm_cname:"))
async def adm_edit_cname_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    cat_id = int(call.data.split(":")[1])
    await state.update_data(edit_cat_id=cat_id)
    await state.set_state(AdminStates.edit_cat_name)
    await call.message.edit_text("◦ введи новое название категории:")

@router.message(AdminStates.edit_cat_name)
async def adm_edit_cname(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute("UPDATE categories SET name=? WHERE id=?",
                     (msg.text.strip(), data['edit_cat_id']))
        conn.commit()
    await state.clear()
    await msg.answer("✔ название обновлено")

@router.callback_query(F.data.startswith("adm_cemoji:"))
async def adm_edit_cemoji_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    cat_id = int(call.data.split(":")[1])
    await state.update_data(edit_cat_id=cat_id)
    await state.set_state(AdminStates.edit_cat_emoji)
    await call.message.edit_text("◦ введи новый символ:")

@router.message(AdminStates.edit_cat_emoji)
async def adm_edit_cemoji(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute("UPDATE categories SET emoji=? WHERE id=?",
                     (msg.text.strip(), data['edit_cat_id']))
        conn.commit()
    await state.clear()
    await msg.answer("✔ символ обновлён")

@router.callback_query(F.data.startswith("adm_cdel:"))
async def adm_del_category(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    cat_id = int(call.data.split(":")[1])
    with get_db() as conn:
        conn.execute("DELETE FROM categories WHERE id=?", (cat_id,))
        conn.execute("UPDATE products SET is_active=0 WHERE category_id=?", (cat_id,))
        conn.commit()
    await call.message.edit_text(
        "✕ категория удалена",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="adm_categories")]
        ])
    )

# ── Товары (admin) ────────────────────────────
@router.callback_query(F.data == "adm_products")
async def adm_products(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.clear()
    await call.message.edit_text("◈ <b>Товары и услуги</b>",
                                  parse_mode=ParseMode.HTML, reply_markup=kb_admin_products())

@router.callback_query(F.data == "adm_add_product")
async def adm_add_product_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    with get_db() as conn:
        cats = conn.execute("SELECT * FROM categories").fetchall()
    if not cats:
        await call.answer("сначала создай хотя бы одну категорию!", show_alert=True); return
    rows = [[InlineKeyboardButton(
        text=f"{c['emoji']} {c['name']}", callback_data=f"adm_prodcat:{c['id']}"
    )] for c in cats]
    rows.append([InlineKeyboardButton(text="← Отмена", callback_data="adm_products")])
    await state.set_state(AdminStates.add_product_cat)
    await call.message.edit_text("◦ выбери категорию:",
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data.startswith("adm_prodcat:"), AdminStates.add_product_cat)
async def adm_add_product_cat(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    await state.update_data(new_prod_cat=cat_id)
    await state.set_state(AdminStates.add_product_name)
    await call.message.edit_text("◦ введи название:")

@router.message(AdminStates.add_product_name)
async def adm_add_product_name(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    await state.update_data(new_prod_name=msg.text.strip())
    await state.set_state(AdminStates.add_product_desc)
    await msg.answer("◦ введи описание:")

@router.message(AdminStates.add_product_desc)
async def adm_add_product_desc(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    await state.update_data(new_prod_desc=msg.text.strip())
    await state.set_state(AdminStates.add_product_price)
    await msg.answer("◦ введи цену (₽):")

@router.message(AdminStates.add_product_price)
async def adm_add_product_price(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        price = float(msg.text.replace(",", "."))
        if price <= 0: raise ValueError
    except ValueError:
        await msg.answer("◦ введи корректную цену"); return
    await state.update_data(new_prod_price=price)
    await state.set_state(AdminStates.add_product_type)
    await msg.answer(
        "◦ выбери тип:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◦ Товар (файл)",    callback_data="adm_settype:product")],
            [InlineKeyboardButton(text="♱ Услуга (форма)",  callback_data="adm_settype:service")],
        ])
    )

@router.callback_query(F.data.startswith("adm_settype:"), AdminStates.add_product_type)
async def adm_add_product_type(call: CallbackQuery, state: FSMContext):
    ptype = call.data.split(":")[1]
    data  = await state.get_data()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO products (category_id, name, description, price, type) VALUES (?,?,?,?,?)",
            (data['new_prod_cat'], data['new_prod_name'], data['new_prod_desc'],
             data['new_prod_price'], ptype)
        )
        conn.commit()
    await state.clear()
    badge = "♱ Услуга" if ptype == 'service' else "◦ Товар"
    await call.message.edit_text(
        f"✔ {badge} ❝{data['new_prod_name']}❞ добавлен за {data['new_prod_price']:.2f}₽\n\n"
        f"◦ теперь можешь загрузить {'файл' if ptype=='product' else 'форму'} через редактирование"
    )

@router.callback_query(F.data.startswith("adm_edit_prod:"))
async def adm_edit_prod(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    prod_id = int(call.data.split(":")[1])
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
    ptype     = p['type'] or 'product'
    type_text = "♱ Услуга" if ptype == 'service' else "◦ Товар"
    file_text = "◦ файл: загружен ✔" if p['prod_file'] else "◦ файл: не загружен"
    form_text = ""
    if ptype == 'service' and p['form_questions']:
        try:
            qs = json.loads(p['form_questions'])
            form_text = f"\n◦ вопросов в форме: {len(qs)}"
        except Exception:
            pass
    allow_repurchase = p['allow_repurchase'] or 0
    repurchase_text  = "◦ повт. покупка: ✔ разрешена" if allow_repurchase else "◦ повт. покупка: ✕ запрещена"
    await call.message.edit_text(
        f"{'♱' if ptype=='service' else '◈'} <b>{p['name']}</b>\n{'─'*22}\n"
        f"{p['description']}\n\n"
        f"◦ тип: {type_text}\n"
        f"✯ {p['price']:.2f}₽\n"
        f"{repurchase_text}\n"
        f"{''+file_text if ptype=='product' else ''+form_text}",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_edit_product(prod_id, ptype, allow_repurchase)
    )

@router.callback_query(F.data.startswith("adm_pname:"))
async def adm_edit_pname_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    prod_id = int(call.data.split(":")[1])
    await state.update_data(edit_prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_name)
    await call.message.edit_text("◦ введи новое название:")

@router.message(AdminStates.edit_prod_name)
async def adm_edit_pname(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute("UPDATE products SET name=? WHERE id=?",
                     (msg.text.strip(), data['edit_prod_id']))
        conn.commit()
    await state.clear()
    await msg.answer("✔ название обновлено")

@router.callback_query(F.data.startswith("adm_pdesc:"))
async def adm_edit_pdesc_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    prod_id = int(call.data.split(":")[1])
    await state.update_data(edit_prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_desc)
    await call.message.edit_text("◦ введи новое описание:")

@router.message(AdminStates.edit_prod_desc)
async def adm_edit_pdesc(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute("UPDATE products SET description=? WHERE id=?",
                     (msg.text.strip(), data['edit_prod_id']))
        conn.commit()
    await state.clear()
    await msg.answer("✔ описание обновлено")

@router.callback_query(F.data.startswith("adm_pprice:"))
async def adm_edit_pprice_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    prod_id = int(call.data.split(":")[1])
    await state.update_data(edit_prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_price)
    await call.message.edit_text("◦ введи новую цену (₽):")

@router.message(AdminStates.edit_prod_price)
async def adm_edit_pprice(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        price = float(msg.text.replace(",", "."))
        if price <= 0: raise ValueError
    except ValueError:
        await msg.answer("◦ введи корректную цену"); return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute("UPDATE products SET price=? WHERE id=?", (price, data['edit_prod_id']))
        conn.commit()
    await state.clear()
    await msg.answer(f"✔ цена обновлена: {price:.2f}₽")

# ── Файл товара (admin) ───────────────────────
@router.callback_query(F.data.startswith("adm_pfile:"))
async def adm_edit_pfile_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    prod_id = int(call.data.split(":")[1])
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
    await state.update_data(edit_prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_file)
    file_status = "◦ текущий файл: <b>загружен ✔</b>" if p['prod_file'] else "◦ файл пока не загружен"
    await call.message.edit_text(
        f"◦ <b>Файл товара: {p['name']}</b>\n\n"
        f"{file_status}\n\n"
        f"☛ отправь новый файл (фото, документ, видео, аудио)\n"
        f"◦ при обновлении файла — все покупатели получат новый файл с уведомлением",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✕ Удалить файл",   callback_data=f"adm_pfile_del:{prod_id}")],
            [InlineKeyboardButton(text="← Назад",          callback_data=f"adm_edit_prod:{prod_id}")],
        ])
    )

@router.message(AdminStates.edit_prod_file,
                F.photo | F.document | F.video | F.audio | F.animation)
async def adm_edit_pfile_receive(msg: Message, state: FSMContext, bot: Bot):
    if not is_admin(msg.from_user.id): return
    data    = await state.get_data()
    prod_id = data['edit_prod_id']
    file_id, file_type = extract_file_from_msg(msg)
    if not file_id:
        await msg.answer("◦ не удалось получить файл"); return

    raw_file = encode_file(file_id, file_type)
    with get_db() as conn:
        conn.execute("UPDATE products SET prod_file=? WHERE id=?", (raw_file, prod_id))
        conn.commit()
        p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
        # Получаем всех покупателей этого товара
        buyers = conn.execute(
            "SELECT DISTINCT u.telegram_id FROM purchases pu "
            "JOIN users u ON u.id=pu.user_id WHERE pu.product_id=?", (prod_id,)
        ).fetchall()

    await state.clear()
    await msg.answer(f"✔ файл товара <b>{p['name']}</b> обновлён\n"
                     f"◦ уведомляем {len(buyers)} покупателей...", parse_mode=ParseMode.HTML)

    # Рассылка обновления покупателям
    sent, failed = 0, 0
    for b in buyers:
        try:
            await bot.send_message(b['telegram_id'],
                f"✦ <b>Обновление файла!</b>\n\n"
                f"◦ товар: <b>{p['name']}</b>\n"
                f"◦ новый файл прикреплён ниже",
                parse_mode=ParseMode.HTML)
            await send_product_file(bot, b['telegram_id'], raw_file)
            sent += 1
        except Exception:
            failed += 1

    await msg.answer(f"◦ уведомлено: {sent} / ошибок: {failed}")

@router.message(AdminStates.edit_prod_file)
async def adm_edit_pfile_wrong(msg: Message):
    await msg.answer("◦ нужно прислать файл (фото, документ, видео, аудио)")

@router.callback_query(F.data.startswith("adm_pfile_del:"))
async def adm_pfile_delete(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.clear()
    prod_id = int(call.data.split(":")[1])
    with get_db() as conn:
        conn.execute("UPDATE products SET prod_file=NULL WHERE id=?", (prod_id,))
        conn.commit()
    await call.message.edit_text(
        "✕ файл удалён",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data=f"adm_edit_prod:{prod_id}")]
        ])
    )

# ── Форма услуги (admin) ──────────────────────
@router.callback_query(F.data.startswith("adm_pform:"))
async def adm_edit_pform_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    prod_id = int(call.data.split(":")[1])
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()

    current_qs = []
    if p['form_questions']:
        try:
            current_qs = json.loads(p['form_questions'])
        except Exception:
            pass

    qs_text = "\n".join(f"{i+1}. {q}" for i, q in enumerate(current_qs)) if current_qs else "◦ вопросов нет"
    await state.update_data(edit_prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_form)
    await call.message.edit_text(
        f"◦ <b>Форма услуги: {p['name']}</b>\n\n"
        f"<b>Текущие вопросы:</b>\n{qs_text}\n\n"
        f"☛ отправь новый список вопросов (каждый с новой строки)\n"
        f"◦ пример:\n  Ваше имя?\n  Опишите задачу\n  Укажите срок",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✕ Очистить форму",  callback_data=f"adm_pform_clear:{prod_id}")],
            [InlineKeyboardButton(text="← Назад",           callback_data=f"adm_edit_prod:{prod_id}")],
        ])
    )

@router.message(AdminStates.edit_prod_form)
async def adm_edit_pform_receive(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    data    = await state.get_data()
    prod_id = data['edit_prod_id']
    lines   = [l.strip() for l in msg.text.strip().split("\n") if l.strip()]
    if not lines:
        await msg.answer("◦ нет вопросов — отправь хотя бы один"); return

    with get_db() as conn:
        conn.execute("UPDATE products SET form_questions=? WHERE id=?",
                     (json.dumps(lines, ensure_ascii=False), prod_id))
        conn.commit()
    await state.clear()
    qs_text = "\n".join(f"{i+1}. {q}" for i, q in enumerate(lines))
    await msg.answer(f"✔ форма обновлена ({len(lines)} вопросов):\n\n{qs_text}")

@router.callback_query(F.data.startswith("adm_pform_clear:"))
async def adm_pform_clear(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.clear()
    prod_id = int(call.data.split(":")[1])
    with get_db() as conn:
        conn.execute("UPDATE products SET form_questions=NULL WHERE id=?", (prod_id,))
        conn.commit()
    await call.message.edit_text(
        "✕ форма очищена — при покупке услуги форма показываться не будет",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data=f"adm_edit_prod:{prod_id}")]
        ])
    )

@router.callback_query(F.data.startswith("adm_pdel:"))
async def adm_del_product(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    prod_id = int(call.data.split(":")[1])
    with get_db() as conn:
        conn.execute("UPDATE products SET is_active=0 WHERE id=?", (prod_id,))
        conn.commit()
    await call.message.edit_text(
        "✕ товар удалён",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="adm_products")]
        ])
    )

# ── Переключение повторной покупки ───────────
@router.callback_query(F.data.startswith("adm_toggle_repurchase:"))
async def adm_toggle_repurchase(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    prod_id = int(call.data.split(":")[1])
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
        new_val = 0 if (p['allow_repurchase'] or 0) else 1
        conn.execute("UPDATE products SET allow_repurchase=? WHERE id=?", (new_val, prod_id))
        conn.commit()
        p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
    status = "✔ разрешена" if new_val else "✕ запрещена"
    await call.answer(f"Повторная покупка: {status}", show_alert=False)
    # Обновляем меню редактирования
    ptype = p['type'] or 'product'
    type_text = "♱ Услуга" if ptype == 'service' else "◦ Товар"
    file_text = "◦ файл: загружен ✔" if p['prod_file'] else "◦ файл: не загружен"
    form_text = ""
    if ptype == 'service' and p['form_questions']:
        try:
            qs = json.loads(p['form_questions'])
            form_text = f"\n◦ вопросов в форме: {len(qs)}"
        except Exception:
            pass
    repurchase_text = "◦ повт. покупка: ✔ разрешена" if new_val else "◦ повт. покупка: ✕ запрещена"
    await call.message.edit_text(
        f"{'♱' if ptype=='service' else '◈'} <b>{p['name']}</b>\n{'─'*22}\n"
        f"{p['description']}\n\n"
        f"◦ тип: {type_text}\n"
        f"✯ {p['price']:.2f}₽\n"
        f"{repurchase_text}\n"
        f"{''+file_text if ptype=='product' else ''+form_text}",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_edit_product(prod_id, ptype, new_val)
    )

# ── GIF стартового сообщения ──────────────────
@router.callback_query(F.data == "adm_start_gif")
async def adm_start_gif_menu(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    gif_set = db_get_setting("start_gif")
    status  = "◦ сейчас: <b>установлен ✔</b>" if gif_set else "◦ сейчас: <b>не задан</b>"
    rows    = [[InlineKeyboardButton(text="☁︎ Загрузить / заменить GIF",
                                     callback_data="adm_gif_upload")]]
    if gif_set:
        rows.append([InlineKeyboardButton(text="✕ Удалить GIF", callback_data="adm_gif_delete")])
    rows.append([InlineKeyboardButton(text="← Назад", callback_data="admin_back")])
    await call.message.edit_text(
        f"☁︎ <b>GIF стартового сообщения</b>\n\n{status}\n\n"
        f"◦ GIF отправляется вместе с приветствием при /start",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )

@router.callback_query(F.data == "adm_gif_upload")
async def adm_gif_upload(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.set_state(AdminStates.set_start_gif)
    await call.message.edit_text(
        "☁︎ <b>Загрузка GIF</b>\n\n◦ пришли GIF-файл или анимацию",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="adm_start_gif")]
        ])
    )

@router.message(AdminStates.set_start_gif, F.animation | F.document)
async def adm_gif_receive(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    file_id = msg.animation.file_id if msg.animation else msg.document.file_id
    db_set_setting("start_gif", file_id)
    await state.clear()
    await msg.answer_animation(
        animation=file_id,
        caption="✔ <b>GIF установлен!</b>\n◦ теперь он отправляется при /start",
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.set_start_gif)
async def adm_gif_wrong_type(msg: Message):
    await msg.answer("◦ нужен <b>GIF-файл</b> или анимация", parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "adm_gif_delete")
async def adm_gif_delete(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.clear()
    db_del_setting("start_gif")
    await call.message.edit_text(
        "✕ <b>GIF удалён</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="adm_start_gif")]
        ])
    )
    await call.answer()

# ── База данных (backup / restore) ───────────
@router.callback_query(F.data == "adm_database")
async def adm_database(call: CallbackQuery):
    if not is_admin(call.from_user.id): return
    import os as _os
    db_size = _os.path.getsize(DB_PATH) if _os.path.exists(DB_PATH) else 0
    await call.message.edit_text(
        f"◈ <b>База данных</b>\n{'─'*22}\n"
        f"◦ файл: <code>{DB_PATH}</code>\n"
        f"◦ размер: <b>{db_size / 1024:.1f} КБ</b>\n\n"
        f"☛ <b>Скачать</b> — получить текущую БД файлом\n"
        f"☛ <b>Загрузить</b> — заменить БД и перезапустить бота",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◦ Скачать БД",   callback_data="adm_db_download")],
            [InlineKeyboardButton(text="◦ Загрузить БД", callback_data="adm_db_upload")],
            [InlineKeyboardButton(text="← Назад",        callback_data="admin_back")],
        ])
    )

@router.callback_query(F.data == "adm_db_download")
async def adm_db_download(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return
    import os as _os
    if not _os.path.exists(DB_PATH):
        await call.answer("◦ файл БД не найден", show_alert=True); return
    await call.answer("◦ отправляем файл…")
    from aiogram.types import FSInputFile
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    await bot.send_document(
        ADMIN_ID,
        FSInputFile(DB_PATH, filename=f"shop_backup_{ts}.db"),
        caption=f"◈ <b>Бэкап БД</b>\n◦ {ts}",
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "adm_db_upload")
async def adm_db_upload(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.set_state(AdminStates.upload_db)
    await call.message.edit_text(
        "◈ <b>Загрузка базы данных</b>\n\n"
        "◦ отправь .db файл (бэкап shop.db)\n"
        "◦ после загрузки бот <b>перезапустится</b>\n\n"
        "⚠️ текущая БД будет заменена!",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="adm_database")]
        ])
    )

@router.message(AdminStates.upload_db, F.document)
async def adm_db_receive(msg: Message, state: FSMContext, bot: Bot):
    if not is_admin(msg.from_user.id): return
    doc = msg.document
    if not (doc.file_name or "").endswith(".db"):
        await msg.answer("◦ нужен файл с расширением <b>.db</b>", parse_mode=ParseMode.HTML); return

    await state.clear()
    # Скачиваем файл во временное место
    tmp_path = DB_PATH + ".incoming"
    file = await bot.get_file(doc.file_id)
    await bot.download_file(file.file_path, destination=tmp_path)

    # Проверяем что это валидная SQLite БД
    try:
        import sqlite3 as _sql
        test_conn = _sql.connect(tmp_path)
        test_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        test_conn.close()
    except Exception as e:
        import os as _os
        _os.remove(tmp_path)
        await msg.answer(f"✕ файл повреждён или не является SQLite БД\n◦ {e}",
                         parse_mode=ParseMode.HTML); return

    # Создаём бэкап текущей БД
    import os as _os, shutil as _sh
    if _os.path.exists(DB_PATH):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        _sh.copy2(DB_PATH, DB_PATH + f".bak_{ts}")

    # Заменяем БД
    _sh.move(tmp_path, DB_PATH)
    await msg.answer(
        "✔ <b>База данных заменена!</b>\n\n"
        "◦ бот перезапускается…\n"
        "◦ через несколько секунд он снова будет доступен",
        parse_mode=ParseMode.HTML
    )
    # Перезапускаем процесс
    await asyncio.sleep(1.5)
    _os.execv(sys.executable, [sys.executable] + sys.argv)

@router.message(AdminStates.upload_db)
async def adm_db_wrong_file(msg: Message):
    await msg.answer("◦ нужно прислать <b>.db файл</b> (документ)", parse_mode=ParseMode.HTML)

# ── Рассылка ──────────────────────────────────
@router.callback_query(F.data == "adm_broadcast")
async def adm_broadcast_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return
    await state.set_state(AdminStates.broadcast_text)
    await call.message.edit_text(
        "⇢ <b>Рассылка</b>\n\n◦ введи текст (поддерживает HTML):",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="admin_back")]
        ])
    )

@router.message(AdminStates.broadcast_text)
async def adm_broadcast(msg: Message, state: FSMContext, bot: Bot):
    if not is_admin(msg.from_user.id): return
    await state.clear()
    with get_db() as conn:
        users = conn.execute("SELECT telegram_id FROM users").fetchall()
    sent, failed = 0, 0
    for u in users:
        try:
            await bot.send_message(u['telegram_id'], msg.text, parse_mode=ParseMode.HTML)
            sent += 1
        except Exception:
            failed += 1
    await msg.answer(
        f"✔ <b>Рассылка завершена</b>\n\n◦ доставлено: {sent}\n◦ ошибок: {failed}",
        parse_mode=ParseMode.HTML
    )

# ─────────────────────────────────────────────
#  ENTRYPOINT
# ─────────────────────────────────────────────
async def main():
    init_db()
    log.info("✅ Database ready")

    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    from aiogram.types import BotCommand, BotCommandScopeDefault, BotCommandScopeChat
    await bot.set_my_commands(
        [BotCommand(command="start", description="Главное меню")],
        scope=BotCommandScopeDefault()
    )
    if ADMIN_ID:
        try:
            await bot.set_my_commands(
                [BotCommand(command="start", description="Главное меню"),
                 BotCommand(command="admin", description="Панель администратора")],
                scope=BotCommandScopeChat(chat_id=ADMIN_ID)
            )
        except Exception as e:
            log.warning(f"Could not set admin commands: {e}")

    log.info("🚀 Bot started")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    asyncio.run(main())
