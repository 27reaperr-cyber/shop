"""
Telegram Shop Bot — aiogram v3
Full-featured shop with catalog, payments, referrals, admin panel
"""

import asyncio
import logging
import sqlite3
import os
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
    conn.execute("PRAGMA journal_mode=WAL")   # better concurrent read/write
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
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            name        TEXT NOT NULL,
            description TEXT,
            price       REAL NOT NULL,
            stock       INTEGER DEFAULT -1,
            is_active   INTEGER DEFAULT 1,
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
    """)
    conn.commit()

    # Seed demo categories and products if empty
    if not c.execute("SELECT id FROM categories LIMIT 1").fetchone():
        c.executemany("INSERT INTO categories (name, emoji) VALUES (?,?)", [
            ("Аккаунты", "☽"),
            ("VPN", "✦"),
            ("Игры", "⬡"),
            ("Софт", "◈"),
        ])
        conn.commit()
        c.executemany(
            "INSERT INTO products (category_id, name, description, price) VALUES (?,?,?,?)",
            [
                (1, "Instagram аккаунт",  "Аккаунт с подтверждённой почтой, возраст 1+ год", 299.0),
                (1, "Spotify Premium",    "Личный аккаунт Spotify Premium на 12 месяцев",     199.0),
                (2, "VPN на 1 месяц",     "Быстрый VPN, 50+ серверов, без логов",             149.0),
                (2, "VPN на 6 месяцев",   "Быстрый VPN — выгода 30%",                         599.0),
                (3, "Steam аккаунт",      "Аккаунт Steam с игровой библиотекой",               499.0),
                (3, "Minecraft Java",     "Лицензионный ключ Minecraft Java Edition",          799.0),
                (4, "Office 365",         "Ключ активации Microsoft Office 365 Personal",      349.0),
                (4, "Adobe Photoshop",    "Серийный номер Adobe Photoshop 2025",               699.0),
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
    if u:
        return u
    return db_create_user(telegram_id, username, full_name, referrer_id)

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

# ─────────────────────────────────────────────
#  KEYBOARDS
# ─────────────────────────────────────────────
def kb_main():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🛍 Купить"),      KeyboardButton(text="👤 Мой профиль")],
        [KeyboardButton(text="ℹ️ О шопе"),      KeyboardButton(text="💬 Поддержка")],
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
        [InlineKeyboardButton(text="♱ По реквизитам",  callback_data="topup_bank")],
        [InlineKeyboardButton(text="✦ Crypto Bot",      callback_data="topup_crypto")],
        [InlineKeyboardButton(text="← Назад",           callback_data="profile_back")],
    ])

def kb_bank_paid(payment_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔ Я оплатил — прислать чек",  callback_data=f"bank_paid:{payment_id}")],
        [InlineKeyboardButton(text="← Отмена",                    callback_data="topup")],
    ])

def kb_crypto(invoice_url: str, payment_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✦ Оплатить",          url=invoice_url)],
        [InlineKeyboardButton(text="⟳ Проверить оплату",  callback_data=f"check_crypto:{payment_id}")],
        [InlineKeyboardButton(text="← Отмена",            callback_data="topup")],
    ])

def kb_categories(cats):
    rows = []
    for cat in cats:
        rows.append([InlineKeyboardButton(
            text=f"{cat['emoji']} {cat['name']}",
            callback_data=f"cat:{cat['id']}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_products(products, cat_id: int):
    rows = []
    for p in products:
        rows.append([InlineKeyboardButton(
            text=f"◦ {p['name']} — {p['price']:.2f}₽",
            callback_data=f"product:{p['id']}"
        )])
    rows.append([InlineKeyboardButton(text="← Категории", callback_data="catalog")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_buy_product(product_id: int, cat_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✔ Купить",  callback_data=f"buy:{product_id}")],
        [InlineKeyboardButton(text="← Назад",   callback_data=f"cat:{cat_id}")],
    ])

def kb_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◈ Товары",        callback_data="adm_products"),
         InlineKeyboardButton(text="⬡ Категории",     callback_data="adm_categories")],
        [InlineKeyboardButton(text="☽ Пользователи",  callback_data="adm_users"),
         InlineKeyboardButton(text="✦ Статистика",    callback_data="adm_stats")],
        [InlineKeyboardButton(text="♱ Заявки оплат",  callback_data="adm_payments")],
        [InlineKeyboardButton(text="⇢ Рассылка",      callback_data="adm_broadcast")],
    ])

def kb_admin_products():
    with get_db() as conn:
        prods = conn.execute(
            "SELECT p.*, c.name AS cat_name FROM products p "
            "JOIN categories c ON c.id=p.category_id WHERE p.is_active=1"
        ).fetchall()
    rows = [[InlineKeyboardButton(text="✦ Добавить товар", callback_data="adm_add_product")]]
    for p in prods:
        rows.append([InlineKeyboardButton(
            text=f"◦ {p['name']} ({p['price']:.0f}₽)",
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

def kb_edit_product(product_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◦ Название",   callback_data=f"adm_pname:{product_id}"),
         InlineKeyboardButton(text="◦ Описание",   callback_data=f"adm_pdesc:{product_id}")],
        [InlineKeyboardButton(text="◦ Цена",       callback_data=f"adm_pprice:{product_id}"),
         InlineKeyboardButton(text="✕ Удалить",    callback_data=f"adm_pdel:{product_id}")],
        [InlineKeyboardButton(text="← Назад",      callback_data="adm_products")],
    ])

def kb_edit_category(cat_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◦ Название",   callback_data=f"adm_cname:{cat_id}"),
         InlineKeyboardButton(text="◦ Символ",     callback_data=f"adm_cemoji:{cat_id}")],
        [InlineKeyboardButton(text="✕ Удалить",    callback_data=f"adm_cdel:{cat_id}")],
        [InlineKeyboardButton(text="← Назад",      callback_data="adm_categories")],
    ])

def kb_confirm_payment(payment_id: int, user_db_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✔ Подтвердить", callback_data=f"adm_confirm:{payment_id}:{user_db_id}"),
        InlineKeyboardButton(text="✕ Отклонить",   callback_data=f"adm_reject:{payment_id}:{user_db_id}"),
    ]])

# ─────────────────────────────────────────────
#  FSM STATES
# ─────────────────────────────────────────────
class TopupStates(StatesGroup):
    amount_bank   = State()
    bank_receipt  = State()   # ожидание скриншота/чека
    amount_crypto = State()

class TransferStates(StatesGroup):
    target_id = State()
    amount    = State()

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
    edit_prod_name       = State()
    edit_prod_desc       = State()
    edit_prod_price      = State()
    broadcast_text       = State()

# ─────────────────────────────────────────────
#  CRYPTO BOT API
# ─────────────────────────────────────────────
CRYPTOBOT_API = "https://pay.crypt.bot/api"

async def get_usdt_rate() -> float:
    """
    Получаем курс USDT/RUB.
    Приоритет: Binance P2P (публичный) → CryptoBot API → хардкод.
    """
    # 1. Binance — публичный API, не требует токена
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": "USDTRUB"},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    rate = float(data["price"])
                    if rate > 1:
                        log.info(f"USDT rate from Binance: {rate}")
                        return rate
    except Exception as e:
        log.warning(f"Binance rate error: {e}")

    # 2. CryptoBot — если задан токен
    if CRYPTOBOT_TOKEN:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{CRYPTOBOT_API}/getExchangeRates",
                    headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    data = await r.json()
                    for item in data.get("result", []):
                        src = item.get("source", "")
                        tgt = item.get("target", "")
                        # CryptoBot возвращает USDT→RUB, нам нужен rate как RUB за 1 USDT
                        if src == "USDT" and tgt == "RUB":
                            rate = float(item["rate"])
                            log.info(f"USDT rate from CryptoBot: {rate}")
                            return rate
                        if src == "RUB" and tgt == "USDT" and float(item["rate"]) > 0:
                            rate = 1.0 / float(item["rate"])
                            log.info(f"USDT rate from CryptoBot (inverted): {rate}")
                            return rate
        except Exception as e:
            log.warning(f"CryptoBot rate error: {e}")

    # 3. Хардкод-фолбэк
    log.warning("Using hardcoded USDT rate: 90.0")
    return 90.0

async def create_crypto_invoice(amount_rub: float) -> dict | None:
    if not CRYPTOBOT_TOKEN:
        return None
    try:
        rate = await get_usdt_rate()
        amount_usdt = round(amount_rub / rate, 2)
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{CRYPTOBOT_API}/createInvoice",
                headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                json={
                    "asset": "USDT",
                    "amount": str(amount_usdt),
                    "description": f"Пополнение баланса {amount_rub:.2f}₽",
                    "expires_in": 3600,
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
                if data.get("ok"):
                    inv = data["result"]
                    return {
                        "pay_url":    inv["pay_url"],
                        "invoice_id": inv["invoice_id"],
                        "rate": rate,
                        "usdt": amount_usdt
                    }
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
                data = await r.json()
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
    args = msg.text.split()
    ref_tg_id = None
    if len(args) > 1:
        try:
            ref_tg_id = int(args[1])
            if ref_tg_id == msg.from_user.id:
                ref_tg_id = None
        except ValueError:
            ref_tg_id = None

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

    await msg.answer(
        f"☁︎ Добро пожаловать, <b>{msg.from_user.first_name}</b> :D\n\n"
        f"❝dreinn.shop❞\n\n"
        f"◦ используй меню ниже",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_main()
    )

# ── Главное меню ──────────────────────────────
@router.message(F.text == "ℹ️ О шопе")
async def about(msg: Message):
    await msg.answer(
        "✹ <b>О нашем шопе</b>\n\n"
        "❝dreinn.shop❞\n\n"
        "◦ ✔ Все товары проверены вручную\n"
        "◦ ♱ Поддержка 24/7\n"
        "◦ ♡ Гарантия на все покупки\n",
        parse_mode=ParseMode.HTML
    )

@router.message(F.text == "💬 Поддержка")
async def support(msg: Message):
    await msg.answer(
        "♱ <b>Поддержка</b>\n\n"
        "Если возникли вопросы:\n\n"
        "→ @support_username\n\n"
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

@router.message(F.text == "👤 Мой профиль")
async def show_profile(msg: Message):
    db_get_or_create_user(msg.from_user.id, msg.from_user.username or "", msg.from_user.full_name or "")
    await msg.answer(
        await _profile_text(msg.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_profile()
    )

@router.callback_query(F.data == "profile_back")
async def profile_back(call: CallbackQuery, state: FSMContext):
    await state.clear()   # сброс любого активного состояния
    await call.message.edit_text(
        await _profile_text(call.from_user.id),
        parse_mode=ParseMode.HTML,
        reply_markup=kb_profile()
    )

# ── Пополнение ────────────────────────────────
@router.callback_query(F.data == "topup")
async def topup_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()   # сброс состояния если пришли через «Отмена»
    await call.message.edit_text(
        "✦ <b>Пополнение баланса</b>\n\n"
        "◦ выбери способ оплаты",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_topup()
    )

# — Банк —
@router.callback_query(F.data == "topup_bank")
async def topup_bank_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(TopupStates.amount_bank)
    await call.message.edit_text(
        "♱ <b>Оплата по реквизитам</b>\n\n"
        "◦ введи сумму пополнения (минимум 10₽)",
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
            await msg.answer("◦ минимальная сумма — 10₽")
            return
    except ValueError:
        await msg.answer("◦ введи корректную сумму")
        return

    user = db_get_user(msg.from_user.id)
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO payments (user_id, amount, method, status, created_at) VALUES (?,?,?,?,?)",
            (user['id'], amount, "bank", "pending", now)
        )
        conn.commit()
        payment_id = cur.lastrowid

    # Сохраняем payment_id в стейт для следующего шага
    await state.update_data(bank_payment_id=payment_id)
    await state.set_state(TopupStates.bank_receipt)

    await msg.answer(
        f"♱ <b>Реквизиты для оплаты</b>\n"
        f"{'─' * 22}\n"
        f"◦ Банк: <b>{BANK_NAME}</b>\n"
        f"◦ Реквизиты: <code>{BANK_CARD}</code>\n"
        f"◦ Получатель: <b>{BANK_RECEIVER}</b>\n"
        f"◦ Сумма: <b>{amount:.2f}₽</b>\n\n"
        f"☛ переведи точную сумму, затем нажми кнопку ниже",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_bank_paid(payment_id)
    )

@router.callback_query(F.data.startswith("bank_paid:"))
async def bank_paid(call: CallbackQuery, state: FSMContext, bot: Bot):
    payment_id = int(call.data.split(":")[1])
    with get_db() as conn:
        pay = conn.execute("SELECT * FROM payments WHERE id=?", (payment_id,)).fetchone()

    if not pay or pay['status'] not in ('pending',):
        await call.answer("◦ заявка уже отправлена или обработана", show_alert=True)
        return

    # Сохраняем payment_id в FSM и переходим к ожиданию чека
    await state.update_data(bank_payment_id=payment_id)
    await state.set_state(TopupStates.bank_receipt)

    await call.message.edit_text(
        "♱ <b>Подтверждение оплаты</b>\n\n"
        "◦ пришли скриншот или фото чека оплаты\n"
        "◦ поддерживаются фото и документы (PDF, JPG и др.)\n\n"
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
        await state.clear()
        await msg.answer("◦ что-то пошло не так, начни заново")
        return

    with get_db() as conn:
        pay = conn.execute("SELECT * FROM payments WHERE id=?", (payment_id,)).fetchone()

    if not pay or pay['status'] != 'pending':
        await state.clear()
        await msg.answer("◦ заявка уже была отправлена ранее")
        return

    # Атомарно помечаем как sent — защита от двойной отправки
    with get_db() as conn:
        updated = conn.execute(
            "UPDATE payments SET status='sent' WHERE id=? AND status='pending'",
            (payment_id,)
        ).rowcount
        conn.commit()

    if not updated:
        await state.clear()
        await msg.answer("◦ заявка уже была отправлена ранее")
        return

    await state.clear()

    user = db_get_user(msg.from_user.id)

    # Подтверждение пользователю
    await msg.answer(
        "✔ <b>Чек получен, заявка отправлена</b>\n\n"
        "◦ проверим платёж и зачислим средства в течение нескольких минут",
        parse_mode=ParseMode.HTML
    )

    # Пересылаем чек администратору
    caption = (
        f"✦ <b>Новая заявка на пополнение</b>\n"
        f"{'─' * 22}\n"
        f"☛ <a href='tg://user?id={msg.from_user.id}'>{msg.from_user.full_name}</a>\n"
        f"◦ ID: <code>{msg.from_user.id}</code>\n"
        f"◦ Сумма: <b>{pay['amount']:.2f}₽</b>\n"
        f"◦ Метод: реквизиты\n"
        f"◦ Чек: ниже ↓"
    )
    try:
        await bot.send_message(
            ADMIN_ID, caption,
            parse_mode=ParseMode.HTML,
            reply_markup=kb_confirm_payment(payment_id, user['id'])
        )
        # Пересылаем сам файл
        if msg.photo:
            await bot.send_photo(ADMIN_ID, msg.photo[-1].file_id)
        elif msg.document:
            await bot.send_document(ADMIN_ID, msg.document.file_id)
    except Exception as e:
        log.error(f"Cannot notify admin: {e}")

@router.message(TopupStates.bank_receipt)
async def topup_bank_receipt_wrong(msg: Message):
    """Пользователь прислал не фото и не документ"""
    await msg.answer(
        "◦ нужно прислать <b>фото</b> или <b>документ</b> (скриншот/чек)\n"
        "☛ просто отправь файл в этот чат",
        parse_mode=ParseMode.HTML
    )

# — Crypto —
@router.callback_query(F.data == "topup_crypto")
async def topup_crypto_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(TopupStates.amount_crypto)
    await call.message.edit_text(
        "✦ <b>Crypto Bot</b>\n\n"
        "◦ введи сумму пополнения в рублях",
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
            await msg.answer("◦ минимальная сумма — 10₽")
            return
    except ValueError:
        await msg.answer("◦ введи корректную сумму")
        return

    await state.clear()

    rate = await get_usdt_rate()
    usdt = round(amount / rate, 2)
    inv  = await create_crypto_invoice(amount)

    if not inv:
        await msg.answer("◦ CryptoBot не настроен. Обратитесь к администратору.")
        return

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
        f"✦ <b>Оплата через Crypto Bot</b>\n"
        f"{'─' * 22}\n"
        f"◦ Пополнение: <b>{amount:.2f}₽</b> (~<b>{usdt} USDT</b>)\n"
        f"◦ Курс: <b>{rate:.2f}₽</b>\n\n"
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
        await call.answer("платёж не найден", show_alert=True)
        return
    if pay['status'] == 'confirmed':
        await call.answer("✔ уже зачислено!", show_alert=True)
        return

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
                    await bot.send_message(
                        ref_tg['telegram_id'],
                        f"✦ реферальный бонус +<b>{ref_bonus:.2f}₽</b> за пополнение реферала",
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass

        await call.message.edit_text(
            f"✔ <b>Оплата подтверждена</b>\n\n"
            f"◦ баланс пополнен на <b>{pay['amount']:.2f}₽</b>",
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
        f"☽ <b>Реферальная программа</b>\n"
        f"{'─' * 22}\n"
        f"◦ за каждое пополнение реферала — <b>{REFERRAL_PCT:.0f}%</b> тебе\n\n"
        f"☛ твоя ссылка:\n<code>{ref_link}</code>\n\n"
        f"✹ статистика\n"
        f"◦ рефералов: <b>{count}</b>\n"
        f"◦ заработано: <b>{earned:.2f}₽</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="profile_back")]
        ])
    )

# ── Передача баланса ──────────────────────────
@router.callback_query(F.data == "transfer")
async def transfer_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(TransferStates.target_id)
    await call.message.edit_text(
        "⇄ <b>Передача баланса</b>\n\n"
        "◦ введи Telegram ID получателя",
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
        await msg.answer("◦ введи числовой ID")
        return
    if target_id == msg.from_user.id:
        await msg.answer("◦ нельзя перевести самому себе")
        return
    if not db_get_user(target_id):
        await msg.answer("◦ пользователь не найден")
        return
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
        await msg.answer("◦ введи корректную сумму")
        return

    data      = await state.get_data()
    target_id = data['target_id']
    sender    = db_get_user(msg.from_user.id)

    if sender['balance'] < amount:
        await msg.answer(f"◦ недостаточно средств. баланс: {sender['balance']:.2f}₽")
        await state.clear()
        return

    db_update_balance(msg.from_user.id, -amount)
    db_update_balance(target_id, amount)
    await state.clear()

    await msg.answer(
        f"✔ переведено <b>{amount:.2f}₽</b> → <code>{target_id}</code>",
        parse_mode=ParseMode.HTML
    )
    try:
        await bot.send_message(
            target_id,
            f"✦ вам переведено <b>{amount:.2f}₽</b> от <code>{msg.from_user.id}</code>",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

# ── Мои покупки ───────────────────────────────
@router.callback_query(F.data == "my_purchases")
async def my_purchases(call: CallbackQuery):
    user = db_get_user(call.from_user.id)
    with get_db() as conn:
        purchases = conn.execute(
            "SELECT pu.*, pr.name AS pname FROM purchases pu "
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
            lines.append(f"◦ {p['pname']} — <b>{p['price']:.2f}₽</b> <i>({date})</i>")
        text = "\n".join(lines)

    await call.message.edit_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="profile_back")]
        ])
    )

# ── Каталог ───────────────────────────────────
@router.message(F.text == "🛍 Купить")
async def show_catalog(msg: Message):
    with get_db() as conn:
        cats = conn.execute("SELECT * FROM categories").fetchall()
    if not cats:
        await msg.answer("◦ каталог пока пуст")
        return
    await msg.answer(
        "✹ <b>Каталог</b>\n\n◦ выбери категорию",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_categories(cats)
    )

@router.callback_query(F.data == "catalog")
async def catalog_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    with get_db() as conn:
        cats = conn.execute("SELECT * FROM categories").fetchall()
    await call.message.edit_text(
        "✹ <b>Каталог</b>\n\n◦ выбери категорию",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_categories(cats)
    )

@router.callback_query(F.data.startswith("cat:"))
async def show_category(call: CallbackQuery):
    cat_id = int(call.data.split(":")[1])
    with get_db() as conn:
        cat   = conn.execute("SELECT * FROM categories WHERE id=?", (cat_id,)).fetchone()
        prods = conn.execute(
            "SELECT * FROM products WHERE category_id=? AND is_active=1", (cat_id,)
        ).fetchall()
    if not prods:
        await call.answer("в этой категории нет товаров", show_alert=True)
        return
    await call.message.edit_text(
        f"{cat['emoji']} <b>{cat['name']}</b>\n\n◦ выбери товар",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_products(prods, cat_id)
    )

@router.callback_query(F.data.startswith("product:"))
async def show_product(call: CallbackQuery):
    product_id = int(call.data.split(":")[1])
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    await call.message.edit_text(
        f"◈ <b>{p['name']}</b>\n"
        f"{'─' * 22}\n"
        f"{p['description']}\n\n"
        f"✯ цена: <b>{p['price']:.2f}₽</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_buy_product(product_id, p['category_id'])
    )

@router.callback_query(F.data.startswith("buy:"))
async def buy_product(call: CallbackQuery, bot: Bot):
    product_id = int(call.data.split(":")[1])
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not p or not p['is_active']:
        await call.answer("◦ товар недоступен", show_alert=True)
        return

    user = db_get_user(call.from_user.id)
    if user['balance'] < p['price']:
        await call.answer(
            f"◦ недостаточно средств\nнужно: {p['price']:.2f}₽\nбаланс: {user['balance']:.2f}₽",
            show_alert=True
        )
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Атомарная операция: списание и запись покупки в одной транзакции
    with get_db() as conn:
        # Проверяем баланс ещё раз внутри транзакции
        fresh = conn.execute(
            "SELECT balance FROM users WHERE telegram_id=?", (call.from_user.id,)
        ).fetchone()
        if not fresh or fresh['balance'] < p['price']:
            await call.answer("◦ недостаточно средств", show_alert=True)
            return
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

# ─────────────────────────────────────────────
#  ADMIN PANEL
# ─────────────────────────────────────────────
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if not is_admin(msg.from_user.id):
        await msg.answer("✕ нет доступа")
        return
    await msg.answer(
        "✹ <b>Админ панель</b>\n\n◦ что делаем?",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_admin()
    )

@router.callback_query(F.data == "admin_back")
async def admin_back(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.clear()   # сброс любого admin-стейта
    await call.message.edit_text(
        "✹ <b>Админ панель</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_admin()
    )

# ── Статистика ────────────────────────────────
@router.callback_query(F.data == "adm_stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    with get_db() as conn:
        users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        revenue     = conn.execute(
            "SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='confirmed'"
        ).fetchone()[0]
        purchases_n = conn.execute("SELECT COUNT(*) FROM purchases").fetchone()[0]
        pending     = conn.execute(
            "SELECT COUNT(*) FROM payments WHERE status='pending'"
        ).fetchone()[0]
    await call.message.edit_text(
        f"✦ <b>Статистика</b>\n"
        f"{'─' * 22}\n"
        f"◦ пользователей: <b>{users_count}</b>\n"
        f"◦ выручка: <b>{revenue:.2f}₽</b>\n"
        f"◦ покупок: <b>{purchases_n}</b>\n"
        f"◦ ждут подтверждения: <b>{pending}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="admin_back")]
        ])
    )

# ── Пользователи ──────────────────────────────
@router.callback_query(F.data == "adm_users")
async def adm_users(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.clear()
    with get_db() as conn:
        users = conn.execute("SELECT * FROM users ORDER BY id DESC LIMIT 15").fetchall()
    lines = ["☽ <b>Пользователи</b>\n"]
    for u in users:
        name = (u['full_name'] or "—")[:20]
        lines.append(f"◦ <code>{u['telegram_id']}</code>  {name}  {u['balance']:.0f}₽")
    await call.message.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✦ Выдать баланс", callback_data="adm_give_balance")],
            [InlineKeyboardButton(text="← Назад",         callback_data="admin_back")],
        ])
    )

@router.callback_query(F.data == "adm_give_balance")
async def adm_give_balance_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.give_balance_id)
    await call.message.edit_text(
        "☛ введи Telegram ID пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="adm_users")]
        ])
    )

@router.message(AdminStates.give_balance_id)
async def adm_give_balance_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        await msg.answer("◦ введи числовой ID")
        return
    if not db_get_user(uid):
        await msg.answer("◦ пользователь не найден")
        return
    await state.update_data(target_uid=uid)
    await state.set_state(AdminStates.give_balance_amount)
    await msg.answer(f"◦ введи сумму для <code>{uid}</code>:", parse_mode=ParseMode.HTML)

@router.message(AdminStates.give_balance_amount)
async def adm_give_balance_amount(msg: Message, state: FSMContext, bot: Bot):
    if not is_admin(msg.from_user.id):
        return
    try:
        amount = float(msg.text.replace(",", "."))
    except ValueError:
        await msg.answer("◦ введи сумму числом")
        return
    data = await state.get_data()
    uid  = data['target_uid']
    db_update_balance(uid, amount)
    await state.clear()
    await msg.answer(
        f"✔ выдано <b>{amount:.2f}₽</b> → <code>{uid}</code>",
        parse_mode=ParseMode.HTML
    )
    try:
        await bot.send_message(
            uid,
            f"✦ вам начислено <b>{amount:.2f}₽</b> от администратора",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

# ── Заявки оплат ──────────────────────────────
@router.callback_query(F.data == "adm_payments")
async def adm_payments(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    with get_db() as conn:
        pays = conn.execute(
            "SELECT p.*, u.telegram_id FROM payments p "
            "JOIN users u ON u.id=p.user_id "
            "WHERE p.status IN ('pending','sent') ORDER BY p.created_at DESC"
        ).fetchall()
    if not pays:
        await call.message.edit_text(
            "✔ нет ожидающих заявок",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data="admin_back")]
            ])
        )
        return
    await call.answer()
    for pay in pays:
        await call.message.answer(
            f"♱ <b>Заявка #{pay['id']}</b>\n"
            f"◦ TG: <code>{pay['telegram_id']}</code>\n"
            f"◦ сумма: <b>{pay['amount']:.2f}₽</b>\n"
            f"◦ метод: {pay['method']}\n"
            f"◦ дата: {pay['created_at'][:16]}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_confirm_payment(pay['id'], pay['user_id'])
        )

@router.callback_query(F.data.startswith("adm_confirm:"))
async def adm_confirm_payment(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return
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
                    await bot.send_message(
                        ref_tg['telegram_id'],
                        f"✦ реферальный бонус +<b>{ref_bonus:.2f}₽</b>",
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
        try:
            await bot.send_message(
                u['telegram_id'],
                f"✔ платёж подтверждён. баланс пополнен на <b>{pay['amount']:.2f}₽</b>",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
        await call.message.edit_text(f"✔ платёж #{payment_id} подтверждён")
    await call.answer()

@router.callback_query(F.data.startswith("adm_reject:"))
async def adm_reject_payment(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return
    parts      = call.data.split(":")
    payment_id = int(parts[1])
    user_db_id = int(parts[2])

    with get_db() as conn:
        u = conn.execute("SELECT telegram_id FROM users WHERE id=?", (user_db_id,)).fetchone()
        conn.execute("UPDATE payments SET status='rejected' WHERE id=?", (payment_id,))
        conn.commit()

    if u:
        try:
            await bot.send_message(u['telegram_id'], "✕ ваш платёж отклонён. обратитесь в поддержку.")
        except Exception:
            pass
    await call.message.edit_text(f"✕ платёж #{payment_id} отклонён")
    await call.answer()

# ── Категории (admin) ─────────────────────────
@router.callback_query(F.data == "adm_categories")
async def adm_categories(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.clear()
    await call.message.edit_text(
        "⬡ <b>Категории</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_admin_categories()
    )

@router.callback_query(F.data == "adm_add_category")
async def adm_add_category(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.add_category_name)
    await call.message.edit_text(
        "◦ введи название новой категории:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Отмена", callback_data="adm_categories")]
        ])
    )

@router.message(AdminStates.add_category_name)
async def adm_add_category_name(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.update_data(cat_name=msg.text.strip())
    await state.set_state(AdminStates.add_category_emoji)
    await msg.answer("◦ введи символ для категории (например ✦ ☽ ◈):")

@router.message(AdminStates.add_category_emoji)
async def adm_add_category_emoji(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO categories (name, emoji) VALUES (?,?)", (data['cat_name'], msg.text.strip())
        )
        conn.commit()
    await state.clear()
    await msg.answer(f"✔ категория ❝{data['cat_name']}❞ добавлена")

@router.callback_query(F.data.startswith("adm_edit_cat:"))
async def adm_edit_cat(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    cat_id = int(call.data.split(":")[1])
    with get_db() as conn:
        cat = conn.execute("SELECT * FROM categories WHERE id=?", (cat_id,)).fetchone()
    await call.message.edit_text(
        f"◦ категория: {cat['emoji']} <b>{cat['name']}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_edit_category(cat_id)
    )

@router.callback_query(F.data.startswith("adm_cname:"))
async def adm_edit_cname_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    cat_id = int(call.data.split(":")[1])
    await state.update_data(edit_cat_id=cat_id)
    await state.set_state(AdminStates.edit_cat_name)
    await call.message.edit_text("◦ введи новое название категории:")

@router.message(AdminStates.edit_cat_name)
async def adm_edit_cname(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute(
            "UPDATE categories SET name=? WHERE id=?", (msg.text.strip(), data['edit_cat_id'])
        )
        conn.commit()
    await state.clear()
    await msg.answer("✔ название обновлено")

@router.callback_query(F.data.startswith("adm_cemoji:"))
async def adm_edit_cemoji_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    cat_id = int(call.data.split(":")[1])
    await state.update_data(edit_cat_id=cat_id)
    await state.set_state(AdminStates.edit_cat_emoji)
    await call.message.edit_text("◦ введи новый символ:")

@router.message(AdminStates.edit_cat_emoji)
async def adm_edit_cemoji(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute(
            "UPDATE categories SET emoji=? WHERE id=?", (msg.text.strip(), data['edit_cat_id'])
        )
        conn.commit()
    await state.clear()
    await msg.answer("✔ символ обновлён")

@router.callback_query(F.data.startswith("adm_cdel:"))
async def adm_del_category(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
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
    if not is_admin(call.from_user.id):
        return
    await state.clear()
    await call.message.edit_text(
        "◈ <b>Товары</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_admin_products()
    )

@router.callback_query(F.data == "adm_add_product")
async def adm_add_product_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    with get_db() as conn:
        cats = conn.execute("SELECT * FROM categories").fetchall()
    if not cats:
        await call.answer("сначала создай хотя бы одну категорию!", show_alert=True)
        return
    rows = [[InlineKeyboardButton(
        text=f"{c['emoji']} {c['name']}",
        callback_data=f"adm_prodcat:{c['id']}"
    )] for c in cats]
    rows.append([InlineKeyboardButton(text="← Отмена", callback_data="adm_products")])
    await state.set_state(AdminStates.add_product_cat)
    await call.message.edit_text(
        "◦ выбери категорию для нового товара:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )

@router.callback_query(F.data.startswith("adm_prodcat:"), AdminStates.add_product_cat)
async def adm_add_product_cat(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    await state.update_data(new_prod_cat=cat_id)
    await state.set_state(AdminStates.add_product_name)
    await call.message.edit_text("◦ введи название товара:")

@router.message(AdminStates.add_product_name)
async def adm_add_product_name(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.update_data(new_prod_name=msg.text.strip())
    await state.set_state(AdminStates.add_product_desc)
    await msg.answer("◦ введи описание товара:")

@router.message(AdminStates.add_product_desc)
async def adm_add_product_desc(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.update_data(new_prod_desc=msg.text.strip())
    await state.set_state(AdminStates.add_product_price)
    await msg.answer("◦ введи цену товара (в рублях):")

@router.message(AdminStates.add_product_price)
async def adm_add_product_price(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    try:
        price = float(msg.text.replace(",", "."))
        if price <= 0:
            raise ValueError
    except ValueError:
        await msg.answer("◦ введи корректную цену")
        return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO products (category_id, name, description, price) VALUES (?,?,?,?)",
            (data['new_prod_cat'], data['new_prod_name'], data['new_prod_desc'], price)
        )
        conn.commit()
    await state.clear()
    await msg.answer(f"✔ товар ❝{data['new_prod_name']}❞ добавлен за {price:.2f}₽")

@router.callback_query(F.data.startswith("adm_edit_prod:"))
async def adm_edit_prod(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    prod_id = int(call.data.split(":")[1])
    with get_db() as conn:
        p = conn.execute("SELECT * FROM products WHERE id=?", (prod_id,)).fetchone()
    await call.message.edit_text(
        f"◈ <b>{p['name']}</b>\n"
        f"{'─' * 22}\n"
        f"{p['description']}\n\n"
        f"✯ {p['price']:.2f}₽",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_edit_product(prod_id)
    )

@router.callback_query(F.data.startswith("adm_pname:"))
async def adm_edit_pname_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    prod_id = int(call.data.split(":")[1])
    await state.update_data(edit_prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_name)
    await call.message.edit_text("◦ введи новое название товара:")

@router.message(AdminStates.edit_prod_name)
async def adm_edit_pname(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute("UPDATE products SET name=? WHERE id=?", (msg.text.strip(), data['edit_prod_id']))
        conn.commit()
    await state.clear()
    await msg.answer("✔ название обновлено")

@router.callback_query(F.data.startswith("adm_pdesc:"))
async def adm_edit_pdesc_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    prod_id = int(call.data.split(":")[1])
    await state.update_data(edit_prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_desc)
    await call.message.edit_text("◦ введи новое описание товара:")

@router.message(AdminStates.edit_prod_desc)
async def adm_edit_pdesc(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute(
            "UPDATE products SET description=? WHERE id=?", (msg.text.strip(), data['edit_prod_id'])
        )
        conn.commit()
    await state.clear()
    await msg.answer("✔ описание обновлено")

@router.callback_query(F.data.startswith("adm_pprice:"))
async def adm_edit_pprice_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    prod_id = int(call.data.split(":")[1])
    await state.update_data(edit_prod_id=prod_id)
    await state.set_state(AdminStates.edit_prod_price)
    await call.message.edit_text("◦ введи новую цену (₽):")

@router.message(AdminStates.edit_prod_price)
async def adm_edit_pprice(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    try:
        price = float(msg.text.replace(",", "."))
        if price <= 0:
            raise ValueError
    except ValueError:
        await msg.answer("◦ введи корректную цену")
        return
    data = await state.get_data()
    with get_db() as conn:
        conn.execute("UPDATE products SET price=? WHERE id=?", (price, data['edit_prod_id']))
        conn.commit()
    await state.clear()
    await msg.answer(f"✔ цена обновлена: {price:.2f}₽")

@router.callback_query(F.data.startswith("adm_pdel:"))
async def adm_del_product(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
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

# ── Рассылка ──────────────────────────────────
@router.callback_query(F.data == "adm_broadcast")
async def adm_broadcast_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
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
    if not is_admin(msg.from_user.id):
        return
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

    # Обычным пользователям — только /start
    await bot.set_my_commands(
        [BotCommand(command="start", description="Главное меню")],
        scope=BotCommandScopeDefault()
    )

    # Администратору — /start + /admin (виден только ему)
    if ADMIN_ID:
        try:
            await bot.set_my_commands(
                [
                    BotCommand(command="start", description="Главное меню"),
                    BotCommand(command="admin", description="Панель администратора"),
                ],
                scope=BotCommandScopeChat(chat_id=ADMIN_ID)
            )
        except Exception as e:
            log.warning(f"Could not set admin commands (admin hasn't started bot yet?): {e}")

    log.info("🚀 Bot started")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    asyncio.run(main())
