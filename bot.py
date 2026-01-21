#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🌟 Telegram-астробот | Версия 2.4.0
⚫ Лилит + 🌙 Узлы Луны + 🌕 Фазы Луны + ⏰ Точный часовой пояс
🎁 Первый расширенный разбор — БЕСПЛАТНО
💳 Оплата через ЮKassa (оптимизировано)
👑 Админы имеют безлимитный доступ и полный контроль
⬅️ Кнопки возврата в каждом меню
✅ Учёт DST (летнее/зимнее время) по историческим данным
"""

import os
import sys
import csv
import datetime as dt
from pathlib import Path
from functools import wraps
from collections import Counter
from typing import Dict, Tuple, Optional, List

import swisseph as swe
from dotenv import load_dotenv
from groq import Groq
from telegram.error import BadRequest

# 🆕 Новые импорты для точного определения часового пояса
import pytz
from timezonefinder import TimezoneFinder

from telegram import (
    Update, KeyboardButton, ReplyKeyboardMarkup,
    InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    PreCheckoutQueryHandler, filters, ContextTypes, ConversationHandler
)

# ---------- CONFIG ----------
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
PAYMENT_TOKEN  = os.getenv("PAYMENT_PROVIDER_TOKEN")

if not TELEGRAM_TOKEN or not GROQ_API_KEY:
    sys.exit("❌ TELEGRAM_TOKEN и GROQ_API_KEY обязательны в .env")

# Проверка токена платежей
PAYMENTS_ENABLED = bool(PAYMENT_TOKEN and ("TEST" in PAYMENT_TOKEN or "LIVE" in PAYMENT_TOKEN))

EPHE_PATH    = BASE_DIR / "ephe"
TOWNS_CSV    = BASE_DIR / "towns.csv"
REPORTS_CSV  = BASE_DIR / "reports.csv"
PAYMENTS_CSV = BASE_DIR / "payments.csv"

# ---------- 👑 АДМИНЫ ----------
ADMINS = {
    7456788249: "Дмитрий (@zadum01)",
    434126413: "Сергей",
    627320643: "Степан (@HuperLemon)",
    7205118: "Darya Bekker (@daryabekker)"
}

ADMIN_IDS = set(ADMINS.keys())

swe.set_ephe_path(str(EPHE_PATH))

# Константы цен (в копейках для Telegram)
PRICE_SINGLE = 30000      # 300₽
PRICE_TRIPLE = 60000      # 600₽
PRICE_SUBSEQUENT = 20000  # 200₽

# ---------- 📡 Groq AI ----------
groq_client = Groq(api_key=GROQ_API_KEY)

def ask_groq(prompt: str, model: str = "llama-3.3-70b-versatile") -> str:
    try:
        resp = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=model,
            temperature=0.8,
            max_tokens=2048
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print("🤖 Groq error:", e)
        return ""

# ---------- 🌍 Точное определение часового пояса с учётом DST ----------
def get_precise_tz_offset(lat: float, lon: float, iso: str, date_str: str) -> Optional[float]:
    """
    Определяет точное смещение от UTC с учётом летнего времени для конкретной даты.
    """
    try:
        # Определяем IANA timezone по координатам
        tf = TimezoneFinder()
        timezone_name = tf.timezone_at(lng=lon, lat=lat)
        
        # Если не нашли по координатам, используем эвристику по стране
        if not timezone_name:
            timezone_name = {
                'RU': 'Europe/Moscow',
                'UA': 'Europe/Kyiv',
                'BY': 'Europe/Minsk',
                'KZ': 'Asia/Almaty',
                'UZ': 'Asia/Tashkent',
                'LT': 'Europe/Vilnius',
                'LV': 'Europe/Riga',
                'EE': 'Europe/Tallinn',
                'GE': 'Asia/Tbilisi',
                'AM': 'Asia/Yerevan',
                'AZ': 'Asia/Baku',
            }.get(iso.upper(), 'Europe/Moscow')
        
        # Парсим дату
        day, month, year = map(int, date_str.split('.'))
        
        # Создаём datetime объект (НЕ используем имя 'dt' для переменной!)
        date_obj = dt.datetime(year, month, day, 12, 0)  # 12:00 чтобы избежать перехода дня
        
        # Получаем timezone и смещение для этой конкретной даты
        tz = pytz.timezone(timezone_name)
        
        # Делаем datetime "aware" (с timezone)
        aware_datetime = tz.localize(date_obj)
        
        # Получаем смещение от UTC
        offset = aware_datetime.utcoffset()
        
        return offset.total_seconds() / 3600.0
        
    except Exception as e:
        print(f"❌ Ошибка определения TZ: {e}, используем базовое значение")
        return None


def calculate_utc_time(local_hour: float, tz_offset: float) -> float:
    """
    Преобразует локальное время в UT (Universal Time) для астрологических расчётов.
    
    Args:
        local_hour: Час рождения по локальному времени
        tz_offset: Смещение локального времени от UTC (в часах)
    
    Returns:
        float: Время в UT (например, 10.5 = 10:30)
    """
    return local_hour - tz_offset


# ---------- 📄 CSV Helpers ----------
def ensure_csv(path: Path, header: List[str]):
    """Создает CSV файл с заголовком, если не существует"""
    if not path.exists():
        path.write_text(",".join(header) + "\n", encoding="utf-8")

def read_csv_dict(path: Path) -> List[Dict[str, str]]:
    """Безопасно читает CSV как список словарей"""
    if not path.exists():
        return []
    try:
        return list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    except Exception as e:
        print(f"❌ CSV read error {path}: {e}")
        return []

def write_csv_dict(path: Path, rows: List[Dict[str, str]], header: List[str]):
    """Безопасно записывает CSV из списка словарей"""
    try:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        print(f"❌ CSV write error {path}: {e}")

# ---------- 💳 Payment Manager ----------
class PaymentManager:
    """Оптимизированное управление платежами и балансом"""
    
    @staticmethod
    def get_user_record(uid: int) -> Optional[Dict[str, str]]:
        rows = read_csv_dict(PAYMENTS_CSV)
        for row in rows:
            if int(row["uid"]) == uid:
                return row
        return None
    
    @staticmethod
    def get_balance(uid: int) -> int:
        if uid in ADMIN_IDS:
            return 999999
        record = PaymentManager.get_user_record(uid)
        return int(record.get("balance", 0)) if record else 0
    
    @staticmethod
    def get_used(uid: int) -> int:
        record = PaymentManager.get_user_record(uid)
        return int(record.get("used", 0)) if record else 0
    
    @staticmethod
    def update_user(uid: int, balance: int = None, used: int = None):
        if uid in ADMIN_IDS:
            return
        
        rows = read_csv_dict(PAYMENTS_CSV)
        header = ["uid", "balance", "used", "last_updated"]
        user_found = False
        
        for row in rows:
            if int(row["uid"]) == uid:
                if balance is not None:
                    row["balance"] = str(balance)
                if used is not None:
                    row["used"] = str(used)
                row["last_updated"] = dt.datetime.now(dt.timezone.utc).isoformat()
                user_found = True
                break
        
        if not user_found:
            rows.append({
                "uid": str(uid),
                "balance": str(balance or 0),
                "used": str(used or 0),
                "last_updated": dt.datetime.now(dt.timezone.utc).isoformat()
            })
        
        write_csv_dict(PAYMENTS_CSV, rows, header)
    
    @staticmethod
    def add_balance(uid: int, amount: int):
        if uid in ADMIN_IDS:
            return
        current = PaymentManager.get_balance(uid)
        PaymentManager.update_user(uid, balance=current + amount)
    
    @staticmethod
    def increment_used(uid: int):
        if uid in ADMIN_IDS:
            return
        current_used = PaymentManager.get_used(uid)
        PaymentManager.update_user(uid, used=current_used + 1)
    
    @staticmethod
    def get_next_price(uid: int) -> int:
        if uid in ADMIN_IDS:
            return 0
        used = PaymentManager.get_used(uid)
        if used == 0:
            return 0
        elif used == 1:
            return PRICE_SINGLE // 100
        elif used <= 3:
            return PRICE_TRIPLE // 100
        else:
            return PRICE_SUBSEQUENT // 100
    
    @staticmethod
    def log_payment(uid: int, amount: int, payload: str, status: str):
        log_file = BASE_DIR / "payment_logs.csv"
        ensure_csv(log_file, ["timestamp", "uid", "amount", "payload", "status"])
        with log_file.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                dt.datetime.now(dt.timezone.utc).isoformat(),
                uid,
                amount,
                payload,
                status
            ])

# ---------- 🌍 Города ----------
CityData = Tuple[float, float, str]

def load_cities() -> Dict[str, CityData]:
    cities: Dict[str, CityData] = {}
    rows = read_csv_dict(TOWNS_CSV)
    for row in rows:
        try:
            cities[row["city"].strip().lower()] = (
                float(row["lat"]),
                float(row["lon"]),
                row["country_iso"].strip().upper()
            )
        except Exception:
            continue
    return cities

def save_city(name: str, lat: float, lon: float, iso: str):
    ensure_csv(TOWNS_CSV, ["city", "lat", "lon", "country_iso"])
    with TOWNS_CSV.open("a", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow([name, lat, lon, iso])

CITY_COORDS = load_cities()

def groq_city(city_input: str) -> Optional[Tuple[str, float, float, str]]:
    prompt = (
        f"Определи город по названию '{city_input}'. "
        "Ответь строго: Город латиницей;широта;долгота;ISO\n"
        "Пример: Moscow;55.7558;37.6173;RU\nЕсли не уверен, напиши NONE"
    )
    raw = ask_groq(prompt)
    if not raw or raw.upper() == "NONE":
        return None
    try:
        name, lat_str, lon_str, iso = [p.strip() for p in raw.split(";")]
        return name, float(lat_str.replace(",", ".")), float(lon_str.replace(",", ".")), iso.upper()
    except Exception:
        return None

def groq_tz(city: str, iso: Optional[str]) -> Optional[float]:
    country = f" (страна ISO {iso})" if iso else ""
    prompt = (
        f"Часовой пояс города '{city}'{country} относительно UTC. "
        "Ответь только числом, например: 3, -5, 5.5"
    )
    try:
        return float(ask_groq(prompt))
    except Exception:
        return None

# ---------- 🌙 Астро ----------
def deg_to_sign(deg: float) -> Tuple[str, int]:
    signs = ["♈ Овен", "♉ Телец", "♊ Близнецы", "♋ Рак", "♌ Лев", "♍ Дева",
             "♎ Весы", "♏ Скорпион", "♐ Стрелец", "♑ Козерог", "♒ Водолей", "♓ Рыбы"]
    d = deg % 360
    sign_idx = int(d // 30)
    d_sign = d % 30
    return f"{int(d_sign)}°{int((d_sign % 1)*60):02d}' {signs[sign_idx]}", sign_idx

def house_for_lon(lon: float, cusps) -> int:
    lon = lon % 360
    best_house, best_diff = 1, 360.0
    for idx, cusp in enumerate(cusps[1:], 1):
        diff = abs((lon - cusp) % 360)
        if diff > 180:
            diff = 360 - diff
        if diff < best_diff:
            best_diff, best_house = diff, idx
    return best_house

def calc_lilith_house(date_str: str, time_str: str, tz_offset: float, lat: float, lon: float):
    """
    Рассчитывает положение Лилит с учётом точного часового пояса.
    """
    d, m, y = map(int, date_str.split("."))
    h, mn = map(int, time_str.split(":"))
    
    # Преобразуем локальное время в UT с учётом DST
    ut = calculate_utc_time(h + mn/60, tz_offset)
    
    jd = swe.julday(y, m, d, ut)
    pos, _ = swe.calc_ut(jd, swe.MEAN_APOG)
    lil_lon = pos[0]
    cusps, _ = swe.houses(jd, lat, lon, b"P")
    return deg_to_sign(lil_lon)[0], deg_to_sign(lil_lon)[1], house_for_lon(lil_lon, cusps), jd, cusps

def calc_nodes(jd: float, true: bool):
    body = swe.TRUE_NODE if true else swe.MEAN_NODE
    pos, _ = swe.calc_ut(jd, body)
    return deg_to_sign(pos[0])[0], deg_to_sign(pos[0])[1], pos[0]

# ---------- 🌕 Фазы Луны ----------
def moon_phase(jd: float) -> str:
    sun, _ = swe.calc_ut(jd, swe.SUN)
    moon, _ = swe.calc_ut(jd, swe.MOON)
    elong = (moon[0] - sun[0]) % 360
    if elong < 45:
        return "🌑 Новолуние (новые начинания)"
    if elong < 90:
        return "🌒 Первая четверть (действие)"
    if elong < 135:
        return "🌕 Полнолуние (результаты)"
    if elong < 180:
        return "🌖 Последняя четверть (завершение)"
    return "🌗 Убывающая Луна (анализ)"

# ---------- 🎹 Клавиатуры ----------
def build_kb(items, row=3, add_back=False, add_cancel=True):
    buttons = [KeyboardButton(str(i)) for i in items]
    if add_back:
        buttons.append(KeyboardButton("⬅ Назад ↩️"))
    if add_cancel:
        buttons.append(KeyboardButton("❌ Отмена"))
    rows = [buttons[i:i+row] for i in range(0, len(buttons), row)]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# ---------- ⬅️ Клавиатура с возвратом ----------
def back_kb():
    return ReplyKeyboardMarkup([["🏠 Главное меню"]], resize_keyboard=True)

CITIES_TOP = ["москва", "санкт-петербург", "новосибирск", "екатеринбург",
              "казань", "нижний новгород", "самара", "омск", "красноярск",
              "ростов-на-дону", "уфа", "пермь", "волгоград", "владивосток"]

# Главное меню
main_kb  = ReplyKeyboardMarkup([
    ["🌙 Расчёт Лилит", "⭐ Расчёт Узлов Луны"],
    ["🛒 Магазин разборов", "💰 Баланс"],
    ["⚙ Админ-меню"]
], resize_keyboard=True)

city_kb  = build_kb([c.title() for c in CITIES_TOP], add_cancel=True)
day_kb   = build_kb(range(1, 32), row=7)
month_kb = build_kb(range(1, 13), row=6)
year_kb  = build_kb(range(1947, 2021), row=6)
hour_kb  = build_kb([f"{i:02d}" for i in range(24)], row=6)

# ---------- 🎬 Состояния ----------
(LIL_CITY, LIL_DAY, LIL_MONTH, LIL_YEAR, LIL_HOUR,
 NOD_CITY, NOD_DAY, NOD_MONTH, NOD_YEAR, NOD_HOUR) = range(10)

# ---------- 👑 Декоратор ----------
def admin_only(func):
    @wraps(func)
    async def wrapped(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("❌ У тебя нет прав доступа к этой команде.")
            return
        return await func(update, ctx)
    return wrapped

# ---------- Хелпер для экранирования Markdown ----------
def escape_markdown(text: str) -> str:
    """Экранирует специальные символы MarkdownV2"""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text

# ---------- 🚀 Команды ----------
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start и коллбэка главного меню"""
    welcome = (
        "✨ *Добро пожаловать в Астрологический Бот!* ✨\n\n"
        "🌌 Я помогу рассчитать:\n"
        "• ⚫ Положение Чёрной Луны (Лилит)\n"
        "• ⭐ Ось Лунных Узлов\n"
        "• 🌕 Фазу Луны в момент рождения\n\n"
        "🎁 *Первый расширенный разбор — в подарок!*\n"
        f"🛒 Далее: {PRICE_SINGLE//100}₽ / {PRICE_TRIPLE//100}₽ / {PRICE_SUBSEQUENT//100}₽\n\n"
        "📖 *Команды:*\n"
        "/balance — проверить баланс\n"
        "/reports — статистика (админы)\n\n"
        "💫 Начнём? Выбирай команду в меню внизу!"
    )
    
    # Обработка коллбэка
    if update.callback_query:
        query = update.callback_query
        try:
            await query.answer()
            await query.message.reply_text(welcome, parse_mode="Markdown", reply_markup=main_kb)
        except BadRequest:
            pass
    else:
        await update.message.reply_text(welcome, parse_mode="Markdown", reply_markup=main_kb)

async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Диалог отменён. Возвращаемся в главное меню.", reply_markup=main_kb)
    return ConversationHandler.END

# ---------- 🏠 Главное меню ----------
async def main_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Вернуться в главное меню"""
    await update.message.reply_text("✅ Возвращаемся в главное меню", reply_markup=main_kb)

# ---------- 💰 Баланс ----------
async def show_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Показать баланс пользователя"""
    uid = update.effective_user.id
    
    if uid in ADMIN_IDS:
        text = (
            f"👑 *Админ-панель*\n\n"
            f"💎 Ты администратор: {ADMINS[uid]}\n"
            f"✅ Безлимитный доступ к разборам\n"
            f"🎁 Использовано разборов: {PaymentManager.get_used(uid)}\n\n"
            f"💰 *Баланс:* ∞ (неограниченно)\n\n"
            "📊 *Команды:*\n"
            "/reports — полная статистика\n"
            "/add_balance — начислить разборы\n"
            "/admin_help — помощь\n"
            "\n⚙️ Админ-меню — для управления"
        )
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_kb)
        return
    
    bal = PaymentManager.get_balance(uid)
    used = PaymentManager.get_used(uid)
    next_price = PaymentManager.get_next_price(uid)
    
    text = (
        f"💰 *Твой баланс*\n\n"
        f"📊 Доступно разборов: *{bal}*\n"
        f"✅ Использовано: *{used}*\n"
        f"💳 Следующий разбор: *{next_price}₽*\n\n"
    )
    
    if bal > 0:
        text += "✨ У тебя есть активные разборы! Можешь использовать их в любое время."
    elif used == 0:
        text += "🎁 *Твой первый разбор БЕСПЛАТНО!* Просто сделай расчёт и нажми «🧠 Расширенный разбор»."
    else:
        text += f"💳 Чтобы продолжить, пополни баланс в «🛒 Магазин разборов»."
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_kb)

# ---------- 🌙 Лилит-разбор ----------
async def lil_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("🏙 *Выбери город рождения* (или напиши вручную):", reply_markup=city_kb, parse_mode="Markdown")
    return LIL_CITY

async def lil_city(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "❌ Отмена":
        return await cancel(update, ctx)
    elif text == "🏠 Главное меню":
        return await cancel(update, ctx)
        
    key = text.lower()
    if key not in CITY_COORDS:
        ai = groq_city(text)
        if not ai:
            await update.message.reply_text("❌ Город не найден в базе и не распознан. Попробуй другой вариант или ближайший крупный город.", reply_markup=city_kb)
            return LIL_CITY
        name, lat, lon, iso = ai
        CITY_COORDS[key] = (lat, lon, iso)
        save_city(name, lat, lon, iso)
    else:
        lat, lon, iso = CITY_COORDS[key]
        name = text
    
    # Определяем базовый часовой пояс (для начального отображения)
    base_tz = groq_tz(name, iso) or 3.0
    ctx.user_data.update({"city": name, "lat": lat, "lon": lon, "iso": iso, "base_tz": base_tz})
    
    await update.message.reply_text(
        f"✅ *Город:* {name}\n🌍 Широта: {lat}, Долгота: {lon}\n⏰ Базовый часовой пояс: UTC{base_tz:+.1f}\n\n"
        "📅 *Теперь выбери день рождения:*\n\n"
        "📌 *ВАЖНО:* Для точных расчётов мы автоматически учтём летнее/зимнее время на момент вашего рождения.",
        reply_markup=day_kb, parse_mode="Markdown"
    )
    return LIL_DAY

async def lil_day(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        return await cancel(update, ctx)
    if not update.message.text.isdigit():
        await update.message.reply_text("❗ Пожалуйста, выбери день кнопкой 1–31.")
        return LIL_DAY
    ctx.user_data["day"] = int(update.message.text)
    await update.message.reply_text("📅 *Месяц рождения:*", reply_markup=month_kb, parse_mode="Markdown")
    return LIL_MONTH

async def lil_month(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        return await cancel(update, ctx)
    if not update.message.text.isdigit():
        await update.message.reply_text("❗ Пожалуйста, выбери месяц кнопкой 1–12.")
        return LIL_MONTH
    ctx.user_data["month"] = int(update.message.text)
    await update.message.reply_text("📅 *Год рождения (1947–2020):*", reply_markup=year_kb, parse_mode="Markdown")
    return LIL_YEAR

async def lil_year(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        return await cancel(update, ctx)
    if not update.message.text.isdigit():
        await update.message.reply_text("❗ Пожалуйста, выбери год кнопкой 1947–2020.")
        return LIL_YEAR
    y = int(update.message.text)
    try:
        dt.date(y, ctx.user_data["month"], ctx.user_data["day"])
    except ValueError:
        await update.message.reply_text("❌ Такой даты не существует! Давай сначала выберем день.", reply_markup=day_kb)
        return LIL_DAY
    ctx.user_data["year"] = y
    await update.message.reply_text("⏰ *Час рождения (00–23):*\n\nНапример: 14 = 14:00", reply_markup=hour_kb, parse_mode="Markdown")
    return LIL_HOUR

async def lil_hour(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        return await cancel(update, ctx)
    if not update.message.text.isdigit():
        await update.message.reply_text("❗ Выбери час кнопкой 00–23.")
        return LIL_HOUR
    
    h = int(update.message.text)
    d, m, y, city, lat, lon, iso = [ctx.user_data[k] for k in ("day", "month", "year", "city", "lat", "lon", "iso")]
    
    # Получаем дату для расчёта DST
    date_str = f"{d:02d}.{m:02d}.{y}"
    
    # Определяем точное смещение с учётом летнего времени
    tz_offset = get_precise_tz_offset(lat, lon, iso, date_str)
    
    # Если не удалось определить точно, используем базовое от groq
    if tz_offset is None:
        tz_offset = ctx.user_data.get("base_tz", 3.0)
    
    # Определяем, был ли применён DST
    base_tz = ctx.user_data.get("base_tz", tz_offset)
    dst_applied = abs(tz_offset - base_tz) > 0.5
    
    time_str = f"{h:02d}:00"
    
    # Рассчитываем позиции с точным временем
    pos, sign_idx, house, jd, cusps = calc_lilith_house(date_str, time_str, tz_offset, lat, lon)
    
    # ... остальной код сохранения данных
    ctx.user_data["tz_offset"] = tz_offset
    
    sign_tx = [
        "♈ Лилит в Овне — импульс, независимость, смелость.",
        "♉ Лилит в Тельце — ценности, стабильность, чувственность.",
        "♊ Лилит в Близнецах — слово, информация, любопытство.",
        "♋ Лилит в Раке — семья, уязвимость, защита.",
        "♌ Лилит во Льве — самовыражение, признание, творчество.",
        "♍ Лилит в Деве — перфекционизм, анализ, полезность.",
        "♎ Лилит в Весах — партнёрство, баланс, гармония.",
        "♏ Лилит в Скорпионе — власть, трансформация, глубина.",
        "♐ Лилит в Стрельце — смысл, вера, путешествия.",
        "♑ Лилит в Козероге — статус, ответственность, цели.",
        "♒ Лилит в Водолее — свобода, уникальность, инновации.",
        "♓ Лилит в Рыбах — интуиция, границы, эмпатия."
    ][sign_idx]
    
    house_tx = {
        1: "🏠 1 дом — самовыражение, личность.",
        2: "💰 2 дом — деньги, ресурсы, ценности.",
        3: "💬 3 дом — общение, обучение, окружение.",
        4: "🏡 4 дом — семья, корни, внутренняя база.",
        5: "🎨 5 дом — творчество, дети, любовь.",
        6: "💼 6 дом — работа, здоровье, рутина.",
        7: "💞 7 дом — партнёрства, брак, отношения.",
        8: "🦋 8 дом — трансформация, общие ресурсы.",
        9: "🌍 9 дом — путешествия, философия, вера.",
        10: "🏆 10 дом — карьера, статус, репутация.",
        11: "👥 11 дом — друзья, группы, мечты.",
        12: "🔮 12 дом — подсознание, тайны, духовность."
    }.get(house, "")
    
    phase = moon_phase(jd)
    
    # Узлы внутри Лилит (бесплатно)
    pos_node_str, node_sign_idx, node_lon = calc_nodes(jd, False)
    node_house = house_for_lon(node_lon, cusps)
    south_lon = (node_lon + 180) % 360
    pos_south_str, south_sign_idx = deg_to_sign(south_lon)
    south_house = house_for_lon(south_lon, cusps)
    
    nodes_block = (
        f"🌟 *Лунные Узлы:*\n"
        f"✅ Северный (рост): {pos_node_str}, дом {node_house}\n"
        f"🔄 Южный (прошлое): {pos_south_str}, дом {south_house}\n\n"
        f"*Как работать:*\n"
        f"Используй энергию Южного узла как базу, но развивайся через темы Северного."
    )
    
    # Формируем информацию о времени
    dst_status = "летнее время" if dst_applied else "зимнее время"
    
    full_text = (
        f"📍 *Данные рождения:* {date_str}  {time_str}\n"
        f"🌍 Место: {city} ({iso})\n"
        f"⏰ Часовой пояс: UTC{tz_offset:+.1f} ({dst_status})\n\n"
        
        f"⚫ *Чёрная Луна (Лилит):*\n"
        f"📍 Позиция: {pos}, дом {house}\n\n"
        f"🎨 *Значение:*\n{sign_tx}\n\n"
        f"🏠 *Дом жизни:*\n{house_tx}\n\n"
        
        f"🌙 *Фаза Луны:* {phase}\n"
        f"Эта фаза показывает, на каком этапе эмоционального цикла ты родился(-лась).\n\n"
        
        f"{nodes_block}"
    )
    
    # Отправляем результат с inline кнопкой
    await update.message.reply_text(full_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🧠 Получить расширенный разбор", callback_data="deep_lilith")]
    ]))
    
    # Восстанавливаем главную клавиатуру
    await update.message.reply_text("👉👉👉 Выбери действие в меню:", reply_markup=main_kb)
    
    # лог
    ensure_csv(REPORTS_CSV, ["ts", "uid", "username", "full_name", "type", "city", "iso", "date", "time", "tz", "tz_offset", "dst_applied"])
    with REPORTS_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            dt.datetime.now(dt.timezone.utc).isoformat(),
            update.effective_user.id,
            update.effective_user.username or "",
            update.effective_user.full_name or "",
            "lilith",
            city,
            iso,
            date_str,
            time_str,
            base_tz,
            tz_offset,
            int(dst_applied)
        ])
    return ConversationHandler.END

# ---------- ⭐ Узлы Луны ----------
async def nodes_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("🏙 *Город рождения для расчёта узлов:*", reply_markup=city_kb, parse_mode="Markdown")
    return NOD_CITY

async def nodes_city(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == "❌ Отмена":
        return await cancel(update, ctx)
    elif text == "🏠 Главное меню":
        return await cancel(update, ctx)
        
    key = text.lower()
    if key not in CITY_COORDS:
        ai = groq_city(text)
        if not ai:
            await update.message.reply_text("❌ Город не найден. Попробуй ещё раз.", reply_markup=city_kb)
            return NOD_CITY
        name, lat, lon, iso = ai
        CITY_COORDS[key] = (lat, lon, iso)
        save_city(name, lat, lon, iso)
    else:
        lat, lon, iso = CITY_COORDS[key]
        name = text
    
    base_tz = groq_tz(name, iso) or 3.0
    ctx.user_data.update({"nodes_city": name, "nodes_lat": lat, "nodes_lon": lon, "nodes_iso": iso, "nodes_base_tz": base_tz})
    
    await update.message.reply_text(
        f"✅ Город: {name}\n🌍 Координаты: {lat}, {lon}\n⏰ Базовый часовой пояс: UTC{base_tz:+.1f}\n\n"
        "📅 *День рождения:*\n\n"
        "📌 *ВАЖНО:* Мы автоматически учтём летнее/зимнее время на момент рождения.",
        reply_markup=day_kb, parse_mode="Markdown"
    )
    return NOD_DAY

async def nodes_day(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        return await cancel(update, ctx)
    if not update.message.text.isdigit():
        await update.message.reply_text("❗ Выбери день кнопкой 1–31.")
        return NOD_DAY
    ctx.user_data["nodes_day"] = int(update.message.text)
    await update.message.reply_text("📅 *Месяц:*", reply_markup=month_kb, parse_mode="Markdown")
    return NOD_MONTH

async def nodes_month(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        return await cancel(update, ctx)
    if not update.message.text.isdigit():
        await update.message.reply_text("❗ Выбери месяц 1–12.")
        return NOD_MONTH
    ctx.user_data["nodes_month"] = int(update.message.text)
    await update.message.reply_text("📅 *Год (1947–2020):*", reply_markup=year_kb, parse_mode="Markdown")
    return NOD_YEAR

async def nodes_year(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        return await cancel(update, ctx)
    if not update.message.text.isdigit():
        await update.message.reply_text("❗ Выбери год 1947–2020.")
        return NOD_YEAR
    y = int(update.message.text)
    try:
        dt.date(y, ctx.user_data["nodes_month"], ctx.user_data["nodes_day"])
    except ValueError:
        await update.message.reply_text("❌ Такой даты нет! Начнём с дня.", reply_markup=day_kb)
        return NOD_DAY
    ctx.user_data["nodes_year"] = y
    await update.message.reply_text("⏰ *Час рождения (00–23):*", reply_markup=hour_kb, parse_mode="Markdown")
    return NOD_HOUR

async def nodes_hour(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        return await cancel(update, ctx)
    if not update.message.text.isdigit():
        await update.message.reply_text("❗ Выбери час 00–23.")
        return NOD_HOUR
    
    h = int(update.message.text)
    d, m, y, city, lat, lon, iso = [ctx.user_data[k] for k in ("nodes_day", "nodes_month", "nodes_year", "nodes_city", "nodes_lat", "nodes_lon", "nodes_iso")]
    
    date_str = f"{d:02d}.{m:02d}.{y}"
    
    # Точное смещение с учётом DST
    tz_offset = get_precise_tz_offset(lat, lon, iso, date_str)
    if tz_offset is None:
        tz_offset = ctx.user_data.get("nodes_base_tz", 3.0)
    
    base_tz = ctx.user_data.get("nodes_base_tz", tz_offset)
    dst_applied = abs(tz_offset - base_tz) > 0.5
    
    time_str = f"{h:02d}:00"

    # Используем date_obj вместо dt_date
    date_obj = dt.datetime.strptime(date_str, "%d.%m.%Y")
    local_hour = h
    ut_hour = calculate_utc_time(local_hour, tz_offset)
    
    jd = swe.julday(date_obj.year, date_obj.month, date_obj.day, ut_hour)

    # Узлы
    pos_node_str, node_sign_idx, node_lon = calc_nodes(jd, False)
    cusps, _ = swe.houses(jd, lat, lon, b"P")
    node_house = house_for_lon(node_lon, cusps)
    south_lon = (node_lon + 180) % 360
    pos_south_str, south_sign_idx = deg_to_sign(south_lon)
    south_house = house_for_lon(south_lon, cusps)

    dst_status = "летнее время" if dst_applied else "зимнее время"
    
    text_out = (
        f"📊 *Расчёт оси Лунных узлов*\n\n"
        f"📍 Данные: {date_str}  {time_str}\n"
        f"🌍 Место: {city} ({iso})\n"
        f"⏰ Часовой пояс: UTC{tz_offset:+.1f} ({dst_status})\n\n"
        
        f"✨ *Северный узел (Рост):*\n"
        f"📍 {pos_node_str}, дом {node_house}\n\n"
        
        f"🔄 *Южный узел (Прошлое):*\n"
        f"📍 {pos_south_str}, дом {south_house}\n\n"
        
        f"💡 *Интерпретация:*\n"
        f"Твой путь — развивать темы Северного узла, используя опыт Южного. "
        f"Это ключ к твоему личностному росту в этой жизни."
    )
    await update.message.reply_text(text_out, parse_mode="Markdown", reply_markup=main_kb)

    # лог
    ensure_csv(REPORTS_CSV, ["ts", "uid", "username", "full_name", "type", "city", "iso", "date", "time", "tz", "tz_offset", "dst_applied"])
    with REPORTS_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            dt.datetime.now(dt.timezone.utc).isoformat(),
            update.effective_user.id,
            update.effective_user.username or "",
            update.effective_user.full_name or "",
            "nodes",
            city,
            iso,
            date_str,
            time_str,
            base_tz,
            tz_offset,
            int(dst_applied)
        ])
    return ConversationHandler.END

# ---------- 🛒 Магазин ----------
async def shop_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    if uid in ADMIN_IDS:
        text = (
            "👑 *Админ-меню магазина*\n\n"
            f"💎 Ты администратор: {ADMINS[uid]}\n"
            f"✅ Безлимитный доступ к разборам\n"
            f"💰 Оплата не требуется\n\n"
            "📊 *Команды:*\n"
            "/add_balance <id> <кол-во> — начислить\n"
            "/reports — статистика\n"
            "/admin_help — помощь\n"
            "\n🏠 Вернуться в главное меню?"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("👥 Список админов", callback_data="admin_list")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
        ])
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
        return
    
    bal = PaymentManager.get_balance(uid)
    used = PaymentManager.get_used(uid)
    next_price = PaymentManager.get_next_price(uid)
    
    text = (
        "🛒 *Магазин Углублённых Разборов*\n\n"
        f"💰 Твой баланс: *{bal}* разбор(ов)\n"
        f"📊 Использовано: *{used}*\n"
        f"💳 Следующий разбор: *{next_price}₽*\n\n"
        "📋 *Цены:*\n"
        f"• 1 разбор — {PRICE_SINGLE//100}₽\n"
        f"• 3 разбора — {PRICE_TRIPLE//100}₽ (экономия {PRICE_SINGLE//100}₽)\n"
        f"• Последующие — {PRICE_SUBSEQUENT//100}₽\n\n"
        "🎁 *Первый разбор — ВСЕГДА БЕСПЛАТНО!*\n\n"
    )
    
    if PAYMENTS_ENABLED:
        text += "💳 *Оплата через ЮKassa (Telegram Payments)*\nБезопасно, быстро, без комиссий.\n\n👇 Выбирай пакет ниже:"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"💳 1 разбор — {PRICE_SINGLE//100}₽", callback_data="buy_1")],
            [InlineKeyboardButton(f"💳 3 разбора — {PRICE_TRIPLE//100}₽", callback_data="buy_3")],
            [InlineKeyboardButton("🎁 Первый БЕСПЛАТНО", callback_data="first_free")]
        ])
    else:
        text += "⚠️ *Платежи временно отключены*. Обратись к администратору."
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎁 Первый БЕСПЛАТНО", callback_data="first_free")]
        ])
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)

# ---------- 🎁 Первый бесплатный ----------
async def first_free(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
        
    uid = query.from_user.id
    
    if uid in ADMIN_IDS:
        await query.message.reply_text("👑 Ты администратор! У тебя уже безлимитный доступ.", reply_markup=main_kb)
        return
        
    used = PaymentManager.get_used(uid)
    if used > 0:
        await query.message.reply_text("❗ Ты уже использовал бесплатный разбор. Выбери платный пакет.", reply_markup=main_kb)
        return
    
    PaymentManager.increment_used(uid)
    await query.message.reply_text(
        "🎁 *Поздравляю! Первый расширенный разбор — в подарок!*\n\n"
        "Теперь можешь получить его в любом расчёте Лилит или Узлов.\n"
        "Просто нажми кнопку «🧠 Расширенный разбор» после расчёта.",
        parse_mode="Markdown",
        reply_markup=main_kb
    )

# ---------- 💳 ОПЛАТА ----------
async def buy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Создать инвойс для оплаты"""
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    if query.from_user.id in ADMIN_IDS:
        await query.message.reply_text("👑 Ты администратор! Оплата не требуется.", reply_markup=main_kb)
        return
    
    if not PAYMENTS_ENABLED:
        await query.message.reply_text("❌ Платёжная система не настроена. Обратись к администратору.", reply_markup=main_kb)
        return
    
    uid = query.from_user.id
    chat_id = query.message.chat_id
    
    if query.data == "buy_1":
        title = "🔮 1 расширенный разбор"
        description = "Индивидуальный психологический разбор Лилит/Узлов"
        payload = f"deep1_{uid}_{dt.datetime.now().timestamp()}"
        prices = [LabeledPrice("1 разбор", PRICE_SINGLE)]
        amount = PRICE_SINGLE
    else:  # buy_3
        title = "🔮 3 расширенных разбора"
        description = "Экономный пакет + скидка 33%"
        payload = f"deep3_{uid}_{dt.datetime.now().timestamp()}"
        prices = [LabeledPrice("3 разбора", PRICE_TRIPLE)]
        amount = PRICE_TRIPLE
    
    PaymentManager.log_payment(uid, amount, payload, "invoice_created")
    
    try:
        await ctx.bot.send_invoice(
            chat_id=chat_id,
            title=title,
            description=description,
            payload=payload,
            provider_token=PAYMENT_TOKEN,
            currency="RUB",
            prices=prices,
            start_parameter="astrology-payment",
            need_name=True,
            need_phone_number=False,
            need_email=False,
            is_flexible=False
        )
    except Exception as e:
        print(f"❌ Invoice error: {e}")
        await query.message.reply_text("❌ Ошибка создания платежа. Попробуй позже или обратись к администратору.", reply_markup=main_kb)

async def precheckout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Проверка перед оплатой"""
    query = update.pre_checkout_query
    try:
        if not query.invoice_payload.startswith(("deep1_", "deep3_")):
            await query.answer(ok=False, error_message="Некорректный формат платежа")
            return
        
        uid = int(query.invoice_payload.split("_")[1])
        if uid != query.from_user.id:
            await query.answer(ok=False, error_message="Платеж не соответствует пользователю")
            return
        
        await query.answer(ok=True)
    except Exception as e:
        print(f"❌ Pre-checkout error: {e}")
        await query.answer(ok=False, error_message="Техническая ошибка. Попробуй позже.")

async def success_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Обработка успешного платежа"""
    payment = update.message.successful_payment
    uid = update.effective_user.id
    
    try:
        if payment.invoice_payload.startswith("deep1_"):
            add_count = 1
            amount = PRICE_SINGLE
        elif payment.invoice_payload.startswith("deep3_"):
            add_count = 3
            amount = PRICE_TRIPLE
        else:
            print(f"❌ Unknown payload: {payment.invoice_payload}")
            await update.message.reply_text("❌ Ошибка обработки платежа. Обратись к администратору.", reply_markup=main_kb)
            return
        
        PaymentManager.add_balance(uid, add_count)
        PaymentManager.log_payment(uid, amount, payment.invoice_payload, "success")
        
        new_balance = PaymentManager.get_balance(uid)
        await update.message.reply_text(
            f"✅ *Оплата успешно завершена!*\n\n"
            f"💳 Сумма: {amount//100}₽\n"
            f"🎁 Начислено разборов: {add_count}\n"
            f"💰 Текущий баланс: {new_balance}\n\n"
            f"Теперь можешь получить расширенный разбор!",
            parse_mode="Markdown",
            reply_markup=main_kb
        )
        
        # Сообщение админам
        for admin in ADMIN_IDS:
            try:
                await ctx.bot.send_message(
                    admin,
                    f"💰 *Новый платёж!*\n\n"
                    f"👤 Пользователь: {uid}\n"
                    f"💳 Сумма: {amount//100}₽\n"
                    f"🎁 Разборов: {add_count}\n"
                    f"💰 Баланс: {new_balance}",
                    parse_mode="Markdown"
                )
            except:
                pass
                
    except Exception as e:
        print(f"❌ Payment processing error: {e}")
        PaymentManager.log_payment(uid, 0, payment.invoice_payload, f"error_{e}")
        await update.message.reply_text("❌ Ошибка обработки платежа. Обратись к администратору.", reply_markup=main_kb)

# ---------- 🧠 Расширенный разбор ----------
async def deep_lilith(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    try:
        await query.answer(cache_time=0)
    except BadRequest as e:
        print(f"⚠️ Предупреждение callback: {e}")
    
    uid = query.from_user.id
    bal = PaymentManager.get_balance(uid)
    used = PaymentManager.get_used(uid)

    # 🎁 Первый бесплатно
    if used == 0:
        PaymentManager.increment_used(uid)
        base = query.message.text
        prompt = (
            "Ты профессиональный астролог-психолог. "
            "Сделай мягкий, поддерживающий, глубокий разбор: Лилит, Узлы, Фазу Луны. "
            "Дай практические советы, как работать с этой энергией, без фатализма. "
            "Отвечай на русском, дружелюбно, структурированно.\n\n" + base
        )
        deep = ask_groq(prompt)
        
        if deep:
            deep_escaped = escape_markdown(deep)
            txt = (
                "🎁 *ПОДАРОК! Первый расширенный разбор — бесплатно!*\n\n"
                "🌟 Вот подробный психологичный анализ:\n\n"
                f"{deep_escaped}"
            )
        else:
            txt = "⏳ Пока не удалось получить разбор. Попробуй позже."
        
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Получить ещё разбор", callback_data="deep_lilith")]])
        await query.message.reply_text(txt, parse_mode="MarkdownV2", reply_markup=kb)
        await query.message.reply_text("✅ Выбери действие:", reply_markup=main_kb)
        return

    # 💰 Платные
    price_rub = PaymentManager.get_next_price(uid)
    
    # Админы не платят за разборы
    if uid in ADMIN_IDS:
        base = query.message.text
        prompt = (
            "Ты профессиональный астролог-психолог. "
            "Сделай мягкий, поддерживающий, глубокий разбор: Лилит, Узлы, Фазу Луны. "
            "Дай практические советы, как работать с этой энергией, без фатализма. "
            "Отвечай на русском, дружелюбно, структурированно.\n\n" + base
        )
        deep = ask_groq(prompt)
        
        if deep:
            deep_escaped = escape_markdown(deep)
            txt = deep_escaped
        else:
            txt = "⏳ Не удалось получить разбор. Попробуй позже."
        
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Админ: Бобер", callback_data="deep_lilith")]])
        await query.message.reply_text(txt, parse_mode="MarkdownV2", reply_markup=kb)
        await query.message.reply_text("✅ Выбери действие:", reply_markup=main_kb)
        return
    
    if bal <= 0:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"💳 Купить 1 разбор — {price_rub}₽", callback_data="buy_1")],
            [InlineKeyboardButton(f"💳 Купить 3 разбора — {PRICE_TRIPLE//100}₽", callback_data="buy_3")]
        ])
        await query.message.reply_text(
            f"💎 *Расширенный разбор стоит {price_rub}₽*\n\n"
            f"💰 Твой баланс: {bal}\n"
            f"📊 Использовано: {used}\n\n"
            "👉 Выбери пакет ниже:",
            parse_mode="Markdown",
            reply_markup=kb
        )
        return

    # ✅ Списываем и выдаём (только не админов)
    if uid not in ADMIN_IDS:
        PaymentManager.update_user(uid, balance=bal - 1)
    
    base = query.message.text
    prompt = (
        "Ты профессиональный астролог-психолог. "
        "Сделай мягкий, поддерживающий, глубокий разбор: Лилит, Узлы, Фазу Луны. "
        "Дай практические советы, как работать с этой энергией, без фатализма. "
        "Отвечай на русском, дружелюбно, структурированно.\n\n" + base
    )
    deep = ask_groq(prompt)
    
    if deep:
        deep_escaped = escape_markdown(deep)
        txt = deep_escaped
    else:
        txt = "⏳ Не удалось получить разбор. Попробуй позже."

    # Кнопка «Ещё» или «Купить»
    kb_lines = []
    if uid in ADMIN_IDS or PaymentManager.get_balance(uid) > 0:
        kb_lines.append([InlineKeyboardButton("🔄 Получить ещё разбор", callback_data="deep_lilith")])
    else:
        kb_lines.append([InlineKeyboardButton(f"💳 Купить разбор — {PaymentManager.get_next_price(uid)}₽", callback_data="buy_1")])
    kb = InlineKeyboardMarkup(kb_lines)

    await query.message.reply_text(txt, parse_mode="MarkdownV2", reply_markup=kb)
    await query.message.reply_text("✅ Выбери действие:", reply_markup=main_kb)

# ---------- 💰 Админ-управление балансом ----------
@admin_only
async def add_balance_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/add_balance <user_id> <amount> - начислить разборы"""
    try:
        args = ctx.args
        if len(args) != 2:
            await update.message.reply_text("❌ Использование: /add_balance <user_id> <количество>\n\nПример: /add_balance 123456789 5", reply_markup=main_kb)
            return
        
        target_uid = int(args[0])
        amount = int(args[1])
        
        if target_uid in ADMIN_IDS:
            await update.message.reply_text("❌ Администраторам не нужно начислять разборы — у них безлимит!", reply_markup=main_kb)
            return
        
        PaymentManager.add_balance(target_uid, amount)
        current = PaymentManager.get_balance(target_uid)
        
        await update.message.reply_text(
            f"✅ Баланс обновлён!\n\n"
            f"👤 Пользователь: {target_uid}\n"
            f"➕ Начислено: {amount} разбор(ов)\n"
            f"💰 Текущий баланс: {current}",
            parse_mode="Markdown",
            reply_markup=main_kb
        )
        
        # Уведомляем пользователя
        try:
            await ctx.bot.send_message(
                target_uid,
                f"🎁 Тебе начислено {amount} разбор(а)!\n"
                f"💰 Твой текущий баланс: {current}\n\n"
                f"Приятного использования! 🌟",
                parse_mode="Markdown"
            )
        except Exception as e:
            print(f"❌ Не удалось уведомить пользователя {target_uid}: {e}")
            
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}\n\nУбедитесь, что вводите числа.", reply_markup=main_kb)

# ---------- 📊 Админ-отчёт ----------
@admin_only
async def reports(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/reports - показать полную статистику (только для админов)"""
    await update.message.reply_text("📊 Собираю статистику, подождите...")
    
    try:
        # Статистика расчетов
        rows = read_csv_dict(REPORTS_CSV)
        total = len(rows)
        by_type = Counter(r["type"] for r in rows)
        by_city = Counter(r["city"] for r in rows)
        by_user = Counter(r["username"] or r["uid"] for r in rows)
        
        # Статистика платежей
        payment_rows = read_csv_dict(PAYMENTS_CSV)
        total_users = len(payment_rows)
        total_balance = sum(int(r.get("balance", 0)) for r in payment_rows)
        total_used = sum(int(r.get("used", 0)) for r in payment_rows)
        
        # Сумма платежей из логов
        log_file = BASE_DIR / "payment_logs.csv"
        total_revenue = 0
        if log_file.exists():
            log_rows = read_csv_dict(log_file)
            total_revenue = sum(int(r.get("amount", 0)) for r in log_rows if r.get("status") == "success")
        
        # Список админов
        admin_list = "\n".join([f"• {name} (`{uid}`)" for uid, name in ADMINS.items()])
        
        # Топ-5 пользователей по балансу
        top_balance = sorted(payment_rows, key=lambda x: int(x.get("balance", 0)), reverse=True)[:5]
        top_balance_text = "\n".join([f"• `{r['uid']}`: {r.get('balance', 0)} разборов" for r in top_balance])
        
        # Статистика DST
        dst_count = sum(int(r.get("dst_applied", 0)) for r in rows if r.get("dst_applied"))
        
        text = (
            f"📊 *Административный отчёт*\n\n"
            
            f"👥 *Расчёты:*\n"
            f"• Всего: {total}\n"
            f"• Лилит: {by_type.get('lilith', 0)}\n"
            f"• Узлы: {by_type.get('nodes', 0)}\n"
            f"• С учётом DST: {dst_count}\n\n"
            
            f"💰 *Финансы:*\n"
            f"• Пользователей: {total_users}\n"
            f"• Общий баланс: {total_balance}\n"
            f"• Использовано: {total_used}\n"
            f"• Выручка: {total_revenue//100}₽\n\n"
            
            f"👑 *Администраторы ({len(ADMINS)}):*\n{admin_list}\n\n"
            
            f"🏆 *Топ-5 городов:*\n{', '.join(f'{c}({v})' for c,v in by_city.most_common(5))}\n\n"
            
            f"💎 *Топ-5 по балансу:*\n{top_balance_text if top_balance_text else 'Нет данных'}\n\n"
            
            f"👤 *Топ-5 активных:*\n{', '.join(f'{u}({v})' for u,v in by_user.most_common(5))}"
        )
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_kb)
        
    except Exception as e:
        print(f"❌ Ошибка в reports: {e}")
        await update.message.reply_text(f"❌ Ошибка при сборе статистики: {e}", reply_markup=main_kb)

# ---------- 👑 Админ-меню ----------
@admin_only
async def admin_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """⚙ Админ-меню — главное меню для администраторов"""
    uid = update.effective_user.id
    
    text = (
        f"👑 *Административное меню*\n\n"
        f"Привет, {ADMINS[uid]}!\n\n"
        "📋 *Доступные функции:*\n\n"
        
        "📊 *Статистика:*\n"
        "• /reports — полный отчёт (с учётом DST)\n"
        "• /balance — твой статус\n\n"
        
        "💰 *Управление:*\n"
        "• /add_balance <id> <кол-во> — начислить разборы\n"
        "• /admin_help — справка по командам\n\n"
        
        "🛒 *Магазин:*\n"
        "• У админов безлимитный доступ\n"
        "• Оплата не требуется\n\n"
        
        "🏠 Вернуться в главное меню?"
    )
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("💰 Начислить разборы", callback_data="admin_add_balance")],
        [InlineKeyboardButton("❓ Помощь", callback_data="admin_help")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ])
    
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)

# ---------- 👑 Список админов ----------
@admin_only
async def admin_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Показать список администраторов"""
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    admin_list_text = "\n".join([f"• {name} (`{uid}`)" for uid, name in ADMINS.items()])
    
    text = (
        f"👑 *Список администраторов ({len(ADMINS)}):*\n\n"
        f"{admin_list_text}\n\n"
        "🏠 Вернуться в главное меню?"
    )
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ])
    
    await query.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)

# ---------- 👑 Инструкция по начислению ----------
@admin_only
async def admin_add_balance_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Инструкция по начислению разборов"""
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest:
        return
    
    text = (
        "💰 *Начисление разборов*\n\n"
        "Используйте команду:\n"
        "`/add_balance <user_id> <количество>`\n\n"
        "Пример:\n"
        "`/add_balance 123456789 5`\n\n"
        "🏠 Вернуться в главное меню?"
    )
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ])
    
    await query.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)

# ---------- 👑 Админ-справка ----------
@admin_only
async def admin_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/admin_help - список всех админских команд"""
    help_text = (
        "👑 *Админ-панель управления*\n\n"
        
        "📊 *Команды статистики:*\n"
        "/reports — полный отчёт по боту (с учётом DST)\n"
        "/balance — твой админ-статус (безлимит)\n\n"
        
        "💰 *Команды управления:*\n"
        "/add_balance <user_id> <количество> — начислить разборы\n"
        "   Пример: `/add_balance 123456789 5`\n\n"
        
        "⚙️ *Команды меню:*\n"
        "/admin — главное админ-меню\n"
        "/admin_help — эта справка\n\n"
        
        "🎁 *Особенности админов:*\n"
        "✅ Безлимитный доступ к разборам\n"
        "✅ Не тратят баланс\n"
        "✅ Полный доступ к статистике\n"
        "✅ Уведомления о всех платежах\n"
        f"\n👑 *Текущие админы ({len(ADMINS)}):*\n"
    )
    
    for uid, name in ADMINS.items():
        help_text += f"• {name} (`{uid}`)\n"
    
    help_text += "\n🏠 Вернуться в главное меню — нажмите кнопку ниже"
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ])
    
    await update.message.reply_text(help_text, parse_mode="Markdown", reply_markup=kb)

# ---------- 🚀 Запуск ----------
def main():
    print("✅ TELEGRAM_TOKEN загружен:", TELEGRAM_TOKEN[:15] + "...")
    print(f"💳 Payments enabled: {PAYMENTS_ENABLED}")
    print(f"👑 Администраторы ({len(ADMINS)}): {', '.join(ADMINS.values())}")
    print("⏰ Точное определение часового пояса: АКТИВИРОВАНО")
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("balance", show_balance))
    app.add_handler(CommandHandler("reports", reports))
    app.add_handler(CommandHandler("add_balance", add_balance_cmd))
    app.add_handler(CommandHandler("admin_help", admin_help))
    app.add_handler(CommandHandler("admin", admin_menu))
    
    # Кнопки меню
    app.add_handler(MessageHandler(filters.Regex("^🛒 Магазин разборов$"), shop_start))
    app.add_handler(MessageHandler(filters.Regex("^💰 Баланс$"), show_balance))
    app.add_handler(MessageHandler(filters.Regex("^⚙ Админ-меню$"), admin_menu))
    app.add_handler(MessageHandler(filters.Regex("^🏠 Главное меню$"), main_menu))
    
    # Callbacks
    app.add_handler(CallbackQueryHandler(deep_lilith, pattern="^deep_lilith$"))
    app.add_handler(CallbackQueryHandler(buy, pattern="^buy_"))
    app.add_handler(CallbackQueryHandler(first_free, pattern="^first_free$"))
    app.add_handler(CallbackQueryHandler(admin_menu, pattern="^admin_menu$"))
    app.add_handler(CallbackQueryHandler(reports, pattern="^admin_stats$"))
    app.add_handler(CallbackQueryHandler(admin_help, pattern="^admin_help$"))
    app.add_handler(CallbackQueryHandler(admin_list, pattern="^admin_list$"))
    app.add_handler(CallbackQueryHandler(admin_add_balance_msg, pattern="^admin_add_balance$"))
    app.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
    
    # Платежи
    app.add_handler(PreCheckoutQueryHandler(precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, success_payment))

    # --- Лилит ---
    lil_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🌙 Расчёт Лилит$"), lil_start)],
        states={
            LIL_CITY:   [MessageHandler(filters.TEXT & ~filters.COMMAND, lil_city)],
            LIL_DAY:    [MessageHandler(filters.TEXT & ~filters.COMMAND, lil_day)],
            LIL_MONTH:  [MessageHandler(filters.TEXT & ~filters.COMMAND, lil_month)],
            LIL_YEAR:   [MessageHandler(filters.TEXT & ~filters.COMMAND, lil_year)],
            LIL_HOUR:   [MessageHandler(filters.TEXT & ~filters.COMMAND, lil_hour)],
        },
        fallbacks=[CommandHandler("cancel", cancel), MessageHandler(filters.Regex("^🏠 Главное меню$"), cancel)],
    )
    app.add_handler(lil_conv)

    # --- Узлы ---
    nodes_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^⭐ Расчёт Узлов Луны$"), nodes_start)],
        states={
            NOD_CITY:   [MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_city)],
            NOD_DAY:    [MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_day)],
            NOD_MONTH:  [MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_month)],
            NOD_YEAR:   [MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_year)],
            NOD_HOUR:   [MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_hour)],
        },
        fallbacks=[CommandHandler("cancel", cancel), MessageHandler(filters.Regex("^🏠 Главное меню$"), cancel)],
    )
    app.add_handler(nodes_conv)

    print("🤖 Bot started successfully!")
    if PAYMENTS_ENABLED:
        print(f"✅ Payment provider token loaded: {PAYMENT_TOKEN[:10]}...")
    else:
        print("⚠️ Payments DISABLED - no valid token")
    
    print("✅ Часовой пояс: Автоматический учёт DST (летнее/зимнее время)")
    
    app.run_polling()

if __name__ == "__main__":
    main()