#!/usr/bin/env python3
import os
import sys
import base64  # ДОБАВЬ ЭТО
from pathlib import Path
from dotenv import load_dotenv
import requests

# Загрузка переменных
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

PAYMENT_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN")

if not PAYMENT_TOKEN:
    print("❌ PAYMENT_PROVIDER_TOKEN не найден в .env")
    sys.exit(1)

print("🔑 Токен загружен:", PAYMENT_TOKEN[:20] + "...")

# Разбираем токен
try:
    shop_id, mode, secret_key = PAYMENT_TOKEN.split(":")
    print(f"✅ Shop ID: {shop_id}")
    print(f"✅ Mode: {mode}")
    print(f"✅ Secret Key: {secret_key[:5]}...")
except:
    print("❌ Неверный формат токена. Должен быть: ShopID:LIVE:SecretKey")
    sys.exit(1)

# Генерируем Basic Auth заголовок
credentials = f"{shop_id}:{secret_key}"
login_pass = base64.b64encode(credentials.encode()).decode()  # ВОТ ЭТА СТРОКА

# Тестируем подключение
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {login_pass}"
}

try:
    print("\n🌐 Проверка подключения к api.yookassa.ru...")
    response = requests.get(
        "https://api.yookassa.ru/v3/me", 
        headers=headers, 
        timeout=10
    )
    
    if response.status_code == 200:
        data = response.json()
        print("✅ Подключение успешно!")
        print(f"✅ Магазин: {data.get('name', 'N/A')}")
        print(f"✅ Статус: {data.get('status', 'N/A')}")
        print(f"✅ test_mode: {data.get('test', 'N/A')}")
    elif response.status_code == 401:
        print("❌ Ошибка авторизации! Проверьте shop_id и secret_key")
        print(f"Ответ: {response.text}")
    elif response.status_code == 404:
        print("❌ Магазин не найден! Проверьте Shop ID")
    else:
        print(f"❌ Ошибка API: {response.status_code}")
        print(f"Ответ: {response.text}")

except requests.exceptions.Timeout:
    print("❌ Таймаут подключения! Проверьте интернет или доступность API")
except requests.exceptions.ConnectionError:
    print("❌ Невозможно подключиться к api.yookassa.ru")
    print("📌 Проверьте: 1) Интернет 2) Firewall 3) DNS")
except Exception as e:
    print(f"❌ Ошибка: {e}")

print("\n" + "="*50)
print("📝 Для активации в BotFather:")
print(f"1. Открой @BotFather")
print(f"2. /mybots → выбери бота")
print(f"3. Bot Settings → Payments")
print(f"4. Выбери YooKassa (LIVE)")
print(f"5. Вставь токен: {PAYMENT_TOKEN}")
print("="*50)