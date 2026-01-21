#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from groq import Groq

# Загрузка переменных
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY or not GROQ_API_KEY.startswith("gsk_"):
    print("❌ GROQ_API_KEY отсутствует или имеет неверный формат!")
    print("   Должен начинаться с 'gsk_'")
    sys.exit(1)

print(f"🔑 Используем ключ: {GROQ_API_KEY[:15]}...")

# Инициализация клиента
try:
    client = Groq(api_key=GROQ_API_KEY)
    print("✅ Клиент Groq создан")
except Exception as e:
    print(f"❌ Ошибка создания клиента: {e}")
    sys.exit(1)

# Проверка доступных моделей
print("\n🌐 Проверка доступа к моделям...")
models_to_test = [
    "llama-3.3-70b-versatile",
    "llama-3.1-70b-versatile",
    "llama3-70b-8192"
]

for model in models_to_test:
    try:
        print(f"\n📡 Тестируем модель: {model}")
        response = client.chat.completions.create(
            messages=[{"role": "user", "content": "Проверка работы"}],
            model=model,
            max_tokens=10
        )
        print(f"✅ Модель {model} — ДОСТУПНА!")
        break  # Если одна модель работает, выходим
    except Exception as e:
        error_msg = str(e).lower()
        if "403" in error_msg or "forbidden" in error_msg:
            print(f"❌ Модель {model} — НЕТ ДОСТУПА (403)")
        elif "429" in error_msg:
            print(f"❌ Модель {model} — ЛИМИТ ПРЕВЫШЕН (429)")
        else:
            print(f"❌ Модель {model} — Ошибка: {e}")

# Финальная проверка счёта
print("\n" + "="*50)
print("💳 Проверка баланса/лимитов...")
print("   Перейдите: https://console.groq.com/account")
print("   Создайте новый ключ, если лимит превышен")
print("="*50)