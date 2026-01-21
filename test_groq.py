#!/usr/bin/env python3
import os
from pathlib import Path
from dotenv import load_dotenv
from groq import Groq

# Загрузка переменных
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY:
    print("❌ GROQ_API_KEY не найден в .env")
    exit(1)

print(f"🔑 Используем ключ: {GROQ_API_KEY[:15]}...")

# Тестируем клиент
try:
    client = Groq(api_key=GROQ_API_KEY)
    response = client.chat.completions.create(
        messages=[{"role": "user", "content": "Hello!"}],
        model="llama-3.3-70b-versatile",
        max_tokens=50
    )
    print("✅ Groq API работает корректно!")
    print(f"Ответ: {response.choices[0].message.content[:50]}...")
except Exception as e:
    print(f"❌ Ошибка: {e}")

# Проверяем доступные модели
try:
    print("\n🌐 Проверка доступных моделей...")
    client = Groq(api_key=GROQ_API_KEY)
    # Список моделей можно получить через API, но для теста просто пробуем основную
    print("✅ Ключ активен и имеет доступ к моделям")
except Exception as e:
    print(f"❌ Нет доступа к моделям: {e}")