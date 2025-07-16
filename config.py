#!/usr/bin/env python3
"""
Topics Scanner Bot v5.16 - Конфигурация
Простое и надежное решение для сканирования топиков
"""

import os
import logging

# === ОСНОВНЫЕ НАСТРОЙКИ ===
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')

# Проверка обязательных переменных
if not all([BOT_TOKEN, API_ID, API_HASH]):
    raise ValueError("❌ Не заданы BOT_TOKEN, API_ID, API_HASH")

# === БАЗА ДАННЫХ ===
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///topics_bot.db')

# Проверка на PostgreSQL URL
if DATABASE_URL and DATABASE_URL.startswith(('postgres://', 'postgresql://')):
    DB_TYPE = 'postgresql'
else:
    DB_TYPE = 'sqlite'

# === ВЕБ-СЕРВЕР ===
PORT = int(os.getenv('PORT', '10000'))

# === НАСТРОЙКИ ЛОГИРОВАНИЯ ===
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger()

logger = setup_logging()

# === ТЕКСТЫ СООБЩЕНИЙ ===
MESSAGES = {
    'welcome': """🤖 **Topics Scanner Bot v5.16**

Привет! Выбери режим работы:

🤖 **Bot API** - ограниченные возможности
👤 **User API** - полный доступ к топикам

Команда /find_ids работает в супергруппах.""",
    
    'bot_api_selected': """🤖 **Bot API режим активен**

⚠️ **Ограничения:** Может показывать не все топики

📋 **Доступные команды:**
• /find_ids - сканировать топики (в группе)
• /start - настройки""",
    
    'user_api_setup': """👤 **Настройка User API**

Для полного доступа нужны твои API данные:

1. Перейди на https://my.telegram.org
2. Войди в аккаунт
3. API development tools
4. Создай приложение
5. Отправь данные в формате:

```
12345678
a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
```
(API_ID в первой строке, API_HASH во второй)""",
    
    'user_api_saved': """✅ **User API настроен!**

Твои данные сохранены безопасно.

📋 **Доступные команды:**
• /find_ids - полное сканирование топиков (в группе)
• /start - настройки""",
    
    'find_ids_group_only': """⚠️ **Команда работает только в супергруппах!**

Перейди в нужную супергруппу и отправь команду там.""",
    
    'scanning_topics': """🔍 **Сканирование топиков...**

Подожди немного, получаю данные...""",
    
    'no_topics_found': """❌ **Топики не найдены**

Возможно:
• Группа не является форумом
• Нет прав доступа
• Используется Bot API (попробуй User API)""",
    
    'error_occurred': """❌ **Произошла ошибка**

{error}

Попробуй позже или смени режим API."""
}

# === КНОПКИ МЕНЮ ===
BUTTONS = {
    'main_menu': [
        [('🤖 Bot API', 'bot_api'), ('👤 User API', 'user_api')],
        [('ℹ️ Помощь', 'help')]
    ],
    
    'settings_menu': [
        [('🔄 Сменить режим', 'change_mode')],
        [('🏠 Главное меню', 'main_menu')]
    ],
    
    'back_to_main': [
        [('🏠 Главное меню', 'main_menu')]
    ]
}

# === ВЕРСИЯ ===
VERSION = "5.16"
APP_NAME = "Topics Scanner Bot"
