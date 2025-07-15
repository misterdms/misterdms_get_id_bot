#!/usr/bin/env python3
"""
Конфигурация гибридного Topics Scanner Bot
Содержит все настройки, константы и переменные окружения
"""

import os
import logging
from typing import Dict, Any

# Основные bot credentials
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')

# Проверка обязательных переменных
if not all([BOT_TOKEN, API_ID, API_HASH]):
    raise ValueError("❌ Не заданы обязательные переменные: BOT_TOKEN, API_ID, API_HASH")

# База данных
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///bot_data.db')
DATABASE_POOL_SIZE = int(os.getenv('DATABASE_POOL_SIZE', '10'))

# Шифрование
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', 'default_32_byte_encryption_key_123')
SALT = os.getenv('SALT', 'default_salt_value_123456789')

# Убеждаемся что ключ шифрования правильной длины
if len(ENCRYPTION_KEY) < 32:
    ENCRYPTION_KEY = (ENCRYPTION_KEY * (32 // len(ENCRYPTION_KEY) + 1))[:32]

# Лимиты системы
MAX_CONCURRENT_SESSIONS = int(os.getenv('MAX_CONCURRENT_SESSIONS', '10'))
MAX_QUEUE_SIZE = int(os.getenv('MAX_QUEUE_SIZE', '100'))
SESSION_TIMEOUT_DAYS = int(os.getenv('SESSION_TIMEOUT_DAYS', '7'))
CLEANUP_INTERVAL_HOURS = int(os.getenv('CLEANUP_INTERVAL_HOURS', '24'))

# Веб-сервер
RENDER_EXTERNAL_URL = os.getenv('RENDER_EXTERNAL_URL', '')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', 'default_secret')
PORT = int(os.getenv('PORT', '10000'))
RESPONSE_DELAY = int(os.getenv('RESPONSE_DELAY', '3'))

# Логирование
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_FORMAT = os.getenv('LOG_FORMAT', 'structured')
ENABLE_PERFORMANCE_LOGS = os.getenv('ENABLE_PERFORMANCE_LOGS', 'true').lower() == 'true'
CORRELATION_ID_HEADER = os.getenv('CORRELATION_ID_HEADER', 'X-Request-ID')

# Лимиты API (адаптированы под MTProto API)
API_LIMITS = {
    'turtle': {
        'max_hour': 30,
        'cooldown': 120,
        'name': '🐢 Черепаха',
        'description': 'Ультраконсервативный режим для избежания блокировок'
    },
    'low': {
        'max_hour': 120,
        'cooldown': 30,
        'name': '🐌 Медленный',
        'description': 'Консервативный режим для стабильной работы'
    },
    'normal': {
        'max_hour': 360,
        'cooldown': 10,
        'name': '⚡ Обычный',
        'description': 'Оптимальный режим для быстрого сканирования'
    },
    'burst': {
        'max_hour': 720,
        'cooldown': 5,
        'name': '🚀 Взрывной',
        'description': 'Агрессивный режим для массового сканирования'
    }
}

# Режимы работы бота
BOT_MODES = {
    'bot': {
        'name': 'Режим бота (ограниченный)',
        'description': 'Использует Bot API с ограничениями',
        'emoji': '🤖',
        'features': [
            'Базовое сканирование доступных топиков',
            'Отслеживание активности пользователей',
            'Альтернативные методы поиска топиков',
            'Ограничения Bot API Telegram'
        ]
    },
    'user': {
        'name': 'Режим пользователя (полный)',
        'description': 'Использует пользовательские API credentials',
        'emoji': '👤',
        'features': [
            'Полное сканирование всех топиков форума',
            'Детальная информация о топиках',
            'Информация о создателях топиков',
            'Все возможности MTProto API'
        ]
    }
}

# Приоритеты очереди
QUEUE_PRIORITIES = {
    'admin': 1,      # Команды администраторов
    'scan': 2,       # Команды сканирования
    'stats': 3,      # Статистические команды
    'maintenance': 4  # Техническое обслуживание
}

# Статусы пользователей
USER_STATUSES = {
    'active': 'Активен',
    'expired': 'Сессия истекла',
    'error': 'Ошибка подключения',
    'blocked': 'Заблокирован',
    'pending': 'Ожидает активации'
}

# Статусы задач в очереди
TASK_STATUSES = {
    'pending': 'Ожидает выполнения',
    'processing': 'Выполняется',
    'completed': 'Завершена',
    'failed': 'Ошибка',
    'cancelled': 'Отменена'
}

# Команды бота
COMMANDS = {
    'basic': [
        {'command': 'start', 'description': 'Приветствие и выбор режима'},
        {'command': 'help', 'description': 'Подробная справка'},
        {'command': 'faq', 'description': 'Частые вопросы'},
        {'command': 'stats', 'description': 'Статистика бота'},
    ],
    'scanning': [
        {'command': 'scan', 'description': 'Сканирование топиков'},
        {'command': 'list', 'description': 'Список топиков'},
        {'command': 'get_all', 'description': 'Все данные (топики + активность)'},
        {'command': 'get_users', 'description': 'Активные пользователи'},
        {'command': 'get_ids', 'description': 'Повторное сканирование'},
    ],
    'management': [
        {'command': 'switch_mode', 'description': 'Переключение режима'},
        {'command': 'renew_my_api_hash', 'description': 'Обновление API credentials'},
        {'command': 'my_status', 'description': 'Статус пользователя'},
        {'command': 'logout', 'description': 'Выход из пользовательского режима'},
    ],
    'limits': [
        {'command': 'setlimit_auto', 'description': 'Автоматический режим'},
        {'command': 'setlimit_turtle', 'description': 'Медленный режим'},
        {'command': 'setlimit_normal', 'description': 'Обычный режим'},
        {'command': 'setlimit_burst', 'description': 'Быстрый режим'},
    ],
    'debug': [
        {'command': 'debug', 'description': 'Диагностическая информация'},
        {'command': 'queue_status', 'description': 'Статус очереди запросов'},
    ]
}

# Сообщения для пользователей
MESSAGES = {
    'welcome': """🤖 **ГИБРИДНЫЙ TOPICS SCANNER BOT v4.0**

👋 Добро пожаловать! Выберите режим работы:

🤖 **Режим бота** - быстрый старт с ограничениями
👤 **Режим пользователя** - полная функциональность

Нажмите на кнопку ниже для выбора режима.""",
    
    'bot_mode_selected': """🤖 **РЕЖИМ БОТА АКТИВИРОВАН**

✅ Быстрый старт без дополнительных настроек
⚠️ Ограничения: может показывать не все топики

📋 Основные команды:
• /scan - сканирование топиков
• /get_users - активные пользователи
• /help - подробная справка""",
    
    'user_mode_instructions': """👤 **НАСТРОЙКА ПОЛЬЗОВАТЕЛЬСКОГО РЕЖИМА**

Для получения полного доступа нужны ваши API credentials:

1️⃣ Перейдите на https://my.telegram.org
2️⃣ Войдите под своим аккаунтом
3️⃣ Перейдите в "API development tools"
4️⃣ Создайте новое приложение:
   • App title: "Topics Scanner Bot"
   • Short name: "topics_bot"
   • Platform: Desktop

5️⃣ Отправьте данные в формате:
```
12345678
a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
```
(Первая строка: API_ID, вторая: API_HASH)

🔐 Данные будут зашифрованы и храниться безопасно.""",
    
    'credentials_saved': """✅ **ПОЛЬЗОВАТЕЛЬСКИЙ РЕЖИМ АКТИВИРОВАН**

🔐 Ваши credentials сохранены и зашифрованы
👤 Теперь доступны все возможности MTProto API

🎯 Преимущества:
• Полное сканирование всех топиков
• Детальная информация о создателях
• Нет ограничений Bot API

📋 Команды: /scan, /get_all, /help""",
    
    'queue_notification': """🕐 **ВЫСОКАЯ НАГРУЗКА**

Ваш запрос добавлен в очередь.
Позиция: {position}
Примерное время ожидания: {estimated_time}

Спасибо за терпение! 🙏""",
    
    'session_expired': """⏰ **СЕССИЯ ИСТЕКЛА**

Ваша пользовательская сессия была закрыта из-за неактивности.

Для возобновления работы используйте:
/renew_my_api_hash""",
    
    'error_general': """❌ **Произошла ошибка**

{error_message}

💡 Попробуйте:
• /debug - диагностика
• /help - справка
• /faq - частые вопросы"""
}

# Валидаторы
API_ID_PATTERN = r'^\d{7,8}$'  # 7-8 цифр
API_HASH_PATTERN = r'^[a-f0-9]{32}$'  # 32 символа hex

# Настройка логирования
def setup_logging():
    """Настройка системы логирования"""
    if LOG_FORMAT == 'structured':
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
    
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, LOG_LEVEL))
    logger.addHandler(handler)
    
    return logger

# Версия приложения
APP_VERSION = "4.0.0"
APP_NAME = "Hybrid Topics Scanner Bot"
APP_DESCRIPTION = "Гибридный бот для сканирования топиков с поддержкой пользовательского режима"

# Экспорт всех настроек
__all__ = [
    'BOT_TOKEN', 'API_ID', 'API_HASH',
    'DATABASE_URL', 'ENCRYPTION_KEY', 'SALT',
    'MAX_CONCURRENT_SESSIONS', 'MAX_QUEUE_SIZE',
    'API_LIMITS', 'BOT_MODES', 'COMMANDS', 'MESSAGES',
    'setup_logging', 'APP_VERSION', 'APP_NAME'
]
