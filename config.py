#!/usr/bin/env python3
"""
Конфигурация гибридного Topics Scanner Bot
Содержит все настройки, константы и переменные окружения
ИСПРАВЛЕНО: Добавлены все недостающие переменные для совместимости + команды связи + проверка DATABASE_URL
"""

import os
import logging
from typing import Dict, Any

# Настройка логирования
def setup_logging():
    """Настройка системы логирования"""
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
    LOG_FORMAT = os.getenv('LOG_FORMAT', 'structured')
    
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

# Инициализируем логирование первым делом
logger = setup_logging()

# Основные bot credentials
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')

# Проверка обязательных переменных
if not all([BOT_TOKEN, API_ID, API_HASH]):
    raise ValueError("❌ Не заданы обязательные переменные: BOT_TOKEN, API_ID, API_HASH")

# База данных + ПРЕФИКС ТАБЛИЦ (КРИТИЧНО!)
# По умолчанию PostgreSQL для production, SQLite для локальной разработки
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///bot_data.db')  # Безопасный fallback на SQLite
DATABASE_POOL_SIZE = int(os.getenv('DATABASE_POOL_SIZE', '10'))
BOT_PREFIX = os.getenv('BOT_PREFIX', 'get_id_bot')  # Префикс для таблиц

# Проверка DATABASE_URL
if DATABASE_URL and ('user:password@host' in DATABASE_URL or 'presave_user:password@localhost' in DATABASE_URL or DATABASE_URL == 'postgresql://user:password@host:5432/dbname'):
    logger.warning("⚠️ ВНИМАНИЕ: DATABASE_URL содержит пример значения!")
    logger.warning("💡 На Render.com получите реальный DATABASE_URL из PostgreSQL addon misterdms-bots-db")
    logger.warning("🔄 Будет использоваться SQLite fallback")

# Для локальной разработки можно переопределить в .env:
# DATABASE_URL=sqlite:///bot_data.db

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

# НОВЫЕ ФИЧИ v4.1 - КОМАНДЫ СВЯЗИ
ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', '471560832'))  # ID создателя для /yo_bro
BUSINESS_CONTACT_ID = int(os.getenv('BUSINESS_CONTACT_ID', '471560832'))  # ID для /buy_bots

# БЕЗОПАСНОСТЬ И ЛИМИТЫ (ИСПРАВЛЕНО)
BLACKLIST_USERS = []
TRUSTED_USERS = []
ALERT_ADMIN_IDS = []

# Безопасная обработка списков пользователей
try:
    blacklist_str = os.getenv('BLACKLIST_USERS', '')
    if blacklist_str:
        BLACKLIST_USERS = [s.strip() for s in blacklist_str.split(',') if s.strip()]
except Exception:
    BLACKLIST_USERS = []

try:
    trusted_str = os.getenv('TRUSTED_USERS', '471560832')
    if trusted_str:
        TRUSTED_USERS = [s.strip() for s in trusted_str.split(',') if s.strip()]
except Exception:
    TRUSTED_USERS = ['471560832']

try:
    alert_str = os.getenv('ALERT_ADMIN_IDS', '471560832')
    if alert_str:
        ALERT_ADMIN_IDS = [s.strip() for s in alert_str.split(',') if s.strip()]
except Exception:
    ALERT_ADMIN_IDS = ['471560832']

WHITELIST_ONLY_MODE = os.getenv('WHITELIST_ONLY_MODE', 'false').lower() == 'true'

# ЛИМИТЫ ИСПОЛЬЗОВАНИЯ
MAX_USERS_PER_HOUR = int(os.getenv('MAX_USERS_PER_HOUR', '100'))
MAX_DAILY_REQUESTS = int(os.getenv('MAX_DAILY_REQUESTS', '10000'))
MAX_REQUESTS_PER_USER_DAY = int(os.getenv('MAX_REQUESTS_PER_USER_DAY', '100'))
MAX_GROUPS_PER_USER_HOUR = int(os.getenv('MAX_GROUPS_PER_USER_HOUR', '5'))
COOLDOWN_BETWEEN_USERS = int(os.getenv('COOLDOWN_BETWEEN_USERS', '3'))

# РЕЖИМ РАЗРАБОТКИ
DEVELOPMENT_MODE = os.getenv('DEVELOPMENT_MODE', 'false').lower() == 'true'
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'

# АНАЛИТИКА
ENABLE_USAGE_ANALYTICS = os.getenv('ENABLE_USAGE_ANALYTICS', 'true').lower() == 'true'

# АЛЕРТЫ
ALERT_TELEGRAM_CHAT = os.getenv('ALERT_TELEGRAM_CHAT', '-1002133156416')

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
USER_STATUSES = ['active', 'expired', 'error', 'blocked', 'pending']

# Статусы задач в очереди
TASK_STATUSES = ['pending', 'processing', 'completed', 'failed', 'cancelled']

# Команды бота - ОБНОВЛЕНО v4.1
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
    'communication': [  # НОВАЯ КАТЕГОРИЯ v4.1
        {'command': 'yo_bro', 'description': 'Связь с создателем бота'},
        {'command': 'buy_bots', 'description': 'Заказ разработки ботов'},
        {'command': 'donate', 'description': 'Поддержать проект донатом'},
    ],
    'debug': [
        {'command': 'debug', 'description': 'Диагностическая информация'},
        {'command': 'queue_status', 'description': 'Статус очереди запросов'},
    ]
}

# Сообщения для пользователей - РАСШИРЕНО
MESSAGES = {
    'welcome': """🤖 **ГИБРИДНЫЙ TOPICS SCANNER BOT v4.1**

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
• /help - подробная справка

🆕 Новое в v4.1:
• /yo_bro - связь с создателем
• /buy_bots - заказ разработки ботов
• /donate - поддержать проект""",
    
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

📋 Команды: /scan, /get_all, /help

🆕 Новое в v4.1:
• /yo_bro - связь с создателем
• /buy_bots - заказ разработки ботов
• /donate - поддержать проект""",
    
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
• /faq - частые вопросы

🆕 Или свяжитесь с разработчиком: /yo_bro""",

    'dev_message': """🔧 **РЕЖИМ РАЗРАБОТКИ**

Сорри, бро, мне тут код правят, я ща не работаю, приболел, так сказать, решаю проблемы с цифровым здоровьем, ахахах 😅

🛠️ **Что происходит:**
• Обновление функций сканирования
• Оптимизация базы данных  
• Исправление багов

⏰ **Примерное время:** 15-30 минут
💬 **Вопросы:** @MisterDMS""",
    
    # НОВЫЕ СООБЩЕНИЯ v4.1 - КОМАНДЫ СВЯЗИ
    'yo_bro': f"""👋 **Связь с создателем бота**

Привет! Это прямая связь с @MisterDMS - создателем Get ID Bot.

📞 **Контакты:**
• Telegram: @MisterDMS
• User ID: {ADMIN_USER_ID}

💬 **О чем можно писать:**
• Баги и ошибки в работе бота
• Предложения новых функций
• Вопросы по использованию
• Техническая поддержка

🤝 **Коммерческие вопросы:** используйте /buy_bots

⚡ **Обычно отвечаю в течение 2-12 часов**""",
    
    'buy_bots': f"""💼 **Заказ разработки ботов**

Нужен свой бот или хотите кастомизировать существующий?

👨‍💻 **Что умею делать:**
• Telegram боты любой сложности
• Интеграция с API и базами данных
• Веб-приложения и дашборды
• ИИ-интеграция (ChatGPT, Claude)
• Автоматизация бизнес-процессов

📋 **Примеры работ:**
• Get ID Bot (Topics Scanner) - этот бот
• Музыкальные боты с ИИ
• Боты для интернет-магазинов
• CRM-системы на базе Telegram

💰 **Стоимость:** от 5,000 до 50,000 рублей
⏱️ **Сроки:** от 3 дней до 2 недель

📞 **Контакт для заказов:**
• Telegram: @MisterDMS  
• User ID: {BUSINESS_CONTACT_ID}

🎯 **Напишите ТЗ и получите оценку стоимости!**""",
    
    'donate': """💝 **Поддержать проект донатом**

Если Get ID Bot приносит пользу, можете поддержать разработку!

💳 **Способы доната:**
• TON: `UQCxS4GUjzxl_TbGQ6YgD-8oF1OEjKQCOz3Ru6KJnkjyEASf`
• BTC: `bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh`
• ETH: `0x742d35Cc6339C4532CE5392D2c9B85877a1Bcb1A`
• USDT (TRC20): `TGwnz8YmUaLLH1EKzRTUaGZULkFHPCLG7c`

🎯 **На что идут средства:**
• Оплата серверов (Render.com)
• Развитие новых функций
• ИИ-интеграция (OpenAI API)
• Техническая поддержка

💪 **Любая сумма поможет проекту развиваться!**

Спасибо за поддержку! ❤️""",
}

# Валидаторы
API_ID_PATTERN = r'^\d{7,8}$'  # 7-8 цифр
API_HASH_PATTERN = r'^[a-f0-9]{32}$'  # 32 символа hex

# Версия приложения - ОБНОВЛЕНО
APP_VERSION = "4.1.1"
APP_NAME = "Hybrid Topics Scanner Bot"
APP_DESCRIPTION = "Гибридный бот для сканирования топиков с поддержкой пользовательского режима и новыми фичами связи"

# Экспорт всех настроек
__all__ = [
    'BOT_TOKEN', 'API_ID', 'API_HASH', 'BOT_PREFIX',
    'DATABASE_URL', 'ENCRYPTION_KEY', 'SALT',
    'MAX_CONCURRENT_SESSIONS', 'MAX_QUEUE_SIZE',
    'SESSION_TIMEOUT_DAYS', 'USER_STATUSES', 'TASK_STATUSES',
    'ADMIN_USER_ID', 'BUSINESS_CONTACT_ID',
    'API_LIMITS', 'BOT_MODES', 'COMMANDS', 'MESSAGES',
    'setup_logging', 'APP_VERSION', 'APP_NAME', 'QUEUE_PRIORITIES',
    'DEVELOPMENT_MODE', 'BLACKLIST_USERS', 'TRUSTED_USERS',
    'WHITELIST_ONLY_MODE', 'MAX_REQUESTS_PER_USER_DAY',
    'COOLDOWN_BETWEEN_USERS', 'ENABLE_USAGE_ANALYTICS'
]