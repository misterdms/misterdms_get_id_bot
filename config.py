#!/usr/bin/env python3
"""
Конфигурация Get ID Bot by Mister DMS
ОБНОВЛЕНО v4.1.1: дружелюбные сообщения + все команды из гайда
"""

import os
import logging

# === ОСНОВНЫЕ CREDENTIALS ===
BOT_TOKEN = os.getenv('BOT_TOKEN')
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')

# Проверка обязательных переменных
if not all([BOT_TOKEN, API_ID, API_HASH]):
    raise ValueError("❌ Не заданы обязательные переменные: BOT_TOKEN, API_ID, API_HASH")

# === БАЗА ДАННЫХ ===
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///bot_data.db')
DATABASE_POOL_SIZE = int(os.getenv('DATABASE_POOL_SIZE', '10'))
BOT_PREFIX = os.getenv('BOT_PREFIX', 'get_id_bot')

# === ШИФРОВАНИЕ ===
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', 'default_32_byte_encryption_key_123')
SALT = os.getenv('SALT', 'default_salt_value_123456789')

# Убеждаемся что ключ шифрования правильной длины
if len(ENCRYPTION_KEY) < 32:
    ENCRYPTION_KEY = (ENCRYPTION_KEY * (32 // len(ENCRYPTION_KEY) + 1))[:32]

# === КОМАНДЫ СВЯЗИ v4.1 ===
ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', '471560832'))
BUSINESS_CONTACT_ID = int(os.getenv('BUSINESS_CONTACT_ID', '471560832'))

# === ЛИМИТЫ ===
MAX_CONCURRENT_SESSIONS = int(os.getenv('MAX_CONCURRENT_SESSIONS', '10'))
MAX_QUEUE_SIZE = int(os.getenv('MAX_QUEUE_SIZE', '100'))
SESSION_TIMEOUT_DAYS = int(os.getenv('SESSION_TIMEOUT_DAYS', '7'))

# === ВЕБ-СЕРВЕР ===
PORT = int(os.getenv('PORT', '10000'))
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', 'default_secret')
RENDER_EXTERNAL_URL = os.getenv('RENDER_EXTERNAL_URL', '')

# === КОМАНДЫ БОТА - ПОЛНЫЙ СПИСОК ===
COMMANDS = {
    'basic': [
        {'command': 'start', 'description': '🚀 Приветствие и выбор режима'},
        {'command': 'help', 'description': '❓ Подробная справка по командам'},
        {'command': 'faq', 'description': '🤔 Частые вопросы'},
        {'command': 'stats', 'description': '📊 Статистика бота и твоя активность'},
    ],
    'scanning': [
        {'command': 'scan', 'description': '🔍 Сканирование топиков в супергруппе'},
        {'command': 'list', 'description': '📝 Список всех топиков'},
        {'command': 'get_all', 'description': '📦 Все данные (топики + активность)'},
        {'command': 'get_users', 'description': '👥 Активные пользователи'},
        {'command': 'get_ids', 'description': '🔄 Повторное сканирование'},
    ],
    'management': [
        {'command': 'switch_mode', 'description': '🔄 Переключение режима бот/пользователь'},
        {'command': 'renew_my_api_hash', 'description': '🔑 Обновление API credentials'},
        {'command': 'my_status', 'description': '👤 Твой статус и настройки'},
        {'command': 'logout', 'description': '🚪 Выход из пользовательского режима'},
    ],
    'communication': [
        {'command': 'yo_bro', 'description': '👋 Прямая связь с создателем'},
        {'command': 'buy_bots', 'description': '💼 Заказ разработки ботов'},
        {'command': 'donate', 'description': '💝 Поддержать проект донатом'},
    ],
    'security': [
        {'command': 'security_status', 'description': '🛡️ Статус безопасности'},
        {'command': 'ai_usage', 'description': '🤖 Статистика использования ИИ'},
        {'command': 'limits', 'description': '⏰ Твои текущие лимиты'},
    ],
    'debug': [
        {'command': 'debug', 'description': '🔧 Диагностическая информация'},
        {'command': 'queue_status', 'description': '📋 Статус очереди запросов'},
        {'command': 'health', 'description': '💚 Проверка здоровья системы'},
        {'command': 'performance', 'description': '⚡ Метрики производительности'},
    ]
}

# === ДРУЖЕЛЮБНЫЕ СООБЩЕНИЯ В СТИЛЕ БРО ===
MESSAGES = {
    'welcome': """🤖 **ЙОУ, БРО! TOPICS SCANNER v4.1 НА СВЯЗИ!**

Здарова, кореш! 🤙 Я твой личный сканер топиков в телеге! 

**Что умею:**
🔍 **Сканирую топики** в супергруппах как босс
👥 **Показываю активность** всех участников
📊 **Собираю статистику** - кто сколько постит
🤖 **Два режима работы** - выбирай что по душе!

**Выбери режим:**
🤖 **Режим бота** - быстрый старт, но с ограничениями
👤 **Режим пользователя** - полная мощь, нужны твои API ключи

**Новинки v4.1:**
• /yo_bro - напиши мне лично! 
• /buy_bots - закажи своего бота
• /donate - угости создателя кофе ☕

Жми кнопки ниже и погнали! 🚀""",
    
    'help': """❓ **СПРАВКА ПО КОМАНДАМ**

Ку, бро! Вот все мои команды:

**🚀 ОСНОВНЫЕ:**
/start - начать сначала
/help - эта справка
/stats - посмотреть статистику
/faq - ответы на вопросы

**🔍 СКАНИРОВАНИЕ:**
/scan - отсканить топики
/get_all - все данные разом
/get_users - кто активный
/get_ids - пересканить

**⚙️ УПРАВЛЕНИЕ:**
/switch_mode - сменить режим
/my_status - мой профиль
/logout - выйти

**💬 ОБЩЕНИЕ:**
/yo_bro - связь со мной
/buy_bots - заказать бота
/donate - поддержать проект

В супергруппах добавляй @misterdms_topic_id_get_bot к командам!""",
    
    'choose_mode': """🤔 **ВЫБЕРИ РЕЖИМ РАБОТЫ, БРО!**

**🤖 РЕЖИМ БОТА:**
✅ Быстрый старт за 10 секунд
✅ Не нужны твои API ключи
❌ Видит только публичные супергруппы
❌ Ограниченная функциональность

**👤 РЕЖИМ ПОЛЬЗОВАТЕЛЯ:**
✅ Полная мощь - все группы
✅ Максимум информации
✅ Все скрытые функции
❌ Нужны API_ID и API_HASH

Что выбираешь?""",
    
    'bot_mode_selected': """🤖 **РЕЖИМ БОТА АКТИВИРОВАН!**

Отлично, бро! Теперь могу работать в публичных супергруппах.

**Что делать дальше:**
1. Добавь меня в супергруппу как АДМИНА
2. Дай все права кроме назначения админов
3. Используй команды с @misterdms_topic_id_get_bot

**Пример:**
`/scan@misterdms_topic_id_get_bot`

Поехали! 🚀""",
    
    'user_mode_setup': """👤 **РЕЖИМ ПОЛЬЗОВАТЕЛЯ - НАСТРОЙКА**

Круто выбрал! Для полной мощи нужны твои Telegram API ключи.

**Как получить:**
1. Иди на https://my.telegram.org
2. Войди под своим аккаунтом  
3. API development tools
4. Создай приложение:
   - App title: "Get ID Bot Personal"
   - Platform: Desktop

**Потом пришли мне:**
```
API_ID: твой_api_id
API_HASH: твой_api_hash
ГРУППА: ссылка_на_группу_где_я_админ
```

⚠️ **ВАЖНО:** Сделай меня админом в группе с ПОЛНЫМИ правами!

Жду твои данные! 🔑""",
    
    'credentials_saved': """✅ **ДАННЫЕ СОХРАНЕНЫ, БРО!**

Кайф! Твои API ключи зашифрованы и сохранены.

**Теперь доступны ВСЕ функции:**
🔍 Сканирование любых групп
👥 Детальная активность
📊 Продвинутая статистика
🤖 ИИ-анализ (скоро)

Можешь тестировать все команды! 🚀""",
    
    'yo_bro': f"""👋 **ПРЯМАЯ СВЯЗЬ С СОЗДАТЕЛЕМ**

Привет, бро! Это @MisterDMS - тот самый чувак, который создал этого бота! 

**О чем можно писать:**
🐛 Нашел баг? Расскажи!
💡 Есть идея? Поделись!
❓ Не понял как работает? Спрашивай!
🚀 Хочешь новую функцию? Предлагай!

**Мой Telegram:** @MisterDMS
**User ID:** {ADMIN_USER_ID}

💬 **Обычно отвечаю быстро, максимум за 12 часов!**

🤝 **Коммерческие вопросы:** используй /buy_bots

Не стесняйся, пиши! Всегда рад общению! 😎""",
    
    'buy_bots': f"""💼 **ЗАКАЗ РАЗРАБОТКИ БОТОВ**

Нужен свой бот? Попал по адресу! 

👨‍💻 **Что умею клепать:**
🤖 Telegram боты любой сложности
🌐 Веб-приложения и дашборды  
🧠 ИИ-интеграция (ChatGPT, Claude)
📊 Боты для бизнеса и автоматизации
🎵 Музыкальные боты (моя страсть!)
🛒 Боты для интернет-магазинов

**Примеры работ:**
• Get ID Bot (ты им пользуешься!)
• Музыкальные боты с ИИ
• CRM-системы в Telegram
• Боты для продаж

💰 **Ценник:** 5К - 50К рублей
⏱️ **Сроки:** 3 дня - 2 недели

📞 **Контакт:** @MisterDMS (ID: {BUSINESS_CONTACT_ID})

🎯 **Напиши ТЗ - оценю стоимость бесплатно!**""",
    
    'donate': """💝 **ПОДДЕРЖАТЬ ПРОЕКТ**

Если бот приносит пользу - можешь угостить создателя кофе! ☕

💳 **Реквизиты для доната:**

**TON:** 
`UQCxS4GUjzxl_TbGQ6YgD-8oF1OEjKQCOz3Ru6KJnkjyEASf`

**Bitcoin:**
`bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh`

**Ethereum:**
`0x742d35Cc6339C4532CE5392D2c9B85877a1Bcb1A`

**USDT (TRC20):**
`TGwnz8YmUaLLH1EKzRTUaGZULkFHPCLG7c`

🎯 **Куда идут донаты:**
• Серверы (Render.com)
• Новые функции  
• ИИ API (OpenAI)
• Кофе для программиста ☕

Любая сумма поможет! Спасибо, бро! ❤️""",
    
    'error': """❌ **ОЙ, ЧТО-ТО ПОШЛО НЕ ТАК!**

Упс, бро! Произошла ошибка, но не переживай!

🔧 **Что делать:**
1. Попробуй команду еще раз
2. Проверь что я админ в группе
3. Используй /debug для диагностики
4. Если не помогает - пиши /yo_bro

Я все исправлю! 💪""",
    
    'no_group': """❌ **НЕТ АКТИВНОЙ ГРУППЫ**

Бро, я не знаю в какой группе работать!

🔧 **Как исправить:**
1. Добавь меня в супергруппу
2. Сделай админом с полными правами
3. Если режим пользователя - пришли ссылку на группу
4. Попробуй команду снова

/help - если нужна помощь!""",
    
    'not_admin': """❌ **Я НЕ АДМИН В ЭТОЙ ГРУППЕ**

Чувак, мне нужны админские права!

🔧 **Что делать:**
1. Сделай меня админом в группе
2. Дай ВСЕ права кроме назначения админов
3. Попробуй команду снова

Без админки я как без рук! 🤷‍♂️""",
    
    'stats_basic': """📊 **ТВОЯ СТАТИСТИКА**

Привет, {username}! Вот твои показатели:

🤖 **Режим:** {mode}
📅 **С нами с:** {join_date}
🔥 **Последняя активность:** {last_active}
📈 **Всего команд:** {total_commands}
🎯 **Любимая команда:** {favorite_command}

⚡ **Статус:** {status}""",
    
    'scanning_in_progress': """🔄 **СКАНИРУЮ ГРУППУ...**

Секундочку, бро! Копаюсь в топиках...

⏳ Это может занять до 30 секунд
🔍 Собираю всю инфу
📊 Готовлю детальный отчет

Не уходи далеко! 🚀"""
}

# === КНОПКИ ДЛЯ МЕНЮ ===
INLINE_KEYBOARDS = {
    'main_menu': [
        [('🤖 Режим бота', 'mode_bot'), ('👤 Режим пользователя', 'mode_user')],
        [('❓ Справка', 'help'), ('📊 Статистика', 'stats')],
        [('👋 Связь с создателем', 'yo_bro'), ('💼 Заказать бота', 'buy_bots')],
    ],
    'bot_mode_menu': [
        [('🔍 Сканировать топики', 'scan'), ('👥 Активные юзеры', 'get_users')],
        [('📦 Все данные', 'get_all'), ('🔄 Пересканить', 'get_ids')],
        [('⚙️ Настройки', 'settings'), ('🏠 Главное меню', 'main_menu')],
    ],
    'user_mode_menu': [
        [('🔍 Сканировать', 'scan'), ('📊 Статистика', 'stats')],
        [('🔑 Обновить ключи', 'renew_credentials'), ('👤 Мой статус', 'my_status')],
        [('🚪 Выйти из режима', 'logout'), ('🏠 Главное меню', 'main_menu')],
    ],
    'help_menu': [
        [('📝 Все команды', 'all_commands'), ('🤔 FAQ', 'faq')],
        [('🔧 Диагностика', 'debug'), ('🏠 Главное меню', 'main_menu')],
    ]
}

# === ВЕРСИЯ ===
APP_VERSION = "4.1.1"
APP_NAME = "Get ID Bot by Mister DMS"
DEVELOPER = "@MisterDMS"

# === ЛОГИРОВАНИЕ ===
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
DEVELOPMENT_MODE = os.getenv('DEVELOPMENT_MODE', 'false').lower() == 'true'