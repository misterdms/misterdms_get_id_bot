#!/usr/bin/env python3
"""
🤖 Гибридный Topics Scanner Bot v4.0
Точка входа приложения с поддержкой режимов бота и пользователя

Архитектура:
- Режим бота: ограниченные возможности Bot API
- Режим пользователя: полный доступ через MTProto API
- Многопользовательский режим с очередью запросов
- Безопасное хранение credentials пользователей
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime
from telethon import TelegramClient, events
from telethon.tl.types import KeyboardButtonCallback
from telethon.tl.custom import Button

# Импорт модулей бота
from config import (
    BOT_TOKEN, API_ID, API_HASH, APP_NAME, APP_VERSION,
    setup_logging, MESSAGES, PORT, RENDER_EXTERNAL_URL
)
from database import init_database, db_manager
from auth_manager import auth_manager, cleanup_auth
from web_server import create_web_server
from handlers import CommandHandler
from services import service_manager

# Настройка логирования
logger = setup_logging()

class HybridTopicsBot:
    """Главный класс гибридного бота"""
    
    def __init__(self):
        # Основной bot клиент (всегда активен)
        self.bot_client = TelegramClient('bot_session', API_ID, API_HASH)
        
        # Объединенные компоненты системы
        self.command_handler = CommandHandler()
        self.service_manager = service_manager
        
        # Веб-сервер
        self.web_server = None
        self.server_runner = None
        
        # Статус системы
        self.is_running = False
        self.startup_time = None
        
    async def initialize(self):
        """Инициализация всех компонентов"""
        logger.info(f"🚀 Инициализация {APP_NAME} v{APP_VERSION}")
        
        try:
            # Инициализация базы данных
            logger.info("🗄️ Инициализация базы данных...")
            await init_database()
            
            # Запуск основного bot клиента
            logger.info("🤖 Запуск основного bot клиента...")
            await self.bot_client.start(bot_token=BOT_TOKEN)
            
            # Получение информации о боте
            me = await self.bot_client.get_me()
            logger.info(f"✅ Бот запущен: @{me.username} ({me.first_name})")
            
            # Инициализация компонентов
            await self.command_handler.initialize(
                self.bot_client, 
                auth_manager, 
                self.service_manager.limiter,
                self.service_manager.activity
            )
            await self.service_manager.initialize(
                self.command_handler.bot_mode,
                self.command_handler.user_mode,
                self.bot_client
            )
            
            # Регистрация обработчиков событий
            self._register_event_handlers()
            
            # Запуск веб-сервера
            if RENDER_EXTERNAL_URL:
                await self._start_web_server()
            
            # Запуск фоновых задач
            await self.service_manager.start_background_tasks()
            
            self.startup_time = datetime.now()
            self.is_running = True
            
            logger.info("✅ Все компоненты инициализированы")
            logger.info("🎯 Бот готов к работе в гибридном режиме!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            raise
    
    def _register_event_handlers(self):
        """Регистрация обработчиков событий Telegram"""
        
        # === ОСНОВНЫЕ КОМАНДЫ ===
        
        @self.bot_client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            """Стартовая команда с выбором режима"""
            try:
                user_id = event.sender_id
                sender = event.sender
                
                # Обновляем информацию о пользователе
                await db_manager.create_or_update_user(
                    user_id=user_id,
                    telegram_username=getattr(sender, 'username', None),
                    first_name=getattr(sender, 'first_name', None)
                )
                
                if event.is_private:
                    # В ЛС показываем выбор режима
                    await self._show_mode_selection(event)
                else:
                    # В группах - работа в выбранном режиме
                    user_data = await db_manager.get_user(user_id)
                    if user_data and user_data['mode'] == 'user':
                        await self.command_handler.handle_start(event, 'user')
                    else:
                        await self.command_handler.handle_start(event, 'bot')
                
            except Exception as e:
                logger.error(f"❌ Ошибка в start_handler: {e}")
                await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
        
        @self.bot_client.on(events.CallbackQuery)
        async def callback_handler(event):
            """Обработчик inline кнопок"""
            try:
                data = event.data.decode('utf-8')
                user_id = event.sender_id
                
                if data == 'mode_bot':
                    await self._set_bot_mode(event, user_id)
                elif data == 'mode_user':
                    await self._set_user_mode(event, user_id)
                elif data == 'show_commands':
                    await self._show_commands_help(event)
                elif data == 'show_faq':
                    # Создаем временный event-подобный объект для FAQ
                    class FakeEvent:
                        def __init__(self, original_event):
                            self.original = original_event
                            self.sender_id = original_event.sender_id
                            self.is_private = True
                        
                        async def reply(self, text, **kwargs):
                            return await self.original.edit(text, **kwargs)
                    
                    fake_event = FakeEvent(event)
                    await self._show_faq_inline(fake_event)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в callback_handler: {e}")
                await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
        
        # === КОМАНДЫ СКАНИРОВАНИЯ ===
        
        @self.bot_client.on(events.NewMessage(pattern=r'/scan|/list'))
        async def scan_handler(event):
            """Сканирование топиков"""
            await self._route_command(event, 'scan')
        
        @self.bot_client.on(events.NewMessage(pattern='/get_all'))
        async def get_all_handler(event):
            """Получение всех данных"""
            await self._route_command(event, 'get_all')
        
        @self.bot_client.on(events.NewMessage(pattern='/get_users'))
        async def get_users_handler(event):
            """Получение активных пользователей"""
            await self._route_command(event, 'get_users')
        
        @self.bot_client.on(events.NewMessage(pattern='/get_ids'))
        async def get_ids_handler(event):
            """Повторное сканирование ID"""
            await self._route_command(event, 'get_ids')
        
        # === КОМАНДЫ УПРАВЛЕНИЯ ===
        
        @self.bot_client.on(events.NewMessage(pattern='/switch_mode'))
        async def switch_mode_handler(event):
            """Переключение режима"""
            if event.is_private:
                await self._show_mode_selection(event)
            else:
                await event.reply("⚠️ Переключение режима доступно только в личных сообщениях")
        
        @self.bot_client.on(events.NewMessage(pattern='/renew_my_api_hash'))
        async def renew_credentials_handler(event):
            """Обновление API credentials"""
            if event.is_private:
                await self._set_user_mode(event, event.sender_id)
            else:
                await event.reply("⚠️ Обновление credentials доступно только в личных сообщениях")
        
        @self.bot_client.on(events.NewMessage(pattern='/my_status'))
        async def my_status_handler(event):
            """Статус пользователя"""
            await self._show_user_status(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/logout'))
        async def logout_handler(event):
            """Выход из пользовательского режима"""
            await self._logout_user(event)
        
        # === ИНФОРМАЦИОННЫЕ КОМАНДЫ ===
        
        @self.bot_client.on(events.NewMessage(pattern='/help'))
        async def help_handler(event):
            """Справка"""
            await self._show_help(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/faq'))
        async def faq_handler(event):
            """Частые вопросы"""
            await self._show_faq(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/stats'))
        async def stats_handler(event):
            """Статистика"""
            await self._show_stats(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/debug'))
        async def debug_handler(event):
            """Отладочная информация"""
            await self._show_debug(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/queue_status'))
        async def queue_status_handler(event):
            """Статус очереди"""
            await self._show_queue_status(event)
        
        # === КОМАНДЫ ЛИМИТОВ ===
        
        @self.bot_client.on(events.NewMessage(pattern='/setlimit_(.+)'))
        async def setlimit_handler(event):
            """Установка лимитов API"""
            await self._handle_setlimit(event)
        
        # === ОТСЛЕЖИВАНИЕ АКТИВНОСТИ ===
        
        @self.bot_client.on(events.NewMessage)
        async def activity_tracker_handler(event):
            """Отслеживание активности пользователей"""
            try:
                # Пропускаем команды и личные сообщения
                if (event.text and event.text.startswith('/')) or event.is_private:
                    return
                
                # Пропускаем сообщения от ботов
                if event.sender and hasattr(event.sender, 'bot') and event.sender.bot:
                    return
                
                # Отслеживаем активность
                await self.service_manager.activity.track_user_activity(event)
                
            except Exception as e:
                logger.debug(f"Ошибка отслеживания активности: {e}")
        
        # === ОБРАБОТКА CREDENTIALS ===
        
        @self.bot_client.on(events.NewMessage)
        async def credentials_handler(event):
            """Обработка пользовательских credentials"""
            try:
                if not event.is_private or not event.text:
                    return
                
                # Пропускаем команды
                if event.text.startswith('/'):
                    return
                
                # Проверяем, ожидает ли пользователь ввод credentials
                user_data = await db_manager.get_user(event.sender_id)
                if (user_data and user_data['mode'] == 'bot' and 
                    not user_data.get('api_id_encrypted')):
                    
                    # Пытаемся обработать как credentials
                    await self._process_credentials(event)
                
            except Exception as e:
                logger.debug(f"Ошибка обработки credentials: {e}")
    
    async def _route_command(self, event, command: str):
        """Маршрутизация команды в зависимости от режима пользователя"""
        try:
            user_id = event.sender_id
            user_data = await db_manager.get_user(user_id)
            
            if user_data and user_data['mode'] == 'user':
                # Добавляем в очередь для пользовательского режима
                task_id = await self.service_manager.queue.add_task(
                    user_id=user_id,
                    command=command,
                    chat_id=event.chat_id,
                    parameters={'event_data': {
                        'chat_id': event.chat_id,
                        'message_id': event.message.id,
                        'sender_id': event.sender_id,
                        'text': event.text
                    }},
                    priority=2
                )
                
                # Уведомляем о добавлении в очередь
                queue_status = await db_manager.get_queue_status(user_id)
                if queue_status['pending'] > 1:
                    position = queue_status['user_position'] or queue_status['pending']
                    estimated_time = f"{position * 30} секунд"
                    
                    await event.reply(MESSAGES['queue_notification'].format(
                        position=position,
                        estimated_time=estimated_time
                    ))
                else:
                    await event.reply("🔄 **Задача добавлена в очередь** - выполнение начнется в ближайшее время...")
            else:
                # Обрабатываем в режиме бота напрямую (без очереди)
                await self.command_handler.route_command(command, event, 'bot')
                
        except Exception as e:
            logger.error(f"❌ Ошибка маршрутизации команды {command}: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_mode_selection(self, event):
        """Показать выбор режима работы"""
        buttons = [
            [Button.inline("🤖 Режим бота (быстрый старт)", b"mode_bot")],
            [Button.inline("👤 Режим пользователя (полный доступ)", b"mode_user")],
            [Button.inline("📋 Показать команды", b"show_commands")],
            [Button.inline("❓ Частые вопросы", b"show_faq")]
        ]
        
        await event.reply(MESSAGES['welcome'], buttons=buttons)
    
    async def _set_bot_mode(self, event, user_id: int):
        """Установить режим бота"""
        await db_manager.create_or_update_user(user_id, mode='bot')
        await event.edit(MESSAGES['bot_mode_selected'])
    
    async def _set_user_mode(self, event, user_id: int):
        """Установить режим пользователя"""
        await event.edit(MESSAGES['user_mode_instructions'])
        await db_manager.create_or_update_user(user_id, mode='bot')  # Временно bot до получения credentials
    
    async def _process_credentials(self, event):
        """Обработка пользовательских credentials"""
        try:
            lines = event.text.strip().split('\n')
            
            if len(lines) != 2:
                await event.reply("❌ Неверный формат. Нужно 2 строки:\n1. API_ID\n2. API_HASH")
                return
            
            api_id = lines[0].strip()
            api_hash = lines[1].strip()
            
            # Сохраняем credentials
            success, message = await auth_manager.save_user_credentials(
                event.sender_id, api_id, api_hash
            )
            
            if success:
                await event.reply(MESSAGES['credentials_saved'])
            else:
                await event.reply(f"❌ {message}\n\nПопробуйте еще раз или используйте /renew_my_api_hash")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки credentials: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_user_status(self, event):
        """Показать статус пользователя"""
        try:
            user_id = event.sender_id
            session_info = await auth_manager.get_session_info(user_id)
            
            response = f"👤 **СТАТУС ПОЛЬЗОВАТЕЛЯ**\n\n"
            response += f"🆔 User ID: `{user_id}`\n"
            response += f"🔧 Режим: {session_info['mode']}\n"
            response += f"📊 Статус: {session_info['status']}\n"
            response += f"🔐 Credentials: {'✅ Есть' if session_info['has_credentials'] else '❌ Нет'}\n"
            response += f"🔗 Активная сессия: {'✅ Да' if session_info['is_session_active'] else '❌ Нет'}\n"
            
            if session_info.get('telegram_user'):
                tg_user = session_info['telegram_user']
                response += f"\n📱 **Telegram аккаунт:**\n"
                response += f"• Username: @{tg_user.get('username', 'не указан')}\n"
                response += f"• Имя: {tg_user.get('first_name', 'не указано')}\n"
                response += f"• Телефон: {tg_user.get('phone', 'скрыт')}\n"
            
            await event.reply(response, parse_mode='markdown')
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _logout_user(self, event):
        """Выход из пользовательского режима"""
        try:
            success, message = await auth_manager.logout_user(event.sender_id)
            await event.reply(message)
            
        except Exception as e:
            logger.error(f"❌ Ошибка logout: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_stats(self, event):
        """Показать статистику системы"""
        try:
            # Статистика БД
            db_stats = await db_manager.get_database_stats()
            
            # Статистика сессий
            session_stats = await auth_manager.get_active_sessions_count()
            
            # Статистика очереди
            queue_stats = await db_manager.get_queue_status()
            
            response = f"📊 **СТАТИСТИКА СИСТЕМЫ**\n\n"
            
            response += f"👥 **Пользователи:**\n"
            response += f"• Всего: {db_stats['users_count']}\n"
            response += f"• Активных: {db_stats['active_users']}\n"
            response += f"• В user режиме: {db_stats['user_mode_users']}\n\n"
            
            response += f"🔗 **Сессии:**\n"
            response += f"• Активных: {session_stats['total_sessions']}\n"
            response += f"• Максимум: {session_stats['max_sessions']}\n"
            response += f"• Свободно: {session_stats['available_slots']}\n\n"
            
            response += f"📋 **Очередь:**\n"
            response += f"• Ожидает: {queue_stats['pending']}\n"
            response += f"• Выполняется: {queue_stats['processing']}\n"
            response += f"• Завершено: {queue_stats['completed']}\n"
            response += f"• Ошибок: {queue_stats['failed']}\n\n"
            
            response += f"📈 **Активность:**\n"
            response += f"• Записей за сегодня: {db_stats['activity_data_count']}\n\n"
            
            if self.startup_time:
                uptime = datetime.now() - self.startup_time
                response += f"⏱️ **Время работы:** {uptime}\n"
            
            response += f"🔧 **Версия:** {APP_VERSION}"
            
            await event.reply(response, parse_mode='markdown')
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_queue_status(self, event):
        """Показать статус очереди"""
        try:
            user_id = event.sender_id
            queue_status = await db_manager.get_queue_status(user_id)
            
            response = f"📋 **СТАТУС ОЧЕРЕДИ**\n\n"
            response += f"⏳ Ожидает выполнения: {queue_status['pending']}\n"
            response += f"🔄 Выполняется: {queue_status['processing']}\n"
            response += f"✅ Завершено за час: {queue_status['completed']}\n"
            response += f"❌ Ошибок за час: {queue_status['failed']}\n\n"
            
            if queue_status['user_position']:
                response += f"👤 **Ваша позиция в очереди:** {queue_status['user_position']}\n"
                estimated_time = queue_status['user_position'] * 30
                response += f"⏱️ **Примерное время ожидания:** {estimated_time} секунд\n"
            else:
                response += f"👤 **У вас нет задач в очереди**\n"
            
            await event.reply(response, parse_mode='markdown')
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса очереди: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_commands_help(self, event):
        """Показать справку по командам"""
        help_text = """📋 **КОМАНДЫ БОТА**

🔍 **Сканирование:**
• /scan, /list - сканирование топиков
• /get_all - все данные (топики + активность)
• /get_users - активные пользователи
• /get_ids - повторное сканирование

⚙️ **Управление:**
• /switch_mode - переключение режима
• /renew_my_api_hash - обновление credentials
• /my_status - статус пользователя
• /logout - выход из user режима

ℹ️ **Информация:**
• /help - подробная справка
• /faq - частые вопросы
• /stats - статистика системы
• /debug - отладочная информация

🔧 **Лимиты:**
• /setlimit_auto - автоматический режим
• /setlimit_turtle - медленный режим
• /setlimit_normal - обычный режим
• /setlimit_burst - быстрый режим

💡 **Совет:** Для полной функциональности используйте режим пользователя!"""
        
        await event.edit(help_text, parse_mode='markdown')
    
    async def _show_help(self, event):
        """Показать справку по командам"""
        help_text = f"""🤖 **TOPICS SCANNER BOT - СПРАВКА**

**🎯 Режимы работы:**
🤖 **Режим бота** - быстрый старт с ограничениями Bot API
👤 **Режим пользователя** - полный доступ через MTProto API

**🔍 Команды сканирования:**
• `/scan`, `/list` - сканирование топиков
• `/get_all` - все данные (топики + активность)
• `/get_users` - активные пользователи за сегодня
• `/get_ids` - повторное сканирование ID

**⚙️ Управление:**
• `/switch_mode` - переключить режим работы
• `/stats` - статистика бота
• `/help` - эта справка
• `/faq` - частые вопросы

**🔧 Лимиты API:**
• `/setlimit_auto` - автоматический режим
• `/setlimit_turtle` - медленный режим (🐢)
• `/setlimit_normal` - обычный режим (⚡)
• `/setlimit_burst` - быстрый режим (🚀)

**💡 Рекомендация:**
Для полного доступа ко всем топикам используйте:
`/switch_mode` → Пользовательский режим

**🔧 Техническая информация:**
• Версия: v{APP_VERSION} (Hybrid Edition)
• Поддержка: форумы, супергруппы
• Отслеживание активности: включено"""

        await event.reply(help_text, parse_mode='markdown')
    
    async def _show_faq(self, event):
        """Показать FAQ"""
        faq_text = """❓ **ЧАСТЫЕ ВОПРОСЫ**

**1. 🤖 Почему не все топики показываются?**
Telegram ограничивает доступ ботов к информации о форумах. Используйте пользовательский режим для полного доступа.

**2. 🔄 Как переключиться на полный режим?**
• В ЛС боту: `/switch_mode`
• Выберите "Пользовательский режим"
• Следуйте инструкциям по настройке

**3. 🔍 Что умеет режим бота?**
• Базовое сканирование доступных топиков
• Отслеживание активности пользователей
• Альтернативные методы поиска топиков
• Все команды работают, но с ограничениями

**4. ⚡ Как работают лимиты?**
• Автоматическое переключение при большой нагрузке
• При >200 участников → медленный режим
• При >500 участников → черепаший режим
• Защита от блокировок Telegram

**5. 👥 Как работает отслеживание активности?**
• Автоматически фиксирует все сообщения
• Подсчитывает количество сообщений за день
• Данные сбрасываются каждый день в 00:00
• Команды бота не считаются активностью

**💡 Совет:** Для максимальной функциональности используйте пользовательский режим через `/switch_mode`"""

        await event.reply(faq_text, parse_mode='markdown')
    
    async def _show_faq_inline(self, event):
        """FAQ для inline кнопок"""
        await self._show_faq(event)
    
    async def _show_debug(self, event):
        """Отладочная информация"""
        try:
            if event.is_private:
                await event.reply("⚠️ **Эта команда работает только в супергруппах!**")
                return
            
            chat = await event.get_chat()
            
            response = "🔧 **ОТЛАДОЧНАЯ ИНФОРМАЦИЯ**\n\n"
            
            response += "📊 **Информация о чате:**\n"
            response += f"• ID: {chat.id}\n"
            response += f"• Название: {chat.title}\n"
            response += f"• Тип: {type(chat).__name__}\n"
            response += f"• Мегагруппа: {hasattr(chat, 'megagroup') and chat.megagroup}\n"
            response += f"• Форум: {hasattr(chat, 'forum') and chat.forum}\n\n"
            
            response += "🤖 **Информация о боте:**\n"
            response += f"• Telethon подключен: {self.bot_client.is_connected()}\n"
            response += f"• Версия: {APP_VERSION}\n\n"
            
            # Статистика сервисов
            health = self.service_manager.get_health_status()
            response += "🔧 **Статус сервисов:**\n"
            response += f"• Активность: {'✅' if health['activity']['is_running'] else '❌'}\n"
            response += f"• Лимитер: {'✅' if health['limiter']['is_running'] else '❌'}\n"
            response += f"• Очередь: {'✅' if health['queue']['is_processing'] else '❌'}\n"
            response += f"• Общее здоровье: {'✅' if health['overall_healthy'] else '❌'}\n"
            
            await event.reply(response, parse_mode='markdown')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в debug: {e}")
            await event.reply(f"❌ Ошибка получения отладочной информации: {str(e)}")
    
    async def _handle_setlimit(self, event):
        """Обработка команд лимитов"""
        try:
            command_text = event.text
            if '/setlimit_' not in command_text:
                return False
            
            mode = command_text.split('/setlimit_')[1].strip()
            limiter = self.service_manager.limiter
            
            if mode == 'auto':
                limiter.auto_mode_enabled = True
                limiter.set_mode('normal', 'auto_enabled')
                response = "🔄 **АВТОМАТИЧЕСКИЙ РЕЖИМ ЛИМИТОВ**\n\n"
                response += "• Подстраивается под размер группы\n"
                response += "• Оптимальная производительность\n"
                response += "• Автоматическое переключение: ВКЛЮЧЕНО"
            elif mode in ['turtle', 'low', 'normal', 'burst']:
                limiter.set_mode(mode, 'manual')
                limiter.auto_mode_enabled = False
                
                from config import API_LIMITS
                mode_info = API_LIMITS[mode]
                status = limiter.get_status()
                
                response = f"{mode_info['name']} **РЕЖИМ ЛИМИТОВ**\n\n"
                response += f"• Запросов в час: {status['max_requests_hour']}\n"
                response += f"• Cooldown: {status['cooldown_seconds']} секунд\n"
                response += f"• Назначение: {mode_info['description']}\n"
                response += f"• Автоматический режим: ОТКЛЮЧЕН"
            else:
                await event.reply("❌ Неизвестный режим лимитов")
                return
            
            await event.reply(response, parse_mode='markdown')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в setlimit: {e}")
            await event.reply(f"❌ Ошибка: {str(e)}")
    
    async def _start_web_server(self):
        """Запуск веб-сервера"""
        try:
            self.web_server, self.server_runner = await create_web_server(
                port=PORT,
                auth_manager=auth_manager,
                db_manager=db_manager,
                queue_manager=self.service_manager.queue
            )
            logger.info(f"🌐 Веб-сервер запущен на порту {PORT}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка запуска веб-сервера: {e}")
    
    async def shutdown(self):
        """Корректное завершение работы"""
        logger.info("🛑 Начало корректного завершения...")
        
        self.is_running = False
        
        try:
            # Закрытие всех сервисов
            await self.service_manager.cleanup()
            
            # Закрытие всех пользовательских сессий
            await cleanup_auth()
            
            # Остановка веб-сервера
            if self.server_runner:
                await self.server_runner.cleanup()
            
            # Отключение основного бота
            if self.bot_client.is_connected():
                await self.bot_client.disconnect()
            
            logger.info("✅ Корректное завершение завершено")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при завершении: {e}")
    
    async def run(self):
        """Основной цикл работы бота"""
        try:
            await self.initialize()
            logger.info("🎯 Бот запущен и готов к работе!")
            
            # Основной цикл
            await self.bot_client.run_until_disconnected()
            
        except KeyboardInterrupt:
            logger.info("⏹️ Получен сигнал остановки")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            raise
        finally:
            await self.shutdown()

# Глобальный экземпляр бота
bot = HybridTopicsBot()

def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения"""
    logger.info(f"📡 Получен сигнал {signum}, завершение работы...")
    asyncio.create_task(bot.shutdown())
    sys.exit(0)

async def main():
    """Главная функция"""
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await bot.run()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в main: {e}")
        sys.exit(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Остановка бота...")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)