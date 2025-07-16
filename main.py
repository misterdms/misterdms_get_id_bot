#!/usr/bin/env python3
"""
🤖 Гибридный Topics Scanner Bot v4.1
Точка входа приложения с поддержкой режимов бота и пользователя
ИСПРАВЛЕНО: Обработчики событий, логика credentials, циклические импорты
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime

# Основные импорты Telegram
from telethon import TelegramClient, events
from telethon.tl.custom import Button

# Безопасные импорты модулей бота
try:
    from config import (
        BOT_TOKEN, API_ID, API_HASH, APP_NAME, APP_VERSION,
        setup_logging, MESSAGES, PORT, RENDER_EXTERNAL_URL,
        DEVELOPMENT_MODE, ADMIN_USER_ID, BUSINESS_CONTACT_ID
    )
except ImportError as e:
    print(f"❌ Ошибка импорта config: {e}")
    sys.exit(1)

try:
    from database import init_database, db_manager
except ImportError as e:
    print(f"❌ Ошибка импорта database: {e}")
    sys.exit(1)

try:
    from auth_manager import auth_manager, cleanup_auth
except ImportError as e:
    print(f"❌ Ошибка импорта auth_manager: {e}")
    sys.exit(1)

try:
    from handlers import CommandHandler
except ImportError as e:
    print(f"❌ Ошибка импорта handlers: {e}")
    sys.exit(1)

try:
    from services import service_manager
except ImportError as e:
    print(f"❌ Ошибка импорта services: {e}")
    sys.exit(1)

try:
    from utils import send_long_message, MessageUtils
except ImportError as e:
    print(f"❌ Ошибка импорта utils: {e}")
    # Fallback функция для отправки сообщений
    async def send_long_message(event, text, **kwargs):
        try:
            await event.reply(text, **kwargs)
        except Exception as ex:
            print(f"Ошибка отправки сообщения: {ex}")

# Опциональные импорты
try:
    from web_server import create_web_server
    web_server_available = True
except ImportError as e:
    print(f"⚠️ Веб-сервер недоступен: {e}")
    web_server_available = False

try:
    from security import security_manager
    security_available = True
except ImportError as e:
    print(f"⚠️ Система безопасности недоступна: {e}")
    security_available = False

try:
    from analytics import analytics
    analytics_available = True
except ImportError as e:
    print(f"⚠️ Аналитика недоступна: {e}")
    analytics_available = False

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
        
        # Флаг для отслеживания обработки команд
        self.is_processing_command = {}
        
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
            if web_server_available and RENDER_EXTERNAL_URL:
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
        """Регистрация обработчиков событий Telegram - ИСПРАВЛЕНО"""
        
        # === ГЛАВНЫЙ ОБРАБОТЧИК КОМАНД И СООБЩЕНИЙ ===
        @self.bot_client.on(events.NewMessage)
        async def main_message_handler(event):
            """Главный обработчик всех сообщений"""
            try:
                # Проверяем, обрабатывается ли уже команда от этого пользователя
                user_id = event.sender_id
                if user_id in self.is_processing_command:
                    logger.debug(f"⏳ Сообщение от {user_id} уже обрабатывается")
                    return
                
                # Устанавливаем флаг обработки
                self.is_processing_command[user_id] = True
                
                try:
                    # 1. ПРОВЕРКА РЕЖИМА РАЗРАБОТКИ
                    if DEVELOPMENT_MODE:
                        if security_available:
                            if not security_manager.is_trusted_user(user_id):
                                logger.info(f"🔧 Режим разработки: блокирован пользователь {user_id}")
                                await send_long_message(event, MESSAGES.get('dev_message', 
                                    "🔧 **Режим разработки**\n\nБот временно недоступен для обновлений."))
                                return
                        else:
                            if user_id != ADMIN_USER_ID:
                                logger.info(f"🔧 Режим разработки: блокирован пользователь {user_id}")
                                await send_long_message(event, MESSAGES.get('dev_message', 
                                    "🔧 **Режим разработки**\n\nБот временно недоступен для обновлений."))
                                return
                    
                    # 2. ПРОВЕРКА БЕЗОПАСНОСТИ
                    if security_available:
                        is_allowed, message = security_manager.is_user_allowed(user_id)
                        if not is_allowed:
                            logger.warning(f"🚫 Доступ запрещен для {user_id}: {message}")
                            await send_long_message(event, message)
                            if analytics_available:
                                analytics.track_error(user_id, 'access_denied', message)
                            return
                        
                        # Записываем разрешенный запрос
                        security_manager.record_request(user_id, event.text or 'message', 
                                                     'private' if event.is_private else 'group')
                    
                    # 3. ОБРАБОТКА КОМАНД
                    if event.text and event.text.startswith('/'):
                        await self._process_command(event)
                    
                    # 4. ОБРАБОТКА CREDENTIALS (ИСПРАВЛЕНО!)
                    elif event.is_private and event.text:
                        credentials_processed = await self.command_handler.process_credentials(event)
                        if credentials_processed:
                            logger.info(f"✅ Credentials обработаны для пользователя {user_id}")
                            return
                    
                    # 5. ОТСЛЕЖИВАНИЕ АКТИВНОСТИ (только в группах)
                    if not event.is_private and event.text and not event.text.startswith('/'):
                        await self._track_activity(event)
                    
                finally:
                    # Убираем флаг обработки
                    if user_id in self.is_processing_command:
                        del self.is_processing_command[user_id]
                        
            except Exception as e:
                logger.error(f"❌ Ошибка в main_message_handler: {e}")
                try:
                    await send_long_message(event, f"❌ Произошла ошибка: {str(e)}")
                except:
                    pass
                finally:
                    # Убираем флаг при ошибке
                    user_id = event.sender_id
                    if user_id in self.is_processing_command:
                        del self.is_processing_command[user_id]
        
        # === ОБРАБОТЧИК CALLBACK КНОПОК ===
        @self.bot_client.on(events.CallbackQuery)
        async def callback_handler(event):
            """Обработчик inline кнопок"""
            try:
                data = event.data.decode('utf-8')
                user_id = event.sender_id
                
                logger.info(f"🔘 Callback {data} от пользователя {user_id}")
                
                # Аналитика callback
                correlation_id = ""
                if analytics_available:
                    correlation_id = analytics.track_command(user_id, f'callback_{data}')
                
                if data == 'mode_bot':
                    await self._set_bot_mode(event, user_id)
                elif data == 'mode_user':
                    await self._set_user_mode(event, user_id)
                elif data == 'show_commands':
                    await self._show_commands_help(event)
                elif data == 'show_faq':
                    await self._show_faq_inline(event)
                elif data == 'main_menu':
                    await self._show_mode_selection(event)
                elif data == 'back':
                    await self._show_mode_selection(event)
                
                # Успешная аналитика
                if analytics_available:
                    analytics.track_event('callback_completed', user_id, 
                                         {'callback_data': data}, correlation_id)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в callback_handler: {e}")
                if analytics_available:
                    analytics.track_error(event.sender_id, 'callback_error', str(e))
                try:
                    await event.edit("❌ Ошибка обработки кнопки")
                except:
                    pass
    
    async def _process_command(self, event):
        """Обработка команды"""
        try:
            user_id = event.sender_id
            command = event.text.split()[0].lower()
            command_name = command[1:] if command.startswith('/') else command
            
            # Обновляем информацию о пользователе
            if hasattr(event, 'sender'):
                sender = event.sender
                await db_manager.create_or_update_user(
                    user_id=user_id,
                    telegram_username=getattr(sender, 'username', None),
                    first_name=getattr(sender, 'first_name', None)
                )
            
            # Основные команды
            if command_name == 'start':
                await self._handle_start(event)
            elif command_name in ['scan', 'list']:
                await self._route_command(event, 'scan')
            elif command_name == 'get_all':
                await self._route_command(event, 'get_all')
            elif command_name == 'get_users':
                await self._route_command(event, 'get_users')
            elif command_name == 'get_ids':
                await self._route_command(event, 'get_ids')
            elif command_name == 'switch_mode':
                await self._handle_switch_mode(event)
            elif command_name == 'renew_my_api_hash':
                await self._handle_renew_credentials(event)
            elif command_name == 'my_status':
                await self._show_user_status(event)
            elif command_name == 'logout':
                await self._logout_user(event)
            elif command_name in ['yo_bro', 'buy_bots', 'donate']:
                await self.command_handler.handle_contact_commands(event, command_name)
            elif command_name == 'help':
                await self._show_help(event)
            elif command_name == 'faq':
                await self._show_faq(event)
            elif command_name == 'stats':
                await self._show_stats(event)
            elif command_name == 'debug':
                await self._show_debug(event)
            elif command_name == 'queue_status':
                await self._show_queue_status(event)
            elif command_name.startswith('setlimit_'):
                await self._handle_setlimit(event, command_name)
            else:
                # Неизвестная команда
                logger.warning(f"⚠️ Неизвестная команда: {command_name}")
                await send_long_message(event, f"❌ Неизвестная команда: /{command_name}\n\nИспользуйте /help для справки")
                
        except Exception as e:
            logger.error(f"❌ Ошибка обработки команды: {e}")
            await send_long_message(event, f"❌ Ошибка выполнения команды: {str(e)}")
    
    async def _handle_start(self, event):
        """Обработка команды /start"""
        try:
            user_id = event.sender_id
            chat_type = 'private' if event.is_private else 'group'
            logger.info(f"🚀 /start от пользователя {user_id} в {chat_type}")
            
            # Аналитика
            correlation_id = ""
            if analytics_available:
                correlation_id = analytics.track_command(user_id, '/start', chat_type)
            
            if event.is_private:
                # В ЛС показываем выбор режима с inline кнопками
                await self._show_mode_selection(event)
            else:
                # В группах - работа в выбранном режиме
                user_data = await db_manager.get_user(user_id)
                if user_data and user_data['mode'] == 'user':
                    await self.command_handler.handle_start(event, 'user')
                else:
                    await self.command_handler.handle_start(event, 'bot')
            
            # Успешная аналитика
            if analytics_available:
                analytics.track_event('start_completed', user_id, 
                                     {'success': True, 'chat_type': chat_type}, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в _handle_start: {e}")
            if analytics_available:
                analytics.track_error(event.sender_id, 'start_error', str(e))
            await send_long_message(event, f"❌ Ошибка: {str(e)}")
    
    async def _route_command(self, event, command: str):
        """Маршрутизация команды в зависимости от режима пользователя"""
        try:
            user_id = event.sender_id
            user_data = await db_manager.get_user(user_id)
            
            # Аналитика команды
            correlation_id = ""
            if analytics_available:
                correlation_id = analytics.track_command(user_id, command, 
                                                       'private' if event.is_private else 'group')
            
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
                        'text': event.text,
                        'correlation_id': correlation_id
                    }},
                    priority=2,
                    original_event=event
                )
                
                # Уведомляем о добавлении в очередь
                queue_status = await self.service_manager.queue.get_queue_status()
                if queue_status['pending'] > 1:
                    position = queue_status.get('pending', 1)
                    estimated_time = f"{position * 30} секунд"
                    
                    queue_msg = MESSAGES.get('queue_notification', 
                        "🕐 Ваш запрос добавлен в очередь.\nПозиция: {position}\nПримерное время: {estimated_time}")
                    
                    await send_long_message(event, queue_msg.format(
                        position=position,
                        estimated_time=estimated_time
                    ))
                else:
                    await send_long_message(event, "🔄 **Задача добавлена в очередь** - выполнение начнется в ближайшее время...")
                    
                if analytics_available:
                    analytics.track_event('command_queued', user_id, {
                        'command': command,
                        'task_id': task_id
                    }, correlation_id)
            else:
                # Обрабатываем в режиме бота напрямую
                success = await self.command_handler.route_command(command, event, 'bot')
                
                if analytics_available:
                    analytics.track_event('command_executed_bot_mode', user_id, {
                        'command': command,
                        'success': success
                    }, correlation_id)
                
        except Exception as e:
            logger.error(f"❌ Ошибка маршрутизации команды {command}: {e}")
            if analytics_available:
                analytics.track_error(event.sender_id, 'command_routing_error', str(e))
            await send_long_message(event, f"❌ Ошибка: {str(e)}")
    
    async def _handle_switch_mode(self, event):
        """Переключение режима"""
        correlation_id = ""
        if analytics_available:
            correlation_id = analytics.track_command(event.sender_id, '/switch_mode')
        
        if event.is_private:
            await self._show_mode_selection(event)
            if analytics_available:
                analytics.track_event('mode_switch_opened', event.sender_id, {}, correlation_id)
        else:
            await send_long_message(event, "⚠️ Переключение режима доступно только в личных сообщениях")
    
    async def _handle_renew_credentials(self, event):
        """Обновление API credentials"""
        correlation_id = ""
        if analytics_available:
            correlation_id = analytics.track_command(event.sender_id, '/renew_my_api_hash')
        
        if event.is_private:
            await self._set_user_mode(event, event.sender_id)
            if analytics_available:
                analytics.track_event('credentials_renewal_started', event.sender_id, {}, correlation_id)
        else:
            await send_long_message(event, "⚠️ Обновление credentials доступно только в личных сообщениях")
    
    async def _track_activity(self, event):
        """Отслеживание активности пользователей"""
        try:
            # Пропускаем сообщения от ботов
            if event.sender and hasattr(event.sender, 'bot') and event.sender.bot:
                return
            
            # Отслеживаем активность
            await self.service_manager.activity.track_user_activity(event)
            
        except Exception as e:
            logger.debug(f"Ошибка отслеживания активности: {e}")
    
    async def _show_mode_selection(self, event):
        """Показать выбор режима работы с inline кнопками"""
        correlation_id = ""
        if analytics_available:
            correlation_id = analytics.track_command(event.sender_id, 'mode_selection_shown')
        
        buttons = [
            [Button.inline("🤖 Режим бота (быстрый старт)", b"mode_bot")],
            [Button.inline("👤 Режим пользователя (полный доступ)", b"mode_user")],
            [Button.inline("📋 Показать команды", b"show_commands")],
            [Button.inline("❓ Частые вопросы", b"show_faq")]
        ]
        
        welcome_msg = MESSAGES.get('welcome', 
            "🤖 **ГИБРИДНЫЙ TOPICS SCANNER BOT v4.1**\n\nВыберите режим работы:")
        
        try:
            if hasattr(event, 'edit'):
                await event.edit(welcome_msg, buttons=buttons, parse_mode='markdown')
            else:
                await send_long_message(event, welcome_msg, buttons=buttons, parse_mode='markdown')
        except:
            await send_long_message(event, welcome_msg, buttons=buttons, parse_mode='markdown')
        
        if analytics_available:
            analytics.track_event('mode_selection_shown', event.sender_id, {}, correlation_id)
    
    async def _set_bot_mode(self, event, user_id: int):
        """Установить режим бота"""
        correlation_id = ""
        if analytics_available:
            correlation_id = analytics.track_command(user_id, 'bot_mode_selected')
        
        await db_manager.create_or_update_user(user_id, mode='bot')
        
        bot_mode_msg = MESSAGES.get('bot_mode_selected', 
            "🤖 **РЕЖИМ БОТА АКТИВИРОВАН**\n\n✅ Быстрый старт без дополнительных настроек")
        
        await event.edit(bot_mode_msg, parse_mode='markdown')
        
        if analytics_available:
            analytics.track_event('mode_changed', user_id, {
                'new_mode': 'bot'
            }, correlation_id)
    
    async def _set_user_mode(self, event, user_id: int):
        """Установить режим пользователя"""
        correlation_id = ""
        if analytics_available:
            correlation_id = analytics.track_command(user_id, 'user_mode_selected')
        
        user_mode_msg = MESSAGES.get('user_mode_instructions', 
            "👤 **НАСТРОЙКА ПОЛЬЗОВАТЕЛЬСКОГО РЕЖИМА**\n\nОтправьте API_ID и API_HASH (по одному на строку)")
        
        await event.edit(user_mode_msg, parse_mode='markdown')
        await db_manager.create_or_update_user(user_id, mode='bot')  # Временно bot до получения credentials
        
        if analytics_available:
            analytics.track_event('user_mode_instructions_shown', user_id, {}, correlation_id)
    
    async def _show_user_status(self, event):
        """Показать статус пользователя"""
        try:
            user_id = event.sender_id
            correlation_id = ""
            if analytics_available:
                correlation_id = analytics.track_command(user_id, '/my_status')
            
            session_info = await auth_manager.get_session_info(user_id)
            
            security_status = {}
            if security_available:
                security_status = security_manager.get_security_status(user_id)
            
            response = f"👤 **СТАТУС ПОЛЬЗОВАТЕЛЯ**\n\n"
            response += f"🆔 User ID: `{user_id}`\n"
            response += f"🔧 Режим: {session_info.get('mode', 'неизвестно')}\n"
            response += f"📊 Статус: {session_info.get('status', 'неизвестно')}\n"
            response += f"🔐 Credentials: {'✅ Есть' if session_info.get('has_credentials') else '❌ Нет'}\n"
            response += f"🔗 Активная сессия: {'✅ Да' if session_info.get('is_session_active') else '❌ Нет'}\n\n"
            
            # Лимиты безопасности
            if security_available:
                user_limits = security_status.get('user_limits', {})
                response += f"🛡️ **БЕЗОПАСНОСТЬ:**\n"
                response += f"• Запросов сегодня: {user_limits.get('requests_today', 0)}\n"
                response += f"• Доверенный: {'✅' if user_limits.get('is_trusted', False) else '❌'}\n"
                response += f"• Cooldown: {user_limits.get('cooldown_remaining', 0):.0f}с\n\n"
            
            await send_long_message(event, response, parse_mode='markdown')
            
            if analytics_available:
                analytics.track_event('user_status_viewed', user_id, {}, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса: {e}")
            await send_long_message(event, f"❌ Ошибка: {str(e)}")
    
    async def _logout_user(self, event):
        """Выход из пользовательского режима"""
        try:
            correlation_id = ""
            if analytics_available:
                correlation_id = analytics.track_command(event.sender_id, '/logout')
            
            success, message = await auth_manager.logout_user(event.sender_id)
            await send_long_message(event, message)
            
            if analytics_available:
                analytics.track_event('user_logout', event.sender_id, {
                    'success': success
                }, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка logout: {e}")
            await send_long_message(event, f"❌ Ошибка: {str(e)}")
    
    async def _show_help(self, event):
        """Показать справку"""
        correlation_id = ""
        if analytics_available:
            correlation_id = analytics.track_command(event.sender_id, '/help')
        
        help_text = f"""📋 **СПРАВКА ПО КОМАНДАМ**

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

💬 **Связь:**
• /yo_bro - связь с создателем
• /buy_bots - заказ разработки
• /donate - поддержать проект

💡 **Версия:** {APP_VERSION}"""
        
        await send_long_message(event, help_text, parse_mode='markdown')
        
        if analytics_available:
            analytics.track_event('help_viewed', event.sender_id, {}, correlation_id)
    
    async def _show_faq(self, event):
        """Показать FAQ"""
        correlation_id = ""
        if analytics_available:
            correlation_id = analytics.track_command(event.sender_id, '/faq')
        
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

**5. 💬 Как связаться с разработчиком?**
• `/yo_bro` - прямая связь с создателем
• `/buy_bots` - заказ разработки ботов
• `/donate` - поддержать проект

💡 **Для максимальной функциональности используйте пользовательский режим!**"""

        await send_long_message(event, faq_text, parse_mode='markdown')
        
        if analytics_available:
            analytics.track_event('faq_viewed', event.sender_id, {}, correlation_id)
    
    async def _show_faq_inline(self, event):
        """FAQ для inline кнопок"""
        await self._show_faq(event)
    
    async def _show_commands_help(self, event):
        """Показать справку по командам"""
        await self._show_help(event)
    
    async def _show_stats(self, event):
        """Показать статистику системы"""
        try:
            correlation_id = ""
            if analytics_available:
                correlation_id = analytics.track_command(event.sender_id, '/stats')
            
            # Статистика БД
            db_stats = await db_manager.get_database_stats()
            
            # Статистика сессий
            session_stats = await auth_manager.get_active_sessions_count()
            
            # Статистика очереди
            queue_stats = await self.service_manager.queue.get_queue_status()
            
            response = f"📊 **СТАТИСТИКА СИСТЕМЫ**\n\n"
            
            response += f"👥 **Пользователи:**\n"
            response += f"• Всего: {db_stats.get('users_count', 0)}\n"
            response += f"• Активных: {db_stats.get('active_users', 0)}\n"
            response += f"• В user режиме: {db_stats.get('user_mode_users', 0)}\n\n"
            
            response += f"🔗 **Сессии:**\n"
            response += f"• Активных: {session_stats['total_sessions']}\n"
            response += f"• Максимум: {session_stats['max_sessions']}\n"
            response += f"• Свободно: {session_stats['available_slots']}\n\n"
            
            response += f"📋 **Очередь:**\n"
            response += f"• Ожидает: {queue_stats.get('pending', 0)}\n"
            response += f"• Выполняется: {queue_stats.get('processing', 0)}\n"
            response += f"• Завершено: {queue_stats.get('completed', 0)}\n"
            response += f"• Ошибок: {queue_stats.get('failed', 0)}\n\n"
            
            if self.startup_time:
                uptime = datetime.now() - self.startup_time
                response += f"⏱️ **Время работы:** {uptime}\n"
            
            response += f"🔧 **Версия:** {APP_VERSION}"
            
            await send_long_message(event, response, parse_mode='markdown')
            
            if analytics_available:
                analytics.track_event('system_stats_viewed', event.sender_id, {}, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            await send_long_message(event, f"❌ Ошибка: {str(e)}")
    
    async def _show_debug(self, event):
        """Показать отладочную информацию"""
        try:
            correlation_id = ""
            if analytics_available:
                correlation_id = analytics.track_command(event.sender_id, '/debug')
            
            if event.is_private:
                await send_long_message(event, "⚠️ **Эта команда работает только в супергруппах!**")
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
            
            await send_long_message(event, response, parse_mode='markdown')
            
            if analytics_available:
                analytics.track_event('debug_info_viewed', event.sender_id, {}, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в debug: {e}")
            await send_long_message(event, f"❌ Ошибка: {str(e)}")
    
    async def _show_queue_status(self, event):
        """Показать статус очереди"""
        try:
            user_id = event.sender_id
            correlation_id = ""
            if analytics_available:
                correlation_id = analytics.track_command(user_id, '/queue_status')
            
            queue_status = await self.service_manager.queue.get_queue_status()
            
            response = f"📋 **СТАТУС ОЧЕРЕДИ**\n\n"
            response += f"⏳ Ожидает выполнения: {queue_status.get('pending', 0)}\n"
            response += f"🔄 Выполняется: {queue_status.get('processing', 0)}\n"
            response += f"✅ Завершено за час: {queue_status.get('completed', 0)}\n"
            response += f"❌ Ошибок за час: {queue_status.get('failed', 0)}\n\n"
            
            response += f"👤 **У вас нет задач в очереди**\n"
            
            await send_long_message(event, response, parse_mode='markdown')
            
            if analytics_available:
                analytics.track_event('queue_status_viewed', user_id, {}, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса очереди: {e}")
            await send_long_message(event, f"❌ Ошибка: {str(e)}")
    
    async def _handle_setlimit(self, event, command_name: str):
        """Обработка команд лимитов"""
        try:
            mode = command_name.split('setlimit_')[1] if 'setlimit_' in command_name else 'unknown'
            correlation_id = ""
            if analytics_available:
                correlation_id = analytics.track_command(event.sender_id, f'/setlimit_{mode}')
            
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
                
                status = limiter.get_status()
                
                response = f"🔧 **{mode.upper()} РЕЖИМ ЛИМИТОВ**\n\n"
                response += f"• Запросов в час: {status['max_requests_hour']}\n"
                response += f"• Cooldown: {status['cooldown_seconds']} секунд\n"
                response += f"• Автоматический режим: ОТКЛЮЧЕН"
            else:
                await send_long_message(event, "❌ Неизвестный режим лимитов")
                return
            
            await send_long_message(event, response, parse_mode='markdown')
            
            if analytics_available:
                analytics.track_event('limit_mode_changed', event.sender_id, {
                    'new_mode': mode,
                    'auto_mode': limiter.auto_mode_enabled
                }, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в setlimit: {e}")
            await send_long_message(event, f"❌ Ошибка: {str(e)}")
    
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
    
    async def notify_admin(self, message: str):
        """ИСПРАВЛЕНО: Уведомление администратора"""
        try:
            if ADMIN_USER_ID:
                await self.bot_client.send_message(
                    ADMIN_USER_ID, 
                    f"🔔 **Уведомление системы**\n\n{message}\n\n📅 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
                )
        except Exception as e:
            logger.debug(f"Не удалось отправить уведомление админу: {e}")
    
    async def shutdown(self):
        """Корректное завершение работы"""
        logger.info("🛑 Начало корректного завершения...")
        
        self.is_running = False
        
        try:
            # Записываем событие завершения
            if analytics_available:
                analytics.track_event('bot_shutdown', 0, {
                    'uptime_seconds': (datetime.now() - self.startup_time).total_seconds() if self.startup_time else 0
                })
            
            # Очищаем старые данные
            if security_available:
                security_manager.cleanup_old_data()
            if analytics_available:
                analytics.cleanup_old_data()
            
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
            
            # Записываем успешный запуск
            if analytics_available:
                analytics.track_event('bot_startup', 0, {
                    'version': APP_VERSION,
                    'development_mode': DEVELOPMENT_MODE
                })
            
            # Основной цикл
            await self.bot_client.run_until_disconnected()
            
        except KeyboardInterrupt:
            logger.info("⏹️ Получен сигнал остановки")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            if analytics_available:
                analytics.track_error(0, 'critical_bot_error', str(e))
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
        if analytics_available:
            analytics.track_error(0, 'main_critical_error', str(e))
        sys.exit(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Остановка бота...")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
