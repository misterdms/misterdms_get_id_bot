#!/usr/bin/env python3
"""
🤖 Гибридный Topics Scanner Bot v4.1
Точка входа приложения с поддержкой режимов бота и пользователя
ИСПРАВЛЕНО: Исправлены импорты, улучшена инициализация, добавлена интеграция безопасности

Архитектура:
- Режим бота: ограниченные возможности Bot API
- Режим пользователя: полный доступ через MTProto API
- Многопользовательский режим с очередью запросов
- Безопасное хранение credentials пользователей
- Система безопасности и аналитики
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime
from telethon import TelegramClient, events
from telethon.tl.types import KeyboardButtonCallback
from telethon.tl.custom import Button

# Импорт модулей бота - ИСПРАВЛЕНЫ
from config import (
    BOT_TOKEN, API_ID, API_HASH, APP_NAME, APP_VERSION,
    setup_logging, MESSAGES, PORT, RENDER_EXTERNAL_URL,
    DEVELOPMENT_MODE, ADMIN_USER_ID, BUSINESS_CONTACT_ID
)
from database import init_database, db_manager
from auth_manager import auth_manager, cleanup_auth
from web_server import create_web_server
from handlers import CommandHandler
from services import service_manager

# ИСПРАВЛЕНО: Добавлены импорты security и analytics
from security import security_manager
from analytics import analytics
from utils import send_long_message

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
        """Регистрация обработчиков событий Telegram - ИСПРАВЛЕНО"""
        
        # === ПРОВЕРКА РЕЖИМА РАЗРАБОТКИ (ПЕРВЫЙ ОБРАБОТЧИК!) ===
        @self.bot_client.on(events.NewMessage)
        async def development_mode_check(event):
            """Проверка режима разработки"""
            # Пропускаем системные команды
            if event.text and event.text.startswith('/debug'):
                return
            
            if DEVELOPMENT_MODE and not security_manager.is_trusted_user(event.sender_id):
                logger.info(f"🔧 Режим разработки: блокирован пользователь {event.sender_id}")
                await send_long_message(event, MESSAGES['dev_message'])
                raise events.StopPropagation  # Останавливаем обработку других команд
        
        # === ПРОВЕРКА БЕЗОПАСНОСТИ ===
        @self.bot_client.on(events.NewMessage)
        async def security_check(event):
            """Проверка безопасности и лимитов"""
            # Пропускаем системные команды
            if event.text and event.text.startswith('/debug'):
                return
                
            is_allowed, message = security_manager.is_user_allowed(event.sender_id)
            if not is_allowed:
                logger.warning(f"🚫 Доступ запрещен для {event.sender_id}: {message}")
                await send_long_message(event, message)
                # Записываем блокировку в аналитику
                analytics.track_error(event.sender_id, 'access_denied', message)
                raise events.StopPropagation

            # Логируем разрешенный запрос
            logger.debug(f"✅ Доступ разрешен для {event.sender_id}")
        
        # === ОСНОВНЫЕ КОМАНДЫ ===
        
        @self.bot_client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            """Стартовая команда с выбором режима"""
            try:
                user_id = event.sender_id
                chat_type = 'private' if event.is_private else 'group'
                logger.info(f"🚀 /start от пользователя {user_id} в {chat_type}")
                sender = event.sender
                
                # Аналитика
                correlation_id = analytics.track_command(user_id, '/start', 
                                                       'private' if event.is_private else 'group')
                
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
                
                # Успешная аналитика
                analytics.track_event('start_completed', user_id, 
                                     {'success': True}, correlation_id)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в start_handler: {e}")
                analytics.track_error(event.sender_id, 'start_error', str(e))
                await send_long_message(event, MESSAGES['error_general'].format(error_message=str(e)))
        
        @self.bot_client.on(events.CallbackQuery)
        async def callback_handler(event):
            """Обработчик inline кнопок"""
            try:
                data = event.data.decode('utf-8')
                user_id = event.sender_id
                
                # Аналитика callback
                correlation_id = analytics.track_command(user_id, f'callback_{data}')
                
                if data == 'mode_bot':
                    await self._set_bot_mode(event, user_id)
                elif data == 'mode_user':
                    await self._set_user_mode(event, user_id)
                elif data == 'show_commands':
                    await self._show_commands_help(event)
                elif data == 'show_faq':
                    await self._show_faq_inline(event)
                
                # Успешная аналитика
                analytics.track_event('callback_completed', user_id, 
                                     {'callback_data': data}, correlation_id)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в callback_handler: {e}")
                analytics.track_error(event.sender_id, 'callback_error', str(e))
                await send_long_message(event, MESSAGES['error_general'].format(error_message=str(e)))
        
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
            correlation_id = analytics.track_command(event.sender_id, '/switch_mode')
            
            if event.is_private:
                await self._show_mode_selection(event)
                analytics.track_event('mode_switch_opened', event.sender_id, {}, correlation_id)
            else:
                await send_long_message(event, "⚠️ Переключение режима доступно только в личных сообщениях")
                analytics.track_error(event.sender_id, 'switch_mode_wrong_chat', 'Попытка в группе')
        
        @self.bot_client.on(events.NewMessage(pattern='/renew_my_api_hash'))
        async def renew_credentials_handler(event):
            """Обновление API credentials"""
            correlation_id = analytics.track_command(event.sender_id, '/renew_my_api_hash')
            
            if event.is_private:
                await self._set_user_mode(event, event.sender_id)
                analytics.track_event('credentials_renewal_started', event.sender_id, {}, correlation_id)
            else:
                await send_long_message(event, "⚠️ Обновление credentials доступно только в личных сообщениях")
                analytics.track_error(event.sender_id, 'renew_credentials_wrong_chat', 'Попытка в группе')
        
        @self.bot_client.on(events.NewMessage(pattern='/my_status'))
        async def my_status_handler(event):
            """Статус пользователя"""
            await self._show_user_status(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/logout'))
        async def logout_handler(event):
            """Выход из пользовательского режима"""
            await self._logout_user(event)
        
        # === НОВЫЕ КОМАНДЫ v4.1 - СВЯЗЬ ===
        
        @self.bot_client.on(events.NewMessage(pattern='/yo_bro'))
        async def yo_bro_handler(event):
            """Связь с создателем"""
            correlation_id = analytics.track_command(event.sender_id, '/yo_bro')
            await send_long_message(event, MESSAGES['yo_bro'], parse_mode='markdown')
            analytics.track_event('creator_contact_used', event.sender_id, {}, correlation_id)
        
        @self.bot_client.on(events.NewMessage(pattern='/buy_bots'))
        async def buy_bots_handler(event):
            """Заказ разработки ботов"""
            correlation_id = analytics.track_command(event.sender_id, '/buy_bots')
            await send_long_message(event, MESSAGES['buy_bots'], parse_mode='markdown')
            analytics.track_event('business_contact_used', event.sender_id, {}, correlation_id)
        
        @self.bot_client.on(events.NewMessage(pattern='/donate'))
        async def donate_handler(event):
            """Информация о донатах"""
            correlation_id = analytics.track_command(event.sender_id, '/donate')
            await send_long_message(event, MESSAGES['donate'], parse_mode='markdown')
            analytics.track_event('donate_info_viewed', event.sender_id, {}, correlation_id)
        
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
                    correlation_id = analytics.track_command(event.sender_id, 'credentials_input')
                    await self._process_credentials(event, correlation_id)
                
            except Exception as e:
                logger.debug(f"Ошибка обработки credentials: {e}")
    
    async def _route_command(self, event, command: str):
        """Маршрутизация команды в зависимости от режима пользователя - ИСПРАВЛЕНО"""
        try:
            user_id = event.sender_id
            user_data = await db_manager.get_user(user_id)
            
            # Аналитика команды
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
                    priority=2
                )
                
                # Уведомляем о добавлении в очередь
                queue_status = await self.service_manager.queue.get_queue_status()
                if queue_status['pending'] > 1:
                    position = queue_status.get('pending', 1)
                    estimated_time = f"{position * 30} секунд"
                    
                    await send_long_message(event, MESSAGES['queue_notification'].format(
                        position=position,
                        estimated_time=estimated_time
                    ))
                    
                    analytics.track_event('queue_notification_sent', user_id, {
                        'position': position,
                        'command': command
                    }, correlation_id)
                else:
                    await send_long_message(event, "🔄 **Задача добавлена в очередь** - выполнение начнется в ближайшее время...")
                    
                analytics.track_event('command_queued', user_id, {
                    'command': command,
                    'task_id': task_id
                }, correlation_id)
            else:
                # Обрабатываем в режиме бота напрямую (без очереди)
                success = await self.command_handler.route_command(command, event, 'bot')
                
                analytics.track_event('command_executed_bot_mode', user_id, {
                    'command': command,
                    'success': success
                }, correlation_id)
                
        except Exception as e:
            logger.error(f"❌ Ошибка маршрутизации команды {command}: {e}")
            analytics.track_error(event.sender_id, 'command_routing_error', str(e))
            await send_long_message(event, MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_mode_selection(self, event):
        """Показать выбор режима работы"""
        correlation_id = analytics.track_command(event.sender_id, 'mode_selection_shown')
        
        buttons = [
            [Button.inline("🤖 Режим бота (быстрый старт)", b"mode_bot")],
            [Button.inline("👤 Режим пользователя (полный доступ)", b"mode_user")],
            [Button.inline("📋 Показать команды", b"show_commands")],
            [Button.inline("❓ Частые вопросы", b"show_faq")]
        ]
        
        await send_long_message(event, MESSAGES['welcome'], buttons=buttons, parse_mode='markdown')
        
        analytics.track_event('mode_selection_shown', event.sender_id, {}, correlation_id)
    
    async def _set_bot_mode(self, event, user_id: int):
        """Установить режим бота"""
        correlation_id = analytics.track_command(user_id, 'bot_mode_selected')
        
        await db_manager.create_or_update_user(user_id, mode='bot')
        await event.edit(MESSAGES['bot_mode_selected'])
        
        analytics.track_event('mode_changed', user_id, {
            'new_mode': 'bot'
        }, correlation_id)
    
    async def _set_user_mode(self, event, user_id: int):
        """Установить режим пользователя"""
        correlation_id = analytics.track_command(user_id, 'user_mode_selected')
        
        await event.edit(MESSAGES['user_mode_instructions'])
        await db_manager.create_or_update_user(user_id, mode='bot')  # Временно bot до получения credentials
        
        analytics.track_event('user_mode_instructions_shown', user_id, {}, correlation_id)
    
    async def _process_credentials(self, event, correlation_id: str = ""):
        """Обработка пользовательских credentials - ИСПРАВЛЕНО"""
        try:
            lines = event.text.strip().split('\n')
            
            if len(lines) != 2:
                await send_long_message(event, "❌ Неверный формат. Нужно 2 строки:\n1. API_ID\n2. API_HASH")
                analytics.track_error(event.sender_id, 'credentials_wrong_format', 'Неверное количество строк')
                return
            
            api_id = lines[0].strip()
            api_hash = lines[1].strip()
            
            # Сохраняем credentials
            success, message = await auth_manager.save_user_credentials(
                event.sender_id, api_id, api_hash
            )
            
            if success:
                await send_long_message(event, MESSAGES['credentials_saved'])
                analytics.track_event('credentials_saved_successfully', event.sender_id, {
                    'method': 'manual_input'
                }, correlation_id)
            else:
                await send_long_message(event, f"❌ {message}\n\nПопробуйте еще раз или используйте /renew_my_api_hash")
                analytics.track_error(event.sender_id, 'credentials_save_failed', message)
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки credentials: {e}")
            analytics.track_error(event.sender_id, 'credentials_processing_error', str(e))
            await send_long_message(event, MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_user_status(self, event):
        """Показать статус пользователя"""
        try:
            user_id = event.sender_id
            correlation_id = analytics.track_command(user_id, '/my_status')
            
            session_info = await auth_manager.get_session_info(user_id)
            security_status = security_manager.get_security_status(user_id)
            
            response = f"👤 **СТАТУС ПОЛЬЗОВАТЕЛЯ**\n\n"
            response += f"🆔 User ID: `{user_id}`\n"
            response += f"🔧 Режим: {session_info['mode']}\n"
            response += f"📊 Статус: {session_info['status']}\n"
            response += f"🔐 Credentials: {'✅ Есть' if session_info['has_credentials'] else '❌ Нет'}\n"
            response += f"🔗 Активная сессия: {'✅ Да' if session_info['is_session_active'] else '❌ Нет'}\n\n"
            
            # Лимиты безопасности
            user_limits = security_status.get('user_limits', {})
            response += f"🛡️ **БЕЗОПАСНОСТЬ:**\n"
            response += f"• Запросов сегодня: {user_limits.get('requests_today', 0)}\n"
            response += f"• Доверенный: {'✅' if user_limits.get('is_trusted', False) else '❌'}\n"
            response += f"• Cooldown: {user_limits.get('cooldown_remaining', 0):.0f}с\n\n"
            
            if session_info.get('telegram_user'):
                tg_user = session_info['telegram_user']
                response += f"📱 **Telegram аккаунт:**\n"
                response += f"• Username: @{tg_user.get('username', 'не указан')}\n"
                response += f"• Имя: {tg_user.get('first_name', 'не указано')}\n"
                response += f"• Телефон: {tg_user.get('phone', 'скрыт')}\n"
            
            await send_long_message(event, response, parse_mode='markdown')
            
            analytics.track_event('user_status_viewed', user_id, {
                'mode': session_info['mode'],
                'has_credentials': session_info['has_credentials']
            }, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса: {e}")
            analytics.track_error(event.sender_id, 'user_status_error', str(e))
            await send_long_message(event, MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _logout_user(self, event):
        """Выход из пользовательского режима"""
        try:
            correlation_id = analytics.track_command(event.sender_id, '/logout')
            
            success, message = await auth_manager.logout_user(event.sender_id)
            await send_long_message(event, message)
            
            analytics.track_event('user_logout', event.sender_id, {
                'success': success
            }, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка logout: {e}")
            analytics.track_error(event.sender_id, 'logout_error', str(e))
            await send_long_message(event, MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_stats(self, event):
        """Показать статистику системы - ИСПРАВЛЕНО"""
        try:
            correlation_id = analytics.track_command(event.sender_id, '/stats')
            
            # Статистика БД
            db_stats = await db_manager.get_database_stats()
            
            # Статистика сессий
            session_stats = await auth_manager.get_active_sessions_count()
            
            # Статистика очереди
            queue_stats = await self.service_manager.queue.get_queue_status()
            
            # Статистика безопасности
            security_status = security_manager.get_security_status()
            
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
            
            response += f"🛡️ **Безопасность:**\n"
            response += f"• Всего запросов: {security_status.get('global_stats', {}).get('total_requests', 0)}\n"
            response += f"• Заблокировано: {security_status.get('global_stats', {}).get('blocked_requests', 0)}\n\n"
            
            response += f"📈 **Активность:**\n"
            response += f"• Записей за сегодня: {db_stats.get('activity_data_count', 0)}\n\n"
            
            if self.startup_time:
                uptime = datetime.now() - self.startup_time
                response += f"⏱️ **Время работы:** {uptime}\n"
            
            response += f"🔧 **Версия:** {APP_VERSION}"
            
            await send_long_message(event, response, parse_mode='markdown')
            
            analytics.track_event('system_stats_viewed', event.sender_id, {
                'total_users': db_stats['users_count'],
                'active_sessions': session_stats['total_sessions']
            }, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            analytics.track_error(event.sender_id, 'stats_error', str(e))
            await send_long_message(event, MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_queue_status(self, event):
        """Показать статус очереди"""
        try:
            user_id = event.sender_id
            correlation_id = analytics.track_command(user_id, '/queue_status')
            
            queue_status = await self.service_manager.queue.get_queue_status()
            
            response = f"📋 **СТАТУС ОЧЕРЕДИ**\n\n"
            response += f"⏳ Ожидает выполнения: {queue_status['pending']}\n"
            response += f"🔄 Выполняется: {queue_status['processing']}\n"
            response += f"✅ Завершено за час: {queue_status['completed']}\n"
            response += f"❌ Ошибок за час: {queue_status['failed']}\n\n"
            
            user_position = queue_status.get('user_position')
            if user_position:
                response += f"👤 **Ваша позиция в очереди:** {user_position}\n"
                estimated_time = user_position * 30
                response += f"⏱️ **Примерное время ожидания:** {estimated_time} секунд\n"
            else:
                response += f"👤 **У вас нет задач в очереди**\n"
            
            await send_long_message(event, response, parse_mode='markdown')
            
            analytics.track_event('queue_status_viewed', user_id, {
                'pending': queue_status['pending'],
                'user_position': user_position or 0
            }, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса очереди: {e}")
            analytics.track_error(event.sender_id, 'queue_status_error', str(e))
            await send_long_message(event, MESSAGES['error_general'].format(error_message=str(e)))
    
    async def _show_commands_help(self, event):
        """Показать справку по командам"""
        correlation_id = analytics.track_command(event.sender_id, 'commands_help_shown')
        
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

💬 **Связь:**
• /yo_bro - связь с создателем
• /buy_bots - заказ разработки
• /donate - поддержать проект

💡 **Совет:** Для полной функциональности используйте режим пользователя!"""
        
        await event.edit(help_text, parse_mode='markdown')
        
        analytics.track_event('commands_help_viewed', event.sender_id, {}, correlation_id)
    
    async def _show_help(self, event):
        """Показать справку по командам"""
        correlation_id = analytics.track_command(event.sender_id, '/help')
        
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

**💬 Связь (НОВОЕ v4.1):**
• `/yo_bro` - связь с создателем
• `/buy_bots` - заказ разработки ботов
• `/donate` - поддержать проект

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

        await send_long_message(event, help_text, parse_mode='markdown')
        
        analytics.track_event('help_viewed', event.sender_id, {}, correlation_id)
    
    async def _show_faq(self, event):
        """Показать FAQ"""
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

**5. 👥 Как работает отслеживание активности?**
• Автоматически фиксирует все сообщения
• Подсчитывает количество сообщений за день
• Данные сбрасываются каждый день в 00:00
• Команды бота не считаются активностью

**6. 💬 Как связаться с разработчиком? (НОВОЕ)**
• `/yo_bro` - прямая связь с создателем
• `/buy_bots` - заказ разработки ботов
• `/donate` - поддержать проект

💡 **Совет:** Для максимальной функциональности используйте пользовательский режим через `/switch_mode`"""

        await send_long_message(event, faq_text, parse_mode='markdown')
        
        analytics.track_event('faq_viewed', event.sender_id, {}, correlation_id)
    
    async def _show_faq_inline(self, event):
        """FAQ для inline кнопок"""
        await self._show_faq(event)
    
    async def _show_debug(self, event):
        """Отладочная информация"""
        try:
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
            
            analytics.track_event('debug_info_viewed', event.sender_id, {
                'chat_id': event.chat_id,
                'overall_healthy': health['overall_healthy']
            }, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в debug: {e}")
            analytics.track_error(event.sender_id, 'debug_error', str(e))
            await send_long_message(event, f"❌ Ошибка получения отладочной информации: {str(e)}")
    
    async def _handle_setlimit(self, event):
        """Обработка команд лимитов"""
        try:
            command_text = event.text
            if '/setlimit_' not in command_text:
                return False
            
            mode = command_text.split('/setlimit_')[1].strip()
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
                
                from config import API_LIMITS
                mode_info = API_LIMITS[mode]
                status = limiter.get_status()
                
                response = f"{mode_info['name']} **РЕЖИМ ЛИМИТОВ**\n\n"
                response += f"• Запросов в час: {status['max_requests_hour']}\n"
                response += f"• Cooldown: {status['cooldown_seconds']} секунд\n"
                response += f"• Назначение: {mode_info['description']}\n"
                response += f"• Автоматический режим: ОТКЛЮЧЕН"
            else:
                await send_long_message(event, "❌ Неизвестный режим лимитов")
                analytics.track_error(event.sender_id, 'unknown_limit_mode', mode)
                return
            
            await send_long_message(event, response, parse_mode='markdown')
            
            analytics.track_event('limit_mode_changed', event.sender_id, {
                'new_mode': mode,
                'auto_mode': limiter.auto_mode_enabled
            }, correlation_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в setlimit: {e}")
            analytics.track_error(event.sender_id, 'setlimit_error', str(e))
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
    
    async def shutdown(self):
        """Корректное завершение работы"""
        logger.info("🛑 Начало корректного завершения...")
        
        self.is_running = False
        
        try:
            # Записываем событие завершения
            analytics.track_event('bot_shutdown', 0, {
                'uptime_seconds': (datetime.now() - self.startup_time).total_seconds() if self.startup_time else 0
            })
            
            # Очищаем старые данные
            security_manager.cleanup_old_data()
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