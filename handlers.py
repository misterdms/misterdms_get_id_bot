#!/usr/bin/env python3
"""
Объединенные обработчики команд для гибридного Topics Scanner Bot
Содержит логику для режима бота и пользовательского режима
ИСПРАВЛЕНО: Правильный импорт TopicScannerFactory из utils, добавлена интеграция с security/analytics
"""

import logging
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any
from telethon import TelegramClient
from telethon.tl.types import Channel
from telethon.tl.functions.channels import GetFullChannelRequest, GetForumTopicsRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.errors import ChatAdminRequiredError, ChannelPrivateError

from config import API_LIMITS, MESSAGES, BOT_MODES
from database import db_manager
from utils import TopicScannerFactory, send_long_message, format_topics_table  # ИСПРАВЛЕН ИМПОРТ

logger = logging.getLogger(__name__)

class CommandHandler:
    """Единый обработчик команд с поддержкой двух режимов"""
    
    def __init__(self):
        self.bot_mode = BotModeHandler()
        self.user_mode = UserModeHandler()
        self.auth_manager = None
        self.api_limiter = None
        self.activity_tracker = None
        
    async def initialize(self, bot_client: TelegramClient, auth_manager, api_limiter, activity_tracker):
        """Инициализация обработчика"""
        self.auth_manager = auth_manager
        self.api_limiter = api_limiter
        self.activity_tracker = activity_tracker
        
        await self.bot_mode.initialize(bot_client, api_limiter)
        await self.user_mode.initialize(auth_manager, api_limiter)
        
        logger.info("✅ CommandHandler инициализирован")
    
    async def route_command(self, command: str, event, user_mode: str = 'bot') -> bool:
        """Маршрутизация команды в зависимости от режима"""
        try:
            if user_mode == 'user':
                return await self.user_mode.handle_command(command, event)
            else:
                return await self.bot_mode.handle_command(command, event)
                
        except Exception as e:
            logger.error(f"❌ Ошибка маршрутизации команды {command}: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_start(self, event, user_mode: str = 'bot') -> bool:
        """Обработка команды /start"""
        if user_mode == 'user':
            return await self.user_mode.handle_start(event)
        else:
            return await self.bot_mode.handle_start(event)

class BaseModeHandler:
    """Базовый класс для обработчиков режимов"""
    
    def __init__(self):
        self.client: Optional[TelegramClient] = None
        self.api_limiter = None
        
    async def _validate_group_chat(self, event) -> tuple[bool, Optional[Channel]]:
        """Валидация группового чата"""
        if event.is_private:
            await event.reply("⚠️ **Эта команда работает только в супергруппах!**")
            return False, None
        
        chat = await event.get_chat()
        if not isinstance(chat, Channel) or not chat.megagroup:
            await event.reply("⚠️ **Работает только в супергруппах!**")
            return False, None
            
        return True, chat
    
    async def _get_participants_count(self, chat) -> int:
        """Получить количество участников"""
        try:
            if hasattr(chat, 'participants_count') and chat.participants_count:
                return chat.participants_count
            
            try:
                full_channel = await self.client(GetFullChannelRequest(chat))
                if hasattr(full_channel, 'full_chat'):
                    return full_channel.full_chat.participants_count
            except:
                pass
            
            return 0
            
        except Exception as e:
            logger.debug(f"Ошибка получения количества участников: {e}")
            return 0
    
    async def _auto_adjust_limits(self, event, participants_count: int, request_type: str):
        """Автоматическое переключение лимитов"""
        try:
            if not self.api_limiter.auto_mode_enabled or participants_count <= 0:
                return
            
            complexity = 'heavy' if request_type in ['get_all', 'full_scan'] else 'normal'
            mode_changed = self.api_limiter.auto_adjust_mode(participants_count, complexity)
            
            if mode_changed and participants_count > 200:
                mode_name = self.api_limiter.get_status()['mode_name']
                warning_msg = (
                    f"🔧 **Автоматическое переключение лимитов**\n\n"
                    f"📊 Участников в группе: {participants_count}\n"
                    f"🔧 Новый режим: {mode_name}\n\n"
                    "Это обеспечивает стабильную работу. Немного подождите! 🙏"
                )
                await event.reply(warning_msg, parse_mode='markdown')
                await asyncio.sleep(2)
                
        except Exception as e:
            logger.debug(f"Ошибка автопереключения лимитов: {e}")

class BotModeHandler(BaseModeHandler):
    """Обработчик команд в режиме бота (ограниченный)"""
    
    async def initialize(self, client: TelegramClient, api_limiter):
        """Инициализация обработчика режима бота"""
        self.client = client
        self.api_limiter = api_limiter
        logger.info("✅ BotModeHandler инициализирован")
    
    async def handle_command(self, command: str, event) -> bool:
        """Обработка команды в режиме бота"""
        try:
            if command == 'scan':
                return await self.handle_scan(event)
            elif command == 'get_all':
                return await self.handle_get_all(event)
            elif command == 'get_users':
                return await self.handle_get_users(event)
            elif command == 'get_ids':
                return await self.handle_get_ids(event)
            else:
                logger.warning(f"⚠️ Неизвестная команда: {command}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка обработки команды {command}: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_start(self, event) -> bool:
        """Обработчик /start в режиме бота"""
        try:
            start_time = datetime.now()
            user_id = event.sender_id
            
            logger.info(f"🤖 /start (bot mode) от пользователя {user_id}")
            
            # Проверка лимитов
            if not self.api_limiter.can_make_request():
                await event.reply("⚠️ **Превышен лимит запросов**\n\nПопробуйте позже")
                return False
            
            self.api_limiter.record_request()
            
            # Валидация чата
            is_valid, chat = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            # Получение информации о чате
            participants_count = await self._get_participants_count(chat)
            await self._auto_adjust_limits(event, participants_count, 'full_scan')
            
            # Сканирование топиков
            scanner = TopicScannerFactory.create_scanner(self.client, 'bot')
            topics_data = await scanner.scan_topics(chat)
            
            # Формирование ответа
            response = self._build_start_response(chat, participants_count, topics_data, start_time)
            
            await send_long_message(event, response)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"⚡ /start (bot mode) выполнен за {duration:.2f}с")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_start (bot mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_scan(self, event) -> bool:
        """Сканирование топиков в режиме бота"""
        try:
            logger.info(f"🤖 /scan (bot mode) от пользователя {event.sender_id}")
            
            if not self.api_limiter.can_make_request():
                await event.reply("⚠️ **Превышен лимит запросов**")
                return False
            
            self.api_limiter.record_request()
            
            is_valid, chat = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            participants_count = await self._get_participants_count(chat)
            await self._auto_adjust_limits(event, participants_count, 'scan')
            
            scanner = TopicScannerFactory.create_scanner(self.client, 'bot')
            topics_data = await scanner.scan_topics(chat)
            
            response = self._build_scan_response(chat, participants_count, topics_data)
            await send_long_message(event, response)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_scan (bot mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_get_all(self, event) -> bool:
        """Получение всех данных в режиме бота"""
        try:
            logger.info(f"🤖 /get_all (bot mode) от пользователя {event.sender_id}")
            
            if not self.api_limiter.can_make_request():
                await event.reply("⚠️ **Превышен лимит запросов**")
                return False
            
            self.api_limiter.record_request()
            
            is_valid, chat = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            processing_msg = await event.reply("🔄 **Получение всех данных (режим бота)...**")
            
            # Получение данных
            scanner = TopicScannerFactory.create_scanner(self.client, 'bot')
            topics_data = await scanner.scan_topics(chat)
            active_users = await db_manager.get_active_users(event.chat_id)
            activity_stats = await db_manager.get_activity_stats(event.chat_id)
            
            response = self._build_get_all_response(chat, topics_data, active_users, activity_stats)
            
            await processing_msg.delete()
            await send_long_message(event, response)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_get_all (bot mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_get_users(self, event) -> bool:
        """Получение активных пользователей"""
        try:
            logger.info(f"🤖 /get_users (bot mode) от пользователя {event.sender_id}")
            
            is_valid, _ = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            active_users = await db_manager.get_active_users(event.chat_id)
            activity_stats = await db_manager.get_activity_stats(event.chat_id)
            
            response = self._build_users_response(active_users, activity_stats)
            await send_long_message(event, response)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_get_users (bot mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_get_ids(self, event) -> bool:
        """Повторное сканирование ID"""
        try:
            logger.info(f"🤖 /get_ids (bot mode) от пользователя {event.sender_id}")
            
            if not self.api_limiter.can_make_request():
                await event.reply("⚠️ **Превышен лимит запросов**")
                return False
            
            self.api_limiter.record_request()
            
            is_valid, chat = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            scanner = TopicScannerFactory.create_scanner(self.client, 'bot')
            topics_data = await scanner.scan_topics(chat)
            
            response = self._build_ids_response(chat, topics_data)
            await send_long_message(event, response)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_get_ids (bot mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    def _build_start_response(self, chat, participants_count, topics_data, start_time) -> str:
        """Построение ответа для /start"""
        response = "🤖 **TOPICS SCANNER BOT - РЕЖИМ БОТА**\n\n"
        response += "⚠️ **Режим:** Ограниченный (Bot API)\n\n"
        
        response += f"🏢 **Супергруппа:** {chat.title}\n"
        response += f"🆔 **ID группы:** `{chat.id}`\n"
        response += f"👥 **Участников:** {participants_count if participants_count > 0 else 'определяется...'}\n"
        response += f"🕒 **Время сканирования:** {start_time.strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        
        if topics_data:
            regular_topics = [t for t in topics_data if t['id'] > 0]
            system_topics = [t for t in topics_data if t['id'] <= 0]
            
            response += f"📊 **НАЙДЕНО ТОПИКОВ: {len(regular_topics)}**\n"
            
            if system_topics:
                response += "⚠️ **Примечание:** Из-за ограничений Bot API некоторые топики могут быть недоступны.\n\n"
            
            response += format_topics_table(regular_topics)
            
            if system_topics:
                response += "\nℹ️ **СИСТЕМНАЯ ИНФОРМАЦИЯ:**\n"
                for topic in system_topics:
                    response += f"• {topic['title']}\n"
                response += "\n💡 **Совет:** Используйте пользовательский режим для полного доступа.\n\n"
        
        # Добавляем информацию о командах
        response += "📋 **КОМАНДЫ (РЕЖИМ БОТА):**\n"
        response += "• `/scan` - сканирование топиков\n"
        response += "• `/get_users` - активные пользователи\n" 
        response += "• `/get_all` - все данные\n"
        response += "• `/switch_mode` - переключить на user режим\n"
        response += "• `/help` - подробная справка\n\n"
        
        response += "💡 **Для полного функционала переключитесь на пользовательский режим!**"
        
        return response
    
    def _build_scan_response(self, chat, participants_count, topics_data) -> str:
        """Построение ответа для /scan"""
        response = f"🤖 **СКАНИРОВАНИЕ ТОПИКОВ (РЕЖИМ БОТА)**\n\n"
        response += f"🏢 **Группа:** {chat.title}\n"
        response += f"👥 **Участников:** {participants_count}\n"
        response += f"🕒 **Время:** {datetime.now().strftime('%H:%M:%S')}\n\n"
        
        if topics_data:
            regular_topics = [t for t in topics_data if t['id'] > 0]
            response += f"📊 **НАЙДЕНО: {len(regular_topics)} топиков**\n\n"
            response += format_topics_table(regular_topics)
            
            response += "\n⚠️ **Режим бота:** Могут быть показаны не все топики\n"
            response += "💡 **Совет:** Используйте `/switch_mode` для полного доступа"
        else:
            response += "❌ **Топики не найдены**\n"
            response += "Возможно, группа не является форумом или топики недоступны для ботов."
        
        return response
    
    def _build_get_all_response(self, chat, topics_data, active_users, activity_stats) -> str:
        """Построение ответа для /get_all"""
        response = f"🤖 **ПОЛНЫЙ ОТЧЕТ (РЕЖИМ БОТА)**\n\n"
        response += f"🏢 **Группа:** {chat.title}\n"
        response += f"🕒 **Время:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        
        # Секция топиков
        regular_topics = [t for t in topics_data if t['id'] > 0]
        if regular_topics:
            response += f"📋 **ТОПИКИ ({len(regular_topics)}):**\n\n"
            for topic in regular_topics:
                response += f"• **{topic['title']}** (ID: {topic['id']})\n"
                response += f"  └ Создатель: {topic['created_by']}\n"
                response += f"  └ Ссылка: {topic['link']}\n"
            response += "\n"
        
        # Секция активности
        response += f"👥 **АКТИВНОСТЬ ЗА СЕГОДНЯ:**\n"
        response += f"• Всего активных: {activity_stats['total_users']}\n"
        response += f"• Всего сообщений: {activity_stats['total_messages']}\n\n"
        
        if active_users:
            top_users = sorted(active_users, key=lambda x: x['message_count'], reverse=True)[:10]
            response += "🏆 **ТОП-10 АКТИВНЫХ:**\n"
            response += "| № | Username | Сообщений |\n"
            response += "|---|----------|----------|\n"
            
            for i, user in enumerate(top_users, 1):
                username = f"@{user['username']}" if user['username'] else (user['first_name'] or 'Без имени')
                username = username[:15] + "..." if len(username) > 15 else username
                response += f"| {i} | {username} | {user['message_count']} |\n"
            
            response += "\n"
        
        response += "⚠️ **Режим бота:** Ограниченная информация\n"
        response += "💡 **Для полных данных:** `/switch_mode` → пользовательский режим"
        
        return response
    
    def _build_users_response(self, active_users, activity_stats) -> str:
        """Построение ответа для /get_users"""
        response = f"🤖 **АКТИВНЫЕ ПОЛЬЗОВАТЕЛИ (РЕЖИМ БОТА)**\n\n"
        response += f"📅 **За сегодня ({activity_stats['date']}):**\n"
        response += f"• Всего активных: {activity_stats['total_users']}\n"
        response += f"• Всего сообщений: {activity_stats['total_messages']}\n\n"
        
        if active_users:
            response += "| Username | User ID | Сообщений |\n"
            response += "|----------|---------|----------|\n"
            
            for user in active_users:
                username = f"@{user['username']}" if user['username'] else (user['first_name'] or 'Без имени')
                username = username[:20] + "..." if len(username) > 20 else username
                response += f"| {username} | `{user['user_id']}` | {user['message_count']} |\n"
            
            # Топ-5 активных
            if len(active_users) > 1:
                top_users = sorted(active_users, key=lambda x: x['message_count'], reverse=True)[:5]
                response += "\n🏆 **ТОП-5 АКТИВНЫХ:**\n"
                for i, user in enumerate(top_users, 1):
                    username = f"@{user['username']}" if user['username'] else (user['first_name'] or 'Без имени')
                    response += f"{i}. {username} - {user['message_count']} сообщений\n"
        else:
            response += "😴 **Пока никто не проявил активность сегодня**\n"
        
        response += "\nℹ️ **Информация:**\n"
        response += "• Данные обновляются автоматически\n"
        response += "• Сброс каждый день в 00:00\n"
        response += "• Команды бота не считаются активностью"
        
        return response
    
    def _build_ids_response(self, chat, topics_data) -> str:
        """Построение ответа для /get_ids"""
        response = f"🤖 **ПОВТОРНОЕ СКАНИРОВАНИЕ ID (РЕЖИМ БОТА)**\n\n"
        response += f"🏢 **Группа:** {chat.title}\n"
        response += f"🆔 **ID группы:** `{chat.id}`\n"
        response += f"🕒 **Время:** {datetime.now().strftime('%H:%M:%S')}\n\n"
        
        if topics_data:
            regular_topics = [t for t in topics_data if t['id'] > 0]
            response += f"📊 **НАЙДЕНО: {len(regular_topics)} топиков**\n\n"
            
            response += "| ID | Название | Ссылка |\n"
            response += "|----|----------|--------|\n"
            
            for topic in regular_topics:
                title = topic['title'][:25] + "..." if len(topic['title']) > 25 else topic['title']
                response += f"| {topic['id']} | {title} | {topic['link']} |\n"
            
            response += "\n🔗 **Все ссылки готовы к использованию!**"
        else:
            response += "❌ **Топики не найдены**"
        
        response += "\n\n⚠️ **Режим бота:** Могут быть показаны не все топики"
        
        return response

class UserModeHandler(BaseModeHandler):
    """Обработчик команд в пользовательском режиме"""
    
    def __init__(self):
        super().__init__()
        self.auth_manager = None
        
    async def initialize(self, auth_manager, api_limiter):
        """Инициализация обработчика пользовательского режима"""
        self.auth_manager = auth_manager
        self.api_limiter = api_limiter
        logger.info("✅ UserModeHandler инициализирован")
    
    async def handle_command(self, command: str, event, task_id: int = None) -> bool:
        """Обработка команды в пользовательском режиме"""
        try:
            user_id = event.sender_id
            
            # Получаем пользовательскую сессию
            user_client = await self.auth_manager.get_user_session(user_id)
            if not user_client:
                await event.reply("❌ **Не удалось получить пользовательскую сессию**\n\nПопробуйте `/renew_my_api_hash`")
                if task_id:
                    await db_manager.complete_task(task_id, error="Нет пользовательской сессии")
                return False
            
            self.client = user_client
            
            # Обрабатываем команду
            success = False
            if command == 'scan':
                success = await self.handle_scan(event)
            elif command == 'get_all':
                success = await self.handle_get_all(event)
            elif command == 'get_users':
                success = await self.handle_get_users(event)
            elif command == 'get_ids':
                success = await self.handle_get_ids(event)
            else:
                logger.warning(f"⚠️ Неизвестная команда в user режиме: {command}")
                if task_id:
                    await db_manager.complete_task(task_id, error=f"Неизвестная команда: {command}")
                return False
            
            if success and task_id:
                await db_manager.complete_task(task_id, result="Команда выполнена успешно")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки команды {command} в user режиме: {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            if task_id:
                await db_manager.complete_task(task_id, error=str(e))
            return False
    
    async def handle_start(self, event) -> bool:
        """Обработчик /start в пользовательском режиме"""
        try:
            start_time = datetime.now()
            user_id = event.sender_id
            
            logger.info(f"👤 /start (user mode) от пользователя {user_id}")
            
            # Получаем пользовательскую сессию
            user_client = await self.auth_manager.get_user_session(user_id)
            if not user_client:
                await event.reply("❌ **Пользовательская сессия недоступна**\n\nИспользуйте `/renew_my_api_hash` для настройки")
                return False
            
            self.client = user_client
            
            # Валидация чата
            is_valid, chat = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            # Получение детальной информации
            try:
                full_chat = await user_client(GetFullChannelRequest(chat))
                participants_count = full_chat.full_chat.participants_count
            except:
                participants_count = getattr(chat, 'participants_count', 0)
            
            await self._auto_adjust_limits(event, participants_count, 'full_scan')
            
            # Полное сканирование топиков
            scanner = TopicScannerFactory.create_scanner(user_client, 'user')
            topics_data = await scanner.scan_topics(chat)
            
            # Получение статистики активности
            activity_stats = await db_manager.get_activity_stats(event.chat_id)
            
            response = self._build_start_response(chat, participants_count, topics_data, activity_stats, start_time)
            await send_long_message(event, response)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"⚡ /start (user mode) выполнен за {duration:.2f}с")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_start (user mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_scan(self, event) -> bool:
        """Сканирование топиков в пользовательском режиме"""
        try:
            logger.info(f"👤 /scan (user mode) от пользователя {event.sender_id}")
            
            is_valid, chat = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            # Получаем детальную информацию
            try:
                full_chat = await self.client(GetFullChannelRequest(chat))
                participants_count = full_chat.full_chat.participants_count
                about = getattr(full_chat.full_chat, 'about', '')
            except:
                participants_count = getattr(chat, 'participants_count', 0)
                about = ''
            
            await self._auto_adjust_limits(event, participants_count, 'scan')
            
            # Полное сканирование
            scanner = TopicScannerFactory.create_scanner(self.client, 'user')
            topics_data = await scanner.scan_topics(chat)
            
            response = self._build_scan_response(chat, participants_count, topics_data, about)
            await send_long_message(event, response)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_scan (user mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_get_all(self, event) -> bool:
        """Получение всех данных в пользовательском режиме"""
        try:
            logger.info(f"👤 /get_all (user mode) от пользователя {event.sender_id}")
            
            is_valid, chat = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            processing_msg = await event.reply("🔄 **Получение полных данных (user режим)...**\n\nЭто может занять несколько секунд...")
            
            # Получение детальной информации
            try:
                full_chat = await self.client(GetFullChannelRequest(chat))
                participants_count = full_chat.full_chat.participants_count
            except:
                participants_count = 0
            
            await self._auto_adjust_limits(event, participants_count, 'get_all')
            
            # Получение всех данных
            scanner = TopicScannerFactory.create_scanner(self.client, 'user')
            topics_data = await scanner.scan_topics(chat)
            active_users = await db_manager.get_active_users(event.chat_id)
            activity_stats = await db_manager.get_activity_stats(event.chat_id)
            
            response = self._build_get_all_response(chat, participants_count, topics_data, active_users, activity_stats)
            
            await processing_msg.delete()
            await send_long_message(event, response)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_get_all (user mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_get_users(self, event) -> bool:
        """Получение активных пользователей с расширенной информацией"""
        try:
            logger.info(f"👤 /get_users (user mode) от пользователя {event.sender_id}")
            
            is_valid, _ = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            active_users = await db_manager.get_active_users(event.chat_id)
            activity_stats = await db_manager.get_activity_stats(event.chat_id)
            
            response = self._build_users_response(active_users, activity_stats)
            await send_long_message(event, response)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_get_users (user mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    async def handle_get_ids(self, event) -> bool:
        """Повторное сканирование ID с полной информацией"""
        try:
            logger.info(f"👤 /get_ids (user mode) от пользователя {event.sender_id}")
            
            is_valid, chat = await self._validate_group_chat(event)
            if not is_valid:
                return False
            
            scanner = TopicScannerFactory.create_scanner(self.client, 'user')
            topics_data = await scanner.scan_topics(chat)
            
            response = self._build_ids_response(chat, topics_data)
            await send_long_message(event, response)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_get_ids (user mode): {e}")
            await event.reply(MESSAGES['error_general'].format(error_message=str(e)))
            return False
    
    def _build_start_response(self, chat, participants_count, topics_data, activity_stats, start_time) -> str:
        """Построение ответа для /start в user режиме"""
        response = "👤 **TOPICS SCANNER BOT - ПОЛЬЗОВАТЕЛЬСКИЙ РЕЖИМ**\n\n"
        response += "✨ **Режим:** Полный доступ (MTProto API)\n\n"
        
        response += f"🏢 **Супергруппа:** {chat.title}\n"
        response += f"🆔 **ID группы:** `{chat.id}`\n"
        response += f"👥 **Участников:** {participants_count}\n"
        response += f"🕒 **Время сканирования:** {start_time.strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        
        # Полное сканирование топиков
        if topics_data:
            topics_data.sort(key=lambda x: x['id'])
            
            response += f"📊 **НАЙДЕНО ТОПИКОВ: {len(topics_data)}**\n"
            response += "✅ **Полная информация доступна в user режиме**\n\n"
            
            # Детальная таблица
            response += "| ID | Название топика | Создатель | Сообщений | Ссылка |\n"
            response += "|----|-----------------|-----------|-----------|--------|\n"
            
            for topic in topics_data:
                title = topic['title'][:15] + "..." if len(topic['title']) > 15 else topic['title']
                creator = topic['created_by'][:10] + "..." if len(topic['created_by']) > 10 else topic['created_by']
                messages = str(topic.get('messages', 0))[:8]
                link = topic['link'][:25] + "..." if len(topic['link']) > 25 else topic['link']
                
                response += f"| {topic['id']} | {title} | {creator} | {messages} | {link} |\n"
            
            response += "\n🔗 **ДЕТАЛЬНАЯ ИНФОРМАЦИЯ:**\n"
            for topic in topics_data[:10]:  # Показываем детали первых 10
                response += f"• **{topic['title']}** (ID: {topic['id']})\n"
                response += f"  └ Создатель: {topic['created_by']}\n"
                response += f"  └ Сообщений: {topic.get('messages', 'неизвестно')}\n"
                response += f"  └ Ссылка: {topic['link']}\n"
            
            if len(topics_data) > 10:
                response += f"... и еще {len(topics_data) - 10} топиков\n"
        else:
            response += "❌ **Топики не найдены**\n"
            response += "Возможно, группа не является форумом.\n\n"
        
        # Статистика активности
        response += f"👥 **АКТИВНОСТЬ ЗА СЕГОДНЯ ({activity_stats['date']}):**\n"
        response += f"• Активных пользователей: {activity_stats['total_users']}\n"
        response += f"• Всего сообщений: {activity_stats['total_messages']}\n"
        response += f"• Среднее на пользователя: {activity_stats['avg_messages']}\n\n"
        
        # Список команд
        response += "📋 **КОМАНДЫ (ПОЛЬЗОВАТЕЛЬСКИЙ РЕЖИМ):**\n"
        response += "• `/scan` - полное сканирование топиков\n"
        response += "• `/get_all` - все данные с полной информацией\n"
        response += "• `/get_users` - активные пользователи\n"
        response += "• `/my_status` - статус вашей сессии\n"
        response += "• `/queue_status` - статус очереди\n"
        response += "• `/logout` - выйти из user режима\n\n"
        
        response += "✨ **Преимущества пользовательского режима:**\n"
        response += "• Полная информация о всех топиках\n"
        response += "• Данные о создателях топиков\n"
        response += "• Количество сообщений в топиках\n"
        response += "• Нет ограничений Bot API"
        
        return response
    
    def _build_scan_response(self, chat, participants_count, topics_data, about) -> str:
        """Построение ответа для /scan в user режиме"""
        response = f"👤 **ПОЛНОЕ СКАНИРОВАНИЕ ТОПИКОВ**\n\n"
        response += f"🏢 **Группа:** {chat.title}\n"
        response += f"👥 **Участников:** {participants_count}\n"
        response += f"🕒 **Время:** {datetime.now().strftime('%H:%M:%S')}\n"
        
        if about:
            response += f"📝 **Описание:** {about[:100]}{'...' if len(about) > 100 else ''}\n"
        
        response += "\n"
        
        if topics_data:
            response += f"📊 **НАЙДЕНО: {len(topics_data)} топиков**\n"
            response += "✨ **Полная информация (user режим)**\n\n"
            
            # Детальная таблица
            response += "| ID | Название | Создатель | Сообщений |\n"
            response += "|----|----------|-----------|----------|\n"
            
            for topic in sorted(topics_data, key=lambda x: x['id']):
                title = topic['title'][:20] + "..." if len(topic['title']) > 20 else topic['title']
                creator = topic['created_by'][:15] + "..." if len(topic['created_by']) > 15 else topic['created_by']
                messages = str(topic.get('messages', 0))
                
                response += f"| {topic['id']} | {title} | {creator} | {messages} |\n"
            
            response += "\n🔗 **ПРЯМЫЕ ССЫЛКИ:**\n"
            for topic in topics_data:
                response += f"• [{topic['title']}]({topic['link']})\n"
            
            response += "\n✅ **Все топики найдены через MTProto API**"
        else:
            response += "❌ **Топики не найдены**\n"
            response += "Группа не является форумом или топики отсутствуют."
        
        return response
    
    def _build_get_all_response(self, chat, participants_count, topics_data, active_users, activity_stats) -> str:
        """Построение ответа для /get_all в user режиме"""
        response = f"👤 **ПОЛНЫЙ ОТЧЕТ (ПОЛЬЗОВАТЕЛЬСКИЙ РЕЖИМ)**\n\n"
        response += f"🏢 **Группа:** {chat.title}\n"
        response += f"🆔 **ID:** `{chat.id}`\n"
        response += f"👥 **Участников:** {participants_count}\n"
        response += f"🕒 **Время:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        
        # Секция топиков с полной информацией
        if topics_data:
            response += f"📋 **ТОПИКИ ({len(topics_data)}) - ПОЛНАЯ ИНФОРМАЦИЯ:**\n\n"
            
            for topic in sorted(topics_data, key=lambda x: x['id']):
                response += f"🔹 **{topic['title']}**\n"
                response += f"   • ID: {topic['id']}\n"
                response += f"   • Создатель: {topic['created_by']}\n"
                response += f"   • Сообщений: {topic.get('messages', 'неизвестно')}\n"
                response += f"   • Ссылка: {topic['link']}\n"
                
                # Дополнительная информация если доступна
                if topic.get('created_date'):
                    response += f"   • Создан: {topic['created_date']}\n"
                if topic.get('last_message_date'):
                    response += f"   • Последнее сообщение: {topic['last_message_date']}\n"
                
                response += "\n"
        
        # Секция активности с расширенной информацией
        response += f"👥 **ДЕТАЛЬНАЯ АКТИВНОСТЬ ЗА СЕГОДНЯ ({activity_stats['date']}):**\n"
        response += f"📊 **Общая статистика:**\n"
        response += f"• Всего активных: {activity_stats['total_users']}\n"
        response += f"• Всего сообщений: {activity_stats['total_messages']}\n"
        response += f"• Максимум от одного: {activity_stats['max_messages']}\n"
        response += f"• Среднее на пользователя: {activity_stats['avg_messages']}\n\n"
        
        if active_users:
            # Топ-10 с детальной информацией
            top_users = sorted(active_users, key=lambda x: x['message_count'], reverse=True)[:10]
            response += "🏆 **ТОП-10 САМЫХ АКТИВНЫХ (ДЕТАЛЬНО):**\n"
            
            for i, user in enumerate(top_users, 1):
                username = f"@{user['username']}" if user['username'] else (user['first_name'] or 'Без имени')
                response += f"{i}. **{username}**\n"
                response += f"   • User ID: `{user['user_id']}`\n"
                response += f"   • Сообщений: {user['message_count']}\n"
                response += f"   • Последняя активность: {user['last_activity']}\n\n"
            
            # Полный список если не очень большой
            if len(active_users) <= 20:
                response += "📋 **ПОЛНЫЙ СПИСОК АКТИВНЫХ:**\n"
                for user in active_users:
                    username = f"@{user['username']}" if user['username'] else (user['first_name'] or 'Без имени')
                    response += f"• {username} (`{user['user_id']}`) - {user['message_count']} сообщений\n"
            else:
                response += f"📋 **Всего активных пользователей: {len(active_users)}** (показаны топ-10)\n"
        else:
            response += "😴 **Пока никто не проявил активность сегодня**\n"
        
        response += f"\n✨ **Отчет создан в пользовательском режиме с полным доступом к MTProto API**"
        
        return response
    
    def _build_users_response(self, active_users, activity_stats) -> str:
        """Построение ответа для /get_users в user режиме"""
        response = f"👤 **АКТИВНЫЕ ПОЛЬЗОВАТЕЛИ (ПОЛЬЗОВАТЕЛЬСКИЙ РЕЖИМ)**\n\n"
        response += f"📅 **За сегодня ({activity_stats['date']}):**\n"
        response += f"📊 **Расширенная статистика:**\n"
        response += f"• Всего активных: {activity_stats['total_users']}\n"
        response += f"• Всего сообщений: {activity_stats['total_messages']}\n"
        response += f"• Максимум от одного: {activity_stats['max_messages']}\n"
        response += f"• Среднее на пользователя: {activity_stats['avg_messages']}\n\n"
        
        if active_users:
            response += "📋 **ДЕТАЛЬНЫЙ СПИСОК:**\n"
            response += "| Username | User ID | Сообщений | Последняя активность |\n"
            response += "|----------|---------|-----------|---------------------|\n"
            
            for user in active_users:
                username = f"@{user['username']}" if user['username'] else (user['first_name'] or 'Без имени')
                username = username[:15] + "..." if len(username) > 15 else username
                last_activity = user['last_activity'][:16] if user['last_activity'] else 'неизвестно'
                
                response += f"| {username} | `{user['user_id']}` | {user['message_count']} | {last_activity} |\n"
            
            # Анализ активности по времени
            response += "\n📈 **АНАЛИЗ АКТИВНОСТИ:**\n"
            
            # Топ-5 активных
            top_users = sorted(active_users, key=lambda x: x['message_count'], reverse=True)[:5]
            response += "🥇 **Самые активные:**\n"
            for i, user in enumerate(top_users, 1):
                username = f"@{user['username']}" if user['username'] else (user['first_name'] or 'Без имени')
                response += f"{i}. {username} - {user['message_count']} сообщений\n"
            
            # Статистика по количеству сообщений
            message_counts = [user['message_count'] for user in active_users]
            response += f"\n📊 **Распределение сообщений:**\n"
            response += f"• 1 сообщение: {len([c for c in message_counts if c == 1])} пользователей\n"
            response += f"• 2-5 сообщений: {len([c for c in message_counts if 2 <= c <= 5])} пользователей\n"
            response += f"• 6-10 сообщений: {len([c for c in message_counts if 6 <= c <= 10])} пользователей\n"
            response += f"• Более 10 сообщений: {len([c for c in message_counts if c > 10])} пользователей\n"
            
        else:
            response += "😴 **Пока никто не проявил активность сегодня**\n"
            response += "Как только кто-то напишет сообщение, я это зафиксирую!\n"
        
        response += "\nℹ️ **Информация (пользовательский режим):**\n"
        response += "• Данные обновляются в реальном времени\n"
        response += "• Расширенная статистика и анализ\n"
        response += "• Точное время последней активности\n"
        response += "• Автоматический сброс данных в 00:00\n"
        response += "• Команды бота не считаются активностью"
        
        return response
    
    def _build_ids_response(self, chat, topics_data) -> str:
        """Построение ответа для /get_ids в user режиме"""
        response = f"👤 **ПОВТОРНОЕ СКАНИРОВАНИЕ ID (ПОЛНАЯ ИНФОРМАЦИЯ)**\n\n"
        response += f"🏢 **Группа:** {chat.title}\n"
        response += f"🆔 **ID группы:** `{chat.id}`\n"
        response += f"🕒 **Время:** {datetime.now().strftime('%H:%M:%S')}\n"
        response += f"✨ **Режим:** Пользовательский (MTProto API)\n\n"
        
        if topics_data:
            response += f"📊 **НАЙДЕНО: {len(topics_data)} топиков**\n\n"
            
            # Детальная таблица с полной информацией
            response += "| ID | Название | Создатель | Сообщений | Ссылка |\n"
            response += "|----|----------|-----------|-----------|--------|\n"
            
            for topic in sorted(topics_data, key=lambda x: x['id']):
                title = topic['title'][:15] + "..." if len(topic['title']) > 15 else topic['title']
                creator = topic['created_by'][:10] + "..." if len(topic['created_by']) > 10 else topic['created_by']
                messages = str(topic.get('messages', 0))
                link = topic['link'][:30] + "..." if len(topic['link']) > 30 else topic['link']
                
                response += f"| {topic['id']} | {title} | {creator} | {messages} | {link} |\n"
            
            response += "\n🔗 **ГОТОВЫЕ ССЫЛКИ ДЛЯ КОПИРОВАНИЯ:**\n"
            for topic in topics_data:
                response += f"• **{topic['title']}**: {topic['link']}\n"
            
            response += "\n✅ **Все данные получены через полный MTProto API доступ**"
        else:
            response += "❌ **Топики не найдены**\n"
            response += "Группа не является форумом или топики отсутствуют."
        
        return response