#!/usr/bin/env python3
"""
Обработчики команд Get ID Bot by Mister DMS
ОБНОВЛЕНО v4.1.1: дружелюбные сообщения + кнопочное меню + все команды
"""

import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from telethon import TelegramClient, events, Button
from telethon.errors import SessionPasswordNeededError, ApiIdInvalidError
from telethon.tl.types import User, Chat, Channel

from config import (
    BOT_TOKEN, API_ID, API_HASH, MESSAGES, INLINE_KEYBOARDS,
    ADMIN_USER_ID, BUSINESS_CONTACT_ID, APP_VERSION, DEVELOPER
)
from utils import (
    MessageUtils, EncryptionUtils, ValidationUtils, 
    format_user_info, format_timespan, is_group_message
)
from scanner_utils import TopicScanner
from database import DatabaseManager

logger = logging.getLogger(__name__)

class BotHandlers:
    """Обработчики команд бота с улучшенной функциональностью"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.bot_client = None
        self.topic_scanner = None
        self.active_sessions = {}  # Активные пользовательские сессии
        
    async def initialize(self):
        """Инициализация обработчиков"""
        try:
            # Инициализация Telegram клиента
            self.bot_client = TelegramClient(
                'bot_session',
                API_ID,
                API_HASH
            )
            
            await self.bot_client.start(bot_token=BOT_TOKEN)
            
            # Инициализация сканера топиков
            self.topic_scanner = TopicScanner(self.db_manager)
            
            # Регистрация обработчиков
            self.register_handlers()
            
            logger.info("✅ Обработчики команд инициализированы")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации обработчиков: {e}")
            raise
    
    def register_handlers(self):
        """Регистрация всех обработчиков команд"""
        
        # === ОСНОВНЫЕ КОМАНДЫ ===
        @self.bot_client.on(events.NewMessage(pattern='/start'))
        async def cmd_start(event):
            await self.handle_start(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/help'))
        async def cmd_help(event):
            await self.handle_help(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/stats'))
        async def cmd_stats(event):
            await self.handle_stats(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/faq'))
        async def cmd_faq(event):
            await self.handle_faq(event)
        
        # === КОМАНДЫ СКАНИРОВАНИЯ ===
        @self.bot_client.on(events.NewMessage(pattern=r'/(?:scan|list)(?:@\w+)?'))
        async def cmd_scan(event):
            await self.handle_scan(event)
        
        @self.bot_client.on(events.NewMessage(pattern=r'/get_all(?:@\w+)?'))
        async def cmd_get_all(event):
            await self.handle_get_all(event)
        
        @self.bot_client.on(events.NewMessage(pattern=r'/get_users(?:@\w+)?'))
        async def cmd_get_users(event):
            await self.handle_get_users(event)
        
        @self.bot_client.on(events.NewMessage(pattern=r'/get_ids(?:@\w+)?'))
        async def cmd_get_ids(event):
            await self.handle_get_ids(event)
        
        # === КОМАНДЫ УПРАВЛЕНИЯ ===
        @self.bot_client.on(events.NewMessage(pattern='/switch_mode'))
        async def cmd_switch_mode(event):
            await self.handle_switch_mode(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/my_status'))
        async def cmd_my_status(event):
            await self.handle_my_status(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/logout'))
        async def cmd_logout(event):
            await self.handle_logout(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/renew_my_api_hash'))
        async def cmd_renew_credentials(event):
            await self.handle_renew_credentials(event)
        
        # === КОМАНДЫ СВЯЗИ v4.1 ===
        @self.bot_client.on(events.NewMessage(pattern='/yo_bro'))
        async def cmd_yo_bro(event):
            await self.handle_yo_bro(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/buy_bots'))
        async def cmd_buy_bots(event):
            await self.handle_buy_bots(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/donate'))
        async def cmd_donate(event):
            await self.handle_donate(event)
        
        # === КОМАНДЫ ОТЛАДКИ ===
        @self.bot_client.on(events.NewMessage(pattern='/debug'))
        async def cmd_debug(event):
            await self.handle_debug(event)
        
        @self.bot_client.on(events.NewMessage(pattern='/health'))
        async def cmd_health(event):
            await self.handle_health(event)
        
        # === CALLBACK ОБРАБОТЧИКИ ===
        @self.bot_client.on(events.CallbackQuery)
        async def callback_handler(event):
            await self.handle_callback(event)
        
        # === ОБРАБОТЧИК API CREDENTIALS ===
        @self.bot_client.on(events.NewMessage(func=lambda e: self.is_credentials_message(e)))
        async def handle_credentials(event):
            await self.process_credentials(event)
    
    # === ОСНОВНЫЕ ОБРАБОТЧИКИ ===
    
    async def handle_start(self, event):
        """Обработка команды /start с кнопочным меню"""
        try:
            # Проверяем тип чата
            if is_group_message(event):
                # В группе краткий ответ
                await MessageUtils.smart_reply(
                    event, 
                    "👋 Привет! Используй команды с упоминанием: `/scan@misterdms_topic_id_get_bot`"
                )
                return
            
            # В ЛС полное приветствие с меню
            user_id = event.sender_id
            user = await event.get_sender()
            username = getattr(user, 'username', 'Anonymous')
            
            # Сохраняем/обновляем пользователя в БД
            await self.db_manager.save_user(user_id, username, user.first_name)
            
            # Отправляем приветствие с кнопками
            buttons = self.create_inline_keyboard('main_menu')
            
            await MessageUtils.smart_reply(
                event,
                MESSAGES['welcome'],
                buttons=buttons
            )
            
            # Логируем статистику
            await self.log_command_usage(user_id, 'start')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в /start: {e}")
            await MessageUtils.smart_reply(event, MESSAGES['error'])
    
    async def handle_help(self, event):
        """Обработка команды /help"""
        try:
            if is_group_message(event):
                # В группе краткая справка
                await MessageUtils.smart_reply(
                    event,
                    "❓ **Основные команды:**\n"
                    "/scan@misterdms_topic_id_get_bot - сканировать топики\n"
                    "/get_all@misterdms_topic_id_get_bot - все данные\n"
                    "Полная справка в ЛС: /start"
                )
                return
            
            # В ЛС подробная справка с меню
            buttons = self.create_inline_keyboard('help_menu')
            
            await MessageUtils.smart_reply(
                event,
                MESSAGES['help'],
                buttons=buttons
            )
            
            await self.log_command_usage(event.sender_id, 'help')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в /help: {e}")
            await MessageUtils.smart_reply(event, MESSAGES['error'])
    
    async def handle_stats(self, event):
        """Обработка команды /stats"""
        try:
            user_id = event.sender_id
            user = await event.get_sender()
            
            # Получаем статистику из БД
            user_data = await self.db_manager.get_user(user_id)
            if not user_data:
                await MessageUtils.smart_reply(event, "❌ Пользователь не найден. Используй /start")
                return
            
            # Получаем дополнительную статистику
            stats = await self.db_manager.get_user_stats(user_id)
            
            stats_text = MESSAGES['stats_basic'].format(
                username=getattr(user, 'username', 'Anonymous'),
                mode=user_data.get('mode', 'bot'),
                join_date=format_timespan(user_data.get('created_at')),
                last_active=format_timespan(user_data.get('last_active')),
                total_commands=stats.get('total_commands', 0),
                favorite_command=stats.get('favorite_command', 'scan'),
                status=user_data.get('status', 'active')
            )
            
            await MessageUtils.smart_reply(event, stats_text)
            await self.log_command_usage(user_id, 'stats')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в /stats: {e}")
            await MessageUtils.smart_reply(event, MESSAGES['error'])
    
    async def handle_faq(self, event):
        """Обработка команды /faq"""
        faq_text = """🤔 **ЧАСТЫЕ ВОПРОСЫ**

**Q: Зачем нужны API_ID и API_HASH?**
A: Для режима пользователя, чтобы видеть все группы и получать больше данных.

**Q: Безопасно ли давать API ключи?**
A: Да! Они шифруются и хранятся безопасно. Можешь поменять в любой момент.

**Q: Почему бот не видит мою группу?**
A: Сделай меня админом с полными правами в супергруппе.

**Q: Работает ли с приватными группами?**
A: В режиме пользователя - да, в режиме бота - только публичные.

**Q: Как связаться с создателем?**
A: Используй /yo_bro для прямой связи!

Еще вопросы? Пиши /yo_bro! 😊"""
        
        await MessageUtils.smart_reply(event, faq_text)
        await self.log_command_usage(event.sender_id, 'faq')
    
    # === КОМАНДЫ СКАНИРОВАНИЯ ===
    
    async def handle_scan(self, event):
        """Обработка команды сканирования топиков"""
        try:
            # Проверяем права в группе
            if is_group_message(event):
                # В группе работаем только с упоминанием
                if '@misterdms_topic_id_get_bot' not in event.text:
                    return  # Игнорируем команды без упоминания
                
                chat = await event.get_chat()
                if not await self.check_admin_rights(chat.id):
                    await MessageUtils.smart_reply(event, MESSAGES['not_admin'])
                    return
            
            # Показываем прогресс
            progress_msg = await MessageUtils.smart_reply(event, MESSAGES['scanning_in_progress'])
            
            # Выполняем сканирование
            user_id = event.sender_id
            chat_id = event.chat_id if is_group_message(event) else None
            
            if not chat_id and is_group_message(event):
                chat_id = event.chat_id
            
            # Получаем пользовательские настройки
            user_data = await self.db_manager.get_user(user_id)
            mode = user_data.get('mode', 'bot') if user_data else 'bot'
            
            # Сканируем топики
            result = await self.topic_scanner.scan_topics(chat_id, user_id, mode)
            
            if result['success']:
                # Форматируем результат
                topics = result['data']['topics']
                
                if not topics:
                    response = "🤷‍♂️ **Топиков не найдено**\n\nВозможно группа не использует топики."
                else:
                    response = f"📋 **НАЙДЕНО ТОПИКОВ: {len(topics)}**\n\n"
                    
                    for topic in topics[:10]:  # Показываем первые 10
                        response += f"📌 **{topic['title']}**\n"
                        response += f"   ID: `{topic['id']}`\n"
                        response += f"   Сообщений: {topic.get('message_count', 0)}\n\n"
                    
                    if len(topics) > 10:
                        response += f"... и еще {len(topics) - 10} топиков\n\n"
                    
                    response += "Используй /get_all для полной информации!"
                
                # Обновляем сообщение
                await progress_msg.edit(response)
            else:
                await progress_msg.edit(f"❌ Ошибка сканирования: {result.get('error', 'Неизвестная ошибка')}")
            
            await self.log_command_usage(user_id, 'scan')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в /scan: {e}")
            await MessageUtils.smart_reply(event, MESSAGES['error'])
    
    # === КОМАНДЫ СВЯЗИ v4.1 ===
    
    async def handle_yo_bro(self, event):
        """Команда связи с создателем"""
        try:
            await MessageUtils.smart_reply(event, MESSAGES['yo_bro'])
            
            # Уведомляем админа
            await self.notify_admin(
                f"👋 **Новое обращение**\n\n"
                f"Пользователь: {event.sender_id}\n"
                f"Username: @{getattr(await event.get_sender(), 'username', 'None')}\n"
                f"Команда: /yo_bro\n"
                f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            await self.log_command_usage(event.sender_id, 'yo_bro')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в /yo_bro: {e}")
            await MessageUtils.smart_reply(event, MESSAGES['error'])
    
    async def handle_buy_bots(self, event):
        """Команда заказа разработки ботов"""
        try:
            await MessageUtils.smart_reply(event, MESSAGES['buy_bots'])
            
            # Уведомляем админа о потенциальном заказе
            await self.notify_admin(
                f"💼 **Потенциальный заказ!**\n\n"
                f"Пользователь: {event.sender_id}\n"
                f"Username: @{getattr(await event.get_sender(), 'username', 'None')}\n"
                f"Команда: /buy_bots\n"
                f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            await self.log_command_usage(event.sender_id, 'buy_bots')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в /buy_bots: {e}")
            await MessageUtils.smart_reply(event, MESSAGES['error'])
    
    async def handle_donate(self, event):
        """Команда поддержки проекта"""
        try:
            await MessageUtils.smart_reply(event, MESSAGES['donate'])
            
            # Уведомляем админа
            await self.notify_admin(
                f"💝 **Интерес к донатам**\n\n"
                f"Пользователь: {event.sender_id}\n"
                f"Username: @{getattr(await event.get_sender(), 'username', 'None')}\n"
                f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            
            await self.log_command_usage(event.sender_id, 'donate')
            
        except Exception as e:
            logger.error(f"❌ Ошибка в /donate: {e}")
            await MessageUtils.smart_reply(event, MESSAGES['error'])
    
    # === CALLBACK ОБРАБОТЧИКИ ===
    
    async def handle_callback(self, event):
        """Обработка нажатий на кнопки"""
        try:
            data = event.data.decode('utf-8')
            user_id = event.sender_id
            
            if data == 'mode_bot':
                await self.set_bot_mode(event, user_id)
            elif data == 'mode_user':
                await self.set_user_mode(event, user_id)
            elif data == 'help':
                await self.show_help_menu(event)
            elif data == 'stats':
                await self.show_stats(event, user_id)
            elif data == 'yo_bro':
                await event.answer()
                await self.handle_yo_bro(event)
            elif data == 'buy_bots':
                await event.answer()
                await self.handle_buy_bots(event)
            elif data == 'main_menu':
                await self.show_main_menu(event)
            else:
                await event.answer("🔧 Функция в разработке!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в callback: {e}")
            await event.answer("❌ Произошла ошибка")
    
    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===
    
    def create_inline_keyboard(self, keyboard_name: str):
        """Создание inline клавиатуры"""
        if keyboard_name not in INLINE_KEYBOARDS:
            return None
        
        buttons = []
        for row in INLINE_KEYBOARDS[keyboard_name]:
            button_row = []
            for text, data in row:
                button_row.append(Button.inline(text, data))
            buttons.append(button_row)
        
        return buttons
    
    async def set_bot_mode(self, event, user_id):
        """Установка режима бота"""
        await self.db_manager.update_user_mode(user_id, 'bot')
        
        buttons = self.create_inline_keyboard('bot_mode_menu')
        await event.edit(MESSAGES['bot_mode_selected'], buttons=buttons)
    
    async def set_user_mode(self, event, user_id):
        """Установка режима пользователя"""
        await self.db_manager.update_user_mode(user_id, 'user')
        await event.edit(MESSAGES['user_mode_setup'])
    
    async def show_main_menu(self, event):
        """Показ главного меню"""
        buttons = self.create_inline_keyboard('main_menu')
        await event.edit(MESSAGES['welcome'], buttons=buttons)
    
    async def notify_admin(self, message: str):
        """Уведомление администратора"""
        try:
            if ADMIN_USER_ID:
                await self.bot_client.send_message(ADMIN_USER_ID, message)
        except Exception as e:
            logger.debug(f"Не удалось отправить уведомление админу: {e}")
    
    async def log_command_usage(self, user_id: int, command: str):
        """Логирование использования команд"""
        try:
            await self.db_manager.log_command_usage(user_id, command)
        except Exception as e:
            logger.debug(f"Ошибка логирования команды: {e}")
    
    async def check_admin_rights(self, chat_id: int) -> bool:
        """Проверка административных прав бота"""
        try:
            # Получаем информацию о боте в чате
            me = await self.bot_client.get_me()
            member = await self.bot_client.get_permissions(chat_id, me.id)
            
            return member.is_admin
        except:
            return False
    
    def is_credentials_message(self, event) -> bool:
        """Проверка является ли сообщение credentials"""
        if is_group_message(event):
            return False
        
        text = event.text.strip()
        return ('API_ID:' in text and 'API_HASH:' in text) or \
               ('api_id' in text.lower() and 'api_hash' in text.lower())
    
    async def process_credentials(self, event):
        """Обработка пользовательских credentials"""
        try:
            text = event.text.strip()
            user_id = event.sender_id
            
            # Парсим credentials
            lines = text.split('\n')
            api_id = None
            api_hash = None
            group_link = None
            
            for line in lines:
                line = line.strip()
                if 'api_id' in line.lower():
                    api_id = line.split(':')[-1].strip()
                elif 'api_hash' in line.lower():
                    api_hash = line.split(':')[-1].strip()
                elif any(x in line.lower() for x in ['группа', 'group', 'http', 't.me']):
                    group_link = line
            
            if not api_id or not api_hash:
                await MessageUtils.smart_reply(
                    event,
                    "❌ Неправильный формат! Пример:\n\n"
                    "API_ID: 12345678\n"
                    "API_HASH: abcdef123456\n"
                    "ГРУППА: https://t.me/your_group"
                )
                return
            
            # Валидация API credentials
            if not ValidationUtils.validate_api_credentials(api_id, api_hash):
                await MessageUtils.smart_reply(event, "❌ Неверные API credentials!")
                return
            
            # Шифруем и сохраняем
            encrypted_id = EncryptionUtils.encrypt(api_id)
            encrypted_hash = EncryptionUtils.encrypt(api_hash)
            
            await self.db_manager.save_user_credentials(
                user_id, encrypted_id, encrypted_hash, group_link
            )
            
            await MessageUtils.smart_reply(event, MESSAGES['credentials_saved'])
            await self.log_command_usage(user_id, 'credentials_saved')
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки credentials: {e}")
            await MessageUtils.smart_reply(event, MESSAGES['error'])
    
    async def shutdown(self):
        """Корректное завершение работы"""
        try:
            if self.bot_client:
                await self.bot_client.disconnect()
            logger.info("✅ Обработчики команд корректно завершены")
        except Exception as e:
            logger.error(f"❌ Ошибка завершения обработчиков: {e}")

    # Заглушки для остальных методов (будут реализованы позже)
    async def handle_get_all(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def handle_get_users(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def handle_get_ids(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def handle_switch_mode(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def handle_my_status(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def handle_logout(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def handle_renew_credentials(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def handle_debug(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def handle_health(self, event): 
        await MessageUtils.smart_reply(event, "🔧 Команда в разработке!")
    
    async def show_help_menu(self, event): 
        await event.answer("🔧 В разработке!")
    
    async def show_stats(self, event, user_id): 
        await event.answer("🔧 В разработке!")