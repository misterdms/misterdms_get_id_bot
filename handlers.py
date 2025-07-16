#!/usr/bin/env python3
"""
Topics Scanner Bot v5.16 - Обработчики команд
Простые и надежные обработчики для всех команд
"""

import logging
from telethon import events
from telethon.tl.types import Channel

from config import MESSAGES, BUTTONS
from database import db
from scanner import scanner
from utils import (
    create_inline_buttons, send_long_message, format_topics_list,
    validate_api_credentials, is_private_chat, is_group_chat, 
    format_error_message
)

logger = logging.getLogger(__name__)

class BotHandlers:
    """Обработчики команд бота"""
    
    def __init__(self, bot_client):
        self.bot = bot_client
        self.setup_handlers()
    
    def setup_handlers(self):
        """Настройка всех обработчиков"""
        
        # === КОМАНДЫ ===
        @self.bot.on(events.NewMessage(pattern='/start'))
        async def handle_start(event):
            """Команда /start"""
            try:
                if is_private_chat(event):
                    # В ЛС показываем меню выбора режима
                    await self._show_main_menu(event)
                else:
                    # В группах показываем краткую информацию
                    await event.reply(
                        "🤖 **Topics Scanner Bot v5.16**\n\n"
                        "Команды:\n"
                        "• /find_ids - сканировать топики\n"
                        "• /start - настройки (в ЛС)"
                    )
            except Exception as e:
                logger.error(f"❌ Ошибка в /start: {e}")
                await event.reply(format_error_message(e))
        
        @self.bot.on(events.NewMessage(pattern='/find_ids'))
        async def handle_find_ids(event):
            """Команда /find_ids - сканирование топиков"""
            try:
                # Проверяем, что команда в группе
                if is_private_chat(event):
                    await event.reply(MESSAGES['find_ids_group_only'])
                    return
                
                # Получаем чат
                chat = await event.get_chat()
                if not isinstance(chat, Channel) or not chat.megagroup:
                    await event.reply("⚠️ **Команда работает только в супергруппах!**")
                    return
                
                # Отправляем уведомление о начале сканирования
                status_msg = await event.reply(MESSAGES['scanning_topics'])
                
                try:
                    # Сканируем топики
                    topics = await scanner.scan_topics(event.sender_id, self.bot, chat)
                    
                    if topics:
                        # Форматируем и отправляем результат
                        result_text = format_topics_list(topics)
                        await status_msg.delete()
                        await send_long_message(event, result_text)
                    else:
                        await status_msg.edit(MESSAGES['no_topics_found'])
                
                except Exception as scan_error:
                    await status_msg.edit(MESSAGES['error_occurred'].format(
                        error=format_error_message(scan_error)
                    ))
            
            except Exception as e:
                logger.error(f"❌ Ошибка в /find_ids: {e}")
                await event.reply(format_error_message(e))
        
        # === CALLBACK КНОПКИ ===
        @self.bot.on(events.CallbackQuery)
        async def handle_callback(event):
            """Обработка нажатий на кнопки"""
            try:
                data = event.data.decode('utf-8')
                user_id = event.sender_id
                
                logger.info(f"🔘 Callback {data} от пользователя {user_id}")
                
                if data == 'main_menu':
                    await self._show_main_menu(event, edit=True)
                
                elif data == 'bot_api':
                    await self._set_bot_api_mode(event, user_id)
                
                elif data == 'user_api':
                    await self._set_user_api_mode(event, user_id)
                
                elif data == 'change_mode':
                    await self._show_main_menu(event, edit=True)
                
                elif data == 'help':
                    await self._show_help(event)
                
                else:
                    await event.answer("❓ Неизвестная команда")
            
            except Exception as e:
                logger.error(f"❌ Ошибка в callback: {e}")
                await event.answer(f"❌ Ошибка: {str(e)}")
        
        # === ОБРАБОТКА CREDENTIALS ===
        @self.bot.on(events.NewMessage)
        async def handle_credentials(event):
            """Обработка ввода API credentials"""
            try:
                # Только в ЛС и только текстовые сообщения
                if not is_private_chat(event) or not event.text:
                    return
                
                # Пропускаем команды
                if event.text.startswith('/'):
                    return
                
                # Проверяем, ожидает ли пользователь ввод credentials
                user = await db.get_user(event.sender_id)
                if not user or user.get('api_mode') != 'waiting_credentials':
                    return
                
                # Валидируем credentials
                api_id, api_hash, message = validate_api_credentials(event.text)
                
                if api_id and api_hash:
                    # Сохраняем credentials
                    success = await db.save_user_credentials(event.sender_id, api_id, api_hash)
                    
                    if success:
                        await event.reply(MESSAGES['user_api_saved'])
                    else:
                        await event.reply("❌ **Ошибка сохранения**\n\nПопробуй еще раз")
                else:
                    await event.reply(f"❌ **Неверный формат**\n\n{message}")
            
            except Exception as e:
                logger.error(f"❌ Ошибка обработки credentials: {e}")
    
    async def _show_main_menu(self, event, edit=False):
        """Показать главное меню"""
        try:
            buttons = create_inline_buttons(BUTTONS['main_menu'])
            
            if edit and hasattr(event, 'edit'):
                await event.edit(MESSAGES['welcome'], buttons=buttons, parse_mode='markdown')
            else:
                await event.reply(MESSAGES['welcome'], buttons=buttons, parse_mode='markdown')
        
        except Exception as e:
            logger.error(f"❌ Ошибка показа главного меню: {e}")
    
    async def _set_bot_api_mode(self, event, user_id: int):
        """Установить режим Bot API"""
        try:
            # Получаем информацию о пользователе
            sender = event.sender
            username = getattr(sender, 'username', None)
            first_name = getattr(sender, 'first_name', None)
            
            # Сохраняем пользователя в режиме bot
            await db.save_user(user_id, username, first_name, 'bot')
            
            # Показываем кнопки настроек
            buttons = create_inline_buttons(BUTTONS['settings_menu'])
            
            await event.edit(
                MESSAGES['bot_api_selected'], 
                buttons=buttons, 
                parse_mode='markdown'
            )
        
        except Exception as e:
            logger.error(f"❌ Ошибка установки Bot API для {user_id}: {e}")
            await event.answer("❌ Ошибка")
    
    async def _set_user_api_mode(self, event, user_id: int):
        """Установить режим User API"""
        try:
            # Получаем информацию о пользователе
            sender = event.sender
            username = getattr(sender, 'username', None)
            first_name = getattr(sender, 'first_name', None)
            
            # Сохраняем пользователя в режиме ожидания credentials
            await db.save_user(user_id, username, first_name, 'waiting_credentials')
            
            # Показываем инструкции
            buttons = create_inline_buttons(BUTTONS['back_to_main'])
            
            await event.edit(
                MESSAGES['user_api_setup'], 
                buttons=buttons, 
                parse_mode='markdown'
            )
        
        except Exception as e:
            logger.error(f"❌ Ошибка установки User API для {user_id}: {e}")
            await event.answer("❌ Ошибка")
    
    async def _show_help(self, event):
        """Показать справку"""
        try:
            help_text = """📋 **Справка по Topics Scanner Bot v5.16**

🤖 **Bot API режим:**
• Быстрая настройка
• Ограниченные возможности
• Может показывать не все топики

👤 **User API режим:**
• Требует API credentials
• Полный доступ к топикам
• Получение на https://my.telegram.org

📝 **Команды:**
• /start - настройки (в ЛС)
• /find_ids - сканирование топиков (в группе)

⚠️ **Важно:**
• Команда /find_ids работает только в супергруппах-форумах
• Для User API нужны ваши собственные API данные
• Bot API может показывать не все топики из-за ограничений Telegram"""
            
            buttons = create_inline_buttons(BUTTONS['back_to_main'])
            
            await event.edit(help_text, buttons=buttons, parse_mode='markdown')
        
        except Exception as e:
            logger.error(f"❌ Ошибка показа справки: {e}")
