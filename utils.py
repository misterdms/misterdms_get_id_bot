#!/usr/bin/env python3
"""
Topics Scanner Bot v5.16 - Утилиты
Простые утилиты для работы с кнопками и сообщениями
"""

import re
import logging
from typing import List, Dict, Any
from telethon.tl.custom import Button

logger = logging.getLogger(__name__)

def create_inline_buttons(buttons_config: List[List[tuple]]) -> List[List[Button]]:
    """Создание inline кнопок из конфигурации"""
    try:
        buttons = []
        for row in buttons_config:
            button_row = []
            for text, callback_data in row:
                button_row.append(Button.inline(text, callback_data))
            buttons.append(button_row)
        return buttons
    except Exception as e:
        logger.error(f"❌ Ошибка создания кнопок: {e}")
        return []

async def send_long_message(event, text: str, buttons=None, parse_mode='markdown', max_length=4000):
    """Отправка длинного сообщения с разбивкой"""
    try:
        if len(text) <= max_length:
            await event.reply(text, buttons=buttons, parse_mode=parse_mode)
            return
        
        # Разбиваем на части
        parts = []
        current_part = ""
        lines = text.split('\n')
        
        for line in lines:
            if len(current_part) + len(line) + 1 > max_length:
                if current_part:
                    parts.append(current_part.strip())
                    current_part = line + '\n'
                else:
                    # Принудительно разбиваем длинную строку
                    while len(line) > max_length:
                        parts.append(line[:max_length])
                        line = line[max_length:]
                    current_part = line + '\n'
            else:
                current_part += line + '\n'
        
        if current_part.strip():
            parts.append(current_part.strip())
        
        # Отправляем части
        for i, part in enumerate(parts):
            if i == 0:
                await event.reply(part, buttons=buttons, parse_mode=parse_mode)
            else:
                header = f"📄 **Продолжение ({i+1}/{len(parts)}):**\n\n"
                await event.respond(header + part, parse_mode=parse_mode)
        
        logger.debug(f"Длинное сообщение разбито на {len(parts)} частей")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки длинного сообщения: {e}")
        try:
            await event.reply(text[:max_length] + "...\n\n❌ Ошибка форматирования")
        except:
            await event.reply("❌ Ошибка отправки сообщения")

def format_topics_list(topics: List[Dict[str, Any]]) -> str:
    """Форматирование списка топиков"""
    if not topics:
        return "❌ **Топики не найдены**"
    
    response = f"📋 **Найдено топиков: {len(topics)}**\n\n"
    
    # Таблица топиков
    response += "| ID | Название | Ссылка |\n"
    response += "|----|---------|---------|\n"
    
    for topic in topics:
        topic_id = topic.get('id', 0)
        title = topic.get('title', 'Без названия')
        link = topic.get('link', '#')
        
        # Обрезаем длинные названия
        if len(title) > 30:
            title = title[:27] + "..."
        
        response += f"| {topic_id} | {title} | [ссылка]({link}) |\n"
    
    response += "\n🔗 **Прямые ссылки:**\n"
    for topic in topics:
        title = topic.get('title', 'Топик')
        link = topic.get('link', '#')
        response += f"• [{title}]({link})\n"
    
    return response

def validate_api_credentials(text: str) -> tuple:
    """Валидация API credentials из текста"""
    try:
        lines = text.strip().split('\n')
        if len(lines) != 2:
            return None, None, "Отправь API_ID и API_HASH в двух строках"
        
        api_id = lines[0].strip()
        api_hash = lines[1].strip()
        
        # Проверка API_ID (должен быть числом 7-8 цифр)
        if not api_id.isdigit() or len(api_id) < 7 or len(api_id) > 8:
            return None, None, "API_ID должен содержать 7-8 цифр"
        
        # Проверка API_HASH (должен быть 32 символа hex)
        if len(api_hash) != 32 or not re.match(r'^[a-f0-9]{32}$', api_hash.lower()):
            return None, None, "API_HASH должен содержать 32 символа (hex)"
        
        return api_id, api_hash, "OK"
        
    except Exception as e:
        logger.error(f"❌ Ошибка валидации credentials: {e}")
        return None, None, f"Ошибка: {str(e)}"

def escape_markdown(text: str) -> str:
    """Экранирование символов markdown"""
    if not text:
        return ""
    
    special_chars = ['*', '_', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    
    escaped_text = str(text)
    for char in special_chars:
        escaped_text = escaped_text.replace(char, f'\\{char}')
    
    return escaped_text

def get_user_mention(user) -> str:
    """Получить упоминание пользователя"""
    try:
        if hasattr(user, 'username') and user.username:
            return f"@{user.username}"
        elif hasattr(user, 'first_name') and user.first_name:
            user_id = getattr(user, 'id', 0)
            first_name = user.first_name[:20]
            return f"[{first_name}](tg://user?id={user_id})"
        else:
            return "Пользователь"
    except Exception:
        return "Пользователь"

def is_private_chat(event) -> bool:
    """Проверка, является ли чат приватным"""
    try:
        return event.is_private
    except:
        return False

def is_group_chat(event) -> bool:
    """Проверка, является ли чат группой"""
    try:
        return event.is_group or event.is_channel
    except:
        return False

async def safe_delete_message(message, delay: int = 0):
    """Безопасное удаление сообщения"""
    try:
        if delay > 0:
            import asyncio
            await asyncio.sleep(delay)
        await message.delete()
    except Exception as e:
        logger.debug(f"Не удалось удалить сообщение: {e}")

def truncate_text(text: str, max_length: int = 50) -> str:
    """Обрезка текста с многоточием"""
    if not text:
        return ""
    
    text = str(text)
    if len(text) <= max_length:
        return text
    
    return text[:max_length-3] + "..."

def clean_username(username: str) -> str:
    """Очистка username"""
    if not username:
        return ""
    
    username = str(username).strip()
    if username.startswith('@'):
        username = username[1:]
    
    return username

def format_error_message(error: str) -> str:
    """Форматирование сообщения об ошибке"""
    try:
        # Убираем техническую информацию
        error = str(error)
        if "Telethon" in error:
            error = "Ошибка Telegram API"
        elif "sqlite" in error.lower():
            error = "Ошибка базы данных"
        elif "connection" in error.lower():
            error = "Ошибка соединения"
        
        return f"❌ {error}"
    except:
        return "❌ Неизвестная ошибка"
