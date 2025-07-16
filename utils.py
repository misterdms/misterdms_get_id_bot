#!/usr/bin/env python3
"""
Объединенный модуль утилит для гибридного Topics Scanner Bot
Включает: TopicScanner + все вспомогательные функции + дополнительные утилиты
ИСПРАВЛЕНО: Импорты, дублирование кода, критические ошибки
"""

import re
import os
import logging
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
from telethon import TelegramClient
from telethon.tl.types import Channel
from telethon.tl.functions.channels import GetForumTopicsRequest, GetFullChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.errors import ChatAdminRequiredError, ChannelPrivateError

logger = logging.getLogger(__name__)

# =============================================================================
# ОСНОВНЫЕ УТИЛИТЫ ДЛЯ РАБОТЫ С СООБЩЕНИЯМИ
# =============================================================================

class MessageUtils:
    """Утилиты для работы с сообщениями"""
    
    @staticmethod
    async def send_long_message(event, text: str, max_length: int = 4000, parse_mode: str = 'markdown', buttons=None):
        """Отправка длинного сообщения с автоматической разбивкой и поддержкой кнопок"""
        try:
            if len(text) <= max_length:
                if buttons:
                    await event.reply(text, parse_mode=parse_mode, buttons=buttons)
                else:
                    await event.reply(text, parse_mode=parse_mode)
                return
            
            # Разбиваем текст на части
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
                    # Первое сообщение с кнопками
                    if buttons:
                        await event.reply(part, parse_mode=parse_mode, buttons=buttons)
                    else:
                        await event.reply(part, parse_mode=parse_mode)
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
    
    @staticmethod
    def get_user_mention(user, fallback_name: str = "Пользователь") -> str:
        """Получить упоминание пользователя для группового чата"""
        try:
            if hasattr(user, 'username') and user.username:
                return f"@{user.username}"
            elif hasattr(user, 'first_name') and user.first_name:
                # Mention по ID если нет username
                user_id = getattr(user, 'id', 0)
                first_name = user.first_name[:20]  # Ограничиваем длину
                return f"[{first_name}](tg://user?id={user_id})"
            else:
                return fallback_name
        except Exception:
            return fallback_name
    
    @staticmethod
    async def smart_reply(event, text: str, parse_mode: str = 'markdown', 
                         force_mention: bool = False) -> None:
        """Умный ответ: Reply в ЛС, Mention в группах"""
        try:
            if event.is_private:
                # В личных сообщениях просто отвечаем
                await event.reply(text, parse_mode=parse_mode)
            else:
                # В группах - ОБЯЗАТЕЛЬНО с упоминанием или reply
                if force_mention:
                    # Принудительное упоминание + reply
                    mention = MessageUtils.get_user_mention(event.sender)
                    full_text = f"{mention}, {text}"
                    await event.reply(full_text, parse_mode=parse_mode)
                else:
                    # Обычный reply (предпочтительнее)
                    await event.reply(text, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"❌ Ошибка smart_reply: {e}")
            # Fallback на обычный ответ
            try:
                await event.reply(text, parse_mode=parse_mode)
            except Exception as fallback_error:
                logger.error(f"❌ Ошибка fallback reply: {fallback_error}")
                try:
                    await event.reply("❌ Ошибка отправки сообщения")
                except:
                    pass

class FormatUtils:
    """Утилиты для форматирования данных"""
    
    @staticmethod
    def format_topics_table(topics: List[Dict[str, Any]], include_links: bool = True) -> str:
        """Форматирование списка топиков в виде таблицы"""
        if not topics:
            return "❌ **Топики не найдены**"
        
        # Фильтруем только обычные топики
        regular_topics = [t for t in topics if t.get('id', 0) > 0]
        
        if not regular_topics:
            return "❌ **Обычные топики не найдены**"
        
        # Базовая таблица
        table = "| ID | Название топика | Создатель |\n"
        table += "|----|-----------------|----------|\n"
        
        for topic in regular_topics:
            topic_id = topic.get('id', 0)
            title = topic.get('title', 'Без названия')
            creator = topic.get('created_by', 'Неизвестно')
            
            # Обрезаем длинные названия
            if len(title) > 25:
                title = title[:22] + "..."
            
            if len(creator) > 15:
                creator = creator[:12] + "..."
            
            table += f"| {topic_id} | {title} | {creator} |\n"
        
        # Добавляем ссылки
        if include_links:
            table += "\n🔗 **ПРЯМЫЕ ССЫЛКИ:**\n"
            for topic in regular_topics:
                link = topic.get('link', f"#topic_{topic.get('id', 0)}")
                table += f"• [{topic.get('title', 'Топик')}]({link})\n"
        
        return table
    
    @staticmethod
    def format_duration(seconds: float) -> str:
        """Форматирование времени выполнения"""
        if seconds < 1:
            return f"{seconds*1000:.0f}мс"
        elif seconds < 60:
            return f"{seconds:.1f}с"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            return f"{minutes:.0f}м {remaining_seconds:.0f}с"
        else:
            hours = seconds // 3600
            remaining_minutes = (seconds % 3600) // 60
            return f"{hours:.0f}ч {remaining_minutes:.0f}м"
    
    @staticmethod
    def format_number(number: int) -> str:
        """Форматирование больших чисел"""
        if not isinstance(number, (int, float)):
            return str(number)
        
        return "{:,}".format(int(number)).replace(',', ' ')
    
    @staticmethod
    def create_progress_bar(current: int, total: int, width: int = 20) -> str:
        """Создание текстового прогресс-бара"""
        if total <= 0:
            return "▓" * width
        
        progress = min(current / total, 1.0)
        filled = int(progress * width)
        empty = width - filled
        
        bar = "▓" * filled + "░" * empty
        percentage = int(progress * 100)
        
        return f"[{bar}] {percentage}%"
    
    @staticmethod
    def format_timestamp(timestamp: datetime) -> str:
        """Форматирование timestamp в читаемый вид"""
        try:
            return timestamp.strftime('%d.%m.%Y %H:%M:%S')
        except Exception:
            return 'неизвестно'
    
    @staticmethod
    def format_date(date) -> str:
        """Форматирование даты"""
        try:
            if hasattr(date, 'strftime'):
                return date.strftime('%d.%m.%Y')
            return str(date)
        except Exception:
            return 'неизвестно'
    
    @staticmethod
    def format_time(time) -> str:
        """Форматирование времени"""
        try:
            if hasattr(time, 'strftime'):
                return time.strftime('%H:%M:%S')
            return str(time)
        except Exception:
            return 'неизвестно'

class TextUtils:
    """Утилиты для работы с текстом"""
    
    @staticmethod
    def sanitize_text(text: str, max_length: int = 100) -> str:
        """Очистка и обрезка текста"""
        if not text:
            return "Не указано"
        
        # Убираем опасные символы для markdown
        text = str(text)
        text = re.sub(r'[*_`\[\]()~>#+\-=|{}.!]', '', text)
        
        # Убираем лишние пробелы
        text = ' '.join(text.split())
        
        # Обрезаем если нужно
        if len(text) > max_length:
            text = text[:max_length-3] + "..."
        
        return text
    
    @staticmethod
    def escape_markdown(text: str) -> str:
        """Экранирование символов markdown"""
        if not text:
            return ""
        
        special_chars = ['*', '_', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        
        escaped_text = str(text)
        for char in special_chars:
            escaped_text = escaped_text.replace(char, f'\\{char}')
        
        return escaped_text
    
    @staticmethod
    def truncate_text(text: str, max_length: int = 50, suffix: str = "...") -> str:
        """Обрезка текста с суффиксом"""
        if not text:
            return ""
        
        text = str(text)
        if len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)] + suffix
    
    @staticmethod
    def clean_html_tags(text: str) -> str:
        """Удаление HTML тегов"""
        if not text:
            return ""
        
        # Простое удаление HTML тегов
        clean = re.sub(r'<[^>]+>', '', str(text))
        
        # Декодирование HTML entities
        html_entities = {
            '&amp;': '&',
            '&lt;': '<',
            '&gt;': '>',
            '&quot;': '"',
            '&#39;': "'",
            '&nbsp;': ' '
        }
        
        for entity, replacement in html_entities.items():
            clean = clean.replace(entity, replacement)
        
        return clean.strip()
    
    @staticmethod
    def safe_str(value, default: str = "") -> str:
        """Безопасное преобразование в str"""
        try:
            return str(value) if value is not None else default
        except Exception:
            return default

class ValidationUtils:
    """Утилиты для валидации данных"""
    
    @staticmethod
    def validate_chat_id(chat_id: str) -> bool:
        """Валидация ID чата"""
        try:
            chat_id_int = int(chat_id)
            if abs(chat_id_int) < 1000000000:
                return False
            return True
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def is_valid_username(username: str) -> bool:
        """Проверка валидности username"""
        if not username:
            return False
        
        username = username.lstrip('@')
        pattern = r'^[a-zA-Z0-9_]{5,32}$'
        return bool(re.match(pattern, username))
    
    @staticmethod
    def is_forum_chat(chat) -> bool:
        """Проверка, является ли чат форумом"""
        try:
            return (isinstance(chat, Channel) and 
                    hasattr(chat, 'forum') and 
                    chat.forum and
                    hasattr(chat, 'megagroup') and 
                    chat.megagroup)
        except:
            return False
    
    @staticmethod
    def safe_int(value, default: int = 0) -> int:
        """Безопасное преобразование в int"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def safe_float(value, default: float = 0.0) -> float:
        """Безопасное преобразование в float"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

class LinkUtils:
    """Утилиты для работы со ссылками"""
    
    @staticmethod
    def get_topic_link(chat, topic_id: int) -> str:
        """Генерация ссылки на топик"""
        try:
            if not chat:
                return f"#topic_{topic_id}"
            
            if hasattr(chat, 'id'):
                chat_id = str(chat.id).replace('-100', '')
            else:
                return f"#topic_{topic_id}"
            
            if not chat_id.isdigit():
                return f"#topic_{topic_id}"
            
            link = f"https://t.me/c/{chat_id}/{topic_id}"
            return link
            
        except Exception as e:
            logger.debug(f"Ошибка генерации ссылки на топик {topic_id}: {e}")
            return f"#topic_{topic_id}"

class UserUtils:
    """Утилиты для работы с пользователями"""
    
    @staticmethod
    def get_username_display(user_data: Dict[str, Any]) -> str:
        """Получение отображаемого имени пользователя"""
        username = user_data.get('username')
        first_name = user_data.get('first_name')
        
        if username:
            return f"@{username}"
        elif first_name:
            return first_name
        else:
            return f"User #{user_data.get('user_id', 'Unknown')}"

class DataUtils:
    """Утилиты для работы с данными"""
    
    @staticmethod
    def dict_get_nested(data: Dict[str, Any], path: str, default=None):
        """Получение значения из вложенного словаря по пути"""
        try:
            keys = path.split('.')
            current = data
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return default
            return current
        except Exception:
            return default
    
    @staticmethod
    def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
        """Объединение словарей"""
        result = {}
        for d in dicts:
            if isinstance(d, dict):
                result.update(d)
        return result

class FileUtils:
    """Утилиты для работы с файлами"""
    
    @staticmethod
    def ensure_directory_exists(path: str) -> bool:
        """Создание директории если не существует"""
        try:
            os.makedirs(path, exist_ok=True)
            return True
        except Exception as e:
            logger.debug(f"Ошибка создания директории {path}: {e}")
            return False
    
    @staticmethod
    def safe_file_write(filepath: str, content: str, encoding: str = 'utf-8') -> bool:
        """Безопасная запись в файл"""
        try:
            with open(filepath, 'w', encoding=encoding) as f:
                f.write(content)
            return True
        except Exception as e:
            logger.debug(f"Ошибка записи в файл {filepath}: {e}")
            return False
    
    @staticmethod
    def safe_file_read(filepath: str, encoding: str = 'utf-8') -> Optional[str]:
        """Безопасное чтение файла"""
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                return f.read()
        except Exception as e:
            logger.debug(f"Ошибка чтения файла {filepath}: {e}")
            return None

class LogUtils:
    """Утилиты для логирования"""
    
    @staticmethod
    def log_performance(operation: str, duration: float, details: Dict[str, Any] = None):
        """Логирование производительности операций"""
        try:
            formatted_duration = FormatUtils.format_duration(duration)
            details_str = f" | {details}" if details else ""
            logger.info(f"⚡ {operation}: {formatted_duration}{details_str}")
        except Exception as e:
            logger.debug(f"Ошибка логирования производительности: {e}")
    
    @staticmethod
    def log_user_action(user_id: int, action: str, details: Optional[str] = None):
        """Логирование действий пользователей"""
        try:
            details_str = f" | {details}" if details else ""
            logger.info(f"👤 User {user_id}: {action}{details_str}")
        except Exception as e:
            logger.debug(f"Ошибка логирования действий: {e}")
    
    @staticmethod
    def log_error(context: str, error: Exception, user_id: Optional[int] = None):
        """Логирование ошибок с контекстом"""
        try:
            user_str = f" | User {user_id}" if user_id else ""
            logger.error(f"❌ {context}: {str(error)}{user_str}")
        except Exception as e:
            logger.debug(f"Ошибка логирования ошибок: {e}")

# =============================================================================
# КОНСТАНТЫ И СТАТУСЫ
# =============================================================================

# Эмодзи для статусов
STATUS_EMOJIS = {
    'active': '✅',
    'inactive': '❌', 
    'pending': '⏳',
    'processing': '🔄',
    'completed': '✅',
    'failed': '❌',
    'expired': '⏰',
    'blocked': '🚫',
    'warning': '⚠️',
    'info': 'ℹ️',
    'success': '🎉',
    'error': '💥'
}

# Константы
MAX_MESSAGE_LENGTH = 4000
TELEGRAM_USERNAME_PATTERN = r'^[a-zA-Z0-9_]{5,32}$'
TELEGRAM_CHAT_ID_PATTERN = r'^-?\d{10,}$'

def get_status_emoji(status: str) -> str:
    """Получение эмодзи для статуса"""
    return STATUS_EMOJIS.get(status.lower(), '❓')

# =============================================================================
# КЛАССЫ СКАНЕРОВ ТОПИКОВ
# =============================================================================

class BaseTopicScanner:
    """Базовый класс для сканеров топиков"""
    
    def __init__(self, client: TelegramClient):
        self.client = client
    
    def create_topic_entry(self, topic_id: int, title: str, created_by: str = "Неизвестно", 
                          messages: Any = "неизвестно", chat=None, **kwargs) -> Dict[str, Any]:
        """Создать запись о топике"""
        entry = {
            'id': topic_id,
            'title': title,
            'created_by': created_by,
            'messages': messages,
            'link': LinkUtils.get_topic_link(chat, topic_id) if chat else f"#topic_{topic_id}"
        }
        entry.update(kwargs)
        return entry

class BotTopicScanner(BaseTopicScanner):
    """Сканер топиков для режима бота (ограниченный)"""
    
    async def scan_topics(self, chat) -> List[Dict[str, Any]]:
        """Сканирование топиков в режиме бота с ограничениями"""
        topics_data = []
        
        try:
            # Всегда добавляем General топик
            topics_data.append(self.create_topic_entry(
                topic_id=1,
                title="General",
                created_by="Telegram",
                messages="много",
                chat=chat
            ))
            
            # Проверяем, является ли чат форумом
            if ValidationUtils.is_forum_chat(chat):
                logger.info("📂 Сканирование форума в режиме бота...")
                
                # Метод 1: GetFullChannelRequest
                additional_topics = await self._try_full_channel_request(chat)
                topics_data.extend(additional_topics)
                
                # Метод 2: Сканирование сообщений
                if len(additional_topics) == 0:
                    message_topics = await self._scan_messages_for_topics(chat)
                    topics_data.extend(message_topics)
                
                # Метод 3: Эвристический поиск
                if len(topics_data) == 1:  # Только General
                    heuristic_topics = await self._heuristic_topic_search(chat)
                    topics_data.extend(heuristic_topics)
                
                # Информационное сообщение если ничего не нашли
                if len(topics_data) == 1:
                    topics_data.append(self.create_topic_entry(
                        topic_id=0,
                        title="⚠️ Ограничения Bot API",
                        created_by="Система",
                        messages="info",
                        chat=chat,
                        link="https://core.telegram.org/bots/api#limitations"
                    ))
                
                logger.info(f"✅ Найдено {len(topics_data)-1} дополнительных топиков (режим бота)")
            else:
                logger.info("ℹ️ Группа не является форумом")
                
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования в режиме бота: {e}")
            topics_data.append(self.create_topic_entry(
                topic_id=-1,
                title="❌ Ошибка сканирования",
                created_by="Система",
                messages="error",
                chat=chat,
                error=str(e)
            ))
        
        return topics_data
    
    async def _try_full_channel_request(self, chat) -> List[Dict[str, Any]]:
        """GetFullChannelRequest для проверки API"""
        topics = []
        try:
            full_channel = await self.client(GetFullChannelRequest(chat))
            logger.debug("✅ GetFullChannelRequest выполнен")
            # В Bot API это не содержит информацию о топиках
        except Exception as e:
            logger.debug(f"GetFullChannelRequest не сработал: {e}")
        
        return topics
    
    async def _scan_messages_for_topics(self, chat) -> List[Dict[str, Any]]:
        """Сканирование сообщений для поиска топиков"""
        topics = []
        found_topic_ids = set()
        
        try:
            logger.info("🔍 Сканирование сообщений для поиска топиков...")
            
            messages = await self.client(GetHistoryRequest(
                peer=chat,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=100,
                max_id=0,
                min_id=0,
                hash=0
            ))
            
            # Анализируем сообщения
            for message in messages.messages:
                topic_id = None
                
                if hasattr(message, 'reply_to') and message.reply_to:
                    if hasattr(message.reply_to, 'reply_to_top_id'):
                        topic_id = message.reply_to.reply_to_top_id
                    elif hasattr(message.reply_to, 'reply_to_msg_id'):
                        topic_id = message.reply_to.reply_to_msg_id
                
                if topic_id and topic_id != 1 and topic_id not in found_topic_ids:
                    found_topic_ids.add(topic_id)
                    topics.append(self.create_topic_entry(
                        topic_id=topic_id,
                        title=f"Topic {topic_id}",
                        created_by="Найдено в сообщениях",
                        messages="активный",
                        chat=chat
                    ))
            
            if found_topic_ids:
                logger.info(f"✅ Найдено {len(found_topic_ids)} топиков через сканирование сообщений")
            
        except Exception as e:
            logger.debug(f"Сканирование сообщений не сработало: {e}")
        
        return topics
    
    async def _heuristic_topic_search(self, chat) -> List[Dict[str, Any]]:
        """Эвристический поиск популярных топиков"""
        topics = []
        
        try:
            logger.info("🎯 Эвристический поиск топиков...")
            
            common_topic_ids = [2, 3, 4, 5, 10, 15, 20, 25, 30, 50, 100]
            
            for topic_id in common_topic_ids:
                try:
                    topic_messages = await self.client(GetHistoryRequest(
                        peer=chat,
                        offset_id=0,
                        offset_date=None,
                        add_offset=0,
                        limit=1,
                        max_id=0,
                        min_id=0,
                        hash=0
                    ))
                    
                    if topic_messages.messages:
                        topics.append(self.create_topic_entry(
                            topic_id=topic_id,
                            title=f"Topic {topic_id}",
                            created_by="Эвристически",
                            messages="предполагаемый",
                            chat=chat
                        ))
                        
                        if len(topics) >= 5:
                            break
                            
                except Exception:
                    continue
                
                await asyncio.sleep(0.2)
            
            if topics:
                logger.info(f"✅ Найдено {len(topics)} топиков эвристически")
                
        except Exception as e:
            logger.debug(f"Эвристический поиск не сработал: {e}")
        
        return topics

class UserTopicScanner(BaseTopicScanner):
    """Сканер топиков для пользовательского режима (полный доступ)"""
    
    async def scan_topics(self, chat) -> List[Dict[str, Any]]:
        """Полное сканирование топиков в пользовательском режиме"""
        topics_data = []
        
        try:
            # Всегда добавляем General топик
            topics_data.append(self.create_topic_entry(
                topic_id=1,
                title="General",
                created_by="Telegram",
                messages="много",
                chat=chat
            ))
            
            # Если это форум, получаем ВСЕ остальные топики
            if ValidationUtils.is_forum_chat(chat):
                logger.info("📂 Полное сканирование форума (пользовательский режим)...")
                
                forum_topics = await self._scan_forum_topics(chat)
                topics_data.extend(forum_topics)
                
                logger.info(f"✅ Найдено {len(forum_topics)} топиков форума")
            else:
                logger.info("ℹ️ Группа не является форумом")
                
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования в пользовательском режиме: {e}")
            topics_data.append(self.create_topic_entry(
                topic_id=-1,
                title="❌ Ошибка сканирования",
                created_by="Система",
                messages="error",
                chat=chat,
                error=str(e)
            ))
        
        return topics_data
    
    async def _scan_forum_topics(self, chat) -> List[Dict[str, Any]]:
        """Полное сканирование топиков форума"""
        topics = []
        
        try:
            offset_date = None
            offset_id = 0
            offset_topic = 0
            limit = 100
            total_scanned = 0
            max_topics = 1000  # Ограничение для безопасности
            
            while total_scanned < max_topics:
                try:
                    result = await self.client(GetForumTopicsRequest(
                        channel=chat,
                        offset_date=offset_date,
                        offset_id=offset_id,
                        offset_topic=offset_topic,
                        limit=limit
                    ))
                    
                    if not result.topics:
                        break
                    
                    # Обрабатываем каждый топик
                    for topic in result.topics:
                        if hasattr(topic, 'id') and hasattr(topic, 'title'):
                            topic_data = await self._process_forum_topic(topic, chat)
                            if topic_data:
                                topics.append(topic_data)
                    
                    total_scanned += len(result.topics)
                    
                    # Обновляем offset для следующей итерации
                    if len(result.topics) < limit:
                        break
                    
                    last_topic = result.topics[-1]
                    offset_topic = last_topic.id
                    offset_date = getattr(last_topic, 'date', None)
                    
                    # Безопасная задержка между запросами
                    await asyncio.sleep(0.5)
                    
                except ChatAdminRequiredError:
                    logger.warning("⚠️ Требуются права администратора для полного сканирования")
                    break
                except ChannelPrivateError:
                    logger.warning("⚠️ Нет доступа к приватному каналу")
                    break
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка при получении топиков: {e}")
                    break
            
            logger.info(f"📊 Просканировано {total_scanned} топиков, обработано {len(topics)}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка полного сканирования: {e}")
        
        return topics
    
    async def _process_forum_topic(self, topic, chat) -> Optional[Dict[str, Any]]:
        """Обработка отдельного топика форума"""
        try:
            # Базовая информация
            topic_id = topic.id
            title = topic.title
            
            # Информация о создателе
            creator = "Неизвестно"
            if hasattr(topic, 'from_id') and topic.from_id:
                try:
                    creator_entity = await self.client.get_entity(topic.from_id)
                    if hasattr(creator_entity, 'username') and creator_entity.username:
                        creator = f"@{creator_entity.username}"
                    elif hasattr(creator_entity, 'first_name'):
                        creator = creator_entity.first_name or "Неизвестно"
                except Exception as e:
                    logger.debug(f"Не удалось получить создателя топика {topic_id}: {e}")
            
            # Количество сообщений
            messages = 0
            if hasattr(topic, 'replies') and topic.replies:
                messages = getattr(topic.replies, 'replies', 0)
            
            # Дополнительная информация
            extra_info = {}
            
            if hasattr(topic, 'date'):
                extra_info['created_date'] = topic.date.strftime('%d.%m.%Y %H:%M')
            
            if hasattr(topic, 'closed'):
                extra_info['is_closed'] = topic.closed
            
            if hasattr(topic, 'pinned'):
                extra_info['is_pinned'] = topic.pinned
            
            if hasattr(topic, 'hidden'):
                extra_info['is_hidden'] = topic.hidden
            
            if hasattr(topic, 'icon_color'):
                extra_info['icon_color'] = topic.icon_color
            
            if hasattr(topic, 'icon_emoji_id'):
                extra_info['icon_emoji_id'] = topic.icon_emoji_id
            
            return self.create_topic_entry(
                topic_id=topic_id,
                title=title,
                created_by=creator,
                messages=messages,
                chat=chat,
                **extra_info
            )
            
        except Exception as e:
            logger.debug(f"Ошибка обработки топика {getattr(topic, 'id', 'unknown')}: {e}")
            return None

class TopicScannerFactory:
    """Фабрика для создания сканеров топиков"""
    
    @staticmethod
    def create_scanner(client: TelegramClient, mode: str = 'bot') -> BaseTopicScanner:
        """Создать сканер в зависимости от режима"""
        if mode == 'user':
            return UserTopicScanner(client)
        else:
            return BotTopicScanner(client)
    
    @staticmethod
    async def scan_with_fallback(bot_client: TelegramClient, user_client: Optional[TelegramClient], 
                                chat) -> List[Dict[str, Any]]:
        """Сканирование с fallback: сначала user режим, потом bot"""
        try:
            # Пытаемся сканировать в user режиме если доступно
            if user_client:
                user_scanner = UserTopicScanner(user_client)
                topics = await user_scanner.scan_topics(chat)
                
                # Проверяем качество результата
                regular_topics = [t for t in topics if t['id'] > 0]
                if len(regular_topics) > 1:  # Более чем просто General
                    logger.info(f"✅ Успешно использован user режим: {len(regular_topics)} топиков")
                    return topics
            
            # Fallback на bot режим
            logger.info("🔄 Переход на bot режим сканирования")
            bot_scanner = BotTopicScanner(bot_client)
            return await bot_scanner.scan_topics(chat)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в scan_with_fallback: {e}")
            # В крайнем случае возвращаем минимум
            return [
                {
                    'id': 1,
                    'title': 'General',
                    'created_by': 'Telegram',
                    'messages': 'неизвестно',
                    'link': LinkUtils.get_topic_link(chat, 1) if chat else '#general'
                }
            ]

# =============================================================================
# ФУНКЦИИ СОВМЕСТИМОСТИ
# =============================================================================

# Основные функции для совместимости с существующим кодом
async def send_long_message(event, text: str, max_length: int = 4000, parse_mode: str = 'markdown', buttons=None):
    """Совместимость: отправка длинного сообщения"""
    return await MessageUtils.send_long_message(event, text, max_length, parse_mode, buttons)

def format_topics_table(topics: List[Dict[str, Any]], include_links: bool = True) -> str:
    """Совместимость: форматирование таблицы топиков"""
    return FormatUtils.format_topics_table(topics, include_links)

def get_topic_link(chat, topic_id: int) -> str:
    """Совместимость: генерация ссылки на топик"""
    return LinkUtils.get_topic_link(chat, topic_id)

def get_username_display(user_data: Dict[str, Any]) -> str:
    """Совместимость: отображаемое имя пользователя"""
    return UserUtils.get_username_display(user_data)

def validate_chat_id(chat_id: str) -> bool:
    """Совместимость: валидация ID чата"""
    return ValidationUtils.validate_chat_id(chat_id)

def sanitize_text(text: str, max_length: int = 100) -> str:
    """Совместимость: очистка текста"""
    return TextUtils.sanitize_text(text, max_length)

def format_duration(seconds: float) -> str:
    """Совместимость: форматирование времени"""
    return FormatUtils.format_duration(seconds)

def is_forum_chat(chat) -> bool:
    """Совместимость: проверка форума"""
    return ValidationUtils.is_forum_chat(chat)

def escape_markdown(text: str) -> str:
    """Совместимость: экранирование markdown"""
    return TextUtils.escape_markdown(text)

def truncate_text(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """Совместимость: обрезка текста"""
    return TextUtils.truncate_text(text, max_length, suffix)

# Дополнительные функции
def create_progress_bar(current: int, total: int, width: int = 20) -> str:
    """Создание текстового прогресс-бара"""
    return FormatUtils.create_progress_bar(current, total, width)

def format_number(number: int) -> str:
    """Форматирование больших чисел"""
    return FormatUtils.format_number(number)

def clean_html_tags(text: str) -> str:
    """Удаление HTML тегов"""
    return TextUtils.clean_html_tags(text)

def is_valid_username(username: str) -> bool:
    """Проверка валидности username"""
    return ValidationUtils.is_valid_username(username)

def log_performance(operation: str, duration: float, details: Dict[str, Any] = None):
    """Логирование производительности операций"""
    return LogUtils.log_performance(operation, duration, details)

def log_user_action(user_id: int, action: str, details: Optional[str] = None):
    """Логирование действий пользователей"""
    return LogUtils.log_user_action(user_id, action, details)

def log_error(context: str, error: Exception, user_id: Optional[int] = None):
    """Логирование ошибок с контекстом"""
    return LogUtils.log_error(context, error, user_id)

def format_timestamp(timestamp: datetime) -> str:
    """Форматирование timestamp в читаемый вид"""
    return FormatUtils.format_timestamp(timestamp)

def format_date(date) -> str:
    """Форматирование даты"""
    return FormatUtils.format_date(date)

def format_time(time) -> str:
    """Форматирование времени"""
    return FormatUtils.format_time(time)

def safe_int(value, default: int = 0) -> int:
    """Безопасное преобразование в int"""
    return ValidationUtils.safe_int(value, default)

def safe_float(value, default: float = 0.0) -> float:
    """Безопасное преобразование в float"""
    return ValidationUtils.safe_float(value, default)

def safe_str(value, default: str = "") -> str:
    """Безопасное преобразование в str"""
    return TextUtils.safe_str(value, default)

def dict_get_nested(data: Dict[str, Any], path: str, default=None):
    """Получение значения из вложенного словаря по пути"""
    return DataUtils.dict_get_nested(data, path, default)

def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """Объединение словарей"""
    return DataUtils.merge_dicts(*dicts)

def ensure_directory_exists(path: str) -> bool:
    """Создание директории если не существует"""
    return FileUtils.ensure_directory_exists(path)

def safe_file_write(filepath: str, content: str, encoding: str = 'utf-8') -> bool:
    """Безопасная запись в файл"""
    return FileUtils.safe_file_write(filepath, content, encoding)

def safe_file_read(filepath: str, encoding: str = 'utf-8') -> Optional[str]:
    """Безопасное чтение файла"""
    return FileUtils.safe_file_read(filepath, encoding)

# =============================================================================
# ЭКСПОРТ ВСЕХ ФУНКЦИЙ И КЛАССОВ
# =============================================================================

__all__ = [
    # Основные классы утилит
    'MessageUtils',
    'FormatUtils',
    'TextUtils',
    'ValidationUtils',
    'LinkUtils',
    'UserUtils',
    'DataUtils',
    'FileUtils',
    'LogUtils',
    
    # Основные классы сканеров
    'BaseTopicScanner',
    'BotTopicScanner',
    'UserTopicScanner',
    'TopicScannerFactory',
    
    # Основные функции совместимости
    'send_long_message',
    'format_topics_table',
    'get_topic_link',
    'get_username_display',
    'validate_chat_id',
    'sanitize_text',
    'format_duration',
    'is_forum_chat',
    'escape_markdown',
    'truncate_text',
    'get_status_emoji',
    
    # Дополнительные функции
    'create_progress_bar',
    'format_number',
    'clean_html_tags',
    'is_valid_username',
    'log_performance',
    'log_user_action',
    'log_error',
    'format_timestamp',
    'format_date',
    'format_time',
    'safe_int',
    'safe_float',
    'safe_str',
    'dict_get_nested',
    'merge_dicts',
    'ensure_directory_exists',
    'safe_file_write',
    'safe_file_read',
    
    # Константы
    'STATUS_EMOJIS',
    'MAX_MESSAGE_LENGTH',
    'TELEGRAM_USERNAME_PATTERN',
    'TELEGRAM_CHAT_ID_PATTERN'
]