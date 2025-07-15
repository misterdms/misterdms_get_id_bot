#!/usr/bin/env python3
"""
Объединенный модуль сканирования топиков с утилитами
Включает: TopicScanner + все вспомогательные функции
"""

import re
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
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И УТИЛИТЫ
# =============================================================================

class MessageUtils:
    """Утилиты для работы с сообщениями"""
    
    @staticmethod
    async def send_long_message(event, text: str, max_length: int = 4000, parse_mode: str = 'markdown'):
        """Отправка длинного сообщения с автоматической разбивкой"""
        try:
            if len(text) <= max_length:
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
    
    async def get_topic_details(self, chat, topic_id: int) -> Optional[Dict[str, Any]]:
        """Получить детальную информацию о конкретном топике"""
        try:
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
            
            # Фильтруем сообщения по топику
            topic_messages = []
            for msg in messages.messages:
                if (hasattr(msg, 'reply_to') and msg.reply_to and
                    hasattr(msg.reply_to, 'reply_to_top_id') and
                    msg.reply_to.reply_to_top_id == topic_id):
                    topic_messages.append(msg)
            
            if topic_messages:
                first_message = topic_messages[-1]  # Самое старое
                last_message = topic_messages[0]    # Самое новое
                
                # Уникальные участники
                participants = set()
                for msg in topic_messages:
                    if hasattr(msg, 'from_id') and msg.from_id:
                        participants.add(msg.from_id.user_id if hasattr(msg.from_id, 'user_id') else msg.from_id)
                
                return {
                    'topic_id': topic_id,
                    'message_count': len(topic_messages),
                    'participant_count': len(participants),
                    'first_message_date': first_message.date if hasattr(first_message, 'date') else None,
                    'last_message_date': last_message.date if hasattr(last_message, 'date') else None,
                    'recent_messages': topic_messages[:5]  # Последние 5 сообщений
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка получения деталей топика {topic_id}: {e}")
            return None

# =============================================================================
# ФАБРИКА СКАНЕРОВ
# =============================================================================

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
# ЭКСПОРТ ФУНКЦИЙ ДЛЯ СОВМЕСТИМОСТИ
# =============================================================================

# Основные функции для совместимости с существующим кодом
async def send_long_message(event, text: str, max_length: int = 4000, parse_mode: str = 'markdown'):
    """Совместимость: отправка длинного сообщения"""
    return await MessageUtils.send_long_message(event, text, max_length, parse_mode)

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

# Константы
MAX_MESSAGE_LENGTH = 4000
TELEGRAM_USERNAME_PATTERN = r'^[a-zA-Z0-9_]{5,32}$'
TELEGRAM_CHAT_ID_PATTERN = r'^-?\d{10,}$'