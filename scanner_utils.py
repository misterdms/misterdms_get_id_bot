#!/usr/bin/env python3
"""
Утилиты сканирования топиков для Get ID Bot by Mister DMS
Wrapper для topic_scanner.py согласно архитектуре
"""

import logging
from typing import List, Dict, Any, Optional
from telethon import TelegramClient

# Импорт основных классов из topic_scanner
from topic_scanner import (
    BaseTopicScanner,
    BotTopicScanner,
    UserTopicScanner,
    TopicScannerFactory,
    get_topic_link,
    is_forum_chat
)

# Импорт утилит форматирования
from utils import (
    format_topics_table,
    send_long_message,
    MessageUtils,
    FormatUtils
)

logger = logging.getLogger(__name__)

class ScannerUtils:
    """Утилиты для сканирования топиков"""
    
    @staticmethod
    async def scan_chat_topics(client: TelegramClient, chat, mode: str = 'bot') -> List[Dict[str, Any]]:
        """
        Сканирование топиков чата
        
        Args:
            client: Telegram клиент
            chat: Объект чата
            mode: Режим сканирования ('bot' или 'user')
            
        Returns:
            Список топиков
        """
        try:
            scanner = TopicScannerFactory.create_scanner(client, mode)
            topics = await scanner.scan_topics(chat)
            
            logger.info(f"✅ Отсканировано {len(topics)} топиков в режиме {mode}")
            return topics
            
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования топиков: {e}")
            return []
    
    @staticmethod
    async def scan_with_fallback(bot_client: TelegramClient, user_client: Optional[TelegramClient], 
                                chat) -> List[Dict[str, Any]]:
        """
        Сканирование с автоматическим fallback
        
        Args:
            bot_client: Bot клиент
            user_client: User клиент (может быть None)
            chat: Объект чата
            
        Returns:
            Список топиков
        """
        return await TopicScannerFactory.scan_with_fallback(bot_client, user_client, chat)
    
    @staticmethod
    def format_scan_results(topics: List[Dict[str, Any]], chat, mode: str = 'bot', 
                           show_details: bool = False) -> str:
        """
        Форматирование результатов сканирования
        
        Args:
            topics: Список топиков
            chat: Объект чата
            mode: Режим сканирования
            show_details: Показать детальную информацию
            
        Returns:
            Отформатированная строка с результатами
        """
        try:
            if not topics:
                return "❌ **Топики не найдены**\n\nВозможно, группа не является форумом."
            
            # Разделяем обычные топики и системные
            regular_topics = [t for t in topics if t.get('id', 0) > 0]
            system_topics = [t for t in topics if t.get('id', 0) <= 0]
            
            # Заголовок
            mode_emoji = "🤖" if mode == 'bot' else "👤"
            mode_name = "РЕЖИМ БОТА" if mode == 'bot' else "ПОЛЬЗОВАТЕЛЬСКИЙ РЕЖИМ"
            
            response = f"{mode_emoji} **РЕЗУЛЬТАТЫ СКАНИРОВАНИЯ ({mode_name})**\n\n"
            response += f"🏢 **Группа:** {getattr(chat, 'title', 'Неизвестно')}\n"
            response += f"🆔 **ID группы:** `{getattr(chat, 'id', 'неизвестно')}`\n"
            response += f"📊 **Найдено топиков:** {len(regular_topics)}\n"
            response += f"🕒 **Время:** {FormatUtils.format_time(None)}\n\n"
            
            # Обычные топики
            if regular_topics:
                if show_details:
                    response += "📋 **ДЕТАЛЬНАЯ ИНФОРМАЦИЯ:**\n\n"
                    for topic in regular_topics:
                        response += f"• **{topic['title']}** (ID: {topic['id']})\n"
                        response += f"  └ Создатель: {topic['created_by']}\n"
                        response += f"  └ Сообщений: {topic.get('messages', 'неизвестно')}\n"
                        response += f"  └ Ссылка: {topic['link']}\n\n"
                else:
                    response += format_topics_table(regular_topics)
            
            # Системные топики (если есть)
            if system_topics:
                response += "\n🔧 **СИСТЕМНАЯ ИНФОРМАЦИЯ:**\n"
                for topic in system_topics:
                    if topic.get('id', 0) == 0:
                        response += f"⚠️ {topic['title']}\n"
                    elif topic.get('id', 0) == -1:
                        response += f"❌ {topic['title']}: {topic.get('error', 'неизвестная ошибка')}\n"
                
                if mode == 'bot':
                    response += "\n💡 **Совет:** Для полного доступа используйте `/switch_mode` → пользовательский режим"
            
            return response
            
        except Exception as e:
            logger.error(f"❌ Ошибка форматирования результатов: {e}")
            return f"❌ **Ошибка форматирования:** {str(e)}"
    
    @staticmethod
    def create_topics_summary(topics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Создание сводки по топикам
        
        Args:
            topics: Список топиков
            
        Returns:
            Словарь с сводкой
        """
        try:
            regular_topics = [t for t in topics if t.get('id', 0) > 0]
            system_topics = [t for t in topics if t.get('id', 0) <= 0]
            
            # Подсчитываем сообщения
            total_messages = 0
            for topic in regular_topics:
                messages = topic.get('messages', 0)
                if isinstance(messages, (int, float)):
                    total_messages += int(messages)
            
            # Создатели
            creators = [t.get('created_by', 'Неизвестно') for t in regular_topics]
            unique_creators = len(set(creators))
            
            return {
                'total_topics': len(topics),
                'regular_topics': len(regular_topics),
                'system_topics': len(system_topics),
                'total_messages': total_messages,
                'unique_creators': unique_creators,
                'has_errors': any(t.get('id', 0) == -1 for t in topics)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания сводки: {e}")
            return {
                'total_topics': 0,
                'regular_topics': 0,
                'system_topics': 0,
                'total_messages': 0,
                'unique_creators': 0,
                'has_errors': True
            }
    
    @staticmethod
    def validate_chat_for_scanning(chat) -> tuple[bool, str]:
        """
        Валидация чата для сканирования
        
        Args:
            chat: Объект чата
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        try:
            if not chat:
                return False, "❌ Чат не найден"
            
            # Проверяем тип чата
            if not hasattr(chat, 'megagroup') or not chat.megagroup:
                return False, "⚠️ **Работает только в супергруппах!**"
            
            # Проверяем, является ли чат форумом
            if not is_forum_chat(chat):
                return False, "ℹ️ **Группа не является форумом**\n\nТопики доступны только в форумах."
            
            return True, "✅ Чат готов для сканирования"
            
        except Exception as e:
            logger.error(f"❌ Ошибка валидации чата: {e}")
            return False, f"❌ Ошибка валидации: {str(e)}"
    
    @staticmethod
    async def send_scan_results(event, topics: List[Dict[str, Any]], chat, mode: str = 'bot'):
        """
        Отправка результатов сканирования пользователю
        
        Args:
            event: Telegram событие
            topics: Список топиков
            chat: Объект чата
            mode: Режим сканирования
        """
        try:
            # Форматируем результаты
            formatted_results = ScannerUtils.format_scan_results(topics, chat, mode)
            
            # Отправляем результаты
            await send_long_message(event, formatted_results, parse_mode='markdown')
            
            # Получаем сводку
            summary = ScannerUtils.create_topics_summary(topics)
            
            # Логируем результат
            logger.info(f"📤 Отправлены результаты сканирования: {summary['regular_topics']} топиков")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки результатов: {e}")
            await MessageUtils.smart_reply(event, 
                f"❌ **Ошибка отправки результатов:** {str(e)}")

# Функции для обратной совместимости
async def scan_chat_topics(client: TelegramClient, chat, mode: str = 'bot') -> List[Dict[str, Any]]:
    """Обратная совместимость: сканирование топиков"""
    return await ScannerUtils.scan_chat_topics(client, chat, mode)

def format_scan_results(topics: List[Dict[str, Any]], chat, mode: str = 'bot') -> str:
    """Обратная совместимость: форматирование результатов"""
    return ScannerUtils.format_scan_results(topics, chat, mode)

async def send_scan_results(event, topics: List[Dict[str, Any]], chat, mode: str = 'bot'):
    """Обратная совместимость: отправка результатов"""
    await ScannerUtils.send_scan_results(event, topics, chat, mode)

# Экспорт
__all__ = [
    'ScannerUtils',
    'scan_chat_topics',
    'format_scan_results',
    'send_scan_results',
    # Реэкспорт из topic_scanner
    'BaseTopicScanner',
    'BotTopicScanner',
    'UserTopicScanner',
    'TopicScannerFactory',
    'get_topic_link',
    'is_forum_chat'
]