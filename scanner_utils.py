#!/usr/bin/env python3
"""
Утилиты сканирования топиков для Get ID Bot by Mister DMS
ОБНОВЛЕНО v4.1.1: улучшенная логика сканирования + безопасность
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
import json

from telethon import TelegramClient
from telethon.errors import (
    ChatAdminRequiredError, ChannelPrivateError, 
    FloodWaitError, ApiIdInvalidError
)
from telethon.tl.types import Channel, Chat, User, MessageMediaDocument
from telethon.tl.functions.channels import GetForumTopicsRequest
from telethon.tl.functions.messages import GetHistoryRequest

from utils import PerformanceUtils, ValidationUtils, EncryptionUtils
from database import DatabaseManager

logger = logging.getLogger(__name__)

class TopicScanner:
    """Сканер топиков с поддержкой bot и user режимов"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.active_scans = {}  # Активные сканирования для предотвращения дублей
        
    @PerformanceUtils.measure_time
    async def scan_topics(self, chat_id: int, user_id: int, mode: str = 'bot') -> Dict[str, Any]:
        """
        Основной метод сканирования топиков
        
        Args:
            chat_id: ID чата для сканирования
            user_id: ID пользователя, инициировавшего сканирование
            mode: Режим работы ('bot' или 'user')
        
        Returns:
            Результат сканирования с топиками и статистикой
        """
        
        # Проверяем не идет ли уже сканирование этого чата
        scan_key = f"{chat_id}_{user_id}"
        if scan_key in self.active_scans:
            return {
                'success': False,
                'error': 'Сканирование уже выполняется',
                'data': None
            }
        
        try:
            self.active_scans[scan_key] = datetime.now()
            
            if mode == 'user':
                return await self._scan_user_mode(chat_id, user_id)
            else:
                return await self._scan_bot_mode(chat_id, user_id)
                
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования топиков: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': None
            }
        finally:
            # Убираем из активных сканирований
            self.active_scans.pop(scan_key, None)
    
    async def _scan_bot_mode(self, chat_id: int, user_id: int) -> Dict[str, Any]:
        """Сканирование в режиме бота (ограниченный функционал)"""
        from handlers import BotHandlers  # Избегаем циклического импорта
        
        try:
            # Используем основного бот-клиента
            # Здесь будет логика получения клиента от BotHandlers
            logger.debug(f"🤖 Сканирование в режиме бота: {chat_id}")
            
            # Пока заглушка - в реальной реализации получим топики через Bot API
            topics = await self._get_topics_bot_api(chat_id)
            
            # Сохраняем результаты в БД
            await self._save_scan_results(chat_id, user_id, topics, 'bot')
            
            return {
                'success': True,
                'error': None,
                'data': {
                    'topics': topics,
                    'mode': 'bot',
                    'timestamp': datetime.now().isoformat(),
                    'chat_id': chat_id,
                    'user_id': user_id
                }
            }
            
        except ChatAdminRequiredError:
            return {
                'success': False,
                'error': 'Бот должен быть администратором в группе',
                'data': None
            }
        except ChannelPrivateError:
            return {
                'success': False,
                'error': 'Группа приватная или недоступна',
                'data': None
            }
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования в режиме бота: {e}")
            return {
                'success': False,
                'error': f'Ошибка сканирования: {str(e)}',
                'data': None
            }
    
    async def _scan_user_mode(self, chat_id: int, user_id: int) -> Dict[str, Any]:
        """Сканирование в режиме пользователя (полный функционал)"""
        
        try:
            logger.debug(f"👤 Сканирование в режиме пользователя: {chat_id}")
            
            # Получаем credentials пользователя
            user_data = await self.db_manager.get_user(user_id)
            if not user_data or not user_data.get('api_id_encrypted'):
                return {
                    'success': False,
                    'error': 'API credentials не настроены. Используйте /renew_my_api_hash',
                    'data': None
                }
            
            # Расшифровываем credentials
            api_id = EncryptionUtils.decrypt(user_data['api_id_encrypted'])
            api_hash = EncryptionUtils.decrypt(user_data['api_hash_encrypted'])
            
            if not api_id or not api_hash:
                return {
                    'success': False,
                    'error': 'Ошибка расшифровки API credentials',
                    'data': None
                }
            
            # Создаем пользовательский клиент
            user_client = TelegramClient(
                f'user_session_{user_id}',
                int(api_id),
                api_hash
            )
            
            try:
                await user_client.start()
                
                # Получаем информацию о чате
                chat_entity = await user_client.get_entity(chat_id)
                
                # Проверяем что это супергруппа с топиками
                if not isinstance(chat_entity, Channel) or not chat_entity.forum:
                    return {
                        'success': False,
                        'error': 'Чат не является супергруппой с топиками',
                        'data': None
                    }
                
                # Получаем топики
                topics = await self._get_topics_user_api(user_client, chat_id)
                
                # Получаем дополнительную информацию по топикам
                enriched_topics = await self._enrich_topics_data(user_client, chat_id, topics)
                
                # Сохраняем результаты
                await self._save_scan_results(chat_id, user_id, enriched_topics, 'user')
                
                return {
                    'success': True,
                    'error': None,
                    'data': {
                        'topics': enriched_topics,
                        'mode': 'user',
                        'timestamp': datetime.now().isoformat(),
                        'chat_id': chat_id,
                        'user_id': user_id,
                        'chat_info': {
                            'title': getattr(chat_entity, 'title', 'Unknown'),
                            'username': getattr(chat_entity, 'username', None),
                            'participants_count': getattr(chat_entity, 'participants_count', 0)
                        }
                    }
                }
                
            finally:
                await user_client.disconnect()
                
        except ApiIdInvalidError:
            return {
                'success': False,
                'error': 'Неверные API credentials. Обновите через /renew_my_api_hash',
                'data': None
            }
        except FloodWaitError as e:
            return {
                'success': False,
                'error': f'Rate limit от Telegram. Попробуйте через {e.seconds} секунд',
                'data': None
            }
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования в режиме пользователя: {e}")
            return {
                'success': False,
                'error': f'Ошибка сканирования: {str(e)}',
                'data': None
            }
    
    async def _get_topics_bot_api(self, chat_id: int) -> List[Dict[str, Any]]:
        """Получение топиков через Bot API (ограниченный функционал)"""
        
        # В режиме бота получаем только базовую информацию
        # Реальная реализация будет использовать Bot API
        
        # Пока возвращаем заглушку
        logger.debug(f"🔍 Получение топиков через Bot API для чата {chat_id}")
        
        # TODO: Реализовать получение топиков через Bot API
        # Telegram Bot API пока не поддерживает полноценную работу с топиками
        
        return [
            {
                'id': 1,
                'title': 'Общий',
                'message_count': 0,
                'created_date': datetime.now().isoformat(),
                'last_message_date': None,
                'creator_id': None,
                'is_closed': False,
                'icon_emoji': '💬',
                'mode': 'bot_api'
            }
        ]
    
    async def _get_topics_user_api(self, client: TelegramClient, chat_id: int) -> List[Dict[str, Any]]:
        """Получение топиков через User API (полный функционал)"""
        
        try:
            logger.debug(f"🔍 Получение топиков через User API для чата {chat_id}")
            
            # Получаем список топиков
            result = await client(GetForumTopicsRequest(
                channel=chat_id,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=100
            ))
            
            topics = []
            
            for topic in result.topics:
                topic_data = {
                    'id': topic.id,
                    'title': topic.title,
                    'created_date': topic.date.isoformat() if topic.date else None,
                    'creator_id': topic.from_id.user_id if hasattr(topic.from_id, 'user_id') else None,
                    'is_closed': getattr(topic, 'closed', False),
                    'is_pinned': getattr(topic, 'pinned', False),
                    'icon_emoji': getattr(topic, 'icon_emoji_id', None),
                    'message_count': 0,  # Будет обновлено в _enrich_topics_data
                    'last_message_date': None,
                    'mode': 'user_api'
                }
                
                topics.append(topic_data)
            
            logger.debug(f"✅ Найдено топиков: {len(topics)}")
            return topics
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения топиков через User API: {e}")
            return []
    
    async def _enrich_topics_data(self, client: TelegramClient, chat_id: int, 
                                topics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Обогащение данных топиков дополнительной информацией"""
        
        enriched_topics = []
        
        for topic in topics:
            try:
                # Получаем статистику сообщений в топике
                topic_stats = await self._get_topic_message_stats(client, chat_id, topic['id'])
                
                # Обогащаем данные
                enriched_topic = {
                    **topic,
                    'message_count': topic_stats.get('message_count', 0),
                    'last_message_date': topic_stats.get('last_message_date'),
                    'unique_users': topic_stats.get('unique_users', 0),
                    'avg_messages_per_day': topic_stats.get('avg_messages_per_day', 0),
                    'most_active_user': topic_stats.get('most_active_user'),
                    'recent_activity': topic_stats.get('recent_activity', False)
                }
                
                enriched_topics.append(enriched_topic)
                
                # Небольшая задержка чтобы не нарваться на rate limit
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка обогащения данных топика {topic['id']}: {e}")
                enriched_topics.append(topic)
        
        return enriched_topics
    
    async def _get_topic_message_stats(self, client: TelegramClient, chat_id: int, 
                                     topic_id: int) -> Dict[str, Any]:
        """Получение статистики сообщений в топике"""
        
        try:
            # Получаем последние сообщения из топика
            history = await client(GetHistoryRequest(
                peer=chat_id,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=100,
                max_id=0,
                min_id=0,
                hash=0,
                reply_to_msg_id=topic_id  # Указываем топик
            ))
            
            stats = {
                'message_count': len(history.messages),
                'last_message_date': None,
                'unique_users': 0,
                'avg_messages_per_day': 0,
                'most_active_user': None,
                'recent_activity': False
            }
            
            if history.messages:
                # Последнее сообщение
                last_message = history.messages[0]
                stats['last_message_date'] = last_message.date.isoformat()
                
                # Проверяем активность за последние 24 часа
                recent_threshold = datetime.now() - timedelta(hours=24)
                stats['recent_activity'] = last_message.date > recent_threshold
                
                # Подсчитываем уникальных пользователей
                user_message_counts = {}
                for msg in history.messages:
                    if hasattr(msg, 'from_id') and msg.from_id:
                        user_id = msg.from_id.user_id if hasattr(msg.from_id, 'user_id') else str(msg.from_id)
                        user_message_counts[user_id] = user_message_counts.get(user_id, 0) + 1
                
                stats['unique_users'] = len(user_message_counts)
                
                # Самый активный пользователь
                if user_message_counts:
                    most_active_user_id = max(user_message_counts.keys(), 
                                            key=lambda x: user_message_counts[x])
                    stats['most_active_user'] = {
                        'user_id': most_active_user_id,
                        'message_count': user_message_counts[most_active_user_id]
                    }
                
                # Средние сообщения в день (примерная оценка)
                if len(history.messages) >= 2:
                    oldest_message = history.messages[-1]
                    time_span = (last_message.date - oldest_message.date).days
                    if time_span > 0:
                        stats['avg_messages_per_day'] = len(history.messages) / time_span
            
            return stats
            
        except Exception as e:
            logger.debug(f"Ошибка получения статистики топика {topic_id}: {e}")
            return {
                'message_count': 0,
                'last_message_date': None,
                'unique_users': 0,
                'avg_messages_per_day': 0,
                'most_active_user': None,
                'recent_activity': False
            }
    
    async def _save_scan_results(self, chat_id: int, user_id: int, 
                               topics: List[Dict[str, Any]], mode: str):
        """Сохранение результатов сканирования в БД"""
        
        try:
            # Сохраняем активность сканирования
            await self.db_manager.save_activity_data(
                chat_id=chat_id,
                user_id=user_id,
                message_count=len(topics)
            )
            
            # Логируем успешное сканирование
            await self.db_manager.log_command_usage(
                user_id=user_id,
                command='scan',
                success=True,
                chat_type='supergroup' if chat_id < 0 else 'private'
            )
            
            logger.debug(f"✅ Результаты сканирования сохранены: {len(topics)} топиков")
            
        except Exception as e:
            logger.warning(f"⚠️ Ошибка сохранения результатов сканирования: {e}")
    
    async def get_active_users(self, chat_id: int, user_id: int, days: int = 7) -> Dict[str, Any]:
        """Получение списка активных пользователей"""
        
        try:
            # Пока заглушка - в будущем будет полная реализация
            logger.debug(f"👥 Получение активных пользователей для чата {chat_id}")
            
            # TODO: Реализовать получение активных пользователей
            
            return {
                'success': True,
                'data': {
                    'active_users': [],
                    'total_users': 0,
                    'period_days': days,
                    'chat_id': chat_id
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения активных пользователей: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': None
            }
    
    async def get_user_activity(self, chat_id: int, user_id: int, target_user_id: int) -> Dict[str, Any]:
        """Получение активности конкретного пользователя"""
        
        try:
            logger.debug(f"📊 Получение активности пользователя {target_user_id} в чате {chat_id}")
            
            # TODO: Реализовать получение активности пользователя
            
            return {
                'success': True,
                'data': {
                    'user_id': target_user_id,
                    'message_count': 0,
                    'topics_participated': [],
                    'last_activity': None,
                    'chat_id': chat_id
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения активности пользователя: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': None
            }
    
    def cleanup_active_scans(self):
        """Очистка зависших сканирований"""
        try:
            current_time = datetime.now()
            timeout = timedelta(minutes=10)  # Таймаут 10 минут
            
            expired_scans = [
                key for key, start_time in self.active_scans.items()
                if current_time - start_time > timeout
            ]
            
            for key in expired_scans:
                del self.active_scans[key]
                logger.warning(f"🧹 Удалено зависшее сканирование: {key}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки активных сканирований: {e}")

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def format_topic_info(topic: Dict[str, Any]) -> str:
    """Форматирование информации о топике для отображения"""
    
    title = topic.get('title', 'Без названия')
    message_count = topic.get('message_count', 0)
    last_activity = topic.get('last_message_date')
    is_closed = topic.get('is_closed', False)
    unique_users = topic.get('unique_users', 0)
    
    status = "🔒 Закрыт" if is_closed else "🟢 Активен"
    
    result = f"📌 **{title}**\n"
    result += f"   ID: `{topic.get('id', 'N/A')}`\n"
    result += f"   Статус: {status}\n"
    result += f"   Сообщений: {message_count}\n"
    
    if unique_users > 0:
        result += f"   Участников: {unique_users}\n"
    
    if last_activity:
        try:
            activity_date = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
            from utils import format_timespan
            result += f"   Последняя активность: {format_timespan(activity_date)}\n"
        except:
            pass
    
    return result

def validate_chat_for_scanning(chat_entity) -> tuple[bool, str]:
    """Валидация чата для сканирования топиков"""
    
    if not isinstance(chat_entity, Channel):
        return False, "Чат не является каналом или супергруппой"
    
    if not getattr(chat_entity, 'forum', False):
        return False, "Супергруппа не использует топики"
    
    if getattr(chat_entity, 'left', False):
        return False, "Вы не являетесь участником этой группы"
    
    return True, "Чат подходит для сканирования"

# === ЭКСПОРТ ===

__all__ = [
    'TopicScanner',
    'format_topic_info',
    'validate_chat_for_scanning'
]