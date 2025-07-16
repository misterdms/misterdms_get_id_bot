#!/usr/bin/env python3
"""
Topics Scanner Bot v5.16 - Сканер топиков
Простое сканирование топиков в Bot API и User API режимах
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from telethon import TelegramClient
from telethon.tl.types import Channel
from telethon.tl.functions.channels import GetForumTopicsRequest, GetFullChannelRequest
from telethon.errors import ChatAdminRequiredError, ChannelPrivateError

logger = logging.getLogger(__name__)

class TopicsScanner:
    """Сканер топиков для супергрупп"""
    
    def __init__(self):
        self.user_clients: Dict[int, TelegramClient] = {}
    
    async def scan_topics_bot_api(self, bot_client: TelegramClient, chat) -> List[Dict[str, Any]]:
        """Сканирование топиков через Bot API (ограниченно)"""
        topics = []
        
        try:
            # Всегда добавляем General топик
            topics.append({
                'id': 1,
                'title': 'General',
                'link': self._get_topic_link(chat, 1)
            })
            
            # Пытаемся найти другие топики через анализ сообщений
            try:
                from telethon.tl.functions.messages import GetHistoryRequest
                
                messages = await bot_client(GetHistoryRequest(
                    peer=chat,
                    offset_id=0,
                    offset_date=None,
                    add_offset=0,
                    limit=100,
                    max_id=0,
                    min_id=0,
                    hash=0
                ))
                
                found_topics = set()
                for message in messages.messages:
                    if hasattr(message, 'reply_to') and message.reply_to:
                        topic_id = None
                        if hasattr(message.reply_to, 'reply_to_top_id'):
                            topic_id = message.reply_to.reply_to_top_id
                        elif hasattr(message.reply_to, 'reply_to_msg_id'):
                            topic_id = message.reply_to.reply_to_msg_id
                        
                        if topic_id and topic_id != 1 and topic_id not in found_topics:
                            found_topics.add(topic_id)
                            topics.append({
                                'id': topic_id,
                                'title': f'Topic {topic_id}',
                                'link': self._get_topic_link(chat, topic_id)
                            })
                
                logger.info(f"🤖 Bot API: найдено {len(topics)} топиков")
                
            except Exception as e:
                logger.debug(f"Поиск через сообщения не удался: {e}")
        
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования Bot API: {e}")
            
        return topics
    
    async def scan_topics_user_api(self, user_id: int, api_id: str, api_hash: str, chat) -> List[Dict[str, Any]]:
        """Полное сканирование топиков через User API"""
        topics = []
        
        try:
            # Создаем или получаем клиент пользователя
            client = await self._get_user_client(user_id, api_id, api_hash)
            
            if not client or not client.is_connected():
                raise Exception("Не удалось подключиться с User API")
            
            # Всегда добавляем General топик
            topics.append({
                'id': 1,
                'title': 'General',
                'link': self._get_topic_link(chat, 1)
            })
            
            # Получаем все топики форума
            try:
                result = await client(GetForumTopicsRequest(
                    channel=chat,
                    offset_date=None,
                    offset_id=0,
                    offset_topic=0,
                    limit=100
                ))
                
                for topic in result.topics:
                    if hasattr(topic, 'id') and hasattr(topic, 'title'):
                        topics.append({
                            'id': topic.id,
                            'title': topic.title,
                            'link': self._get_topic_link(chat, topic.id)
                        })
                
                logger.info(f"👤 User API: найдено {len(topics)} топиков")
                
            except ChatAdminRequiredError:
                logger.warning("⚠️ Требуются права администратора")
            except ChannelPrivateError:
                logger.warning("⚠️ Нет доступа к приватному каналу")
        
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования User API: {e}")
            raise
        
        return topics
    
    async def _get_user_client(self, user_id: int, api_id: str, api_hash: str) -> Optional[TelegramClient]:
        """Получить или создать клиент пользователя"""
        try:
            if user_id in self.user_clients:
                client = self.user_clients[user_id]
                if client.is_connected():
                    return client
                else:
                    await self._disconnect_user_client(user_id)
            
            # Создаем новый клиент
            session_name = f'user_session_{user_id}'
            client = TelegramClient(session_name, int(api_id), api_hash)
            
            await client.start()
            
            if await client.is_user_authorized():
                self.user_clients[user_id] = client
                logger.info(f"✅ User клиент создан для {user_id}")
                return client
            else:
                logger.error(f"❌ Пользователь {user_id} не авторизован")
                await client.disconnect()
                return None
        
        except Exception as e:
            logger.error(f"❌ Ошибка создания User клиента для {user_id}: {e}")
            return None
    
    async def _disconnect_user_client(self, user_id: int):
        """Отключить клиент пользователя"""
        try:
            if user_id in self.user_clients:
                client = self.user_clients[user_id]
                if client.is_connected():
                    await client.disconnect()
                del self.user_clients[user_id]
                logger.info(f"🔌 User клиент отключен для {user_id}")
        except Exception as e:
            logger.debug(f"Ошибка отключения клиента {user_id}: {e}")
    
    def _get_topic_link(self, chat, topic_id: int) -> str:
        """Генерация ссылки на топик"""
        try:
            if hasattr(chat, 'id'):
                chat_id = str(chat.id).replace('-100', '')
                return f"https://t.me/c/{chat_id}/{topic_id}"
            else:
                return f"#topic_{topic_id}"
        except Exception:
            return f"#topic_{topic_id}"
    
    def _is_forum_chat(self, chat) -> bool:
        """Проверка, является ли чат форумом"""
        try:
            return (isinstance(chat, Channel) and 
                    hasattr(chat, 'forum') and 
                    chat.forum and
                    hasattr(chat, 'megagroup') and 
                    chat.megagroup)
        except:
            return False
    
    async def scan_topics(self, user_id: int, bot_client: TelegramClient, chat) -> List[Dict[str, Any]]:
        """Основной метод сканирования"""
        try:
            # Проверяем, является ли чат форумом
            if not self._is_forum_chat(chat):
                raise Exception("Группа не является форумом")
            
            # Получаем данные пользователя
            from database import db
            user = await db.get_user(user_id)
            
            if user and user['api_mode'] == 'user' and user['api_id'] and user['api_hash']:
                # Используем User API
                return await self.scan_topics_user_api(
                    user_id, user['api_id'], user['api_hash'], chat
                )
            else:
                # Используем Bot API
                return await self.scan_topics_bot_api(bot_client, chat)
        
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования для пользователя {user_id}: {e}")
            raise
    
    async def cleanup(self):
        """Очистка всех соединений"""
        logger.info("🧹 Закрытие всех User API соединений...")
        
        for user_id in list(self.user_clients.keys()):
            await self._disconnect_user_client(user_id)
        
        logger.info("✅ Все соединения закрыты")

# Глобальный экземпляр
scanner = TopicsScanner()
