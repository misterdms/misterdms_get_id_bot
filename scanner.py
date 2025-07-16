#!/usr/bin/env python3
"""
Topics Scanner Bot v5.18 - Сканер топиков
Логика: админ дает credentials для группы, любой пользователь может сканировать
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
        self.admin_clients: Dict[int, TelegramClient] = {}  # admin_user_id -> client
    
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
    
    async def scan_topics_user_api(self, chat_id: int, admin_api_id: str, admin_api_hash: str, 
                                  admin_user_id: int, chat) -> List[Dict[str, Any]]:
        """Полное сканирование топиков через User API админа"""
        topics = []
        
        try:
            # Создаем или получаем клиент админа
            client = await self._get_admin_client(admin_user_id, admin_api_id, admin_api_hash)
            
            if not client or not client.is_connected():
                raise Exception("Не удалось подключиться с credentials админа")
            
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
                
                logger.info(f"👤 User API (админ {admin_user_id}): найдено {len(topics)} топиков")
                
            except ChatAdminRequiredError:
                logger.warning("⚠️ Требуются права администратора")
            except ChannelPrivateError:
                logger.warning("⚠️ Нет доступа к приватному каналу")
        
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования User API: {e}")
            raise
        
        return topics
    
    async def _get_admin_client(self, admin_user_id: int, api_id: str, api_hash: str) -> Optional[TelegramClient]:
        """Получить или создать клиент админа"""
        try:
            if admin_user_id in self.admin_clients:
                client = self.admin_clients[admin_user_id]
                if client.is_connected():
                    return client
                else:
                    await self._disconnect_admin_client(admin_user_id)
            
            # Создаем новый клиент
            session_name = f'admin_session_{admin_user_id}'
            client = TelegramClient(session_name, int(api_id), api_hash)
            
            await client.start()
            
            if await client.is_user_authorized():
                self.admin_clients[admin_user_id] = client
                logger.info(f"✅ Admin клиент создан для {admin_user_id}")
                return client
            else:
                logger.error(f"❌ Админ {admin_user_id} не авторизован")
                await client.disconnect()
                return None
        
        except Exception as e:
            logger.error(f"❌ Ошибка создания Admin клиента для {admin_user_id}: {e}")
            return None
    
    async def _disconnect_admin_client(self, admin_user_id: int):
        """Отключить клиент админа"""
        try:
            if admin_user_id in self.admin_clients:
                client = self.admin_clients[admin_user_id]
                if client.is_connected():
                    await client.disconnect()
                del self.admin_clients[admin_user_id]
                logger.info(f"🔌 Admin клиент отключен для {admin_user_id}")
        except Exception as e:
            logger.debug(f"Ошибка отключения клиента {admin_user_id}: {e}")
    
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
            
            chat_id = chat.id
            
            # Ищем админа группы с credentials
            from database import db
            group_info = await db.get_group_admin_credentials(chat_id)
            
            if group_info and group_info['admin_api_id'] and group_info['admin_api_hash']:
                # Используем User API админа группы
                logger.info(f"🔑 Используем credentials админа {group_info['admin_user_id']} для группы {chat_id}")
                return await self.scan_topics_user_api(
                    chat_id, 
                    group_info['admin_api_id'], 
                    group_info['admin_api_hash'],
                    group_info['admin_user_id'],
                    chat
                )
            else:
                # Используем Bot API
                logger.info(f"🤖 Используем Bot API для группы {chat_id}")
                return await self.scan_topics_bot_api(bot_client, chat)
        
        except Exception as e:
            logger.error(f"❌ Ошибка сканирования для пользователя {user_id}: {e}")
            raise
    
    async def cleanup(self):
        """Очистка всех соединений"""
        logger.info("🧹 Закрытие всех Admin API соединений...")
        
        for admin_user_id in list(self.admin_clients.keys()):
            await self._disconnect_admin_client(admin_user_id)
        
        logger.info("✅ Все соединения закрыты")

# Глобальный экземпляр
scanner = TopicsScanner()
