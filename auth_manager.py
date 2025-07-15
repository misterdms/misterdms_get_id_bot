#!/usr/bin/env python3
"""
Управление аутентификацией и пользовательскими сессиями
Включает шифрование credentials и управление Telethon сессиями
"""

import re
import os
import asyncio
import logging
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.errors import AuthKeyError, ApiIdInvalidError, PhoneNumberInvalidError
from cryptography.fernet import Fernet
import base64
import hashlib

from config import (
    ENCRYPTION_KEY, SALT, MAX_CONCURRENT_SESSIONS, 
    API_ID_PATTERN, API_HASH_PATTERN, SESSION_TIMEOUT_DAYS
)
from database import db_manager

logger = logging.getLogger(__name__)

class AuthenticationManager:
    """Менеджер аутентификации и пользовательских сессий"""
    
    def __init__(self):
        self.active_sessions: Dict[int, TelegramClient] = {}
        self.session_locks: Dict[int, asyncio.Lock] = {}
        self.cipher = self._init_cipher()
        
    def _init_cipher(self) -> Fernet:
        """Инициализация шифрования"""
        # Создаем ключ на основе ENCRYPTION_KEY и SALT
        key_material = (ENCRYPTION_KEY + SALT).encode()
        key = base64.urlsafe_b64encode(hashlib.sha256(key_material).digest())
        return Fernet(key)
    
    def encrypt_data(self, data: str) -> str:
        """Зашифровать данные"""
        return self.cipher.encrypt(data.encode()).decode()
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """Расшифровать данные"""
        return self.cipher.decrypt(encrypted_data.encode()).decode()
    
    def validate_api_credentials(self, api_id: str, api_hash: str) -> Tuple[bool, str]:
        """Валидация API credentials"""
        # Проверка API_ID
        if not re.match(API_ID_PATTERN, api_id.strip()):
            return False, "❌ API_ID должен содержать 7-8 цифр"
        
        # Проверка API_HASH
        if not re.match(API_HASH_PATTERN, api_hash.strip().lower()):
            return False, "❌ API_HASH должен содержать 32 символа (hex)"
        
        return True, "✅ Credentials валидны"
    
    async def save_user_credentials(self, user_id: int, api_id: str, api_hash: str) -> Tuple[bool, str]:
        """Сохранить зашифрованные credentials пользователя"""
        try:
            # Валидация
            is_valid, message = self.validate_api_credentials(api_id, api_hash)
            if not is_valid:
                return False, message
            
            # Тестирование подключения
            is_connected, test_message = await self.test_connection(api_id, api_hash)
            if not is_connected:
                return False, f"❌ Не удалось подключиться: {test_message}"
            
            # Шифрование
            api_id_encrypted = self.encrypt_data(api_id.strip())
            api_hash_encrypted = self.encrypt_data(api_hash.strip())
            session_file = f"user_session_{user_id}"
            
            # Сохранение в БД
            await db_manager.save_user_credentials(
                user_id, api_id_encrypted, api_hash_encrypted, session_file
            )
            
            logger.info(f"✅ Credentials сохранены для пользователя {user_id}")
            return True, "✅ Credentials сохранены и проверены!"
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения credentials для {user_id}: {e}")
            return False, f"❌ Ошибка сохранения: {str(e)}"
    
    async def test_connection(self, api_id: str, api_hash: str) -> Tuple[bool, str]:
        """Тестирование подключения с credentials"""
        temp_session = f"temp_test_{datetime.now().timestamp()}"
        client = None
        
        try:
            # Создаем временный клиент для тестирования
            client = TelegramClient(
                temp_session, 
                int(api_id), 
                api_hash,
                device_model="Topics Scanner Bot",
                system_version="1.0",
                app_version="4.0.0"
            )
            
            # Пытаемся подключиться
            await client.connect()
            
            if not await client.is_user_authorized():
                # Для бота это нормально - главное что API credentials валидны
                await client.disconnect()
                return True, "✅ API credentials валидны"
            else:
                # Пользователь уже авторизован
                await client.disconnect()
                return True, "✅ Подключение успешно"
                
        except ApiIdInvalidError:
            return False, "Неверный API_ID"
        except Exception as e:
            return False, f"Ошибка подключения: {str(e)}"
        finally:
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            # Удаляем временный session файл
            try:
                os.remove(f"{temp_session}.session")
            except:
                pass
    
    async def get_user_session(self, user_id: int) -> Optional[TelegramClient]:
        """Получить активную сессию пользователя"""
        # Если сессия уже активна
        if user_id in self.active_sessions:
            client = self.active_sessions[user_id]
            if client.is_connected():
                return client
            else:
                # Сессия неактивна, удаляем
                await self.close_user_session(user_id)
        
        # Проверяем лимит активных сессий
        if len(self.active_sessions) >= MAX_CONCURRENT_SESSIONS:
            logger.warning(f"⚠️ Превышен лимит сессий ({MAX_CONCURRENT_SESSIONS})")
            return None
        
        # Создаем новую сессию
        return await self.create_user_session(user_id)
    
    async def create_user_session(self, user_id: int) -> Optional[TelegramClient]:
        """Создать новую пользовательскую сессию"""
        try:
            # Получаем блокировку для пользователя
            if user_id not in self.session_locks:
                self.session_locks[user_id] = asyncio.Lock()
            
            async with self.session_locks[user_id]:
                # Проверяем, не создана ли сессия уже
                if user_id in self.active_sessions:
                    return self.active_sessions[user_id]
                
                # Получаем credentials из БД
                user_data = await db_manager.get_user(user_id)
                if not user_data or user_data['mode'] != 'user':
                    logger.warning(f"⚠️ Пользователь {user_id} не в user режиме")
                    return None
                
                if not user_data['api_id_encrypted'] or not user_data['api_hash_encrypted']:
                    logger.warning(f"⚠️ Нет credentials для пользователя {user_id}")
                    return None
                
                # Расшифровываем credentials
                api_id = int(self.decrypt_data(user_data['api_id_encrypted']))
                api_hash = self.decrypt_data(user_data['api_hash_encrypted'])
                session_file = user_data['session_file'] or f"user_session_{user_id}"
                
                # Создаем клиент
                client = TelegramClient(
                    session_file,
                    api_id,
                    api_hash,
                    device_model="Topics Scanner Bot",
                    system_version="1.0",
                    app_version="4.0.0"
                )
                
                # Подключаемся
                await client.connect()
                
                # Проверяем авторизацию
                if not await client.is_user_authorized():
                    logger.error(f"❌ Пользователь {user_id} не авторизован")
                    await client.disconnect()
                    await db_manager.update_user_status(user_id, 'error')
                    return None
                
                # Сохраняем активную сессию
                self.active_sessions[user_id] = client
                await db_manager.update_user_status(user_id, 'active')
                
                logger.info(f"✅ Создана сессия для пользователя {user_id}")
                return client
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания сессии для {user_id}: {e}")
            await db_manager.update_user_status(user_id, 'error')
            return None
    
    async def close_user_session(self, user_id: int) -> bool:
        """Закрыть пользовательскую сессию"""
        try:
            if user_id in self.active_sessions:
                client = self.active_sessions[user_id]
                if client.is_connected():
                    await client.disconnect()
                del self.active_sessions[user_id]
                
            if user_id in self.session_locks:
                del self.session_locks[user_id]
                
            logger.info(f"✅ Сессия пользователя {user_id} закрыта")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка закрытия сессии {user_id}: {e}")
            return False
    
    async def close_all_sessions(self):
        """Закрыть все активные сессии"""
        logger.info("🔄 Закрытие всех активных сессий...")
        
        for user_id in list(self.active_sessions.keys()):
            await self.close_user_session(user_id)
        
        self.active_sessions.clear()
        self.session_locks.clear()
        
        logger.info("✅ Все сессии закрыты")
    
    async def cleanup_expired_sessions(self) -> int:
        """Очистка истекших сессий"""
        cleaned = 0
        
        try:
            # Обновляем статусы в БД
            expired_count = await db_manager.cleanup_expired_users()
            
            # Закрываем активные сессии истекших пользователей
            expired_users = await db_manager.get_users_by_mode('user')
            for user_data in expired_users:
                if user_data['status'] == 'expired':
                    user_id = user_data['user_id']
                    if user_id in self.active_sessions:
                        await self.close_user_session(user_id)
                        cleaned += 1
            
            if cleaned > 0:
                logger.info(f"🧹 Очищено {cleaned} истекших сессий")
            
            return cleaned
            
        except Exception as e:
            logger.error(f"❌ Ошибка очистки сессий: {e}")
            return 0
    
    async def get_session_info(self, user_id: int) -> Dict[str, Any]:
        """Получить информацию о сессии пользователя"""
        user_data = await db_manager.get_user(user_id)
        if not user_data:
            return {'status': 'not_found'}
        
        info = {
            'user_id': user_id,
            'mode': user_data['mode'],
            'status': user_data['status'],
            'created_at': user_data['created_at'],
            'last_active': user_data['last_active'],
            'has_credentials': bool(user_data.get('api_id_encrypted')),
            'is_session_active': user_id in self.active_sessions
        }
        
        if user_id in self.active_sessions:
            client = self.active_sessions[user_id]
            info['is_connected'] = client.is_connected()
            
            try:
                # Получаем информацию о текущем пользователе
                me = await client.get_me()
                info['telegram_user'] = {
                    'id': me.id,
                    'username': me.username,
                    'first_name': me.first_name,
                    'phone': me.phone
                }
            except:
                info['telegram_user'] = None
        
        return info
    
    async def logout_user(self, user_id: int) -> Tuple[bool, str]:
        """Выйти из пользовательского режима"""
        try:
            # Закрываем сессию
            await self.close_user_session(user_id)
            
            # Удаляем credentials из БД
            user_data = await db_manager.get_user(user_id)
            if user_data:
                await db_manager.create_or_update_user(
                    user_id, 
                    user_data['telegram_username'],
                    user_data['first_name'],
                    'bot'
                )
            
            # Удаляем session файл
            try:
                session_file = f"user_session_{user_id}.session"
                if os.path.exists(session_file):
                    os.remove(session_file)
            except:
                pass
            
            logger.info(f"✅ Пользователь {user_id} вышел из user режима")
            return True, "✅ Выход выполнен. Переключен в режим бота."
            
        except Exception as e:
            logger.error(f"❌ Ошибка logout для {user_id}: {e}")
            return False, f"❌ Ошибка выхода: {str(e)}"
    
    async def get_active_sessions_count(self) -> Dict[str, int]:
        """Получить количество активных сессий"""
        return {
            'total_sessions': len(self.active_sessions),
            'max_sessions': MAX_CONCURRENT_SESSIONS,
            'available_slots': MAX_CONCURRENT_SESSIONS - len(self.active_sessions)
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Проверка здоровья всех активных сессий"""
        health_info = {
            'total_sessions': len(self.active_sessions),
            'healthy_sessions': 0,
            'unhealthy_sessions': 0,
            'session_details': {}
        }
        
        for user_id, client in list(self.active_sessions.items()):
            try:
                is_connected = client.is_connected()
                if is_connected:
                    # Попробуем выполнить простой запрос
                    await client.get_me()
                    health_info['healthy_sessions'] += 1
                    health_info['session_details'][user_id] = 'healthy'
                else:
                    health_info['unhealthy_sessions'] += 1
                    health_info['session_details'][user_id] = 'disconnected'
                    # Закрываем неработающую сессию
                    await self.close_user_session(user_id)
                    
            except Exception as e:
                health_info['unhealthy_sessions'] += 1
                health_info['session_details'][user_id] = f'error: {str(e)[:50]}'
                # Закрываем проблемную сессию
                await self.close_user_session(user_id)
        
        return health_info
    
    def get_encryption_info(self) -> Dict[str, str]:
        """Получить информацию о шифровании (для отладки)"""
        return {
            'cipher_available': bool(self.cipher),
            'key_length': len(ENCRYPTION_KEY),
            'salt_length': len(SALT),
            'algorithm': 'Fernet (AES 128 + HMAC SHA256)'
        }

# Глобальный экземпляр менеджера аутентификации
auth_manager = AuthenticationManager()

# Функции для быстрого доступа
async def get_auth_manager():
    """Получить менеджер аутентификации"""
    return auth_manager

async def cleanup_auth():
    """Очистка всех сессий при завершении"""
    await auth_manager.close_all_sessions()
