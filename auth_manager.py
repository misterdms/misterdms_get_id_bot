#!/usr/bin/env python3
"""
Управление аутентификацией и пользовательскими сессиями
Включает шифрование credentials и управление Telethon сессиями
ИСПРАВЛЕНО: Улучшена обработка ошибок, исправлены импорты, добавлена интеграция с database
"""

import re
import os
import asyncio
import logging
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.errors import AuthKeyError, ApiIdInvalidError, PhoneNumberInvalidError, FloodWaitError
from cryptography.fernet import Fernet
import base64
import hashlib

from config import (
    ENCRYPTION_KEY, SALT, MAX_CONCURRENT_SESSIONS, 
    SESSION_TIMEOUT_DAYS
)

logger = logging.getLogger(__name__)

# Паттерны валидации (вынесены из config.py для безопасности)
API_ID_PATTERN = r'^\d{7,8}$'  # 7-8 цифр
API_HASH_PATTERN = r'^[a-f0-9]{32}$'  # 32 символа hex

class AuthenticationManager:
    """Менеджер аутентификации и пользовательских сессий"""
    
    def __init__(self):
        self.active_sessions: Dict[int, TelegramClient] = {}
        self.session_locks: Dict[int, asyncio.Lock] = {}
        self.cipher = self._init_cipher()
        
        # Статистика и мониторинг
        self.session_stats = {
            'created': 0,
            'failed': 0,
            'expired': 0,
            'active': 0
        }
        
    def _init_cipher(self) -> Fernet:
        """Инициализация шифрования с улучшенной безопасностью"""
        try:
            # Создаем ключ на основе ENCRYPTION_KEY и SALT
            key_material = (ENCRYPTION_KEY + SALT).encode()
            key = base64.urlsafe_b64encode(hashlib.sha256(key_material).digest())
            return Fernet(key)
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации шифрования: {e}")
            raise
    
    def encrypt_data(self, data: str) -> str:
        """Зашифровать данные с обработкой ошибок"""
        try:
            if not data:
                raise ValueError("Данные для шифрования не могут быть пустыми")
            return self.cipher.encrypt(data.encode()).decode()
        except Exception as e:
            logger.error(f"❌ Ошибка шифрования данных: {e}")
            raise
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """Расшифровать данные с обработкой ошибок"""
        try:
            if not encrypted_data:
                raise ValueError("Зашифрованные данные не могут быть пустыми")
            return self.cipher.decrypt(encrypted_data.encode()).decode()
        except Exception as e:
            logger.error(f"❌ Ошибка расшифровки данных: {e}")
            raise
    
    def validate_api_credentials(self, api_id: str, api_hash: str) -> Tuple[bool, str]:
        """Валидация API credentials с улучшенной проверкой"""
        try:
            # Очистка входных данных
            api_id = api_id.strip()
            api_hash = api_hash.strip().lower()
            
            # Проверка API_ID
            if not re.match(API_ID_PATTERN, api_id):
                return False, "❌ API_ID должен содержать 7-8 цифр"
            
            # Проверка API_HASH
            if not re.match(API_HASH_PATTERN, api_hash):
                return False, "❌ API_HASH должен содержать 32 символа (hex)"
            
            # Дополнительные проверки
            try:
                api_id_int = int(api_id)
                if api_id_int <= 0:
                    return False, "❌ API_ID должен быть положительным числом"
            except ValueError:
                return False, "❌ API_ID должен быть числом"
            
            return True, "✅ Credentials валидны"
            
        except Exception as e:
            logger.error(f"❌ Ошибка валидации credentials: {e}")
            return False, f"❌ Ошибка валидации: {str(e)}"
    
    async def save_user_credentials(self, user_id: int, api_id: str, api_hash: str) -> Tuple[bool, str]:
        """Сохранить зашифрованные credentials пользователя"""
        try:
            logger.info(f"🔐 Сохранение credentials для пользователя {user_id}")
            
            # Валидация
            is_valid, message = self.validate_api_credentials(api_id, api_hash)
            if not is_valid:
                return False, message
            
            # Тестирование подключения с таймаутом
            is_connected, test_message = await self.test_connection(api_id, api_hash)
            if not is_connected:
                return False, f"❌ Не удалось подключиться: {test_message}"
            
            # Шифрование
            api_id_encrypted = self.encrypt_data(api_id.strip())
            api_hash_encrypted = self.encrypt_data(api_hash.strip())
            session_file = f"user_session_{user_id}"
            
            # Сохранение в БД (используем прямой импорт здесь для избежания циклических импортов)
            try:
                from database import db_manager
                await db_manager.save_user_credentials(
                    user_id, api_id_encrypted, api_hash_encrypted, session_file
                )
            except ImportError:
                logger.warning("⚠️ database module недоступен, credentials не сохранены в БД")
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения в БД: {e}")
                return False, f"❌ Ошибка сохранения в базу данных: {str(e)}"
            
            self.session_stats['created'] += 1
            logger.info(f"✅ Credentials сохранены для пользователя {user_id}")
            return True, "✅ Credentials сохранены и проверены!"
            
        except Exception as e:
            self.session_stats['failed'] += 1
            logger.error(f"❌ Ошибка сохранения credentials для {user_id}: {e}")
            return False, f"❌ Ошибка сохранения: {str(e)}"
    
    async def test_connection(self, api_id: str, api_hash: str, timeout: int = 15) -> Tuple[bool, str]:
        """Тестирование подключения с credentials с улучшенной обработкой ошибок"""
        temp_session = f"temp_test_{datetime.now().timestamp()}"
        client = None
        
        try:
            logger.debug(f"🔍 Тестирование подключения с API_ID: {api_id}")
            
            # Создаем временный клиент для тестирования
            client = TelegramClient(
                temp_session, 
                int(api_id), 
                api_hash,
                device_model="Topics Scanner Bot Test",
                system_version="1.0",
                app_version="4.1.0",
                timeout=timeout
            )
            
            # Пытаемся подключиться с таймаутом
            try:
                await asyncio.wait_for(client.connect(), timeout=timeout)
            except asyncio.TimeoutError:
                return False, "Таймаут подключения"
            
            if not await client.is_user_authorized():
                # Для тестирования credentials это нормально - главное что API credentials валидны
                logger.debug("✅ API credentials валидны (пользователь не авторизован)")
                return True, "✅ API credentials валидны"
            else:
                # Пользователь уже авторизован - отлично!
                logger.debug("✅ API credentials валидны (пользователь авторизован)")
                return True, "✅ Подключение успешно"
                
        except ApiIdInvalidError:
            return False, "Неверный API_ID"
        except FloodWaitError as e:
            return False, f"Слишком много попыток, подождите {e.seconds} секунд"
        except Exception as e:
            logger.debug(f"Ошибка тестирования подключения: {e}")
            return False, f"Ошибка подключения: {str(e)}"
        finally:
            # Принудительная очистка ресурсов
            if client:
                try:
                    if client.is_connected():
                        await asyncio.wait_for(client.disconnect(), timeout=5)
                except Exception as e:
                    logger.debug(f"Ошибка отключения тестового клиента: {e}")
            
            # Удаляем временный session файл
            try:
                session_files = [
                    f"{temp_session}.session",
                    f"{temp_session}.session-journal"
                ]
                for session_file in session_files:
                    if os.path.exists(session_file):
                        os.remove(session_file)
            except Exception as e:
                logger.debug(f"Ошибка удаления временного файла сессии: {e}")
    
    async def get_user_session(self, user_id: int) -> Optional[TelegramClient]:
        """Получить активную сессию пользователя с улучшенной обработкой"""
        try:
            # Если сессия уже активна, проверяем её состояние
            if user_id in self.active_sessions:
                client = self.active_sessions[user_id]
                try:
                    if client.is_connected():
                        # Проверяем, что сессия действительно работает
                        await asyncio.wait_for(client.get_me(), timeout=5)
                        return client
                    else:
                        logger.warning(f"⚠️ Сессия пользователя {user_id} отключена")
                        await self.close_user_session(user_id)
                except Exception as e:
                    logger.warning(f"⚠️ Проблема с сессией пользователя {user_id}: {e}")
                    await self.close_user_session(user_id)
            
            # Проверяем лимит активных сессий
            if len(self.active_sessions) >= MAX_CONCURRENT_SESSIONS:
                logger.warning(f"⚠️ Превышен лимит сессий ({MAX_CONCURRENT_SESSIONS})")
                return None
            
            # Создаем новую сессию
            return await self.create_user_session(user_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения сессии для {user_id}: {e}")
            return None
    
    async def create_user_session(self, user_id: int) -> Optional[TelegramClient]:
        """Создать новую пользовательскую сессию с улучшенной безопасностью"""
        try:
            # Получаем блокировку для пользователя
            if user_id not in self.session_locks:
                self.session_locks[user_id] = asyncio.Lock()
            
            async with self.session_locks[user_id]:
                # Проверяем, не создана ли сессия уже (double-check)
                if user_id in self.active_sessions:
                    return self.active_sessions[user_id]
                
                logger.info(f"🔄 Создание новой сессии для пользователя {user_id}")
                
                # Получаем credentials из БД
                try:
                    from database import db_manager
                    user_data = await db_manager.get_user(user_id)
                except ImportError:
                    logger.error("❌ database module недоступен")
                    return None
                except Exception as e:
                    logger.error(f"❌ Ошибка получения пользователя из БД: {e}")
                    return None
                
                if not user_data or user_data['mode'] != 'user':
                    logger.warning(f"⚠️ Пользователь {user_id} не в user режиме")
                    return None
                
                if not user_data['api_id_encrypted'] or not user_data['api_hash_encrypted']:
                    logger.warning(f"⚠️ Нет credentials для пользователя {user_id}")
                    return None
                
                # Расшифровываем credentials
                try:
                    api_id = int(self.decrypt_data(user_data['api_id_encrypted']))
                    api_hash = self.decrypt_data(user_data['api_hash_encrypted'])
                    session_file = user_data['session_file'] or f"user_session_{user_id}"
                except Exception as e:
                    logger.error(f"❌ Ошибка расшифровки credentials для {user_id}: {e}")
                    try:
                        await db_manager.update_user_status(user_id, 'error')
                    except:
                        pass
                    return None
                
                # Создаем клиент
                client = TelegramClient(
                    session_file,
                    api_id,
                    api_hash,
                    device_model="Topics Scanner Bot",
                    system_version="1.0",
                    app_version="4.1.0",
                    timeout=30
                )
                
                # Подключаемся с таймаутом
                try:
                    await asyncio.wait_for(client.connect(), timeout=30)
                except asyncio.TimeoutError:
                    logger.error(f"❌ Таймаут подключения для пользователя {user_id}")
                    await client.disconnect()
                    return None
                
                # Проверяем авторизацию
                if not await client.is_user_authorized():
                    logger.error(f"❌ Пользователь {user_id} не авторизован")
                    await client.disconnect()
                    try:
                        await db_manager.update_user_status(user_id, 'error')
                    except:
                        pass
                    return None
                
                # Сохраняем активную сессию
                self.active_sessions[user_id] = client
                self.session_stats['active'] = len(self.active_sessions)
                
                try:
                    await db_manager.update_user_status(user_id, 'active')
                except:
                    pass
                
                logger.info(f"✅ Создана сессия для пользователя {user_id}")
                return client
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания сессии для {user_id}: {e}")
            try:
                from database import db_manager
                await db_manager.update_user_status(user_id, 'error')
            except:
                pass
            return None
    
    async def close_user_session(self, user_id: int) -> bool:
        """Закрыть пользовательскую сессию с улучшенной очисткой"""
        try:
            logger.info(f"🔄 Закрытие сессии пользователя {user_id}")
            
            if user_id in self.active_sessions:
                client = self.active_sessions[user_id]
                try:
                    if client.is_connected():
                        await asyncio.wait_for(client.disconnect(), timeout=10)
                except Exception as e:
                    logger.debug(f"Ошибка отключения клиента {user_id}: {e}")
                
                del self.active_sessions[user_id]
                self.session_stats['active'] = len(self.active_sessions)
            
            if user_id in self.session_locks:
                del self.session_locks[user_id]
                
            logger.info(f"✅ Сессия пользователя {user_id} закрыта")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка закрытия сессии {user_id}: {e}")
            return False
    
    async def close_all_sessions(self):
        """Закрыть все активные сессии с улучшенной обработкой"""
        logger.info("🔄 Закрытие всех активных сессий...")
        
        # Создаем список пользователей для безопасного итерирования
        user_ids = list(self.active_sessions.keys())
        
        # Закрываем сессии параллельно с ограничением времени
        close_tasks = []
        for user_id in user_ids:
            task = asyncio.create_task(self.close_user_session(user_id))
            close_tasks.append(task)
        
        if close_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*close_tasks, return_exceptions=True),
                    timeout=30
                )
            except asyncio.TimeoutError:
                logger.warning("⚠️ Таймаут при закрытии сессий")
        
        # Принудительная очистка
        self.active_sessions.clear()
        self.session_locks.clear()
        self.session_stats['active'] = 0
        
        logger.info("✅ Все сессии закрыты")
    
    async def cleanup_expired_sessions(self) -> int:
        """Очистка истекших сессий с улучшенной логикой"""
        cleaned = 0
        
        try:
            logger.info("🧹 Проверка истекших сессий...")
            
            # Обновляем статусы в БД
            try:
                from database import db_manager
                expired_count = await db_manager.cleanup_expired_users()
                logger.info(f"📊 Найдено {expired_count} истекших пользователей в БД")
            except ImportError:
                logger.warning("⚠️ database module недоступен для очистки")
                return 0
            except Exception as e:
                logger.error(f"❌ Ошибка очистки БД: {e}")
                return 0
            
            # Закрываем активные сессии истекших пользователей
            try:
                expired_users = await db_manager.get_users_by_mode('user')
                for user_data in expired_users:
                    if user_data['status'] == 'expired':
                        user_id = user_data['user_id']
                        if user_id in self.active_sessions:
                            await self.close_user_session(user_id)
                            cleaned += 1
                            self.session_stats['expired'] += 1
            except Exception as e:
                logger.error(f"❌ Ошибка закрытия истекших сессий: {e}")
            
            if cleaned > 0:
                logger.info(f"🧹 Очищено {cleaned} истекших сессий")
                self.session_stats['active'] = len(self.active_sessions)
            
            return cleaned
            
        except Exception as e:
            logger.error(f"❌ Ошибка очистки сессий: {e}")
            return 0
    
    async def get_session_info(self, user_id: int) -> Dict[str, Any]:
        """Получить информацию о сессии пользователя с расширенными данными"""
        try:
            from database import db_manager
            user_data = await db_manager.get_user(user_id)
        except ImportError:
            user_data = None
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных пользователя: {e}")
            user_data = None
        
        if not user_data:
            return {'status': 'not_found'}
        
        info = {
            'user_id': user_id,
            'mode': user_data['mode'],
            'status': user_data['status'],
            'created_at': user_data['created_at'],
            'last_active': user_data['last_active'],
            'has_credentials': bool(user_data.get('api_id_encrypted')),
            'is_session_active': user_id in self.active_sessions,
            'session_file': user_data.get('session_file'),
            'telegram_user': None
        }
        
        if user_id in self.active_sessions:
            client = self.active_sessions[user_id]
            info['is_connected'] = client.is_connected()
            
            try:
                # Получаем информацию о текущем пользователе с таймаутом
                me = await asyncio.wait_for(client.get_me(), timeout=10)
                info['telegram_user'] = {
                    'id': me.id,
                    'username': me.username,
                    'first_name': me.first_name,
                    'phone': me.phone
                }
            except Exception as e:
                logger.debug(f"Ошибка получения информации о пользователе {user_id}: {e}")
                info['telegram_user'] = None
        
        return info
    
    async def logout_user(self, user_id: int) -> Tuple[bool, str]:
        """Выйти из пользовательского режима с полной очисткой"""
        try:
            logger.info(f"🔄 Logout пользователя {user_id}")
            
            # Закрываем сессию
            await self.close_user_session(user_id)
            
            # Удаляем credentials из БД
            try:
                from database import db_manager
                user_data = await db_manager.get_user(user_id)
                if user_data:
                    await db_manager.create_or_update_user(
                        user_id, 
                        user_data['telegram_username'],
                        user_data['first_name'],
                        'bot'  # Переключаем в режим бота
                    )
            except ImportError:
                logger.warning("⚠️ database module недоступен для logout")
            except Exception as e:
                logger.error(f"❌ Ошибка обновления БД при logout: {e}")
            
            # Удаляем session файлы
            try:
                session_files = [
                    f"user_session_{user_id}.session",
                    f"user_session_{user_id}.session-journal"
                ]
                for session_file in session_files:
                    if os.path.exists(session_file):
                        os.remove(session_file)
                        logger.debug(f"🗑️ Удален файл {session_file}")
            except Exception as e:
                logger.debug(f"Ошибка удаления session файлов: {e}")
            
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
            'session_details': {},
            'stats': self.session_stats.copy()
        }
        
        # Проверяем каждую активную сессию
        check_tasks = []
        for user_id, client in list(self.active_sessions.items()):
            task = asyncio.create_task(self._check_session_health(user_id, client))
            check_tasks.append(task)
        
        if check_tasks:
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*check_tasks, return_exceptions=True),
                    timeout=30
                )
                
                for result in results:
                    if isinstance(result, dict):
                        user_id = result['user_id']
                        is_healthy = result['is_healthy']
                        status = result['status']
                        
                        if is_healthy:
                            health_info['healthy_sessions'] += 1
                        else:
                            health_info['unhealthy_sessions'] += 1
                        
                        health_info['session_details'][user_id] = status
                    elif isinstance(result, Exception):
                        logger.debug(f"Ошибка проверки сессии: {result}")
                        health_info['unhealthy_sessions'] += 1
                        
            except asyncio.TimeoutError:
                logger.warning("⚠️ Таймаут при проверке здоровья сессий")
        
        # Обновляем статистику
        self.session_stats['active'] = len(self.active_sessions)
        
        return health_info
    
    async def _check_session_health(self, user_id: int, client: TelegramClient) -> Dict[str, Any]:
        """Проверка здоровья отдельной сессии"""
        try:
            is_connected = client.is_connected()
            if is_connected:
                # Попробуем выполнить простой запрос
                await asyncio.wait_for(client.get_me(), timeout=10)
                return {
                    'user_id': user_id,
                    'is_healthy': True,
                    'status': 'healthy'
                }
            else:
                # Сессия отключена
                await self.close_user_session(user_id)
                return {
                    'user_id': user_id,
                    'is_healthy': False,
                    'status': 'disconnected'
                }
                
        except Exception as e:
            # Проблемная сессия
            await self.close_user_session(user_id)
            return {
                'user_id': user_id,
                'is_healthy': False,
                'status': f'error: {str(e)[:50]}'
            }
    
    def get_encryption_info(self) -> Dict[str, str]:
        """Получить информацию о шифровании (для отладки)"""
        return {
            'cipher_available': bool(self.cipher),
            'key_length': len(ENCRYPTION_KEY),
            'salt_length': len(SALT),
            'algorithm': 'Fernet (AES 128 + HMAC SHA256)',
            'session_timeout_days': SESSION_TIMEOUT_DAYS
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить статистику работы auth manager"""
        return {
            'session_stats': self.session_stats.copy(),
            'active_sessions': len(self.active_sessions),
            'max_sessions': MAX_CONCURRENT_SESSIONS,
            'session_locks': len(self.session_locks)
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