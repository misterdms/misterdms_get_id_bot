#!/usr/bin/env python3
"""
Управление базой данных для Get ID Bot by Mister DMS
ОБНОВЛЕНО v4.1.1: работа с общей misterdms-bots-db + статистика + префиксы
"""

import aiosqlite
import asyncio
import logging
import urllib.parse as urlparse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from contextlib import asynccontextmanager
import json

from config import DATABASE_URL, BOT_PREFIX
from utils import JSONUtils, PerformanceUtils

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Менеджер базы данных с поддержкой общей misterdms-bots-db + статистика"""
    
    def __init__(self, database_url: str = DATABASE_URL, bot_prefix: str = BOT_PREFIX):
        # Валидация и fallback
        if not database_url or 'user:password@host' in database_url:
            logger.warning("⚠️ Некорректный DATABASE_URL, переключение на SQLite")
            database_url = 'sqlite:///bot_data.db'
        
        self.database_url = database_url
        self.bot_prefix = bot_prefix.lower()
        self.db_type = 'sqlite' if database_url.startswith('sqlite') else 'postgresql'
        
        # Имена таблиц с префиксами для изоляции в общей БД
        self.tables = {
            'users': f"{self.bot_prefix}_users",
            'activity_data': f"{self.bot_prefix}_activity_data",
            'request_queue': f"{self.bot_prefix}_request_queue",
            'bot_settings': f"{self.bot_prefix}_bot_settings",
            'bot_logs': f"{self.bot_prefix}_bot_logs",
            'command_stats': f"{self.bot_prefix}_command_stats",
            'user_sessions': f"{self.bot_prefix}_user_sessions",
            'performance_metrics': f"{self.bot_prefix}_performance_metrics"
        }
        
        self._connection_pool = None
        logger.info(f"🗄️ DatabaseManager инициализирован: {self.db_type} (префикс: {self.bot_prefix})")
    
    async def initialize(self):
        """Инициализация базы данных и создание таблиц"""
        logger.info(f"🗄️ Инициализация базы данных: {self.db_type}")
        
        try:
            await self.create_tables()
            await self.create_indexes()
            await self.cleanup_old_data()
            logger.info("✅ База данных инициализирована с префиксами")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            
            # Автофоллбэк на SQLite при проблемах с PostgreSQL
            if self.db_type == 'postgresql' and ('hostname' in str(e) or 'address' in str(e)):
                logger.warning("🔄 Автофоллбэк на SQLite из-за проблем с PostgreSQL")
                self.database_url = 'sqlite:///bot_data.db'
                self.db_type = 'sqlite'
                
                try:
                    await self.create_tables()
                    await self.create_indexes()
                    logger.info("✅ База данных инициализирована на SQLite (fallback)")
                    return
                except Exception as sqlite_error:
                    logger.error(f"❌ Ошибка инициализации SQLite fallback: {sqlite_error}")
            
            raise
    
    @asynccontextmanager
    async def get_connection(self):
        """Контекстный менеджер для получения соединения с БД"""
        if self.db_type == 'sqlite':
            db_path = self.database_url.replace('sqlite:///', '')
            async with aiosqlite.connect(db_path) as conn:
                conn.row_factory = aiosqlite.Row
                yield conn
        else:
            # PostgreSQL поддержка для misterdms-bots-db
            try:
                import asyncpg
            except ImportError:
                logger.error("❌ asyncpg не установлен! Установите: pip install asyncpg")
                raise
            
            url = urlparse.urlparse(self.database_url)
            
            conn = None
            try:
                # Проверяем валидность URL
                if not url.hostname or url.hostname in ['host', 'localhost', 'example.com']:
                    raise ValueError(f"Invalid DATABASE_URL hostname: {url.hostname}")

                conn = await asyncpg.connect(
                    host=url.hostname,
                    port=url.port or 5432,
                    user=url.username,
                    password=url.password,
                    database=url.path[1:] if url.path else 'postgres',
                    timeout=10
                )
                yield conn
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
                if 'No address associated with hostname' in str(e):
                    logger.error("🔧 Проверьте DATABASE_URL в переменных окружения")
                raise
            finally:
                if conn:
                    await conn.close()
    
    async def create_tables(self):
        """Создание всех таблиц с префиксами"""
        
        # SQL схемы для разных типов БД
        if self.db_type == 'sqlite':
            await self._create_sqlite_tables()
        else:
            await self._create_postgresql_tables()
    
    async def _create_sqlite_tables(self):
        """Создание таблиц для SQLite"""
        async with self.get_connection() as conn:
            
            # Таблица пользователей
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['users']} (
                    user_id INTEGER PRIMARY KEY,
                    telegram_username TEXT,
                    first_name TEXT,
                    mode TEXT DEFAULT 'bot',
                    api_id_encrypted TEXT,
                    api_hash_encrypted TEXT,
                    session_file TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'active',
                    security_level TEXT DEFAULT 'normal',
                    total_commands INTEGER DEFAULT 0,
                    favorite_command TEXT DEFAULT 'scan'
                )
            """)
            
            # Таблица активности
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['activity_data']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    message_count INTEGER DEFAULT 1,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_tracked DATE DEFAULT CURRENT_DATE,
                    topics_scanned INTEGER DEFAULT 0,
                    command_type TEXT DEFAULT 'scan'
                )
            """)
            
            # Таблица очереди запросов
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['request_queue']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER,
                    command TEXT NOT NULL,
                    priority INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    result TEXT,
                    correlation_id TEXT
                )
            """)
            
            # Таблица статистики команд
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['command_stats']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    command TEXT NOT NULL,
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN DEFAULT TRUE,
                    execution_time_ms INTEGER,
                    chat_type TEXT DEFAULT 'private',
                    error_message TEXT
                )
            """)
            
            # Таблица настроек бота
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['bot_settings']} (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    description TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_by INTEGER
                )
            """)
            
            # Таблица логов
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['bot_logs']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    user_id INTEGER,
                    chat_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    module TEXT,
                    function_name TEXT
                )
            """)
            
            # Таблица пользовательских сессий
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['user_sessions']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    session_type TEXT DEFAULT 'bot',
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'active',
                    metadata TEXT
                )
            """)
            
            # Таблица метрик производительности
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['performance_metrics']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tags TEXT,
                    user_id INTEGER
                )
            """)
            
            await conn.commit()
    
    async def _create_postgresql_tables(self):
        """Создание таблиц для PostgreSQL"""
        async with self.get_connection() as conn:
            
            # Таблица пользователей
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['users']} (
                    user_id BIGINT PRIMARY KEY,
                    telegram_username TEXT,
                    first_name TEXT,
                    mode TEXT DEFAULT 'bot',
                    api_id_encrypted TEXT,
                    api_hash_encrypted TEXT,
                    session_file TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'active',
                    security_level TEXT DEFAULT 'normal',
                    total_commands INTEGER DEFAULT 0,
                    favorite_command TEXT DEFAULT 'scan'
                )
            """)
            
            # Остальные таблицы аналогично SQLite, но с BIGINT для ID
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['activity_data']} (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    message_count INTEGER DEFAULT 1,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_tracked DATE DEFAULT CURRENT_DATE,
                    topics_scanned INTEGER DEFAULT 0,
                    command_type TEXT DEFAULT 'scan'
                )
            """)
            
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['request_queue']} (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT,
                    command TEXT NOT NULL,
                    priority INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    result TEXT,
                    correlation_id TEXT
                )
            """)
            
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['command_stats']} (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    command TEXT NOT NULL,
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN DEFAULT TRUE,
                    execution_time_ms INTEGER,
                    chat_type TEXT DEFAULT 'private',
                    error_message TEXT
                )
            """)
            
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['bot_settings']} (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    description TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_by BIGINT
                )
            """)
            
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['bot_logs']} (
                    id SERIAL PRIMARY KEY,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    user_id BIGINT,
                    chat_id BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    module TEXT,
                    function_name TEXT
                )
            """)
            
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['user_sessions']} (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    session_type TEXT DEFAULT 'bot',
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'active',
                    metadata TEXT
                )
            """)
            
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.tables['performance_metrics']} (
                    id SERIAL PRIMARY KEY,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tags TEXT,
                    user_id BIGINT
                )
            """)
    
    async def create_indexes(self):
        """Создание индексов для оптимизации"""
        async with self.get_connection() as conn:
            try:
                if self.db_type == 'sqlite':
                    # SQLite индексы
                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_users_last_active ON {self.tables['users']} (last_active)")
                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_activity_chat_user ON {self.tables['activity_data']} (chat_id, user_id)")
                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_command_stats_user ON {self.tables['command_stats']} (user_id, command)")
                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_request_queue_status ON {self.tables['request_queue']} (status)")
                    await conn.commit()
                else:
                    # PostgreSQL индексы
                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_users_last_active ON {self.tables['users']} (last_active)")
                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_activity_chat_user ON {self.tables['activity_data']} (chat_id, user_id)")
                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_command_stats_user ON {self.tables['command_stats']} (user_id, command)")
                    await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_request_queue_status ON {self.tables['request_queue']} (status)")
                
                logger.debug("✅ Индексы созданы")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось создать индексы: {e}")
    
    # === МЕТОДЫ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ===
    
    @PerformanceUtils.measure_time
    async def save_user(self, user_id: int, username: str = None, first_name: str = None, mode: str = 'bot'):
        """Сохранение или обновление пользователя"""
        async with self.get_connection() as conn:
            try:
                if self.db_type == 'sqlite':
                    await conn.execute(f"""
                        INSERT OR REPLACE INTO {self.tables['users']} 
                        (user_id, telegram_username, first_name, mode, last_active)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (user_id, username, first_name, mode))
                    await conn.commit()
                else:
                    await conn.execute(f"""
                        INSERT INTO {self.tables['users']} 
                        (user_id, telegram_username, first_name, mode, last_active)
                        VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                        ON CONFLICT (user_id) DO UPDATE SET
                            telegram_username = EXCLUDED.telegram_username,
                            first_name = EXCLUDED.first_name,
                            last_active = CURRENT_TIMESTAMP
                    """, user_id, username, first_name, mode)
                
                logger.debug(f"✅ Пользователь {user_id} сохранен")
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения пользователя {user_id}: {e}")
    
    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получение данных пользователя"""
        async with self.get_connection() as conn:
            try:
                if self.db_type == 'sqlite':
                    async with conn.execute(f"""
                        SELECT * FROM {self.tables['users']} WHERE user_id = ?
                    """, (user_id,)) as cursor:
                        row = await cursor.fetchone()
                else:
                    row = await conn.fetchrow(f"""
                        SELECT * FROM {self.tables['users']} WHERE user_id = $1
                    """, user_id)
                
                if row:
                    return dict(row) if self.db_type == 'sqlite' else dict(row)
                return None
                
            except Exception as e:
                logger.error(f"❌ Ошибка получения пользователя {user_id}: {e}")
                return None
    
    async def update_user_mode(self, user_id: int, mode: str):
        """Обновление режима пользователя"""
        async with self.get_connection() as conn:
            try:
                if self.db_type == 'sqlite':
                    await conn.execute(f"""
                        UPDATE {self.tables['users']} 
                        SET mode = ?, last_active = CURRENT_TIMESTAMP
                        WHERE user_id = ?
                    """, (mode, user_id))
                    await conn.commit()
                else:
                    await conn.execute(f"""
                        UPDATE {self.tables['users']} 
                        SET mode = $1, last_active = CURRENT_TIMESTAMP
                        WHERE user_id = $2
                    """, mode, user_id)
                
                logger.debug(f"✅ Режим пользователя {user_id} обновлен: {mode}")
            except Exception as e:
                logger.error(f"❌ Ошибка обновления режима пользователя {user_id}: {e}")
    
    async def save_user_credentials(self, user_id: int, encrypted_api_id: str, encrypted_api_hash: str, group_link: str = None):
        """Сохранение зашифрованных credentials пользователя"""
        async with self.get_connection() as conn:
            try:
                if self.db_type == 'sqlite':
                    await conn.execute(f"""
                        UPDATE {self.tables['users']} 
                        SET api_id_encrypted = ?, api_hash_encrypted = ?, 
                            session_file = ?, last_active = CURRENT_TIMESTAMP
                        WHERE user_id = ?
                    """, (encrypted_api_id, encrypted_api_hash, group_link, user_id))
                    await conn.commit()
                else:
                    await conn.execute(f"""
                        UPDATE {self.tables['users']} 
                        SET api_id_encrypted = $1, api_hash_encrypted = $2, 
                            session_file = $3, last_active = CURRENT_TIMESTAMP
                        WHERE user_id = $4
                    """, encrypted_api_id, encrypted_api_hash, group_link, user_id)
                
                logger.debug(f"✅ Credentials пользователя {user_id} сохранены")
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения credentials пользователя {user_id}: {e}")
    
    # === МЕТОДЫ ДЛЯ СТАТИСТИКИ ===
    
    async def log_command_usage(self, user_id: int, command: str, success: bool = True, 
                              execution_time_ms: int = None, chat_type: str = 'private', 
                              error_message: str = None):
        """Логирование использования команд"""
        async with self.get_connection() as conn:
            try:
                if self.db_type == 'sqlite':
                    await conn.execute(f"""
                        INSERT INTO {self.tables['command_stats']} 
                        (user_id, command, success, execution_time_ms, chat_type, error_message)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (user_id, command, success, execution_time_ms, chat_type, error_message))
                    await conn.commit()
                else:
                    await conn.execute(f"""
                        INSERT INTO {self.tables['command_stats']} 
                        (user_id, command, success, execution_time_ms, chat_type, error_message)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """, user_id, command, success, execution_time_ms, chat_type, error_message)
                
                # Обновляем счетчик команд пользователя
                await self._update_user_command_count(user_id, command)
                
            except Exception as e:
                logger.debug(f"Ошибка логирования команды: {e}")
    
    async def _update_user_command_count(self, user_id: int, command: str):
        """Обновление счетчика команд пользователя"""
        async with self.get_connection() as conn:
            try:
                if self.db_type == 'sqlite':
                    await conn.execute(f"""
                        UPDATE {self.tables['users']} 
                        SET total_commands = total_commands + 1,
                            favorite_command = ?,
                            last_active = CURRENT_TIMESTAMP
                        WHERE user_id = ?
                    """, (command, user_id))
                    await conn.commit()
                else:
                    await conn.execute(f"""
                        UPDATE {self.tables['users']} 
                        SET total_commands = total_commands + 1,
                            favorite_command = $1,
                            last_active = CURRENT_TIMESTAMP
                        WHERE user_id = $2
                    """, command, user_id)
            except Exception as e:
                logger.debug(f"Ошибка обновления счетчика команд: {e}")
    
    async def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """Получение статистики пользователя"""
        async with self.get_connection() as conn:
            try:
                if self.db_type == 'sqlite':
                    async with conn.execute(f"""
                        SELECT 
                            COUNT(*) as total_commands,
                            COUNT(CASE WHEN success = 1 THEN 1 END) as successful_commands,
                            AVG(execution_time_ms) as avg_execution_time,
                            command as favorite_command,
                            COUNT(*) as command_count
                        FROM {self.tables['command_stats']} 
                        WHERE user_id = ?
                        GROUP BY command
                        ORDER BY command_count DESC
                        LIMIT 1
                    """, (user_id,)) as cursor:
                        row = await cursor.fetchone()
                else:
                    row = await conn.fetchrow(f"""
                        SELECT 
                            COUNT(*) as total_commands,
                            COUNT(CASE WHEN success = true THEN 1 END) as successful_commands,
                            AVG(execution_time_ms) as avg_execution_time,
                            command as favorite_command,
                            COUNT(*) as command_count
                        FROM {self.tables['command_stats']} 
                        WHERE user_id = $1
                        GROUP BY command
                        ORDER BY command_count DESC
                        LIMIT 1
                    """, user_id)
                
                if row:
                    return dict(row)
                
                return {
                    'total_commands': 0,
                    'successful_commands': 0,
                    'avg_execution_time': 0,
                    'favorite_command': 'scan'
                }
                
            except Exception as e:
                logger.error(f"❌ Ошибка получения статистики пользователя {user_id}: {e}")
                return {}
    
    async def get_bot_stats(self) -> Dict[str, Any]:
        """Получение общей статистики бота"""
        async with self.get_connection() as conn:
            try:
                stats = {}
                
                # Общее количество пользователей
                if self.db_type == 'sqlite':
                    async with conn.execute(f"SELECT COUNT(*) FROM {self.tables['users']}") as cursor:
                        row = await cursor.fetchone()
                        stats['total_users'] = row[0] if row else 0
                else:
                    row = await conn.fetchval(f"SELECT COUNT(*) FROM {self.tables['users']}")
                    stats['total_users'] = row or 0
                
                # Активные пользователи за последние 24 часа
                if self.db_type == 'sqlite':
                    async with conn.execute(f"""
                        SELECT COUNT(*) FROM {self.tables['users']} 
                        WHERE last_active > datetime('now', '-1 day')
                    """) as cursor:
                        row = await cursor.fetchone()
                        stats['active_users_24h'] = row[0] if row else 0
                else:
                    row = await conn.fetchval(f"""
                        SELECT COUNT(*) FROM {self.tables['users']} 
                        WHERE last_active > NOW() - INTERVAL '1 day'
                    """)
                    stats['active_users_24h'] = row or 0
                
                # Общее количество команд
                if self.db_type == 'sqlite':
                    async with conn.execute(f"SELECT COUNT(*) FROM {self.tables['command_stats']}") as cursor:
                        row = await cursor.fetchone()
                        stats['total_commands'] = row[0] if row else 0
                else:
                    row = await conn.fetchval(f"SELECT COUNT(*) FROM {self.tables['command_stats']}")
                    stats['total_commands'] = row or 0
                
                return stats
                
            except Exception as e:
                logger.error(f"❌ Ошибка получения статистики бота: {e}")
                return {}
    
    # === МЕТОДЫ ОЧИСТКИ ===
    
    async def cleanup_old_data(self):
        """Очистка старых данных"""
        async with self.get_connection() as conn:
            try:
                # Удаляем логи старше 30 дней
                if self.db_type == 'sqlite':
                    await conn.execute(f"""
                        DELETE FROM {self.tables['bot_logs']} 
                        WHERE created_at < datetime('now', '-30 days')
                    """)
                    
                    # Удаляем старые метрики производительности
                    await conn.execute(f"""
                        DELETE FROM {self.tables['performance_metrics']} 
                        WHERE timestamp < datetime('now', '-7 days')
                    """)
                    
                    await conn.commit()
                else:
                    await conn.execute(f"""
                        DELETE FROM {self.tables['bot_logs']} 
                        WHERE created_at < NOW() - INTERVAL '30 days'
                    """)
                    
                    await conn.execute(f"""
                        DELETE FROM {self.tables['performance_metrics']} 
                        WHERE timestamp < NOW() - INTERVAL '7 days'
                    """)
                
                logger.debug("✅ Старые данные очищены")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка очистки старых данных: {e}")
    
    async def close(self):
        """Закрытие соединений с БД"""
        try:
            # Для asyncpg pool будет здесь логика закрытия
            logger.info("✅ База данных корректно закрыта")
        except Exception as e:
            logger.error(f"❌ Ошибка закрытия БД: {e}")

    # === ЗАГЛУШКИ ДЛЯ БУДУЩИХ МЕТОДОВ ===
    
    async def add_to_queue(self, user_id: int, chat_id: int, command: str, priority: int = 1):
        """Добавление задачи в очередь"""
        # TODO: Реализовать
        pass
    
    async def get_queue_status(self) -> Dict[str, Any]:
        """Получение статуса очереди"""
        # TODO: Реализовать
        return {'pending': 0, 'processing': 0, 'completed': 0}
    
    async def save_activity_data(self, chat_id: int, user_id: int, username: str = None, 
                               first_name: str = None, message_count: int = 1):
        """Сохранение данных активности"""
        # TODO: Реализовать
        pass