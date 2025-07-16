#!/usr/bin/env python3
"""
Управление базой данных для гибридного Topics Scanner Bot
Поддерживает SQLite и PostgreSQL с асинхронными операциями
ИСПРАВЛЕНО: PostgreSQL запросы, импорты, валидация, fallback на SQLite
"""

import aiosqlite
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
import json
import urllib.parse as urlparse

# Безопасные импорты
try:
    from config import DATABASE_URL, SESSION_TIMEOUT_DAYS, USER_STATUSES, TASK_STATUSES, BOT_PREFIX
except ImportError:
    # Fallback значения если config недоступен
    DATABASE_URL = 'sqlite:///bot_data.db'
    SESSION_TIMEOUT_DAYS = 7
    USER_STATUSES = ['active', 'expired', 'error', 'blocked', 'pending']
    TASK_STATUSES = ['pending', 'processing', 'completed', 'failed', 'cancelled']
    BOT_PREFIX = 'get_id_bot'

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Менеджер базы данных с поддержкой SQLite и PostgreSQL + префиксы таблиц"""
    
    def __init__(self, database_url: str = DATABASE_URL, bot_prefix: str = BOT_PREFIX):
        # Валидация DATABASE_URL с улучшенной проверкой
        if (not database_url or 
            'user:password@host' in database_url or 
            'presave_user:password@localhost' in database_url or
            database_url == 'postgresql://user:password@host:5432/dbname' or
            'example.com' in database_url):
            logger.warning("⚠️ Некорректный DATABASE_URL, переключение на SQLite")
            database_url = 'sqlite:///bot_data.db'
        
        self.database_url = database_url
        self.bot_prefix = bot_prefix.lower()
        self.db_type = 'sqlite' if database_url.startswith('sqlite') else 'postgresql'
        
        # Имена таблиц с префиксами
        self.tables = {
            'users': f"{self.bot_prefix}_users",
            'activity_data': f"{self.bot_prefix}_activity_data", 
            'request_queue': f"{self.bot_prefix}_request_queue",
            'bot_settings': f"{self.bot_prefix}_bot_settings",
            'bot_logs': f"{self.bot_prefix}_bot_logs"
        }
        
        logger.info(f"🗄️ DatabaseManager инициализирован: {self.db_type} с префиксом {self.bot_prefix}")
        
    async def initialize(self):
        """Инициализация базы данных и создание таблиц"""
        logger.info(f"🗄️ Инициализация базы данных: {self.db_type} (префикс: {self.bot_prefix})")
        
        try:
            await self.create_tables()
            await self.create_indexes()
            await self.migrate_existing_data()
            logger.info("✅ База данных инициализирована с префиксами")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            
            # Автофоллбэк на SQLite при проблемах с PostgreSQL
            if self.db_type == 'postgresql':
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
            # PostgreSQL поддержка
            try:
                import asyncpg
            except ImportError:
                logger.error("❌ asyncpg не установлен! Установите: pip install asyncpg")
                raise
            
            # Парсим DATABASE_URL
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
                    logger.error("💡 На Render.com получите правильный URL из PostgreSQL addon misterdms-bots-db")
                raise
            finally:
                if conn:
                    await conn.close()
    
    async def create_tables(self):
        """Создание всех необходимых таблиц с префиксами"""
        async with self.get_connection() as conn:
            if self.db_type == 'sqlite':
                await self._create_sqlite_tables(conn)
            else:
                await self._create_postgresql_tables(conn)
    
    async def _create_sqlite_tables(self, conn):
        """Создание таблиц для SQLite с префиксами"""
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
                status TEXT DEFAULT 'active'
            )
        """)
        
        # Таблица активности пользователей
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['activity_data']} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                message_count INTEGER DEFAULT 1,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date_tracked DATE DEFAULT CURRENT_DATE
            )
        """)
        
        # Таблица очереди запросов
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['request_queue']} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER,
                command TEXT NOT NULL,
                parameters TEXT,
                priority INTEGER DEFAULT 2,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                result TEXT,
                error_message TEXT
            )
        """)
        
        # Таблица настроек бота
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['bot_settings']} (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                command TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT
            )
        """)
        
        await conn.commit()
    
    async def _create_postgresql_tables(self, conn):
        """Создание таблиц для PostgreSQL с префиксами"""
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
                status TEXT DEFAULT 'active'
            )
        """)
        
        # Таблица активности пользователей
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['activity_data']} (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                message_count INTEGER DEFAULT 1,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date_tracked DATE DEFAULT CURRENT_DATE
            )
        """)
        
        # Таблица очереди запросов
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['request_queue']} (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT,
                command TEXT NOT NULL,
                parameters TEXT,
                priority INTEGER DEFAULT 2,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                result TEXT,
                error_message TEXT
            )
        """)
        
        # Таблица настроек бота
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['bot_settings']} (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица логов
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tables['bot_logs']} (
                id SERIAL PRIMARY KEY,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                user_id BIGINT,
                chat_id BIGINT,
                command TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT
            )
        """)
    
    async def create_indexes(self):
        """Создание индексов для оптимизации запросов с префиксами"""
        async with self.get_connection() as conn:
            indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_users_mode ON {self.tables['users']}(mode)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_users_status ON {self.tables['users']}(status)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_users_last_active ON {self.tables['users']}(last_active)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_activity_chat_id ON {self.tables['activity_data']}(chat_id)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_activity_date ON {self.tables['activity_data']}(date_tracked)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_queue_status ON {self.tables['request_queue']}(status)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_queue_priority ON {self.tables['request_queue']}(priority)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_queue_user ON {self.tables['request_queue']}(user_id)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_logs_timestamp ON {self.tables['bot_logs']}(timestamp)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.bot_prefix}_logs_user ON {self.tables['bot_logs']}(user_id)"
            ]
            
            for index_sql in indexes:
                try:
                    if self.db_type == 'sqlite':
                        await conn.execute(index_sql)
                    else:
                        await conn.execute(index_sql)
                except Exception as e:
                    logger.debug(f"Индекс уже существует: {e}")
            
            if self.db_type == 'sqlite':
                await conn.commit()
    
    async def migrate_existing_data(self):
        """Миграция данных из таблиц без префиксов (если они существуют)"""
        try:
            async with self.get_connection() as conn:
                old_tables = ['users', 'activity_data', 'request_queue', 'bot_settings', 'bot_logs']
                
                for old_table in old_tables:
                    new_table = self.tables[old_table]
                    
                    # Проверяем существование старой таблицы
                    if self.db_type == 'sqlite':
                        check_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{old_table}'"
                        cursor = await conn.execute(check_query)
                        exists = await cursor.fetchone()
                    else:
                        check_query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)"
                        exists = await conn.fetchval(check_query, old_table)
                    
                    if exists:
                        # Проверяем, есть ли данные в старой таблице
                        count_query = f"SELECT COUNT(*) FROM {old_table}"
                        if self.db_type == 'sqlite':
                            cursor = await conn.execute(count_query)
                            count = (await cursor.fetchone())[0]
                        else:
                            count = await conn.fetchval(count_query)
                        
                        if count > 0:
                            logger.info(f"🔄 Миграция данных: {old_table} → {new_table} ({count} записей)")
                            
                            # Копируем данные
                            copy_query = f"INSERT INTO {new_table} SELECT * FROM {old_table}"
                            
                            if self.db_type == 'sqlite':
                                await conn.execute(copy_query)
                                await conn.commit()
                            else:
                                await conn.execute(copy_query)
                            
                            # Удаляем старую таблицу
                            drop_query = f"DROP TABLE {old_table}"
                            if self.db_type == 'sqlite':
                                await conn.execute(drop_query)
                                await conn.commit()
                            else:
                                await conn.execute(drop_query)
                            
                            logger.info(f"✅ Миграция {old_table} завершена")
                
        except Exception as e:
            logger.debug(f"Миграция данных: {e} (нормально, если таблиц без префикса нет)")
    
    # === УНИВЕРСАЛЬНЫЕ МЕТОДЫ ДЛЯ ОБЕИХ БД ===
    
    async def _execute_query(self, query: str, params: tuple = None, fetch_one: bool = False, fetch_all: bool = False):
        """Универсальный метод выполнения запросов - ИСПРАВЛЕН"""
        async with self.get_connection() as conn:
            if self.db_type == 'sqlite':
                cursor = await conn.execute(query, params or ())
                
                if fetch_one:
                    result = await cursor.fetchone()
                    return dict(result) if result else None
                elif fetch_all:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
                else:
                    await conn.commit()
                    return cursor.rowcount
            else:
                # PostgreSQL - ИСПРАВЛЕНЫ ПАРАМЕТРЫ
                if fetch_one:
                    result = await conn.fetchrow(query, *(params or ()))
                    return dict(result) if result else None
                elif fetch_all:
                    rows = await conn.fetch(query, *(params or ()))
                    return [dict(row) for row in rows]
                else:
                    result = await conn.execute(query, *(params or ()))
                    # Извлекаем количество затронутых строк
                    if hasattr(result, 'split'):
                        # Для INSERT/UPDATE/DELETE результат выглядит как "INSERT 0 1"
                        parts = result.split()
                        return int(parts[-1]) if parts else 1
                    return 1
    
    # === УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ ===
    
    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получить информацию о пользователе"""
        if self.db_type == 'postgresql':
            query = f"SELECT * FROM {self.tables['users']} WHERE user_id = $1"
        else:
            query = f"SELECT * FROM {self.tables['users']} WHERE user_id = ?"
        
        return await self._execute_query(query, (user_id,), fetch_one=True)
    
    async def create_or_update_user(self, user_id: int, telegram_username: str = None, 
                                  first_name: str = None, mode: str = 'bot') -> bool:
        """Создать или обновить пользователя"""
        if self.db_type == 'postgresql':
            query = f"""
                INSERT INTO {self.tables['users']} (user_id, telegram_username, first_name, mode, last_active)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id) DO UPDATE SET
                    telegram_username = EXCLUDED.telegram_username,
                    first_name = EXCLUDED.first_name,
                    mode = EXCLUDED.mode,
                    last_active = EXCLUDED.last_active
            """
        else:
            query = f"""
                INSERT OR REPLACE INTO {self.tables['users']} 
                (user_id, telegram_username, first_name, mode, last_active)
                VALUES (?, ?, ?, ?, ?)
            """
        
        await self._execute_query(query, (user_id, telegram_username, first_name, mode, datetime.now()))
        return True
    
    async def save_user_credentials(self, user_id: int, api_id_encrypted: str, 
                                  api_hash_encrypted: str, session_file: str) -> bool:
        """Сохранить зашифрованные credentials пользователя"""
        if self.db_type == 'postgresql':
            query = f"""
                UPDATE {self.tables['users']} SET 
                    api_id_encrypted = $1,
                    api_hash_encrypted = $2,
                    session_file = $3,
                    mode = 'user',
                    status = 'active',
                    last_active = $4
                WHERE user_id = $5
            """
        else:
            query = f"""
                UPDATE {self.tables['users']} SET 
                    api_id_encrypted = ?,
                    api_hash_encrypted = ?,
                    session_file = ?,
                    mode = 'user',
                    status = 'active',
                    last_active = ?
                WHERE user_id = ?
            """
        
        await self._execute_query(query, (api_id_encrypted, api_hash_encrypted, session_file, datetime.now(), user_id))
        return True
    
    async def update_user_status(self, user_id: int, status: str) -> bool:
        """Обновить статус пользователя"""
        if status not in USER_STATUSES:
            raise ValueError(f"Неверный статус: {status}")
        
        if self.db_type == 'postgresql':
            query = f"""
                UPDATE {self.tables['users']} SET status = $1, last_active = $2
                WHERE user_id = $3
            """
        else:
            query = f"""
                UPDATE {self.tables['users']} SET status = ?, last_active = ?
                WHERE user_id = ?
            """
        
        await self._execute_query(query, (status, datetime.now(), user_id))
        return True
    
    async def get_users_by_mode(self, mode: str) -> List[Dict[str, Any]]:
        """Получить всех пользователей по режиму"""
        if self.db_type == 'postgresql':
            query = f"""
                SELECT * FROM {self.tables['users']} WHERE mode = $1 AND status = 'active'
            """
        else:
            query = f"""
                SELECT * FROM {self.tables['users']} WHERE mode = ? AND status = 'active'
            """
        
        return await self._execute_query(query, (mode,), fetch_all=True)
    
    async def cleanup_expired_users(self) -> int:
        """Очистка пользователей с истекшими сессиями"""
        expiry_date = datetime.now() - timedelta(days=SESSION_TIMEOUT_DAYS)
        
        if self.db_type == 'postgresql':
            query = f"""
                UPDATE {self.tables['users']} SET status = 'expired'
                WHERE last_active < $1 AND status = 'active' AND mode = 'user'
            """
        else:
            query = f"""
                UPDATE {self.tables['users']} SET status = 'expired'
                WHERE last_active < ? AND status = 'active' AND mode = 'user'
            """
        
        return await self._execute_query(query, (expiry_date,))
    
    # === АКТИВНОСТЬ ПОЛЬЗОВАТЕЛЕЙ ===
    
    async def add_user_activity(self, chat_id: int, user_id: int, username: str = None, 
                               first_name: str = None) -> bool:
        """Добавить или обновить активность пользователя"""
        today = datetime.now().date()
        
        # Проверяем, есть ли запись за сегодня
        if self.db_type == 'postgresql':
            check_query = f"""
                SELECT id, message_count FROM {self.tables['activity_data']} 
                WHERE chat_id = $1 AND user_id = $2 AND date_tracked = $3
            """
        else:
            check_query = f"""
                SELECT id, message_count FROM {self.tables['activity_data']} 
                WHERE chat_id = ? AND user_id = ? AND date_tracked = ?
            """
        
        existing = await self._execute_query(check_query, (chat_id, user_id, today), fetch_one=True)
        
        if existing:
            # Обновляем существующую запись
            if self.db_type == 'postgresql':
                update_query = f"""
                    UPDATE {self.tables['activity_data']} SET 
                        message_count = message_count + 1,
                        last_activity = $1,
                        username = COALESCE($2, username),
                        first_name = COALESCE($3, first_name)
                    WHERE id = $4
                """
            else:
                update_query = f"""
                    UPDATE {self.tables['activity_data']} SET 
                        message_count = message_count + 1,
                        last_activity = ?,
                        username = COALESCE(?, username),
                        first_name = COALESCE(?, first_name)
                    WHERE id = ?
                """
            
            await self._execute_query(update_query, (datetime.now(), username, first_name, existing['id']))
        else:
            # Создаем новую запись
            if self.db_type == 'postgresql':
                insert_query = f"""
                    INSERT INTO {self.tables['activity_data']} 
                    (chat_id, user_id, username, first_name, date_tracked, last_activity)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """
            else:
                insert_query = f"""
                    INSERT INTO {self.tables['activity_data']} 
                    (chat_id, user_id, username, first_name, date_tracked, last_activity)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
            
            await self._execute_query(insert_query, (chat_id, user_id, username, first_name, today, datetime.now()))
        
        return True
    
    async def get_active_users(self, chat_id: int, date: datetime.date = None) -> List[Dict[str, Any]]:
        """Получить активных пользователей за день"""
        if date is None:
            date = datetime.now().date()
        
        if self.db_type == 'postgresql':
            query = f"""
                SELECT * FROM {self.tables['activity_data']} 
                WHERE chat_id = $1 AND date_tracked = $2
                ORDER BY message_count DESC, last_activity DESC
            """
        else:
            query = f"""
                SELECT * FROM {self.tables['activity_data']} 
                WHERE chat_id = ? AND date_tracked = ?
                ORDER BY message_count DESC, last_activity DESC
            """
        
        return await self._execute_query(query, (chat_id, date), fetch_all=True)
    
    async def get_activity_stats(self, chat_id: int, date: datetime.date = None) -> Dict[str, Any]:
        """Получить статистику активности"""
        if date is None:
            date = datetime.now().date()
        
        if self.db_type == 'postgresql':
            query = f"""
                SELECT 
                    COUNT(*) as total_users,
                    SUM(message_count) as total_messages,
                    MAX(message_count) as max_messages,
                    AVG(message_count) as avg_messages
                FROM {self.tables['activity_data']} 
                WHERE chat_id = $1 AND date_tracked = $2
            """
        else:
            query = f"""
                SELECT 
                    COUNT(*) as total_users,
                    SUM(message_count) as total_messages,
                    MAX(message_count) as max_messages,
                    AVG(message_count) as avg_messages
                FROM {self.tables['activity_data']} 
                WHERE chat_id = ? AND date_tracked = ?
            """
        
        result = await self._execute_query(query, (chat_id, date), fetch_one=True)
        
        if result and result['total_users'] > 0:
            return {
                'total_users': result['total_users'],
                'total_messages': result['total_messages'] or 0,
                'max_messages': result['max_messages'] or 0,
                'avg_messages': round(float(result['avg_messages']) if result['avg_messages'] else 0, 1),
                'date': date.strftime('%d.%m.%Y')
            }
        else:
            return {
                'total_users': 0,
                'total_messages': 0,
                'max_messages': 0,
                'avg_messages': 0,
                'date': date.strftime('%d.%m.%Y')
            }
    
    # === ЗАДАЧИ В ОЧЕРЕДИ ===
    
    async def add_task(self, user_id: int, command: str, chat_id: int = None, 
                      parameters: str = None, priority: int = 2) -> int:
        """Добавить задачу в очередь"""
        if self.db_type == 'postgresql':
            query = f"""
                INSERT INTO {self.tables['request_queue']} 
                (user_id, chat_id, command, parameters, priority, status, created_at)
                VALUES ($1, $2, $3, $4, $5, 'pending', $6)
                RETURNING id
            """
        else:
            query = f"""
                INSERT INTO {self.tables['request_queue']} 
                (user_id, chat_id, command, parameters, priority, status, created_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """
        
        if self.db_type == 'postgresql':
            result = await self._execute_query(query, (user_id, chat_id, command, parameters, priority, datetime.now()), fetch_one=True)
            return result['id'] if result else 0
        else:
            await self._execute_query(query, (user_id, chat_id, command, parameters, priority, datetime.now()))
            # Для SQLite получаем последний ID
            cursor_query = "SELECT last_insert_rowid()"
            result = await self._execute_query(cursor_query, fetch_one=True)
            return result['last_insert_rowid()'] if result else 0
    
    async def complete_task(self, task_id: int, result: str = None, error: str = None) -> bool:
        """Завершить задачу"""
        if self.db_type == 'postgresql':
            query = f"""
                UPDATE {self.tables['request_queue']} SET 
                    status = 'completed',
                    completed_at = $1,
                    result = $2,
                    error_message = $3
                WHERE id = $4
            """
        else:
            query = f"""
                UPDATE {self.tables['request_queue']} SET 
                    status = 'completed',
                    completed_at = ?,
                    result = ?,
                    error_message = ?
                WHERE id = ?
            """
        
        await self._execute_query(query, (datetime.now(), result, error, task_id))
        return True
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Получить статистику базы данных"""
        stats = {}
        
        # Количество записей в таблицах
        for table_key, table_name in self.tables.items():
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            try:
                result = await self._execute_query(query, fetch_one=True)
                stats[f'{table_key}_count'] = result['count'] if result else 0
            except Exception as e:
                logger.debug(f"Ошибка получения статистики для {table_name}: {e}")
                stats[f'{table_key}_count'] = 0
        
        # Активные пользователи
        active_query = f"""
            SELECT COUNT(*) as count FROM {self.tables['users']} WHERE status = 'active'
        """
        try:
            result = await self._execute_query(active_query, fetch_one=True)
            stats['active_users'] = result['count'] if result else 0
        except:
            stats['active_users'] = 0
        
        # Пользователи в user mode
        user_mode_query = f"""
            SELECT COUNT(*) as count FROM {self.tables['users']} WHERE mode = 'user' AND status = 'active'
        """
        try:
            result = await self._execute_query(user_mode_query, fetch_one=True)
            stats['user_mode_users'] = result['count'] if result else 0
        except:
            stats['user_mode_users'] = 0
        
        return stats
    
    async def health_check(self) -> bool:
        """Проверка здоровья базы данных"""
        try:
            if self.db_type == 'postgresql':
                await self._execute_query("SELECT 1", fetch_one=True)
            else:
                await self._execute_query("SELECT 1", fetch_one=True)
            return True
        except Exception as e:
            logger.error(f"❌ Health check БД неудачен: {e}")
            return False

# Глобальный экземпляр менеджера БД
db_manager = DatabaseManager()

# Функции для быстрого доступа
async def init_database():
    """Инициализация базы данных"""
    await db_manager.initialize()

async def get_db():
    """Получить менеджер базы данных"""
    return db_manager