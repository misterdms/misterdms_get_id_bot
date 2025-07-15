#!/usr/bin/env python3
"""
Управление базой данных для гибридного Topics Scanner Bot
Поддерживает SQLite и PostgreSQL с асинхронными операциями
ОПТИМИЗИРОВАНО: Префиксы таблиц для разделения данных между ботами
"""

import aiosqlite
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
import json
import urllib.parse as urlparse

from config import DATABASE_URL, SESSION_TIMEOUT_DAYS, USER_STATUSES, TASK_STATUSES, BOT_PREFIX

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Менеджер базы данных с поддержкой SQLite и PostgreSQL + префиксы таблиц"""
    
    def __init__(self, database_url: str = DATABASE_URL, bot_prefix: str = BOT_PREFIX):
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
        
        logger.info(f"🗄️ DatabaseManager инициализирован с префиксом: {self.bot_prefix}")
        
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
            import asyncpg
            
            # Парсим DATABASE_URL
            url = urlparse.urlparse(self.database_url)
            
            conn = await asyncpg.connect(
                host=url.hostname,
                port=url.port or 5432,
                user=url.username,
                password=url.password,
                database=url.path[1:] if url.path else 'postgres'
            )
            
            try:
                yield conn
            finally:
                await conn.close()
    
    async def migrate_existing_data(self):
        """Миграция данных из таблиц без префиксов (если они существуют)"""
        try:
            async with self.get_connection() as conn:
                # Проверяем наличие старых таблиц без префикса
                old_tables = ['users', 'activity_data', 'request_queue', 'bot_settings', 'bot_logs']
                
                for old_table in old_tables:
                    new_table = self.tables[old_table]
                    
                    # Проверяем существование старой таблицы
                    if self.db_type == 'sqlite':
                        check_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{old_table}'"
                        cursor = await conn.execute(check_query)
                        exists = await cursor.fetchone()
                    else:
                        check_query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{old_table}')"
                        exists = await conn.fetchval(check_query)
                    
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
                        # PostgreSQL
                        await conn.execute(index_sql)
                except Exception as e:
                    logger.debug(f"Индекс уже существует: {e}")
            
            if self.db_type == 'sqlite':
                await conn.commit()
    
    # === УНИВЕРСАЛЬНЫЕ МЕТОДЫ ДЛЯ ОБЕИХ БД ===
    
    async def _execute_query(self, query: str, params: tuple = None, fetch_one: bool = False, fetch_all: bool = False):
        """Универсальный метод выполнения запросов"""
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
                # PostgreSQL
                if fetch_one:
                    result = await conn.fetchrow(query, *(params or ()))
                    return dict(result) if result else None
                elif fetch_all:
                    rows = await conn.fetch(query, *(params or ()))
                    return [dict(row) for row in rows]
                else:
                    result = await conn.execute(query, *(params or ()))
                    return int(result.split()[-1]) if 'UPDATE' in result or 'DELETE' in result else 1
    
    # === УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ ===
    
    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получить информацию о пользователе"""
        query = f"SELECT * FROM {self.tables['users']} WHERE user_id = " + ("$1" if self.db_type == 'postgresql' else "?")
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
        query = f"""
            UPDATE {self.tables['users']} SET 
                api_id_encrypted = {('$1' if self.db_type == 'postgresql' else '?')},
                api_hash_encrypted = {('$2' if self.db_type == 'postgresql' else '?')},
                session_file = {('$3' if self.db_type == 'postgresql' else '?')},
                mode = 'user',
                status = 'active',
                last_active = {('$4' if self.db_type == 'postgresql' else '?')}
            WHERE user_id = {('$5' if self.db_type == 'postgresql' else '?')}
        """
        
        await self._execute_query(query, (api_id_encrypted, api_hash_encrypted, session_file, datetime.now(), user_id))
        return True
    
    async def update_user_status(self, user_id: int, status: str) -> bool:
        """Обновить статус пользователя"""
        if status not in USER_STATUSES:
            raise ValueError(f"Неверный статус: {status}")
        
        query = f"""
            UPDATE {self.tables['users']} SET status = {('$1' if self.db_type == 'postgresql' else '?')}, 
            last_active = {('$2' if self.db_type == 'postgresql' else '?')}
            WHERE user_id = {('$3' if self.db_type == 'postgresql' else '?')}
        """
        
        await self._execute_query(query, (status, datetime.now(), user_id))
        return True
    
    async def get_users_by_mode(self, mode: str) -> List[Dict[str, Any]]:
        """Получить всех пользователей по режиму"""
        query = f"""
            SELECT * FROM {self.tables['users']} WHERE mode = {('$1' if self.db_type == 'postgresql' else '?')} AND status = 'active'
        """
        
        return await self._execute_query(query, (mode,), fetch_all=True)
    
    async def cleanup_expired_users(self) -> int:
        """Очистка пользователей с истекшими сессиями"""
        expiry_date = datetime.now() - timedelta(days=SESSION_TIMEOUT_DAYS)
        
        query = f"""
            UPDATE {self.tables['users']} SET status = 'expired'
            WHERE last_active < {('$1' if self.db_type == 'postgresql' else '?')} AND status = 'active' AND mode = 'user'
        """
        
        return await self._execute_query(query, (expiry_date,))
    
    # === АКТИВНОСТЬ ПОЛЬЗОВАТЕЛЕЙ ===
    
    async def add_user_activity(self, chat_id: int, user_id: int, username: str = None, 
                               first_name: str = None) -> bool:
        """Добавить или обновить активность пользователя"""
        today = datetime.now().date()
        
        # Проверяем, есть ли запись за сегодня
        check_query = f"""
            SELECT id, message_count FROM {self.tables['activity_data']} 
            WHERE chat_id = {('$1' if self.db_type == 'postgresql' else '?')} AND 
                  user_id = {('$2' if self.db_type == 'postgresql' else '?')} AND 
                  date_tracked = {('$3' if self.db_type == 'postgresql' else '?')}
        """
        
        existing = await self._execute_query(check_query, (chat_id, user_id, today), fetch_one=True)
        
        if existing:
            # Обновляем существующую запись
            update_query = f"""
                UPDATE {self.tables['activity_data']} SET 
                    message_count = message_count + 1,
                    last_activity = {('$1' if self.db_type == 'postgresql' else '?')},
                    username = COALESCE({('$2' if self.db_type == 'postgresql' else '?')}, username),
                    first_name = COALESCE({('$3' if self.db_type == 'postgresql' else '?')}, first_name)
                WHERE id = {('$4' if self.db_type == 'postgresql' else '?')}
            """
            
            await self._execute_query(update_query, (datetime.now(), username, first_name, existing['id']))
        else:
            # Создаем новую запись
            insert_query = f"""
                INSERT INTO {self.tables['activity_data']} 
                (chat_id, user_id, username, first_name, date_tracked, last_activity)
                VALUES ({('$1' if self.db_type == 'postgresql' else '?')}, 
                        {('$2' if self.db_type == 'postgresql' else '?')}, 
                        {('$3' if self.db_type == 'postgresql' else '?')}, 
                        {('$4' if self.db_type == 'postgresql' else '?')}, 
                        {('$5' if self.db_type == 'postgresql' else '?')}, 
                        {('$6' if self.db_type == 'postgresql' else '?')})
            """
            
            await self._execute_query(insert_query, (chat_id, user_id, username, first_name, today, datetime.now()))
        
        return True
    
    async def get_active_users(self, chat_id: int, date: datetime.date = None) -> List[Dict[str, Any]]:
        """Получить активных пользователей за день"""
        if date is None:
            date = datetime.now().date()
        
        query = f"""
            SELECT * FROM {self.tables['activity_data']} 
            WHERE chat_id = {('$1' if self.db_type == 'postgresql' else '?')} AND 
                  date_tracked = {('$2' if self.db_type == 'postgresql' else '?')}
            ORDER BY message_count DESC, last_activity DESC
        """
        
        return await self._execute_query(query, (chat_id, date), fetch_all=True)
    
    async def get_activity_stats(self, chat_id: int, date: datetime.date = None) -> Dict[str, Any]:
        """Получить статистику активности"""
        if date is None:
            date = datetime.now().date()
        
        query = f"""
            SELECT 
                COUNT(*) as total_users,
                SUM(message_count) as total_messages,
                MAX(message_count) as max_messages,
                AVG(message_count) as avg_messages
            FROM {self.tables['activity_data']} 
            WHERE chat_id = {('$1' if self.db_type == 'postgresql' else '?')} AND 
                  date_tracked = {('$2' if self.db_type == 'postgresql' else '?')}
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
    
    async def cleanup_old_activity(self, days_to_keep: int = 30) -> int:
        """Очистка старых данных активности"""
        cutoff_date = datetime.now().date() - timedelta(days=days_to_keep)
        
        query = f"""
            DELETE FROM {self.tables['activity_data']} WHERE date_tracked < {('$1' if self.db_type == 'postgresql' else '?')}
        """
        
        return await self._execute_query(query, (cutoff_date,))
    
    # === ОЧЕРЕДЬ ЗАПРОСОВ ===
    
    async def add_to_queue(self, user_id: int, command: str, chat_id: int = None, 
                          parameters: Dict[str, Any] = None, priority: int = 2) -> int:
        """Добавить задачу в очередь"""
        if self.db_type == 'postgresql':
            query = f"""
                INSERT INTO {self.tables['request_queue']} 
                (user_id, chat_id, command, parameters, priority)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
            """
            async with self.get_connection() as conn:
                result = await conn.fetchval(query, user_id, chat_id, command, json.dumps(parameters) if parameters else None, priority)
                return result
        else:
            query = f"""
                INSERT INTO {self.tables['request_queue']} 
                (user_id, chat_id, command, parameters, priority)
                VALUES (?, ?, ?, ?, ?)
            """
            async with self.get_connection() as conn:
                cursor = await conn.execute(query, (user_id, chat_id, command, json.dumps(parameters) if parameters else None, priority))
                await conn.commit()
                return cursor.lastrowid
    
    async def get_next_task(self) -> Optional[Dict[str, Any]]:
        """Получить следующую задачу из очереди"""
        select_query = f"""
            SELECT * FROM {self.tables['request_queue']} 
            WHERE status = 'pending'
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
        """
        
        task = await self._execute_query(select_query, fetch_one=True)
        
        if task:
            # Отмечаем как выполняющуюся
            update_query = f"""
                UPDATE {self.tables['request_queue']} SET status = 'processing', 
                started_at = {('$1' if self.db_type == 'postgresql' else '?')}
                WHERE id = {('$2' if self.db_type == 'postgresql' else '?')}
            """
            
            await self._execute_query(update_query, (datetime.now(), task['id']))
            
            if task['parameters']:
                task['parameters'] = json.loads(task['parameters'])
            
            return task
        
        return None
    
    async def complete_task(self, task_id: int, result: str = None, error: str = None) -> bool:
        """Завершить задачу"""
        status = 'completed' if error is None else 'failed'
        
        query = f"""
            UPDATE {self.tables['request_queue']} SET 
                status = {('$1' if self.db_type == 'postgresql' else '?')}, 
                completed_at = {('$2' if self.db_type == 'postgresql' else '?')},
                result = {('$3' if self.db_type == 'postgresql' else '?')},
                error_message = {('$4' if self.db_type == 'postgresql' else '?')}
            WHERE id = {('$5' if self.db_type == 'postgresql' else '?')}
        """
        
        await self._execute_query(query, (status, datetime.now(), result, error, task_id))
        return True
    
    async def get_queue_status(self, user_id: int = None) -> Dict[str, Any]:
        """Получить статус очереди"""
        # Общая статистика
        stats_query = f"""
            SELECT status, COUNT(*) as count FROM {self.tables['request_queue']} 
            WHERE created_at > {('$1' if self.db_type == 'postgresql' else '?')}
            GROUP BY status
        """
        
        hour_ago = datetime.now() - timedelta(hours=1)
        stats_rows = await self._execute_query(stats_query, (hour_ago,), fetch_all=True)
        
        status_counts = {row['status']: row['count'] for row in stats_rows}
        
        # Позиция пользователя в очереди
        user_position = None
        if user_id:
            position_query = f"""
                SELECT COUNT(*) + 1 as position FROM {self.tables['request_queue']} 
                WHERE status = 'pending' AND 
                      (priority < (SELECT priority FROM {self.tables['request_queue']} WHERE user_id = {('$1' if self.db_type == 'postgresql' else '?')} AND status = 'pending' LIMIT 1)
                       OR (priority = (SELECT priority FROM {self.tables['request_queue']} WHERE user_id = {('$2' if self.db_type == 'postgresql' else '?')} AND status = 'pending' LIMIT 1)
                           AND created_at < (SELECT created_at FROM {self.tables['request_queue']} WHERE user_id = {('$3' if self.db_type == 'postgresql' else '?')} AND status = 'pending' LIMIT 1)))
            """
            
            position_result = await self._execute_query(position_query, (user_id, user_id, user_id), fetch_one=True)
            if position_result and position_result['position'] > 0:
                user_position = position_result['position']
        
        return {
            'pending': status_counts.get('pending', 0),
            'processing': status_counts.get('processing', 0),
            'completed': status_counts.get('completed', 0),
            'failed': status_counts.get('failed', 0),
            'user_position': user_position
        }
    
    async def cleanup_old_tasks(self, hours_to_keep: int = 24) -> int:
        """Очистка старых завершенных задач"""
        cutoff_time = datetime.now() - timedelta(hours=hours_to_keep)
        
        query = f"""
            DELETE FROM {self.tables['request_queue']} 
            WHERE status IN ('completed', 'failed') AND completed_at < {('$1' if self.db_type == 'postgresql' else '?')}
        """
        
        return await self._execute_query(query, (cutoff_time,))
    
    # === НАСТРОЙКИ БОТА ===
    
    async def get_setting(self, key: str) -> Optional[str]:
        """Получить настройку бота"""
        query = f"""
            SELECT value FROM {self.tables['bot_settings']} WHERE key = {('$1' if self.db_type == 'postgresql' else '?')}
        """
        
        result = await self._execute_query(query, (key,), fetch_one=True)
        return result['value'] if result else None
    
    async def set_setting(self, key: str, value: str) -> bool:
        """Установить настройку бота"""
        if self.db_type == 'postgresql':
            query = f"""
                INSERT INTO {self.tables['bot_settings']} (key, value, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
            """
        else:
            query = f"""
                INSERT OR REPLACE INTO {self.tables['bot_settings']} (key, value, updated_at)
                VALUES (?, ?, ?)
            """
        
        await self._execute_query(query, (key, value, datetime.now()))
        return True
    
    # === ЛОГИРОВАНИЕ ===
    
    async def log_event(self, level: str, message: str, user_id: int = None, 
                       chat_id: int = None, command: str = None, metadata: Dict = None) -> bool:
        """Записать событие в лог"""
        query = f"""
            INSERT INTO {self.tables['bot_logs']} 
            (level, message, user_id, chat_id, command, metadata)
            VALUES ({('$1' if self.db_type == 'postgresql' else '?')}, 
                    {('$2' if self.db_type == 'postgresql' else '?')}, 
                    {('$3' if self.db_type == 'postgresql' else '?')}, 
                    {('$4' if self.db_type == 'postgresql' else '?')}, 
                    {('$5' if self.db_type == 'postgresql' else '?')}, 
                    {('$6' if self.db_type == 'postgresql' else '?')})
        """
        
        await self._execute_query(query, (level, message, user_id, chat_id, command, json.dumps(metadata) if metadata else None))
        return True
    
    async def get_recent_logs(self, limit: int = 100, level: str = None) -> List[Dict[str, Any]]:
        """Получить последние записи лога"""
        if level:
            query = f"""
                SELECT * FROM {self.tables['bot_logs']} WHERE level = {('$1' if self.db_type == 'postgresql' else '?')}
                ORDER BY timestamp DESC LIMIT {('$2' if self.db_type == 'postgresql' else '?')}
            """
            params = (level, limit)
        else:
            query = f"""
                SELECT * FROM {self.tables['bot_logs']} 
                ORDER BY timestamp DESC LIMIT {('$1' if self.db_type == 'postgresql' else '?')}
            """
            params = (limit,)
        
        logs = await self._execute_query(query, params, fetch_all=True)
        
        for log_entry in logs:
            if log_entry['metadata']:
                log_entry['metadata'] = json.loads(log_entry['metadata'])
        
        return logs
    
    # === СИСТЕМНЫЕ ОПЕРАЦИИ ===
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Получить статистику базы данных"""
        stats = {}
        
        # Количество записей в таблицах
        for table_key, table_name in self.tables.items():
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = await self._execute_query(query, fetch_one=True)
            stats[f'{table_key}_count'] = result['count'] if result else 0
        
        # Активные пользователи
        active_query = f"""
            SELECT COUNT(*) as count FROM {self.tables['users']} WHERE status = 'active'
        """
        result = await self._execute_query(active_query, fetch_one=True)
        stats['active_users'] = result['count'] if result else 0
        
        # Пользователи в user mode
        user_mode_query = f"""
            SELECT COUNT(*) as count FROM {self.tables['users']} WHERE mode = 'user' AND status = 'active'
        """
        result = await self._execute_query(user_mode_query, fetch_one=True)
        stats['user_mode_users'] = result['count'] if result else 0
        
        return stats
    
    async def health_check(self) -> bool:
        """Проверка здоровья базы данных"""
        try:
            await self._execute_query("SELECT 1", fetch_one=True)
            return True
        except Exception as e:
            logger.error(f"❌ Health check БД неудачен: {e}")
            return False
    
    def get_table_prefix_info(self) -> Dict[str, str]:
        """Получить информацию о префиксах таблиц"""
        return {
            'bot_prefix': self.bot_prefix,
            'tables': self.tables.copy(),
            'db_type': self.db_type
        }

# Глобальный экземпляр менеджера БД
db_manager = DatabaseManager()

# Функции для быстрого доступа
async def init_database():
    """Инициализация базы данных"""
    await db_manager.initialize()

async def get_db():
    """Получить менеджер базы данных"""
    return db_manager