#!/usr/bin/env python3
"""
Управление базой данных для Topics Scanner Bot
Поддержка PostgreSQL и SQLite с автофоллбэком
ИСПРАВЛЕНО v4.1.2: Критические исправления save_user_credentials и _execute_query с подробным логированием
"""

import os
import logging
import aiosqlite
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

from config import (
    DATABASE_URL, DATABASE_POOL_SIZE, BOT_PREFIX,
    SESSION_TIMEOUT_DAYS
)

logger = logging.getLogger(__name__)

# Константы для пользователей
USER_MODES = ['bot', 'user']
USER_STATUSES = ['active', 'inactive', 'banned', 'expired']

class DatabaseManager:
    """Управление базой данных с поддержкой PostgreSQL и SQLite"""
    
    def __init__(self):
        self.database_url = DATABASE_URL
        self.pool = None
        self.db_type = None
        self.tables = {}
        self._determine_db_type()
        self._setup_table_names()
    
    def _determine_db_type(self):
        """Определение типа базы данных"""
        if self.database_url.startswith('postgresql://') or self.database_url.startswith('postgres://'):
            self.db_type = 'postgresql'
        else:
            self.db_type = 'sqlite'
        
        logger.info(f"🗄️ DatabaseManager инициализирован: {self.db_type} с префиксом {BOT_PREFIX}")
    
    def _setup_table_names(self):
        """Настройка имен таблиц с префиксами"""
        self.tables = {
            'users': f'{BOT_PREFIX}_users',
            'activity_data': f'{BOT_PREFIX}_activity_data',
            'request_queue': f'{BOT_PREFIX}_request_queue'
        }
    
    async def initialize(self):
        """Инициализация базы данных"""
        try:
            logger.info(f"🗄️ Инициализация базы данных: {self.db_type} (префикс: {BOT_PREFIX})")
            
            if self.db_type == 'postgresql':
                await self._init_postgresql()
            else:
                await self._init_sqlite()
            
            await self.create_tables()
            await self.create_indexes()
            await self._migrate_old_data()
            
            logger.info("✅ База данных инициализирована с префиксами")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            
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
    
    async def _init_postgresql(self):
        """Инициализация PostgreSQL"""
        try:
            import asyncpg
            
            # Создаем пул соединений
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=DATABASE_POOL_SIZE,
                command_timeout=30
            )
            
            logger.info(f"✅ PostgreSQL пул создан (размер: {DATABASE_POOL_SIZE})")
            
        except ImportError:
            logger.error("❌ asyncpg не установлен! pip install asyncpg")
            raise
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
            raise
    
    async def _init_sqlite(self):
        """Инициализация SQLite"""
        db_path = self.database_url.replace('sqlite:///', '')
        
        # Создаем директорию если нужно
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
        
        logger.info(f"✅ SQLite путь: {db_path}")
    
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
                logger.error("❌ asyncpg не установлен!")
                raise
            
            async with self.pool.acquire() as conn:
                yield conn
    
    async def create_tables(self):
        """Создание таблиц с префиксами"""
        async with self.get_connection() as conn:
            
            # Таблица пользователей
            if self.db_type == 'postgresql':
                users_query = f"""
                    CREATE TABLE IF NOT EXISTS {self.tables['users']} (
                        user_id BIGINT PRIMARY KEY,
                        telegram_username VARCHAR(100),
                        first_name VARCHAR(255),
                        mode VARCHAR(20) NOT NULL DEFAULT 'bot',
                        status VARCHAR(20) NOT NULL DEFAULT 'active',
                        api_id_encrypted TEXT,
                        api_hash_encrypted TEXT,
                        session_file VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            else:
                users_query = f"""
                    CREATE TABLE IF NOT EXISTS {self.tables['users']} (
                        user_id INTEGER PRIMARY KEY,
                        telegram_username TEXT,
                        first_name TEXT,
                        mode TEXT NOT NULL DEFAULT 'bot',
                        status TEXT NOT NULL DEFAULT 'active',
                        api_id_encrypted TEXT,
                        api_hash_encrypted TEXT,
                        session_file TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            
            # Таблица активности
            if self.db_type == 'postgresql':
                activity_query = f"""
                    CREATE TABLE IF NOT EXISTS {self.tables['activity_data']} (
                        id SERIAL PRIMARY KEY,
                        chat_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        username VARCHAR(100),
                        first_name VARCHAR(255),
                        message_count INTEGER DEFAULT 1,
                        date_tracked DATE NOT NULL,
                        last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            else:
                activity_query = f"""
                    CREATE TABLE IF NOT EXISTS {self.tables['activity_data']} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chat_id INTEGER NOT NULL,
                        user_id INTEGER NOT NULL,
                        username TEXT,
                        first_name TEXT,
                        message_count INTEGER DEFAULT 1,
                        date_tracked DATE NOT NULL,
                        last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            
            # Таблица очереди запросов
            if self.db_type == 'postgresql':
                queue_query = f"""
                    CREATE TABLE IF NOT EXISTS {self.tables['request_queue']} (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        chat_id BIGINT NOT NULL,
                        command VARCHAR(100) NOT NULL,
                        parameters TEXT,
                        priority INTEGER DEFAULT 1,
                        status VARCHAR(20) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        result TEXT,
                        error_message TEXT
                    )
                """
            else:
                queue_query = f"""
                    CREATE TABLE IF NOT EXISTS {self.tables['request_queue']} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        chat_id INTEGER NOT NULL,
                        command TEXT NOT NULL,
                        parameters TEXT,
                        priority INTEGER DEFAULT 1,
                        status TEXT DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP,
                        result TEXT,
                        error_message TEXT
                    )
                """
            
            # Выполняем создание таблиц
            if self.db_type == 'sqlite':
                await conn.execute(users_query)
                await conn.execute(activity_query)
                await conn.execute(queue_query)
                await conn.commit()
            else:
                await conn.execute(users_query)
                await conn.execute(activity_query)
                await conn.execute(queue_query)
    
    async def create_indexes(self):
        """Создание индексов для оптимизации"""
        async with self.get_connection() as conn:
            indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables['users']}_mode ON {self.tables['users']} (mode)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables['users']}_status ON {self.tables['users']} (status)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables['activity_data']}_chat_user_date ON {self.tables['activity_data']} (chat_id, user_id, date_tracked)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.tables['request_queue']}_user_status ON {self.tables['request_queue']} (user_id, status)"
            ]
            
            for index_query in indexes:
                if self.db_type == 'sqlite':
                    await conn.execute(index_query)
                else:
                    await conn.execute(index_query)
            
            if self.db_type == 'sqlite':
                await conn.commit()
    
    async def _migrate_old_data(self):
        """Миграция данных из старых таблиц без префиксов"""
        try:
            old_tables = ['users', 'activity_data', 'request_queue']
            
            for old_table in old_tables:
                new_table = self.tables[old_table]
                
                async with self.get_connection() as conn:
                    # Проверяем, существует ли старая таблица
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
        """ИСПРАВЛЕНО v4.1.2: Универсальный метод выполнения запросов с подробным логированием"""
        logger.debug(f"🔐 DB: Выполнение запроса: {query[:100]}...")
        logger.debug(f"🔐 DB: Параметры: {params}")
        
        async with self.get_connection() as conn:
            if self.db_type == 'sqlite':
                logger.debug(f"🔐 DB: Использование SQLite")
                cursor = await conn.execute(query, params or ())
                
                if fetch_one:
                    result = await cursor.fetchone()
                    result_dict = dict(result) if result else None
                    logger.debug(f"🔐 DB: SQLite fetch_one результат: {result_dict}")
                    return result_dict
                elif fetch_all:
                    rows = await cursor.fetchall()
                    result_list = [dict(row) for row in rows]
                    logger.debug(f"🔐 DB: SQLite fetch_all результат: {len(result_list)} строк")
                    return result_list
                else:
                    await conn.commit()
                    rowcount = cursor.rowcount
                    logger.debug(f"🔐 DB: SQLite rowcount: {rowcount}")
                    return rowcount
            else:
                # PostgreSQL - ИСПРАВЛЕНЫ ПАРАМЕТРЫ
                logger.debug(f"🔐 DB: Использование PostgreSQL")
                try:
                    if fetch_one:
                        if params:
                            result = await conn.fetchrow(query, *params)
                        else:
                            result = await conn.fetchrow(query)
                        result_dict = dict(result) if result else None
                        logger.debug(f"🔐 DB: PostgreSQL fetch_one результат: {result_dict}")
                        return result_dict
                    elif fetch_all:
                        if params:
                            rows = await conn.fetch(query, *params)
                        else:
                            rows = await conn.fetch(query)
                        result_list = [dict(row) for row in rows]
                        logger.debug(f"🔐 DB: PostgreSQL fetch_all результат: {len(result_list)} строк")
                        return result_list
                    else:
                        if params:
                            result = await conn.execute(query, *params)
                        else:
                            result = await conn.execute(query)
                        
                        logger.debug(f"🔐 DB: PostgreSQL execute результат: {result}")
                        
                        # Извлекаем количество затронутых строк
                        if isinstance(result, str):
                            if 'UPDATE' in result or 'INSERT' in result or 'DELETE' in result:
                                parts = result.split()
                                rowcount = int(parts[-1]) if parts and parts[-1].isdigit() else 1
                                logger.debug(f"🔐 DB: PostgreSQL извлеченный rowcount: {rowcount}")
                                return rowcount
                        return 1
                except Exception as e:
                    logger.error(f"❌ DB: Ошибка выполнения PostgreSQL запроса: {e}")
                    logger.error(f"❌ DB: Query: {query}")
                    logger.error(f"❌ DB: Params: {params}")
                    logger.error(f"❌ DB: Трассировка PostgreSQL: ", exc_info=True)
                    raise
    
    # === УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ ===
    
    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получить информацию о пользователе"""
        query = f"SELECT * FROM {self.tables['users']} WHERE user_id = " + ("$1" if self.db_type == 'postgresql' else "?")
        return await self._execute_query(query, (user_id,), fetch_one=True)
    
    async def create_or_update_user(self, user_id: int, telegram_username: str = None, 
                                  first_name: str = None, mode: str = 'bot') -> bool:
        """ИСПРАВЛЕНО: Создать или обновить пользователя с правильной логикой UPSERT"""
        try:
            # Сначала пытаемся получить существующего пользователя
            existing_user = await self.get_user(user_id)
            
            if existing_user:
                # Обновляем существующего пользователя
                if self.db_type == 'postgresql':
                    query = f"""
                        UPDATE {self.tables['users']} SET
                            telegram_username = $1,
                            first_name = $2,
                            mode = $3,
                            last_active = $4
                        WHERE user_id = $5
                    """
                    params = (telegram_username, first_name, mode, datetime.now(), user_id)
                else:
                    query = f"""
                        UPDATE {self.tables['users']} SET
                            telegram_username = ?,
                            first_name = ?,
                            mode = ?,
                            last_active = ?
                        WHERE user_id = ?
                    """
                    params = (telegram_username, first_name, mode, datetime.now(), user_id)
                
                await self._execute_query(query, params)
                logger.debug(f"✅ Пользователь {user_id} обновлен")
                
            else:
                # Создаем нового пользователя
                if self.db_type == 'postgresql':
                    query = f"""
                        INSERT INTO {self.tables['users']} 
                        (user_id, telegram_username, first_name, mode, last_active, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """
                    params = (user_id, telegram_username, first_name, mode, datetime.now(), datetime.now())
                else:
                    query = f"""
                        INSERT INTO {self.tables['users']} 
                        (user_id, telegram_username, first_name, mode, last_active, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """
                    params = (user_id, telegram_username, first_name, mode, datetime.now(), datetime.now())
                
                await self._execute_query(query, params)
                logger.debug(f"✅ Пользователь {user_id} создан")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка create_or_update_user для {user_id}: {e}")
            return False
    
    async def save_user_credentials(self, user_id: int, api_id_encrypted: str, 
                                  api_hash_encrypted: str, session_file: str) -> bool:
        """ИСПРАВЛЕНО v4.1.2: Сохранить зашифрованные credentials пользователя"""
        try:
            logger.info(f"🔐 DB: Начало сохранения credentials для пользователя {user_id}")
            logger.info(f"🔐 DB: Тип БД: {self.db_type}")
            
            # Сначала убеждаемся, что пользователь существует
            logger.info(f"🔐 DB: Проверка существования пользователя {user_id}")
            existing_user = await self.get_user(user_id)
            
            if not existing_user:
                logger.warning(f"⚠️ DB: Пользователь {user_id} не найден, создаем")
                # Создаем пользователя
                await self.create_or_update_user(user_id, mode='bot')
                logger.info(f"✅ DB: Пользователь {user_id} создан")
            else:
                logger.info(f"✅ DB: Пользователь {user_id} найден")
            
            # Обновляем credentials
            logger.info(f"🔐 DB: Обновление credentials для пользователя {user_id}")
            
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
                params = (api_id_encrypted, api_hash_encrypted, session_file, datetime.now(), user_id)
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
                params = (api_id_encrypted, api_hash_encrypted, session_file, datetime.now(), user_id)
            
            logger.info(f"🔐 DB: Выполнение SQL запроса для пользователя {user_id}")
            logger.debug(f"🔐 DB: Query: {query}")
            logger.debug(f"🔐 DB: Params: {[str(p)[:50] + '...' if len(str(p)) > 50 else str(p) for p in params]}")
            
            rows_affected = await self._execute_query(query, params)
            
            logger.info(f"🔐 DB: Затронуто строк: {rows_affected}")
            
            if rows_affected > 0:
                logger.info(f"✅ DB: Credentials сохранены для пользователя {user_id}")
                
                # Проверяем, что данные действительно сохранились
                logger.info(f"🔐 DB: Проверка сохранения для пользователя {user_id}")
                updated_user = await self.get_user(user_id)
                
                if updated_user:
                    logger.info(f"✅ DB: Проверка пройдена - режим: {updated_user.get('mode')}, есть credentials: {bool(updated_user.get('api_id_encrypted'))}")
                    return True
                else:
                    logger.error(f"❌ DB: Проверка не пройдена - пользователь не найден после сохранения")
                    return False
            else:
                logger.error(f"❌ DB: Не удалось сохранить credentials для пользователя {user_id} - не затронуто ни одной строки")
                return False
            
        except Exception as e:
            logger.error(f"❌ DB: Критическая ошибка save_user_credentials для {user_id}: {e}")
            logger.error(f"❌ DB: Трассировка: ", exc_info=True)
            return False
    
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
            ORDER BY message_count DESC
        """
        
        return await self._execute_query(query, (chat_id, date), fetch_all=True)
    
    # === ОЧЕРЕДЬ ЗАПРОСОВ ===
    
    async def add_task(self, user_id: int, chat_id: int, command: str, 
                      parameters: str = None, priority: int = 1) -> int:
        """Добавить задачу в очередь"""
        if self.db_type == 'postgresql':
            query = f"""
                INSERT INTO {self.tables['request_queue']} 
                (user_id, chat_id, command, parameters, priority, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
            """
            result = await self._execute_query(query, 
                (user_id, chat_id, command, parameters, priority, datetime.now()), fetch_one=True)
            return result['id'] if result else 0
        else:
            # SQLite
            query = f"""
                INSERT INTO {self.tables['request_queue']} 
                (user_id, chat_id, command, parameters, priority, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            await self._execute_query(query, (user_id, chat_id, command, parameters, priority, datetime.now()))
            
            # Получаем последний ID для SQLite
            last_id_query = "SELECT last_insert_rowid() as id"
            result = await self._execute_query(last_id_query, fetch_one=True)
            return result['id'] if result else 0
    
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
        
        rows_affected = await self._execute_query(query, (datetime.now(), result, error, task_id))
        return rows_affected > 0
    
    async def get_pending_tasks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Получить задачи в ожидании"""
        query = f"""
            SELECT * FROM {self.tables['request_queue']} 
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT {('$1' if self.db_type == 'postgresql' else '?')}
        """
        
        return await self._execute_query(query, (limit,), fetch_all=True)
    
    async def cleanup_old_tasks(self, days: int = 7) -> int:
        """Очистка старых выполненных задач"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = f"""
            DELETE FROM {self.tables['request_queue']} 
            WHERE status = 'completed' AND completed_at < {('$1' if self.db_type == 'postgresql' else '?')}
        """
        
        return await self._execute_query(query, (cutoff_date,))

# Глобальный экземпляр
db_manager = DatabaseManager()

async def init_database():
    """Функция инициализации базы данных"""
    await db_manager.initialize()
