#!/usr/bin/env python3
"""
Topics Scanner Bot v5.17 - База данных (PostgreSQL + SQLite)
Поддержка PostgreSQL для Render.com и SQLite для локальной разработки
"""

import os
import sqlite3
import aiosqlite
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from urllib.parse import urlparse
from config import DATABASE_URL

logger = logging.getLogger(__name__)

class Database:
    """База данных с поддержкой PostgreSQL и SQLite"""
    
    def __init__(self):
        self.database_url = DATABASE_URL
        self.db_type = self._detect_db_type()
        self.pool = None
        
    def _detect_db_type(self) -> str:
        """Определение типа базы данных"""
        if self.database_url.startswith(('postgres://', 'postgresql://')):
            return 'postgresql'
        else:
            return 'sqlite'
    
    async def init_db(self):
        """Инициализация базы данных"""
        try:
            logger.info(f"🗄️ Инициализация {self.db_type} базы данных")
            
            if self.db_type == 'postgresql':
                await self._init_postgresql()
            else:
                await self._init_sqlite()
            
            await self._create_tables()
            logger.info("✅ База данных инициализирована")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            raise
    
    async def _init_postgresql(self):
        """Инициализация PostgreSQL"""
        try:
            import asyncpg
            
            # Создаем пул соединений
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=10,
                command_timeout=30
            )
            
            logger.info("✅ PostgreSQL пул создан")
            
        except ImportError:
            logger.error("❌ asyncpg не установлен! pip install asyncpg")
            raise
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
            raise
    
    async def _init_sqlite(self):
        """Инициализация SQLite"""
        try:
            db_path = self.database_url.replace('sqlite:///', '')
            
            # Создаем директорию если нужно
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            logger.info(f"✅ SQLite путь: {db_path}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка настройки SQLite: {e}")
            raise
    
    async def _create_tables(self):
        """Создание таблиц"""
        if self.db_type == 'postgresql':
            await self._create_postgresql_tables()
        else:
            await self._create_sqlite_tables()
    
    async def _create_postgresql_tables(self):
        """Создание таблиц PostgreSQL"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS topics_bot_users (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(100),
                    first_name VARCHAR(255),
                    api_mode VARCHAR(20) DEFAULT 'bot',
                    api_id VARCHAR(50),
                    api_hash VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
    
    async def _create_sqlite_tables(self):
        """Создание таблиц SQLite"""
        db_path = self.database_url.replace('sqlite:///', '')
        async with aiosqlite.connect(db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS topics_bot_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    api_mode TEXT DEFAULT 'bot',
                    api_id TEXT,
                    api_hash TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.commit()
    
    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получить пользователя"""
        if self.db_type == 'postgresql':
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM topics_bot_users WHERE user_id = $1", 
                    user_id
                )
                return dict(row) if row else None
        else:
            db_path = self.database_url.replace('sqlite:///', '')
            async with aiosqlite.connect(db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM topics_bot_users WHERE user_id = ?", 
                    (user_id,)
                )
                row = await cursor.fetchone()
                return dict(row) if row else None
    
    async def save_user(self, user_id: int, username: str = None, 
                       first_name: str = None, api_mode: str = 'bot',
                       api_id: str = None, api_hash: str = None) -> bool:
        """Сохранить или обновить пользователя"""
        try:
            now = datetime.now()
            
            if self.db_type == 'postgresql':
                async with self.pool.acquire() as conn:
                    # Проверяем существование
                    exists = await conn.fetchval(
                        "SELECT user_id FROM topics_bot_users WHERE user_id = $1",
                        user_id
                    )
                    
                    if exists:
                        # Обновляем
                        await conn.execute("""
                            UPDATE topics_bot_users SET 
                                username = COALESCE($1, username),
                                first_name = COALESCE($2, first_name),
                                api_mode = $3,
                                api_id = COALESCE($4, api_id),
                                api_hash = COALESCE($5, api_hash),
                                updated_at = $6
                            WHERE user_id = $7
                        """, username, first_name, api_mode, api_id, api_hash, now, user_id)
                    else:
                        # Создаем
                        await conn.execute("""
                            INSERT INTO topics_bot_users 
                            (user_id, username, first_name, api_mode, api_id, api_hash, created_at, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """, user_id, username, first_name, api_mode, api_id, api_hash, now, now)
            else:
                # SQLite
                db_path = self.database_url.replace('sqlite:///', '')
                async with aiosqlite.connect(db_path) as db:
                    # Проверяем существование
                    cursor = await db.execute(
                        "SELECT user_id FROM topics_bot_users WHERE user_id = ?", 
                        (user_id,)
                    )
                    exists = await cursor.fetchone()
                    
                    now_str = now.isoformat()
                    
                    if exists:
                        # Обновляем
                        await db.execute("""
                            UPDATE topics_bot_users SET 
                                username = COALESCE(?, username),
                                first_name = COALESCE(?, first_name),
                                api_mode = ?,
                                api_id = COALESCE(?, api_id),
                                api_hash = COALESCE(?, api_hash),
                                updated_at = ?
                            WHERE user_id = ?
                        """, (username, first_name, api_mode, api_id, api_hash, now_str, user_id))
                    else:
                        # Создаем
                        await db.execute("""
                            INSERT INTO topics_bot_users 
                            (user_id, username, first_name, api_mode, api_id, api_hash, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (user_id, username, first_name, api_mode, api_id, api_hash, now_str, now_str))
                    
                    await db.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения пользователя {user_id}: {e}")
            return False
    
    async def set_user_mode(self, user_id: int, mode: str) -> bool:
        """Установить режим пользователя"""
        try:
            now = datetime.now()
            
            if self.db_type == 'postgresql':
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE topics_bot_users SET api_mode = $1, updated_at = $2 WHERE user_id = $3",
                        mode, now, user_id
                    )
            else:
                db_path = self.database_url.replace('sqlite:///', '')
                async with aiosqlite.connect(db_path) as db:
                    await db.execute(
                        "UPDATE topics_bot_users SET api_mode = ?, updated_at = ? WHERE user_id = ?",
                        (mode, now.isoformat(), user_id)
                    )
                    await db.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка установки режима для {user_id}: {e}")
            return False
    
    async def save_user_credentials(self, user_id: int, api_id: str, api_hash: str) -> bool:
        """Сохранить API credentials пользователя"""
        try:
            now = datetime.now()
            
            if self.db_type == 'postgresql':
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE topics_bot_users SET 
                            api_id = $1, 
                            api_hash = $2, 
                            api_mode = 'user',
                            updated_at = $3 
                        WHERE user_id = $4
                    """, api_id, api_hash, now, user_id)
            else:
                db_path = self.database_url.replace('sqlite:///', '')
                async with aiosqlite.connect(db_path) as db:
                    await db.execute("""
                        UPDATE topics_bot_users SET 
                            api_id = ?, 
                            api_hash = ?, 
                            api_mode = 'user',
                            updated_at = ? 
                        WHERE user_id = ?
                    """, (api_id, api_hash, now.isoformat(), user_id))
                    await db.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения credentials для {user_id}: {e}")
            return False
    
    async def get_user_mode(self, user_id: int) -> str:
        """Получить режим пользователя"""
        user = await self.get_user(user_id)
        return user['api_mode'] if user else 'bot'
    
    async def has_user_credentials(self, user_id: int) -> bool:
        """Проверить, есть ли у пользователя сохраненные credentials"""
        user = await self.get_user(user_id)
        return bool(user and user['api_id'] and user['api_hash'])
    
    async def cleanup(self):
        """Очистка ресурсов"""
        try:
            if self.db_type == 'postgresql' and self.pool:
                await self.pool.close()
                logger.info("✅ PostgreSQL пул закрыт")
        except Exception as e:
            logger.error(f"❌ Ошибка очистки базы данных: {e}")

# Глобальный экземпляр
db = Database()
