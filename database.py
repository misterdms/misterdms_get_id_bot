#!/usr/bin/env python3
"""
Topics Scanner Bot v5.16 - База данных
Простое хранение пользователей и настроек
"""

import sqlite3
import aiosqlite
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from config import DATABASE_URL

logger = logging.getLogger(__name__)

class Database:
    """Простая база данных для бота"""
    
    def __init__(self):
        self.db_path = DATABASE_URL.replace('sqlite:///', '')
    
    async def init_db(self):
        """Создание таблиц"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
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
        
        logger.info("✅ База данных инициализирована")
    
    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получить пользователя"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM users WHERE user_id = ?", 
                (user_id,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def save_user(self, user_id: int, username: str = None, 
                       first_name: str = None, api_mode: str = 'bot',
                       api_id: str = None, api_hash: str = None) -> bool:
        """Сохранить или обновить пользователя"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Проверяем, существует ли пользователь
                cursor = await db.execute(
                    "SELECT user_id FROM users WHERE user_id = ?", 
                    (user_id,)
                )
                exists = await cursor.fetchone()
                
                now = datetime.now().isoformat()
                
                if exists:
                    # Обновляем
                    await db.execute("""
                        UPDATE users SET 
                            username = COALESCE(?, username),
                            first_name = COALESCE(?, first_name),
                            api_mode = ?,
                            api_id = COALESCE(?, api_id),
                            api_hash = COALESCE(?, api_hash),
                            updated_at = ?
                        WHERE user_id = ?
                    """, (username, first_name, api_mode, api_id, api_hash, now, user_id))
                else:
                    # Создаем
                    await db.execute("""
                        INSERT INTO users 
                        (user_id, username, first_name, api_mode, api_id, api_hash, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (user_id, username, first_name, api_mode, api_id, api_hash, now, now))
                
                await db.commit()
                return True
        
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения пользователя {user_id}: {e}")
            return False
    
    async def set_user_mode(self, user_id: int, mode: str) -> bool:
        """Установить режим пользователя"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "UPDATE users SET api_mode = ?, updated_at = ? WHERE user_id = ?",
                    (mode, datetime.now().isoformat(), user_id)
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка установки режима для {user_id}: {e}")
            return False
    
    async def save_user_credentials(self, user_id: int, api_id: str, api_hash: str) -> bool:
        """Сохранить API credentials пользователя"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    UPDATE users SET 
                        api_id = ?, 
                        api_hash = ?, 
                        api_mode = 'user',
                        updated_at = ? 
                    WHERE user_id = ?
                """, (api_id, api_hash, datetime.now().isoformat(), user_id))
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

# Глобальный экземпляр
db = Database()
