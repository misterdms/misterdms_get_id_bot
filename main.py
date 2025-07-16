#!/usr/bin/env python3
"""
Get ID Bot by Mister DMS - Точка входа приложения
ОБНОВЛЕНО v4.1.1: Keep Alive механизм + улучшенная стабильность
"""

import asyncio
import logging
import signal
import sys
import os
from datetime import datetime
import aiohttp
from contextlib import asynccontextmanager

# Импорты модулей бота
from config import (
    BOT_TOKEN, API_ID, API_HASH, PORT, 
    RENDER_EXTERNAL_URL, APP_VERSION, APP_NAME
)
from database import DatabaseManager
from handlers import BotHandlers
from web_server import WebServer
from utils import setup_logging

# Настройка логирования
logger = setup_logging()

class GetIdBot:
    """Основной класс Get ID Bot с Keep Alive механизмом"""
    
    def __init__(self):
        self.db_manager = None
        self.bot_handlers = None
        self.web_server = None
        self.keep_alive_task = None
        self.shutdown_event = asyncio.Event()
        
        logger.info(f"🤖 {APP_NAME} v{APP_VERSION} - Инициализация")
    
    async def keep_alive_ping(self):
        """Keep Alive механизм - пингует сам себя каждые 5 минут"""
        if not RENDER_EXTERNAL_URL:
            logger.debug("⚠️ RENDER_EXTERNAL_URL не задан, keep alive отключен")
            return
            
        ping_url = f"https://{RENDER_EXTERNAL_URL}/health"
        logger.info(f"🔄 Keep Alive запущен: {ping_url}")
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            while not self.shutdown_event.is_set():
                try:
                    # Ждем 5 минут или до сигнала завершения
                    await asyncio.wait_for(
                        self.shutdown_event.wait(), 
                        timeout=300  # 5 минут
                    )
                    break  # Если получили сигнал завершения
                except asyncio.TimeoutError:
                    # Время вышло, делаем ping
                    try:
                        async with session.get(ping_url) as response:
                            if response.status == 200:
                                logger.debug("💚 Keep Alive ping успешен")
                            else:
                                logger.warning(f"⚠️ Keep Alive ping неудачен: {response.status}")
                    except Exception as e:
                        logger.debug(f"⚠️ Keep Alive ping ошибка: {e}")
                        # Не критично, продолжаем работу
    
    async def initialize_components(self):
        """Инициализация всех компонентов бота"""
        try:
            # 1. База данных
            logger.info("🗄️ Инициализация базы данных...")
            self.db_manager = DatabaseManager()
            await self.db_manager.initialize()
            
            # 2. Обработчики команд
            logger.info("📡 Инициализация обработчиков команд...")
            self.bot_handlers = BotHandlers(self.db_manager)
            await self.bot_handlers.initialize()
            
            # 3. Веб-сервер
            logger.info("🌐 Инициализация веб-сервера...")
            self.web_server = WebServer(self.db_manager, self.bot_handlers)
            await self.web_server.start(PORT)
            
            # 4. Keep Alive задача
            logger.info("🔄 Запуск Keep Alive механизма...")
            self.keep_alive_task = asyncio.create_task(self.keep_alive_ping())
            
            logger.info("✅ Все компоненты инициализированы")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            raise
    
    async def start(self):
        """Запуск бота"""
        try:
            await self.initialize_components()
            
            logger.info("🚀 Get ID Bot запущен и готов к работе!")
            logger.info(f"📊 Веб-интерфейс: http://localhost:{PORT}/health")
            if RENDER_EXTERNAL_URL:
                logger.info(f"🌐 Внешний URL: https://{RENDER_EXTERNAL_URL}/health")
            
            # Ожидание сигнала завершения
            await self.shutdown_event.wait()
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Корректное завершение работы"""
        logger.info("🛑 Завершение работы бота...")
        
        # Устанавливаем флаг завершения
        self.shutdown_event.set()
        
        # Останавливаем Keep Alive
        if self.keep_alive_task and not self.keep_alive_task.done():
            self.keep_alive_task.cancel()
            try:
                await self.keep_alive_task
            except asyncio.CancelledError:
                pass
        
        # Останавливаем компоненты
        if self.bot_handlers:
            await self.bot_handlers.shutdown()
        
        if self.web_server:
            await self.web_server.stop()
        
        if self.db_manager:
            await self.db_manager.close()
        
        logger.info("✅ Бот корректно завершен")

# Глобальная переменная для бота
bot_instance = None

def signal_handler(signum, frame):
    """Обработчик сигналов завершения"""
    logger.info(f"📡 Получен сигнал {signum}, завершение работы...")
    if bot_instance and bot_instance.shutdown_event:
        bot_instance.shutdown_event.set()

async def main():
    """Главная функция"""
    global bot_instance
    
    # Валидация переменных окружения
    if not all([BOT_TOKEN, API_ID, API_HASH]):
        logger.error("❌ Не заданы обязательные переменные: BOT_TOKEN, API_ID, API_HASH")
        logger.error("💡 Проверьте переменные окружения на Render.com")
        sys.exit(1)
    
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Создание и запуск бота
        bot_instance = GetIdBot()
        await bot_instance.start()
        
    except KeyboardInterrupt:
        logger.info("🛑 Получен Ctrl+C, завершение работы...")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        # Запуск в asyncio event loop
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Завершение работы")
    except Exception as e:
        logger.error(f"❌ Фатальная ошибка: {e}")
        sys.exit(1)