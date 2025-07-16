#!/usr/bin/env python3
"""
Topics Scanner Bot v5.16 - Главный файл
Простой и надежный Telegram бот для сканирования топиков
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime

from telethon import TelegramClient

# Импортируем наши модули
from config import BOT_TOKEN, API_ID, API_HASH, APP_NAME, VERSION, PORT, logger
from database import db
from scanner import scanner
from handlers import BotHandlers

class TopicsScannerBot:
    """Главный класс бота"""
    
    def __init__(self):
        self.bot = TelegramClient('bot_session', API_ID, API_HASH)
        self.handlers = None
        self.is_running = False
        self.start_time = None
    
    async def initialize(self):
        """Инициализация бота"""
        try:
            logger.info(f"🚀 Запуск {APP_NAME} v{VERSION}")
            
            # Инициализируем базу данных
            await db.init_db()
            
            # Запускаем бота
            await self.bot.start(bot_token=BOT_TOKEN)
            
            # Получаем информацию о боте
            me = await self.bot.get_me()
            logger.info(f"✅ Бот запущен: @{me.username}")
            
            # Настраиваем обработчики
            self.handlers = BotHandlers(self.bot)
            
            # Запускаем веб-сервер (если нужен)
            if PORT:
                asyncio.create_task(self._start_web_server())
            
            self.start_time = datetime.now()
            self.is_running = True
            
            logger.info("🎯 Бот готов к работе!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            raise
    
    async def _start_web_server(self):
        """Простой веб-сервер для health check"""
        try:
            from aiohttp import web
            
            async def health_check(request):
                """Health check endpoint"""
                uptime = datetime.now() - self.start_time if self.start_time else "неизвестно"
                
                return web.json_response({
                    'status': 'healthy',
                    'version': VERSION,
                    'uptime': str(uptime),
                    'bot_connected': self.bot.is_connected()
                })
            
            async def root_handler(request):
                """Корневая страница"""
                uptime = datetime.now() - self.start_time if self.start_time else "неизвестно"
                
                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>{APP_NAME} v{VERSION}</title>
                    <meta charset="utf-8">
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                        .container {{ max-width: 600px; background: white; padding: 30px; border-radius: 10px; }}
                        .status {{ padding: 10px; background: #4CAF50; color: white; border-radius: 5px; }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>🤖 {APP_NAME}</h1>
                        <div class="status">✅ Система работает</div>
                        <p><strong>Версия:</strong> {VERSION}</p>
                        <p><strong>Время работы:</strong> {uptime}</p>
                        <p><strong>API:</strong> <a href="/health">Health Check</a></p>
                    </div>
                </body>
                </html>
                """
                return web.Response(text=html, content_type='text/html')
            
            app = web.Application()
            app.router.add_get('/', root_handler)
            app.router.add_get('/health', health_check)
            
            runner = web.AppRunner(app)
            await runner.setup()
            
            site = web.TCPSite(runner, '0.0.0.0', PORT)
            await site.start()
            
            logger.info(f"🌐 Веб-сервер запущен на порту {PORT}")
            
        except Exception as e:
            logger.warning(f"⚠️ Не удалось запустить веб-сервер: {e}")
    
    async def run(self):
        """Основной цикл работы"""
        try:
            await self.initialize()
            logger.info("🎯 Бот запущен и готов к работе!")
            
            # Основной цикл
            await self.bot.run_until_disconnected()
            
        except KeyboardInterrupt:
            logger.info("⏹️ Получен сигнал остановки")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Корректное завершение работы"""
        logger.info("🛑 Завершение работы...")
        
        try:
            self.is_running = False
            
            # Закрываем все соединения сканера
            await scanner.cleanup()
            
            # Отключаем бота
            if self.bot.is_connected():
                await self.bot.disconnect()
            
            logger.info("✅ Корректное завершение завершено")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при завершении: {e}")

# Обработчик сигналов
def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения"""
    logger.info(f"📡 Получен сигнал {signum}")
    sys.exit(0)

async def main():
    """Главная функция"""
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Создаем и запускаем бота
    bot = TopicsScannerBot()
    
    try:
        await bot.run()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в main: {e}")
        sys.exit(1)

if __name__ == '__main__':
    try:
        # Проверяем версию Python
        if sys.version_info < (3, 8):
            print("❌ Требуется Python 3.8 или выше")
            sys.exit(1)
        
        # Запускаем бота
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("⏹️ Остановка бота...")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
