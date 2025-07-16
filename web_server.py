#!/usr/bin/env python3
"""
Веб-сервер для Get ID Bot by Mister DMS
ОБНОВЛЕНО v4.1.1: улучшенные health checks + мониторинг + безопасность
"""

import asyncio
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import aiohttp
from aiohttp import web, ClientSession
import aiohttp_cors

from config import WEBHOOK_SECRET, APP_VERSION, APP_NAME, DEVELOPER, BOT_PREFIX
from utils import JSONUtils, format_timespan, format_file_size
from database import DatabaseManager

logger = logging.getLogger(__name__)

class WebServer:
    """Веб-сервер с мониторингом и health checks"""
    
    def __init__(self, db_manager: DatabaseManager, bot_handlers=None):
        self.db_manager = db_manager
        self.bot_handlers = bot_handlers
        self.app = None
        self.runner = None
        self.site = None
        self.start_time = datetime.now()
        
        # Метрики
        self.metrics = {
            'requests_total': 0,
            'requests_success': 0,
            'requests_error': 0,
            'last_request_time': None,
            'health_checks': 0,
            'uptime_start': self.start_time
        }
    
    async def start(self, port: int = 10000):
        """Запуск веб-сервера"""
        try:
            self.app = web.Application()
            
            # Настройка CORS
            cors = aiohttp_cors.setup(self.app, defaults={
                "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_headers="*",
                    allow_methods="*"
                )
            })
            
            # Регистрация роутов
            self.setup_routes()
            
            # Добавляем CORS ко всем роутам
            for route in list(self.app.router.routes()):
                cors.add(route)
            
            # Запуск сервера
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            self.site = web.TCPSite(self.runner, '0.0.0.0', port)
            await self.site.start()
            
            logger.info(f"🌐 Веб-сервер запущен на порту {port}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка запуска веб-сервера: {e}")
            raise
    
    def setup_routes(self):
        """Настройка маршрутов"""
        
        # Health Check эндпоинты
        self.app.router.add_get('/', self.handle_root)
        self.app.router.add_get('/health', self.handle_health)
        self.app.router.add_get('/metrics', self.handle_metrics)
        self.app.router.add_get('/status', self.handle_status)
        
        # Административные эндпоинты
        self.app.router.add_get('/admin/stats', self.handle_admin_stats)
        self.app.router.add_get('/admin/users', self.handle_admin_users)
        self.app.router.add_get('/admin/logs', self.handle_admin_logs)
        
        # Отладочные эндпоинты
        self.app.router.add_get('/debug/database', self.handle_debug_database)
        self.app.router.add_get('/debug/queue', self.handle_debug_queue)
        self.app.router.add_get('/debug/sessions', self.handle_debug_sessions)
        
        # API эндпоинты
        self.app.router.add_post('/api/webhook', self.handle_webhook)
        self.app.router.add_get('/api/version', self.handle_version)
        
        # Статические файлы (если нужно)
        self.app.router.add_get('/favicon.ico', self.handle_favicon)
        
        # Middleware для логирования
        self.app.middlewares.append(self.logging_middleware)
    
    @web.middleware
    async def logging_middleware(self, request, handler):
        """Middleware для логирования запросов"""
        start_time = time.time()
        self.metrics['requests_total'] += 1
        self.metrics['last_request_time'] = datetime.now()
        
        try:
            response = await handler(request)
            self.metrics['requests_success'] += 1
            
            # Логируем успешные запросы
            duration = (time.time() - start_time) * 1000
            logger.debug(f"📡 {request.method} {request.path} - {response.status} - {duration:.1f}ms")
            
            return response
            
        except Exception as e:
            self.metrics['requests_error'] += 1
            logger.error(f"❌ {request.method} {request.path} - ERROR: {e}")
            raise
    
    # === ОСНОВНЫЕ ОБРАБОТЧИКИ ===
    
    async def handle_root(self, request):
        """Главная страница"""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{APP_NAME}</title>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .status {{ padding: 10px; border-radius: 5px; margin: 10px 0; }}
                .healthy {{ background: #d4edda; color: #155724; }}
                .info {{ background: #d1ecf1; color: #0c5460; }}
                .warning {{ background: #fff3cd; color: #856404; }}
                .links {{ margin-top: 30px; }}
                .links a {{ display: inline-block; margin: 5px 10px; padding: 8px 15px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; }}
                .links a:hover {{ background: #0056b3; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🤖 {APP_NAME}</h1>
                    <p>Версия: {APP_VERSION} | Разработчик: {DEVELOPER}</p>
                </div>
                
                <div class="status healthy">
                    ✅ Бот работает и готов к использованию!
                </div>
                
                <div class="status info">
                    📊 Время работы: {format_timespan(self.start_time)}
                </div>
                
                <div class="status info">
                    🗄️ База данных: {self.db_manager.db_type.upper()}
                </div>
                
                <div class="links">
                    <h3>🔗 Полезные ссылки:</h3>
                    <a href="/health">Health Check</a>
                    <a href="/metrics">Метрики</a>
                    <a href="/status">Статус системы</a>
                    <a href="/api/version">API Version</a>
                </div>
                
                <div style="margin-top: 30px; text-align: center; color: #666;">
                    <p>Для использования бота найдите @misterdms_topic_id_get_bot в Telegram</p>
                </div>
            </div>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')
    
    async def handle_health(self, request):
        """Health Check эндпоинт"""
        try:
            self.metrics['health_checks'] += 1
            
            # Проверяем компоненты системы
            health_data = await self.get_health_status()
            
            status_code = 200 if health_data['status'] == 'healthy' else 503
            
            return web.json_response(health_data, status=status_code)
            
        except Exception as e:
            logger.error(f"❌ Ошибка health check: {e}")
            return web.json_response({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, status=503)
    
    async def handle_metrics(self, request):
        """Метрики системы"""
        try:
            metrics_data = await self.get_metrics_data()
            return web.json_response(metrics_data)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения метрик: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_status(self, request):
        """Детальный статус системы"""
        try:
            status_data = await self.get_detailed_status()
            return web.json_response(status_data)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    # === АДМИНИСТРАТИВНЫЕ ОБРАБОТЧИКИ ===
    
    async def handle_admin_stats(self, request):
        """Административная статистика"""
        try:
            # Простая авторизация по секретному ключу
            auth_key = request.headers.get('X-Admin-Key')
            if auth_key != WEBHOOK_SECRET:
                return web.json_response({'error': 'Unauthorized'}, status=401)
            
            stats = await self.db_manager.get_bot_stats()
            return web.json_response(stats)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения админ статистики: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_admin_users(self, request):
        """Список пользователей (админский)"""
        try:
            auth_key = request.headers.get('X-Admin-Key')
            if auth_key != WEBHOOK_SECRET:
                return web.json_response({'error': 'Unauthorized'}, status=401)
            
            # TODO: Реализовать получение списка пользователей
            users_data = {
                'total_users': 0,
                'active_users': 0,
                'users': []
            }
            
            return web.json_response(users_data)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения списка пользователей: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_admin_logs(self, request):
        """Системные логи (админский)"""
        try:
            auth_key = request.headers.get('X-Admin-Key')
            if auth_key != WEBHOOK_SECRET:
                return web.json_response({'error': 'Unauthorized'}, status=401)
            
            # TODO: Реализовать получение логов из БД
            logs_data = {
                'recent_logs': [],
                'error_count': 0,
                'warning_count': 0
            }
            
            return web.json_response(logs_data)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения логов: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    # === ОТЛАДОЧНЫЕ ОБРАБОТЧИКИ ===
    
    async def handle_debug_database(self, request):
        """Отладка состояния БД"""
        try:
            db_status = {
                'type': self.db_manager.db_type,
                'prefix': self.db_manager.bot_prefix,
                'url_masked': self.mask_db_url(self.db_manager.database_url),
                'tables': list(self.db_manager.tables.keys()),
                'connection_test': 'unknown'
            }
            
            # Тестируем подключение
            try:
                async with self.db_manager.get_connection() as conn:
                    db_status['connection_test'] = 'success'
            except Exception as e:
                db_status['connection_test'] = f'failed: {str(e)}'
            
            return web.json_response(db_status)
            
        except Exception as e:
            logger.error(f"❌ Ошибка отладки БД: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_debug_queue(self, request):
        """Отладка очереди запросов"""
        try:
            queue_status = await self.db_manager.get_queue_status()
            return web.json_response(queue_status)
            
        except Exception as e:
            logger.error(f"❌ Ошибка отладки очереди: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_debug_sessions(self, request):
        """Отладка активных сессий"""
        try:
            sessions_info = {
                'bot_handlers_active': self.bot_handlers is not None,
                'active_scans': 0,
                'total_sessions': 0
            }
            
            if hasattr(self.bot_handlers, 'topic_scanner'):
                scanner = self.bot_handlers.topic_scanner
                sessions_info['active_scans'] = len(getattr(scanner, 'active_scans', {}))
            
            return web.json_response(sessions_info)
            
        except Exception as e:
            logger.error(f"❌ Ошибка отладки сессий: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    # === API ОБРАБОТЧИКИ ===
    
    async def handle_webhook(self, request):
        """Webhook для внешних интеграций"""
        try:
            # Проверяем заголовки
            content_type = request.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                return web.json_response({'error': 'Invalid content type'}, status=400)
            
            # Получаем данные
            data = await request.json()
            
            # TODO: Обработка webhook данных (для будущей интеграции с n8n.io)
            
            return web.json_response({'status': 'received', 'data': data})
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки webhook: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_version(self, request):
        """API версии"""
        return web.json_response({
            'name': APP_NAME,
            'version': APP_VERSION,
            'developer': DEVELOPER,
            'bot_prefix': BOT_PREFIX,
            'timestamp': datetime.now().isoformat()
        })
    
    async def handle_favicon(self, request):
        """Заглушка для favicon"""
        return web.Response(status=204)
    
    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Получение статуса здоровья системы"""
        
        components = {}
        overall_status = 'healthy'
        
        # Проверка базы данных
        try:
            async with self.db_manager.get_connection() as conn:
                components['database'] = {
                    'status': 'healthy',
                    'type': self.db_manager.db_type,
                    'prefix': self.db_manager.bot_prefix
                }
        except Exception as e:
            components['database'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
            overall_status = 'degraded'
        
        # Проверка бот-обработчиков
        if self.bot_handlers:
            components['bot_handlers'] = {
                'status': 'healthy',
                'client_connected': hasattr(self.bot_handlers, 'bot_client') and 
                                  self.bot_handlers.bot_client is not None
            }
        else:
            components['bot_handlers'] = {
                'status': 'unhealthy',
                'error': 'Bot handlers not initialized'
            }
            overall_status = 'degraded'
        
        # Проверка очереди
        try:
            queue_status = await self.db_manager.get_queue_status()
            components['queue'] = {
                'status': 'healthy',
                **queue_status
            }
        except Exception as e:
            components['queue'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
        
        return {
            'status': overall_status,
            'version': APP_VERSION,
            'uptime': format_timespan(self.start_time),
            'timestamp': datetime.now().isoformat(),
            'components': components
        }
    
    async def get_metrics_data(self) -> Dict[str, Any]:
        """Получение метрик системы"""
        
        uptime_seconds = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'uptime_seconds': uptime_seconds,
            'requests_total': self.metrics['requests_total'],
            'requests_success': self.metrics['requests_success'],
            'requests_error': self.metrics['requests_error'],
            'success_rate': (self.metrics['requests_success'] / max(1, self.metrics['requests_total'])) * 100,
            'health_checks': self.metrics['health_checks'],
            'last_request': self.metrics['last_request_time'].isoformat() if self.metrics['last_request_time'] else None,
            'memory_info': self.get_memory_info(),
            'timestamp': datetime.now().isoformat()
        }
    
    async def get_detailed_status(self) -> Dict[str, Any]:
        """Получение детального статуса"""
        
        # Объединяем health и metrics
        health = await self.get_health_status()
        metrics = await self.get_metrics_data()
        
        # Дополнительная информация
        system_info = {
            'python_version': f"{__import__('sys').version_info.major}.{__import__('sys').version_info.minor}.{__import__('sys').version_info.micro}",
            'platform': __import__('platform').platform(),
            'start_time': self.start_time.isoformat(),
            'configuration': {
                'database_type': self.db_manager.db_type,
                'bot_prefix': BOT_PREFIX,
                'version': APP_VERSION
            }
        }
        
        return {
            **health,
            'metrics': metrics,
            'system': system_info
        }
    
    def get_memory_info(self) -> Dict[str, Any]:
        """Получение информации о памяти"""
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            
            return {
                'rss': memory_info.rss,
                'vms': memory_info.vms,
                'rss_mb': round(memory_info.rss / 1024 / 1024, 2),
                'vms_mb': round(memory_info.vms / 1024 / 1024, 2)
            }
        except ImportError:
            return {'error': 'psutil not available'}
        except Exception as e:
            return {'error': str(e)}
    
    def mask_db_url(self, url: str) -> str:
        """Маскировка пароля в URL БД"""
        try:
            if '://' in url and '@' in url:
                parts = url.split('://')
                if len(parts) == 2:
                    protocol = parts[0]
                    rest = parts[1]
                    if '@' in rest:
                        credentials, host_part = rest.split('@', 1)
                        if ':' in credentials:
                            user, password = credentials.split(':', 1)
                            masked_password = '*' * len(password)
                            return f"{protocol}://{user}:{masked_password}@{host_part}"
            return url
        except:
            return "masked_url"
    
    async def stop(self):
        """Остановка веб-сервера"""
        try:
            if self.site:
                await self.site.stop()
            if self.runner:
                await self.runner.cleanup()
            
            logger.info("✅ Веб-сервер остановлен")
            
        except Exception as e:
            logger.error(f"❌ Ошибка остановки веб-сервера: {e}")

# === ЭКСПОРТ ===

__all__ = ['WebServer']