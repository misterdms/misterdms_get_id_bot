#!/usr/bin/env python3
"""
Веб-сервер для мониторинга и health check гибридного Topics Scanner Bot
Предоставляет REST API для мониторинга состояния системы
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from aiohttp import web, ClientSession
from aiohttp.web_runner import AppRunner, TCPSite
import aiohttp_cors

from config import WEBHOOK_SECRET, APP_VERSION, APP_NAME

logger = logging.getLogger(__name__)

class WebServer:
    """Веб-сервер для мониторинга бота"""
    
    def __init__(self, auth_manager, db_manager, queue_manager):
        self.auth_manager = auth_manager
        self.db_manager = db_manager
        self.queue_manager = queue_manager
        self.app = None
        self.runner = None
        self.site = None
        self.start_time = datetime.now()
        
    def create_app(self) -> web.Application:
        """Создание веб-приложения"""
        app = web.Application()
        
        # Настройка CORS
        cors = aiohttp_cors.setup(app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*"
            )
        })
        
        # Основные эндпоинты
        app.router.add_get('/', self.handle_root)
        app.router.add_get('/health', self.handle_health)
        app.router.add_get('/metrics', self.handle_metrics)
        app.router.add_get('/queue', self.handle_queue)
        app.router.add_get('/sessions', self.handle_sessions)
        app.router.add_get('/database', self.handle_database)
        app.router.add_get('/status', self.handle_full_status)
        
        # API эндпоинты
        app.router.add_get('/api/v1/health', self.handle_health)
        app.router.add_get('/api/v1/metrics', self.handle_metrics_json)
        app.router.add_get('/api/v1/queue', self.handle_queue_json)
        app.router.add_get('/api/v1/sessions', self.handle_sessions_json)
        
        # Webhook эндпоинты (если нужны)
        app.router.add_post('/webhook', self.handle_webhook)
        
        # Добавляем CORS ко всем эндпоинтам
        for route in list(app.router.routes()):
            cors.add(route)
        
        # Middleware для логирования
        app.middlewares.append(self.logging_middleware)
        
        self.app = app
        return app
    
    @web.middleware
    async def logging_middleware(self, request, handler):
        """Middleware для логирования запросов"""
        start_time = datetime.now()
        
        try:
            response = await handler(request)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"🌐 {request.method} {request.path} - {response.status} ({duration:.3f}s)")
            
            return response
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"🌐 {request.method} {request.path} - ERROR: {e} ({duration:.3f}s)")
            raise
    
    async def handle_root(self, request):
        """Корневая страница с информацией о боте"""
        uptime = datetime.now() - self.start_time
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{APP_NAME} - Monitoring</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                .container {{ max-width: 800px; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                h1 {{ color: #2196F3; }}
                .status {{ padding: 10px; margin: 10px 0; border-radius: 5px; }}
                .healthy {{ background: #4CAF50; color: white; }}
                .warning {{ background: #FF9800; color: white; }}
                .error {{ background: #F44336; color: white; }}
                .metric {{ margin: 10px 0; padding: 10px; background: #e3f2fd; border-radius: 5px; }}
                .api-links {{ margin: 20px 0; }}
                .api-links a {{ display: inline-block; margin: 5px; padding: 8px 15px; background: #2196F3; color: white; text-decoration: none; border-radius: 5px; }}
                .footer {{ margin-top: 30px; text-align: center; color: #666; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🤖 {APP_NAME}</h1>
                <div class="status healthy">✅ Система работает</div>
                
                <div class="metric">
                    <strong>Версия:</strong> {APP_VERSION}<br>
                    <strong>Время работы:</strong> {uptime}<br>
                    <strong>Запущен:</strong> {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}
                </div>
                
                <h3>📊 API Эндпоинты</h3>
                <div class="api-links">
                    <a href="/health">Health Check</a>
                    <a href="/metrics">Метрики</a>
                    <a href="/queue">Очередь</a>
                    <a href="/sessions">Сессии</a>
                    <a href="/database">База данных</a>
                    <a href="/status">Полный статус</a>
                </div>
                
                <h3>🔧 JSON API</h3>
                <div class="api-links">
                    <a href="/api/v1/health">JSON Health</a>
                    <a href="/api/v1/metrics">JSON Метрики</a>
                    <a href="/api/v1/queue">JSON Очередь</a>
                    <a href="/api/v1/sessions">JSON Сессии</a>
                </div>
                
                <div class="footer">
                    <p>Гибридный Topics Scanner Bot для Telegram</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return web.Response(text=html, content_type='text/html')
    
    async def handle_health(self, request):
        """Health check эндпоинт"""
        try:
            # Проверяем основные компоненты
            db_healthy = await self.db_manager.health_check()
            queue_healthy = self.queue_manager.is_processing
            
            # Получаем статус сессий
            session_stats = await self.auth_manager.get_active_sessions_count()
            
            overall_health = db_healthy and queue_healthy
            
            status_text = "healthy" if overall_health else "unhealthy"
            status_code = 200 if overall_health else 503
            
            uptime = datetime.now() - self.start_time
            
            health_data = {
                'status': status_text,
                'timestamp': datetime.now().isoformat(),
                'uptime_seconds': uptime.total_seconds(),
                'version': APP_VERSION,
                'components': {
                    'database': 'healthy' if db_healthy else 'unhealthy',
                    'queue': 'healthy' if queue_healthy else 'unhealthy',
                    'sessions': f"{session_stats['total_sessions']}/{session_stats['max_sessions']}"
                }
            }
            
            # HTML ответ для браузера
            if 'text/html' in request.headers.get('Accept', ''):
                component_status = []
                for component, status in health_data['components'].items():
                    emoji = "✅" if "healthy" in status or "/" in status else "❌"
                    component_status.append(f"{emoji} {component}: {status}")
                
                html = f"""
                <h2>🏥 Health Check</h2>
                <div class="status {'healthy' if overall_health else 'error'}">
                    {status_text.upper()}
                </div>
                <div class="metric">
                    <strong>Timestamp:</strong> {health_data['timestamp']}<br>
                    <strong>Uptime:</strong> {uptime}<br>
                    <strong>Version:</strong> {APP_VERSION}
                </div>
                <h3>Компоненты:</h3>
                <ul>
                {"".join(f"<li>{status}</li>" for status in component_status)}
                </ul>
                <a href="/">← Назад</a>
                """
                return web.Response(text=html, content_type='text/html', status=status_code)
            
            # JSON ответ для API
            return web.json_response(health_data, status=status_code)
            
        except Exception as e:
            logger.error(f"❌ Ошибка health check: {e}")
            error_data = {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            return web.json_response(error_data, status=500)
    
    async def handle_metrics(self, request):
        """Метрики производительности"""
        try:
            # Собираем метрики
            db_stats = await self.db_manager.get_database_stats()
            queue_status = await self.queue_manager.get_queue_status()
            session_stats = await self.auth_manager.get_active_sessions_count()
            
            uptime = datetime.now() - self.start_time
            
            metrics = {
                'system': {
                    'uptime_seconds': uptime.total_seconds(),
                    'start_time': self.start_time.isoformat(),
                    'version': APP_VERSION
                },
                'database': db_stats,
                'queue': {
                    'pending': queue_status.get('pending', 0),
                    'processing': queue_status.get('processing', 0),
                    'completed': queue_status.get('completed', 0),
                    'failed': queue_status.get('failed', 0),
                    'active_tasks': queue_status.get('active_tasks', 0),
                    'success_rate': queue_status.get('stats', {}).get('success_rate', 0)
                },
                'sessions': session_stats,
                'timestamp': datetime.now().isoformat()
            }
            
            # HTML ответ
            if 'text/html' in request.headers.get('Accept', ''):
                html = f"""
                <h2>📊 Метрики системы</h2>
                <div class="metric">
                    <h3>🖥️ Система</h3>
                    <strong>Время работы:</strong> {uptime}<br>
                    <strong>Запущен:</strong> {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}<br>
                    <strong>Версия:</strong> {APP_VERSION}
                </div>
                
                <div class="metric">
                    <h3>🗄️ База данных</h3>
                    <strong>Пользователей:</strong> {db_stats.get('users_count', 0)}<br>
                    <strong>Активных:</strong> {db_stats.get('active_users', 0)}<br>
                    <strong>В user режиме:</strong> {db_stats.get('user_mode_users', 0)}<br>
                    <strong>Записей активности:</strong> {db_stats.get('activity_data_count', 0)}
                </div>
                
                <div class="metric">
                    <h3>📋 Очередь</h3>
                    <strong>Ожидает:</strong> {queue_status.get('pending', 0)}<br>
                    <strong>Выполняется:</strong> {queue_status.get('processing', 0)}<br>
                    <strong>Завершено:</strong> {queue_status.get('completed', 0)}<br>
                    <strong>Ошибок:</strong> {queue_status.get('failed', 0)}<br>
                    <strong>Активных задач:</strong> {queue_status.get('active_tasks', 0)}
                </div>
                
                <div class="metric">
                    <h3>🔗 Сессии</h3>
                    <strong>Активных:</strong> {session_stats['total_sessions']}<br>
                    <strong>Максимум:</strong> {session_stats['max_sessions']}<br>
                    <strong>Свободно:</strong> {session_stats['available_slots']}
                </div>
                
                <a href="/">← Назад</a>
                """
                return web.Response(text=html, content_type='text/html')
            
            return web.json_response(metrics)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения метрик: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_queue(self, request):
        """Статус очереди"""
        try:
            queue_status = await self.queue_manager.get_queue_status()
            
            # HTML ответ
            if 'text/html' in request.headers.get('Accept', ''):
                html = f"""
                <h2>📋 Статус очереди</h2>
                <div class="metric">
                    <strong>Ожидает выполнения:</strong> {queue_status.get('pending', 0)}<br>
                    <strong>Выполняется:</strong> {queue_status.get('processing', 0)}<br>
                    <strong>Завершено:</strong> {queue_status.get('completed', 0)}<br>
                    <strong>Ошибок:</strong> {queue_status.get('failed', 0)}<br>
                    <strong>Активных задач:</strong> {queue_status.get('active_tasks', 0)}<br>
                    <strong>Максимум параллельных:</strong> {queue_status.get('max_concurrent', 5)}
                </div>
                
                <h3>🔄 Активные задачи</h3>
                """
                
                processing_tasks = queue_status.get('processing_tasks', [])
                if processing_tasks:
                    html += "<ul>"
                    for task in processing_tasks:
                        html += f"""
                        <li>
                            <strong>ID {task['id']}:</strong> {task['command']} 
                            (Пользователь: {task['user_id']}, 
                            Время: {task['processing_time']:.1f}с)
                        </li>
                        """
                    html += "</ul>"
                else:
                    html += "<p>Нет активных задач</p>"
                
                html += '<a href="/">← Назад</a>'
                return web.Response(text=html, content_type='text/html')
            
            return web.json_response(queue_status)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса очереди: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_sessions(self, request):
        """Информация о сессиях"""
        try:
            session_stats = await self.auth_manager.get_active_sessions_count()
            health_info = await self.auth_manager.health_check()
            
            # HTML ответ
            if 'text/html' in request.headers.get('Accept', ''):
                html = f"""
                <h2>🔗 Активные сессии</h2>
                <div class="metric">
                    <strong>Всего активных:</strong> {session_stats['total_sessions']}<br>
                    <strong>Максимум:</strong> {session_stats['max_sessions']}<br>
                    <strong>Свободных слотов:</strong> {session_stats['available_slots']}<br>
                    <strong>Здоровых сессий:</strong> {health_info.get('healthy_sessions', 0)}<br>
                    <strong>Проблемных сессий:</strong> {health_info.get('unhealthy_sessions', 0)}
                </div>
                
                <h3>📊 Детали сессий</h3>
                """
                
                session_details = health_info.get('session_details', {})
                if session_details:
                    html += "<ul>"
                    for user_id, status in session_details.items():
                        emoji = "✅" if status == "healthy" else "❌"
                        html += f"<li>{emoji} Пользователь {user_id}: {status}</li>"
                    html += "</ul>"
                else:
                    html += "<p>Нет активных сессий</p>"
                
                html += '<a href="/">← Назад</a>'
                return web.Response(text=html, content_type='text/html')
            
            return web.json_response({
                'session_stats': session_stats,
                'health_info': health_info,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения информации о сессиях: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_database(self, request):
        """Статус базы данных"""
        try:
            db_stats = await self.db_manager.get_database_stats()
            db_healthy = await self.db_manager.health_check()
            
            # HTML ответ
            if 'text/html' in request.headers.get('Accept', ''):
                status_class = "healthy" if db_healthy else "error"
                status_text = "Работает нормально" if db_healthy else "Проблемы"
                
                html = f"""
                <h2>🗄️ Статус базы данных</h2>
                <div class="status {status_class}">
                    {status_text}
                </div>
                
                <div class="metric">
                    <h3>📊 Статистика таблиц</h3>
                    <strong>Пользователи:</strong> {db_stats.get('users_count', 0)}<br>
                    <strong>Активные пользователи:</strong> {db_stats.get('active_users', 0)}<br>
                    <strong>User режим:</strong> {db_stats.get('user_mode_users', 0)}<br>
                    <strong>Записи активности:</strong> {db_stats.get('activity_data_count', 0)}<br>
                    <strong>Задачи в очереди:</strong> {db_stats.get('request_queue_count', 0)}<br>
                    <strong>Настройки:</strong> {db_stats.get('bot_settings_count', 0)}<br>
                    <strong>Логи:</strong> {db_stats.get('bot_logs_count', 0)}
                </div>
                
                <a href="/">← Назад</a>
                """
                return web.Response(text=html, content_type='text/html')
            
            return web.json_response({
                'healthy': db_healthy,
                'stats': db_stats,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса БД: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_full_status(self, request):
        """Полный статус системы"""
        try:
            # Собираем всю информацию
            db_stats = await self.db_manager.get_database_stats()
            db_healthy = await self.db_manager.health_check()
            queue_status = await self.queue_manager.get_queue_status()
            session_stats = await self.auth_manager.get_active_sessions_count()
            session_health = await self.auth_manager.health_check()
            
            uptime = datetime.now() - self.start_time
            
            full_status = {
                'system': {
                    'healthy': db_healthy and queue_status.get('is_processing', False),
                    'uptime_seconds': uptime.total_seconds(),
                    'start_time': self.start_time.isoformat(),
                    'version': APP_VERSION,
                    'name': APP_NAME
                },
                'database': {
                    'healthy': db_healthy,
                    'stats': db_stats
                },
                'queue': queue_status,
                'sessions': {
                    'stats': session_stats,
                    'health': session_health
                },
                'timestamp': datetime.now().isoformat()
            }
            
            return web.json_response(full_status)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения полного статуса: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    # JSON API эндпоинты
    async def handle_metrics_json(self, request):
        """JSON метрики"""
        return await self.handle_metrics(request)
    
    async def handle_queue_json(self, request):
        """JSON статус очереди"""
        return await self.handle_queue(request)
    
    async def handle_sessions_json(self, request):
        """JSON информация о сессиях"""
        return await self.handle_sessions(request)
    
    async def handle_webhook(self, request):
        """Webhook эндпоинт"""
        try:
            # Проверяем секретный ключ
            auth_header = request.headers.get('Authorization')
            if auth_header != f"Bearer {WEBHOOK_SECRET}":
                return web.json_response({'error': 'Unauthorized'}, status=401)
            
            # Получаем данные
            data = await request.json()
            
            # Логируем webhook
            logger.info(f"📥 Webhook получен: {data}")
            
            # Здесь можно добавить обработку webhook данных
            
            return web.json_response({'status': 'ok', 'received': True})
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки webhook: {e}")
            return web.json_response({'error': str(e)}, status=500)

async def create_web_server(port: int, auth_manager, db_manager, queue_manager):
    """
    Создание и запуск веб-сервера
    
    Args:
        port: Порт для сервера
        auth_manager: Менеджер аутентификации
        db_manager: Менеджер базы данных
        queue_manager: Менеджер очереди
    
    Returns:
        Tuple[WebServer, AppRunner]: Сервер и runner
    """
    try:
        logger.info(f"🌐 Создание веб-сервера на порту {port}")
        
        # Создаем сервер
        web_server = WebServer(auth_manager, db_manager, queue_manager)
        app = web_server.create_app()
        
        # Создаем runner
        runner = AppRunner(app)
        await runner.setup()
        
        # Создаем site
        site = TCPSite(runner, '0.0.0.0', port)
        await site.start()
        
        web_server.runner = runner
        web_server.site = site
        
        logger.info(f"✅ Веб-сервер запущен на http://0.0.0.0:{port}")
        
        return web_server, runner
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска веб-сервера: {e}")
        raise

async def stop_web_server(runner: AppRunner):
    """Остановка веб-сервера"""
    try:
        if runner:
            await runner.cleanup()
            logger.info("✅ Веб-сервер остановлен")
    except Exception as e:
        logger.error(f"❌ Ошибка остановки веб-сервера: {e}")

# Функции для быстрого доступа
def create_simple_health_response() -> Dict[str, Any]:
    """Создание простого health check ответа"""
    return {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': APP_NAME,
        'version': APP_VERSION
    }
