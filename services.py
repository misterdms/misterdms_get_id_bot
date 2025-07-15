#!/usr/bin/env python3
"""
Объединенные сервисы для гибридного Topics Scanner Bot
Включает: ActivityTracker, APILimiter, QueueManager
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

from config import (
    MAX_QUEUE_SIZE, QUEUE_PRIORITIES, TASK_STATUSES, API_LIMITS,
    SESSION_TIMEOUT_DAYS, USER_STATUSES
)
from database import db_manager

logger = logging.getLogger(__name__)

# =============================================================================
# DATACLASSES И ВСПОМОГАТЕЛЬНЫЕ ТИПЫ
# =============================================================================

@dataclass
class QueueTask:
    """Задача в очереди"""
    id: int
    user_id: int
    chat_id: Optional[int]
    command: str
    parameters: Optional[Dict[str, Any]]
    priority: int
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

@dataclass
class RequestRecord:
    """Запись о выполненном запросе"""
    timestamp: float
    request_type: str
    chat_id: Optional[int] = None
    user_id: Optional[int] = None
    duration: Optional[float] = None
    success: bool = True

@dataclass
class UserActivity:
    """Данные активности пользователя"""
    user_id: int
    username: Optional[str]
    first_name: Optional[str]
    message_count: int
    last_activity: datetime
    date_tracked: date

# =============================================================================
# ГЛАВНЫЙ МЕНЕДЖЕР СЕРВИСОВ
# =============================================================================

class ServiceManager:
    """Объединенный менеджер всех сервисов бота"""
    
    def __init__(self):
        self.activity = ActivityService()
        self.limiter = APILimiterService()
        self.queue = QueueService()
        self.is_initialized = False
        
    async def initialize(self, bot_handler=None, user_handler=None, bot_client=None):
        """Инициализация всех сервисов"""
        try:
            logger.info("🔄 Инициализация ServiceManager...")
            
            # Инициализируем все сервисы параллельно
            await asyncio.gather(
                self.activity.initialize(),
                self.limiter.initialize(),
                self.queue.initialize(bot_handler, user_handler, bot_client)
            )
            
            self.is_initialized = True
            logger.info("✅ ServiceManager инициализирован")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации ServiceManager: {e}")
            raise
    
    async def start_background_tasks(self):
        """Запуск всех фоновых задач"""
        if not self.is_initialized:
            raise RuntimeError("ServiceManager не инициализирован")
        
        # Запускаем фоновые задачи всех сервисов
        asyncio.create_task(self.activity.background_worker())
        asyncio.create_task(self.limiter.background_worker())
        asyncio.create_task(self.queue.background_worker())
        
        logger.info("✅ Все фоновые задачи запущены")
    
    async def cleanup(self):
        """Очистка всех сервисов"""
        logger.info("🛑 Завершение работы ServiceManager...")
        
        await asyncio.gather(
            self.activity.cleanup(),
            self.limiter.cleanup(),
            self.queue.cleanup(),
            return_exceptions=True
        )
        
        logger.info("✅ ServiceManager завершил работу")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Получить общий статус здоровья всех сервисов"""
        return {
            'activity': self.activity.get_health_status(),
            'limiter': self.limiter.get_health_status(),
            'queue': self.queue.get_health_status(),
            'overall_healthy': (
                self.activity.is_healthy() and
                self.limiter.is_healthy() and
                self.queue.is_healthy()
            )
        }

# =============================================================================
# СЕРВИС ОТСЛЕЖИВАНИЯ АКТИВНОСТИ
# =============================================================================

class ActivityService:
    """Сервис отслеживания активности пользователей"""
    
    def __init__(self):
        self.memory_cache: Dict[int, Dict[int, UserActivity]] = {}  # {chat_id: {user_id: activity}}
        self.last_reset_date = datetime.now().date()
        self.cache_size_limit = 10000
        self.batch_write_interval = 60
        self.pending_writes: List[Dict] = []
        self.is_running = False
        
    async def initialize(self):
        """Инициализация сервиса активности"""
        try:
            await self._load_today_data()
            self.is_running = True
            logger.info("✅ ActivityService инициализирован")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации ActivityService: {e}")
            raise
    
    async def track_user_activity(self, event) -> bool:
        """Отследить активность пользователя"""
        try:
            # Проверки валидности
            if event.text and event.text.startswith('/'):
                return False
            
            if event.sender and hasattr(event.sender, 'bot') and event.sender.bot:
                return False
            
            if event.is_private:
                return False
            
            # Получаем информацию о пользователе
            sender = event.sender
            if not sender:
                return False
            
            await self._update_user_activity(
                chat_id=event.chat_id,
                user_id=sender.id,
                username=getattr(sender, 'username', None),
                first_name=getattr(sender, 'first_name', None)
            )
            
            return True
            
        except Exception as e:
            logger.debug(f"Ошибка отслеживания активности: {e}")
            return False
    
    async def _update_user_activity(self, chat_id: int, user_id: int, 
                                   username: str = None, first_name: str = None) -> bool:
        """Обновить активность пользователя"""
        try:
            current_date = datetime.now().date()
            current_time = datetime.now()
            
            # Проверяем сброс данных за новый день
            if current_date > self.last_reset_date:
                await self._reset_daily_data()
                self.last_reset_date = current_date
            
            # Инициализируем кэш для чата
            if chat_id not in self.memory_cache:
                self.memory_cache[chat_id] = {}
            
            # Обновляем активность
            if user_id in self.memory_cache[chat_id]:
                activity = self.memory_cache[chat_id][user_id]
                activity.message_count += 1
                activity.last_activity = current_time
                
                if username:
                    activity.username = username
                if first_name:
                    activity.first_name = first_name
            else:
                activity = UserActivity(
                    user_id=user_id,
                    username=username,
                    first_name=first_name,
                    message_count=1,
                    last_activity=current_time,
                    date_tracked=current_date
                )
                self.memory_cache[chat_id][user_id] = activity
            
            # Добавляем в очередь на запись
            self.pending_writes.append({
                'chat_id': chat_id,
                'user_id': user_id,
                'username': username,
                'first_name': first_name,
                'action': 'update'
            })
            
            await self._check_cache_limit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления активности: {e}")
            return False
    
    async def get_active_users(self, chat_id: int, date_filter: date = None) -> List[Dict[str, Any]]:
        """Получить список активных пользователей"""
        try:
            current_date = date_filter or datetime.now().date()
            
            if current_date == datetime.now().date() and chat_id in self.memory_cache:
                users = []
                for activity in self.memory_cache[chat_id].values():
                    users.append({
                        'user_id': activity.user_id,
                        'username': activity.username,
                        'first_name': activity.first_name,
                        'message_count': activity.message_count,
                        'last_activity': activity.last_activity.isoformat()
                    })
                
                users.sort(key=lambda x: x['message_count'], reverse=True)
                return users
            
            return await db_manager.get_active_users(chat_id, current_date)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения активных пользователей: {e}")
            return []
    
    async def get_activity_stats(self, chat_id: int, date_filter: date = None) -> Dict[str, Any]:
        """Получить статистику активности"""
        try:
            current_date = date_filter or datetime.now().date()
            
            if current_date == datetime.now().date() and chat_id in self.memory_cache:
                activities = self.memory_cache[chat_id].values()
                
                if activities:
                    total_users = len(activities)
                    total_messages = sum(a.message_count for a in activities)
                    max_messages = max(a.message_count for a in activities)
                    avg_messages = total_messages / total_users if total_users > 0 else 0
                    
                    return {
                        'total_users': total_users,
                        'total_messages': total_messages,
                        'max_messages': max_messages,
                        'avg_messages': round(avg_messages, 1),
                        'date': current_date.strftime('%d.%m.%Y')
                    }
            
            return await db_manager.get_activity_stats(chat_id, current_date)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {
                'total_users': 0,
                'total_messages': 0,
                'max_messages': 0,
                'avg_messages': 0,
                'date': current_date.strftime('%d.%m.%Y')
            }
    
    async def background_worker(self):
        """Фоновый процесс обработки активности"""
        while self.is_running:
            try:
                # Пакетная запись в БД
                await asyncio.sleep(self.batch_write_interval)
                
                if self.pending_writes:
                    writes_to_process = self.pending_writes.copy()
                    self.pending_writes.clear()
                    
                    # Группируем записи по пользователям
                    user_updates = {}
                    for write in writes_to_process:
                        key = (write['chat_id'], write['user_id'])
                        user_updates[key] = write
                    
                    # Записываем в БД
                    for write in user_updates.values():
                        await db_manager.add_user_activity(
                            chat_id=write['chat_id'],
                            user_id=write['user_id'],
                            username=write['username'],
                            first_name=write['first_name']
                        )
                    
                    logger.debug(f"💾 Записано {len(user_updates)} обновлений активности")
                
                # Проверяем сброс дня
                current_date = datetime.now().date()
                if current_date > self.last_reset_date:
                    await self._reset_daily_data()
                    self.last_reset_date = current_date
                
                # Очистка кэша
                await self._check_cache_limit()
                
            except Exception as e:
                logger.error(f"❌ Ошибка фонового процесса активности: {e}")
    
    async def _reset_daily_data(self):
        """Сброс данных за день"""
        try:
            old_count = sum(len(users) for users in self.memory_cache.values())
            self.memory_cache.clear()
            logger.info(f"🔄 Сброс данных активности: очищено {old_count} записей")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сброса данных: {e}")
    
    async def _check_cache_limit(self):
        """Проверка лимита кэша"""
        try:
            total_entries = sum(len(users) for users in self.memory_cache.values())
            
            if total_entries > self.cache_size_limit:
                entries_to_remove = total_entries - self.cache_size_limit // 2
                removed = 0
                
                for chat_id in list(self.memory_cache.keys()):
                    if removed >= entries_to_remove:
                        break
                    
                    users = self.memory_cache[chat_id]
                    sorted_users = sorted(users.items(), key=lambda x: x[1].last_activity)
                    
                    for user_id, _ in sorted_users:
                        if removed >= entries_to_remove:
                            break
                        del users[user_id]
                        removed += 1
                    
                    if not users:
                        del self.memory_cache[chat_id]
                
                logger.info(f"🧹 Очищено {removed} старых записей из кэша")
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки кэша: {e}")
    
    async def _load_today_data(self):
        """Загрузка данных за сегодня"""
        logger.debug("📥 Загрузка данных активности в кэш (упрощенная версия)")
    
    async def cleanup(self):
        """Очистка ресурсов"""
        try:
            self.is_running = False
            await self._force_sync_to_db()
            self.memory_cache.clear()
            self.pending_writes.clear()
            logger.info("✅ ActivityService завершил работу")
            
        except Exception as e:
            logger.error(f"❌ Ошибка завершения ActivityService: {e}")
    
    async def _force_sync_to_db(self):
        """Принудительная синхронизация с БД"""
        if self.pending_writes:
            logger.info("🔄 Принудительная синхронизация активности с БД...")
            # Используем существующий механизм пакетной записи
            await self.background_worker()
    
    def get_health_status(self) -> Dict[str, Any]:
        """Статус здоровья сервиса активности"""
        return {
            'is_running': self.is_running,
            'cache_size': sum(len(users) for users in self.memory_cache.values()),
            'pending_writes': len(self.pending_writes),
            'last_reset_date': self.last_reset_date.isoformat()
        }
    
    def is_healthy(self) -> bool:
        """Проверка здоровья сервиса"""
        return self.is_running

# =============================================================================
# СЕРВИС УПРАВЛЕНИЯ ЛИМИТАМИ API
# =============================================================================

class APILimiterService:
    """Сервис управления лимитами API с автоматическим переключением"""
    
    def __init__(self):
        self.current_mode = 'normal'
        self.auto_mode_enabled = True
        self.request_history: List[RequestRecord] = []
        self.disabled_chats = set()
        self.user_limits: Dict[int, Dict] = {}
        
        # Статистика
        self.error_count = 0
        self.success_count = 0
        self.last_error_time = 0
        self.mode_change_history: List[Dict] = []
        
        # Настройки
        self.error_threshold = 3
        self.success_threshold = 10
        self.mode_change_cooldown = 300
        self.is_running = False
    
    async def initialize(self):
        """Инициализация сервиса лимитов"""
        self.is_running = True
        logger.info("✅ APILimiterService инициализирован")
    
    def can_make_request(self, user_id: Optional[int] = None, request_type: str = 'general') -> bool:
        """Проверка возможности выполнения запроса"""
        now = time.time()
        limits = API_LIMITS[self.current_mode]
        
        # Проверка глобального cooldown
        if self.request_history:
            last_request = self.request_history[-1]
            if now - last_request.timestamp < limits['cooldown']:
                return False
        
        # Очистка старых запросов
        hour_ago = now - 3600
        self.request_history = [r for r in self.request_history if r.timestamp > hour_ago]
        
        # Проверка глобального лимита
        if len(self.request_history) >= limits['max_hour']:
            return False
        
        # Проверка индивидуальных лимитов
        if user_id and user_id in self.user_limits:
            user_limit = self.user_limits[user_id]
            user_requests = [r for r in self.request_history if r.user_id == user_id]
            
            if len(user_requests) >= user_limit.get('max_hour', limits['max_hour']):
                return False
        
        return True
    
    def record_request(self, request_type: str = 'general', chat_id: Optional[int] = None, 
                      user_id: Optional[int] = None, duration: Optional[float] = None, 
                      success: bool = True):
        """Записать выполненный запрос"""
        now = time.time()
        
        record = RequestRecord(
            timestamp=now,
            request_type=request_type,
            chat_id=chat_id,
            user_id=user_id,
            duration=duration,
            success=success
        )
        
        self.request_history.append(record)
        
        # Обновляем статистику
        if success:
            self.success_count += 1
            self.error_count = max(0, self.error_count - 1)
        else:
            self.error_count += 1
            self.last_error_time = now
            self.success_count = 0
        
        # Проверяем автопереключение
        if self.auto_mode_enabled:
            asyncio.create_task(self._check_auto_mode_switch())
    
    def set_mode(self, mode: str, reason: str = "manual") -> bool:
        """Установить режим лимитов"""
        if mode not in API_LIMITS:
            return False
        
        old_mode = self.current_mode
        self.current_mode = mode
        
        # Записываем изменение
        change_record = {
            'timestamp': time.time(),
            'old_mode': old_mode,
            'new_mode': mode,
            'reason': reason,
            'auto_mode': self.auto_mode_enabled
        }
        self.mode_change_history.append(change_record)
        
        # Ограничиваем историю
        if len(self.mode_change_history) > 100:
            self.mode_change_history = self.mode_change_history[-50:]
        
        logger.info(f"🔧 Режим лимитов: {API_LIMITS[old_mode]['name']} → {API_LIMITS[mode]['name']} ({reason})")
        return True
    
    def auto_adjust_mode(self, participants_count: int, request_complexity: str = 'normal') -> bool:
        """Автоматическое переключение режима"""
        if not self.auto_mode_enabled:
            return False
        
        optimal_mode = self._calculate_optimal_mode(participants_count, request_complexity)
        
        if optimal_mode != self.current_mode:
            now = time.time()
            if self.mode_change_history:
                last_change = self.mode_change_history[-1]
                if now - last_change['timestamp'] < self.mode_change_cooldown:
                    return False
            
            return self.set_mode(optimal_mode, f"auto_adjust: {participants_count} participants, {request_complexity}")
        
        return False
    
    def _calculate_optimal_mode(self, participants_count: int, request_complexity: str) -> str:
        """Расчет оптимального режима"""
        # Базовые правила
        if participants_count > 1000:
            base_mode = 'turtle'
        elif participants_count > 500:
            base_mode = 'low'
        elif participants_count > 200:
            base_mode = 'low'
        else:
            base_mode = 'normal'
        
        # Корректировка на основе сложности
        complexity_adjustments = {
            'heavy': -1,
            'normal': 0,
            'light': 1
        }
        
        modes_list = ['turtle', 'low', 'normal', 'burst']
        current_index = modes_list.index(base_mode)
        adjustment = complexity_adjustments.get(request_complexity, 0)
        
        # Корректировка на основе ошибок
        if self.error_count >= 2:
            adjustment -= 1
        elif self.success_count >= 20:
            adjustment += 1
        
        new_index = max(0, min(len(modes_list) - 1, current_index + adjustment))
        return modes_list[new_index]
    
    async def _check_auto_mode_switch(self):
        """Проверка автопереключения режима"""
        try:
            now = time.time()
            
            # Понижение из-за ошибок
            if self.error_count >= self.error_threshold:
                if now - self.last_error_time < 300:
                    current_index = ['turtle', 'low', 'normal', 'burst'].index(self.current_mode)
                    if current_index > 0:
                        new_mode = ['turtle', 'low', 'normal', 'burst'][current_index - 1]
                        self.set_mode(new_mode, f"auto_downgrade: {self.error_count} errors")
                        self.error_count = 0
            
            # Повышение из-за успехов
            elif self.success_count >= self.success_threshold:
                if self.error_count == 0:
                    current_index = ['turtle', 'low', 'normal', 'burst'].index(self.current_mode)
                    if current_index < 3:
                        new_mode = ['turtle', 'low', 'normal', 'burst'][current_index + 1]
                        self.set_mode(new_mode, f"auto_upgrade: {self.success_count} successes")
                        self.success_count = 0
                        
        except Exception as e:
            logger.debug(f"Ошибка автопереключения режима: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Получить статус лимитера"""
        now = time.time()
        hour_ago = now - 3600
        recent_requests = [r for r in self.request_history if r.timestamp > hour_ago]
        
        limits = API_LIMITS[self.current_mode]
        
        return {
            'mode': self.current_mode,
            'mode_name': limits['name'],
            'requests_last_hour': len(recent_requests),
            'max_requests_hour': limits['max_hour'],
            'cooldown_seconds': limits['cooldown'],
            'can_make_request': self.can_make_request(),
            'auto_mode_enabled': self.auto_mode_enabled,
            'disabled_chats_count': len(self.disabled_chats),
            'error_count': self.error_count,
            'success_count': self.success_count,
            'user_limits_count': len(self.user_limits),
            'last_request_time': self.request_history[-1].timestamp if self.request_history else 0
        }
    
    async def background_worker(self):
        """Фоновый процесс лимитера"""
        while self.is_running:
            try:
                await asyncio.sleep(60)  # Каждую минуту
                
                # Очистка старых записей
                now = time.time()
                hour_ago = now - 3600
                old_count = len(self.request_history)
                self.request_history = [r for r in self.request_history if r.timestamp > hour_ago]
                
                if len(self.request_history) < old_count:
                    logger.debug(f"🧹 Очищено {old_count - len(self.request_history)} старых запросов")
                
            except Exception as e:
                logger.error(f"❌ Ошибка фонового процесса лимитера: {e}")
    
    async def cleanup(self):
        """Очистка сервиса"""
        self.is_running = False
        logger.info("✅ APILimiterService завершил работу")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Статус здоровья лимитера"""
        return {
            'is_running': self.is_running,
            'current_mode': self.current_mode,
            'requests_count': len(self.request_history),
            'error_count': self.error_count,
            'auto_mode_enabled': self.auto_mode_enabled
        }
    
    def is_healthy(self) -> bool:
        """Проверка здоровья сервиса"""
        return self.is_running

# =============================================================================
# СЕРВИС УПРАВЛЕНИЯ ОЧЕРЕДЬЮ
# =============================================================================

class QueueService:
    """Сервис управления очередью запросов"""
    
    def __init__(self):
        self.is_processing = False
        self.processing_tasks: Dict[int, QueueTask] = {}
        self.user_locks: Dict[int, asyncio.Lock] = {}
        self.stats = {
            'total_processed': 0,
            'total_failed': 0,
            'total_completed': 0,
            'processing_time_total': 0.0,
            'average_processing_time': 0.0
        }
        
        # Ссылки на обработчики
        self.bot_handler = None
        self.user_handler = None
        self.bot_client = None
        
        # Настройки
        self.max_concurrent_tasks = 5
        self.task_timeout_seconds = 300
        self.queue_check_interval = 1
        self.stats_update_interval = 60
    
    async def initialize(self, bot_handler=None, user_handler=None, bot_client=None):
        """Инициализация сервиса очереди"""
        try:
            logger.info("🔄 Инициализация QueueService...")
            
            # Сохраняем ссылки на обработчики
            self.bot_handler = bot_handler
            self.user_handler = user_handler
            self.bot_client = bot_client
            
            # Сбрасываем зависшие задачи
            await self._reset_stuck_tasks()
            
            self.is_processing = True
            logger.info("✅ QueueService инициализирован")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации QueueService: {e}")
            raise
    
    async def add_task(self, user_id: int, command: str, chat_id: int = None, 
                      parameters: Dict[str, Any] = None, priority: int = None) -> int:
        """Добавить задачу в очередь"""
        try:
            if priority is None:
                priority = self._get_command_priority(command)
            
            # Проверяем лимит очереди
            queue_status = await db_manager.get_queue_status()
            if queue_status['pending'] >= MAX_QUEUE_SIZE:
                raise ValueError(f"Очередь переполнена ({MAX_QUEUE_SIZE} задач)")
            
            # Проверяем лимит пользователя
            user_pending = await self._get_user_pending_count(user_id)
            if user_pending >= 3:
                raise ValueError("Превышен лимит задач на пользователя (3)")
            
            # Добавляем в БД
            task_id = await db_manager.add_to_queue(
                user_id=user_id,
                command=command,
                chat_id=chat_id,
                parameters=parameters,
                priority=priority
            )
            
            logger.info(f"📋 Добавлена задача {task_id}: {command} для пользователя {user_id}")
            return task_id
            
        except Exception as e:
            logger.error(f"❌ Ошибка добавления задачи: {e}")
            raise
    
    async def background_worker(self):
        """Основной процесс обработки очереди"""
        logger.info("🔄 Запуск обработки очереди...")
        
        while self.is_processing:
            try:
                # Проверяем, можем ли брать новые задачи
                if len(self.processing_tasks) < self.max_concurrent_tasks:
                    task = await db_manager.get_next_task()
                    
                    if task:
                        # Запускаем обработку в фоне
                        asyncio.create_task(self._process_task(task))
                
                await asyncio.sleep(self.queue_check_interval)
                
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле очереди: {e}")
                await asyncio.sleep(5)
    
    async def _process_task(self, task_data: Dict[str, Any]):
        """Обработка отдельной задачи"""
        task_id = task_data['id']
        start_time = datetime.now()
        
        try:
            # Создаем объект задачи
            task = QueueTask(
                id=task_data['id'],
                user_id=task_data['user_id'],
                chat_id=task_data['chat_id'],
                command=task_data['command'],
                parameters=task_data.get('parameters'),
                priority=task_data['priority'],
                status='processing',
                created_at=task_data['created_at'],
                started_at=start_time
            )
            
            # Добавляем в активные
            self.processing_tasks[task_id] = task
            
            logger.info(f"🔄 Обработка задачи {task_id}: {task.command}")
            
            # Получаем блокировку пользователя
            user_lock = await self._get_user_lock(task.user_id)
            
            async with user_lock:
                # Выполняем команду
                result = await self._execute_command(task)
                
                # Завершаем задачу
                await db_manager.complete_task(task_id, result=json.dumps(result))
                
                # Обновляем статистику
                processing_time = (datetime.now() - start_time).total_seconds()
                self._update_stats('completed', processing_time)
                
                logger.info(f"✅ Задача {task_id} завершена за {processing_time:.2f}с")
            
        except Exception as e:
            # Обработка ошибки
            error_msg = str(e)
            await db_manager.complete_task(task_id, error=error_msg)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_stats('failed', processing_time)
            
            logger.error(f"❌ Ошибка в задаче {task_id}: {e}")
            
        finally:
            # Удаляем из активных
            if task_id in self.processing_tasks:
                del self.processing_tasks[task_id]
    
    async def _execute_command(self, task: QueueTask) -> Dict[str, Any]:
        """Выполнение команды задачи"""
        try:
            logger.info(f"🔄 Выполнение команды {task.command} для пользователя {task.user_id}")
            
            # Здесь должна быть интеграция с обработчиками команд
            # Пока используем эмуляцию
            if task.command in ['scan', 'get_all', 'get_users', 'get_ids']:
                processing_times = {
                    'scan': 2.0,
                    'get_all': 3.5,
                    'get_users': 1.0,
                    'get_ids': 1.5
                }
                await asyncio.sleep(processing_times.get(task.command, 1.0))
            
            return {
                'status': 'success',
                'command': task.command,
                'user_id': task.user_id,
                'chat_id': task.chat_id,
                'processed_at': datetime.now().isoformat(),
                'message': f"Команда {task.command} выполнена через очередь",
                'queue_processed': True
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения команды {task.command}: {e}")
            return {
                'status': 'error',
                'command': task.command,
                'user_id': task.user_id,
                'error': str(e),
                'processed_at': datetime.now().isoformat()
            }
    
    async def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        """Получить блокировку пользователя"""
        if user_id not in self.user_locks:
            self.user_locks[user_id] = asyncio.Lock()
        return self.user_locks[user_id]
    
    async def _get_user_pending_count(self, user_id: int) -> int:
        """Количество ожидающих задач пользователя"""
        try:
            queue_status = await db_manager.get_queue_status(user_id)
            return queue_status.get('pending', 0)
        except:
            return 0
    
    def _get_command_priority(self, command: str) -> int:
        """Определить приоритет команды"""
        command_priorities = {
            'start': QUEUE_PRIORITIES['admin'],
            'scan': QUEUE_PRIORITIES['scan'],
            'get_all': QUEUE_PRIORITIES['scan'],
            'get_users': QUEUE_PRIORITIES['stats'],
            'get_ids': QUEUE_PRIORITIES['scan'],
            'debug': QUEUE_PRIORITIES['maintenance'],
            'stats': QUEUE_PRIORITIES['stats']
        }
        
        return command_priorities.get(command, QUEUE_PRIORITIES['scan'])
    
    def _update_stats(self, status: str, processing_time: float):
        """Обновление статистики"""
        self.stats['total_processed'] += 1
        self.stats['processing_time_total'] += processing_time
        
        if status == 'completed':
            self.stats['total_completed'] += 1
        elif status == 'failed':
            self.stats['total_failed'] += 1
        
        if self.stats['total_processed'] > 0:
            self.stats['average_processing_time'] = (
                self.stats['processing_time_total'] / self.stats['total_processed']
            )
    
    async def _reset_stuck_tasks(self):
        """Сброс зависших задач"""
        try:
            logger.info("🔄 Проверка зависших задач...")
            queue_status = await db_manager.get_queue_status()
            if queue_status['processing'] > 0:
                logger.warning(f"⚠️ Найдено {queue_status['processing']} зависших задач")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сброса зависших задач: {e}")
    
    async def get_queue_status(self) -> Dict[str, Any]:
        """Получить статус очереди"""
        try:
            db_stats = await db_manager.get_queue_status()
            
            status = {
                **db_stats,
                'active_tasks': len(self.processing_tasks),
                'max_concurrent': self.max_concurrent_tasks,
                'available_slots': self.max_concurrent_tasks - len(self.processing_tasks),
                'is_processing': self.is_processing,
                'stats': self.stats.copy(),
                'processing_tasks': [
                    {
                        'id': task.id,
                        'user_id': task.user_id,
                        'command': task.command,
                        'started_at': task.started_at.isoformat() if task.started_at else None,
                        'processing_time': (datetime.now() - task.started_at).total_seconds() 
                                         if task.started_at else 0
                    }
                    for task in self.processing_tasks.values()
                ]
            }
            
            return status
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса очереди: {e}")
            return {
                'pending': 0,
                'processing': 0,
                'completed': 0,
                'failed': 0,
                'active_tasks': len(self.processing_tasks),
                'is_processing': self.is_processing,
                'error': str(e)
            }
    
    async def cleanup(self):
        """Очистка сервиса"""
        logger.info("🛑 Остановка QueueService...")
        self.is_processing = False
        
        # Ждем завершения активных задач
        wait_time = 0
        while self.processing_tasks and wait_time < 30:
            await asyncio.sleep(1)
            wait_time += 1
        
        # Принудительно завершаем оставшиеся
        if self.processing_tasks:
            logger.warning(f"⚠️ Принудительное завершение {len(self.processing_tasks)} задач")
            for task_id in list(self.processing_tasks.keys()):
                await db_manager.complete_task(task_id, error="Остановка сервера")
                del self.processing_tasks[task_id]
        
        self.user_locks.clear()
        logger.info("✅ QueueService завершил работу")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Статус здоровья очереди"""
        return {
            'is_processing': self.is_processing,
            'active_tasks': len(self.processing_tasks),
            'max_capacity': self.max_concurrent_tasks,
            'utilization': len(self.processing_tasks) / self.max_concurrent_tasks,
            'total_processed': self.stats['total_processed'],
            'success_rate': (
                self.stats['total_completed'] / max(self.stats['total_processed'], 1)
            ),
            'average_processing_time': self.stats['average_processing_time']
        }
    
    def is_healthy(self) -> bool:
        """Проверка здоровья сервиса"""
        return self.is_processing

# =============================================================================
# ГЛОБАЛЬНЫЕ ЭКЗЕМПЛЯРЫ
# =============================================================================

# Главный менеджер сервисов
service_manager = ServiceManager()

# Быстрый доступ к отдельным сервисам
activity_tracker = service_manager.activity
api_limiter = service_manager.limiter
queue_manager = service_manager.queue

# Функции совместимости
async def get_activity_tracker():
    return activity_tracker

async def get_api_limiter():
    return api_limiter

async def get_queue_manager():
    return queue_manager