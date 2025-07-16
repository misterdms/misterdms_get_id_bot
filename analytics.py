#!/usr/bin/env python3
"""
Аналитика использования для Get ID Bot by Mister DMS
Включает: отслеживание событий, метрики производительности, correlation ID
"""

import uuid
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict, Counter

from config import ENABLE_USAGE_ANALYTICS, CORRELATION_ID_HEADER

logger = logging.getLogger(__name__)

@dataclass
class AnalyticsEvent:
    """Событие аналитики"""
    correlation_id: str
    timestamp: datetime
    event_type: str
    user_id: int
    properties: Dict[str, Any]
    duration_ms: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None

@dataclass
class PerformanceMetric:
    """Метрика производительности"""
    operation: str
    timestamp: datetime
    duration_ms: float
    success: bool
    details: Dict[str, Any]

class AnalyticsManager:
    """Менеджер аналитики использования"""
    
    def __init__(self):
        self.enabled = ENABLE_USAGE_ANALYTICS
        self.events: List[AnalyticsEvent] = []
        self.performance_metrics: List[PerformanceMetric] = []
        self.max_events = 10000  # Лимит событий в памяти
        self.max_metrics = 5000  # Лимит метрик в памяти
        
        # Кеши для быстрой статистики
        self.command_stats = Counter()
        self.user_stats = defaultdict(lambda: {
            'commands_used': Counter(),
            'total_requests': 0,
            'first_seen': None,
            'last_seen': None,
            'errors_count': 0
        })
        
        # Метрики производительности
        self.performance_stats = {
            'avg_response_time': 0.0,
            'total_operations': 0,
            'error_rate': 0.0,
            'slowest_operations': []
        }
        
        if self.enabled:
            logger.info("📊 AnalyticsManager инициализирован")
        else:
            logger.info("📊 AnalyticsManager инициализирован (отключен)")
    
    def track_event(self, event_type: str, user_id: int, 
                   properties: Dict[str, Any] = None, 
                   correlation_id: str = None) -> str:
        """
        Отслеживание события
        
        Args:
            event_type: Тип события (command_used, error_occurred, etc.)
            user_id: ID пользователя
            properties: Дополнительные свойства события
            correlation_id: ID для трейсинга (создается автоматически если не указан)
        
        Returns:
            correlation_id: ID для отслеживания связанных событий
        """
        if not self.enabled:
            return ""
        
        try:
            # Генерируем correlation_id если не передан
            if not correlation_id:
                correlation_id = str(uuid.uuid4())[:8]  # Короткий ID для удобства
            
            event_data = AnalyticsEvent(
                correlation_id=correlation_id,
                timestamp=datetime.now(),
                event_type=event_type,
                user_id=user_id,
                properties=properties or {}
            )
            
            self.events.append(event_data)
            
            # Обновляем кеши статистики
            self._update_stats_cache(event_data)
            
            # Ограничиваем размер в памяти
            if len(self.events) > self.max_events:
                self.events = self.events[-self.max_events//2:]
            
            logger.debug(f"📊 Событие {event_type} для пользователя {user_id} (ID: {correlation_id})")
            return correlation_id
            
        except Exception as e:
            logger.error(f"❌ Ошибка записи события аналитики: {e}")
            return ""
    
    def track_command(self, user_id: int, command: str, chat_type: str = 'private', 
                     chat_id: Optional[int] = None, success: bool = True,
                     correlation_id: str = None) -> str:
        """Отслеживание использования команды"""
        return self.track_event('command_used', user_id, {
            'command': command,
            'chat_type': chat_type,
            'chat_id': chat_id,
            'success': success
        }, correlation_id)
    
    def track_error(self, user_id: int, error_type: str, error_message: str = "",
                   command: str = "", correlation_id: str = None) -> str:
        """Отслеживание ошибки"""
        # Обрезаем длинные сообщения об ошибках
        error_message = error_message[:200] if error_message else ""
        
        return self.track_event('error_occurred', user_id, {
            'error_type': error_type,
            'error_message': error_message,
            'command': command
        }, correlation_id)
    
    def track_performance(self, operation: str, duration_ms: float, 
                         success: bool = True, details: Dict[str, Any] = None,
                         correlation_id: str = None) -> str:
        """Отслеживание производительности операции"""
        try:
            metric = PerformanceMetric(
                operation=operation,
                timestamp=datetime.now(),
                duration_ms=duration_ms,
                success=success,
                details=details or {}
            )
            
            self.performance_metrics.append(metric)
            
            # Обновляем статистику производительности
            self._update_performance_stats(metric)
            
            # Ограничиваем размер
            if len(self.performance_metrics) > self.max_metrics:
                self.performance_metrics = self.performance_metrics[-self.max_metrics//2:]
            
            # Создаем событие аналитики если нужно
            if self.enabled:
                correlation_id = self.track_event('performance_metric', 0, {
                    'operation': operation,
                    'duration_ms': duration_ms,
                    'success': success
                }, correlation_id)
            
            logger.debug(f"⚡ {operation}: {duration_ms:.1f}ms")
            return correlation_id or ""
            
        except Exception as e:
            logger.error(f"❌ Ошибка записи метрики производительности: {e}")
            return ""
    
    def start_operation(self, operation_name: str) -> 'OperationTracker':
        """Начать отслеживание операции (context manager)"""
        return OperationTracker(self, operation_name)
    
    def get_user_analytics(self, user_id: int, days: int = 7) -> Dict[str, Any]:
        """Получить аналитику по пользователю"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Фильтруем события пользователя
            user_events = [
                event for event in self.events
                if event.user_id == user_id and event.timestamp >= cutoff_date
            ]
            
            if not user_events:
                return {
                    'user_id': user_id,
                    'period_days': days,
                    'total_events': 0,
                    'commands_used': {},
                    'errors': [],
                    'first_activity': None,
                    'last_activity': None
                }
            
            # Анализируем команды
            commands_counter = Counter()
            errors = []
            
            for event in user_events:
                if event.event_type == 'command_used':
                    command = event.properties.get('command', 'unknown')
                    commands_counter[command] += 1
                elif event.event_type == 'error_occurred':
                    errors.append({
                        'timestamp': event.timestamp.isoformat(),
                        'error_type': event.properties.get('error_type'),
                        'message': event.properties.get('error_message', '')[:100]
                    })
            
            return {
                'user_id': user_id,
                'period_days': days,
                'total_events': len(user_events),
                'commands_used': dict(commands_counter.most_common()),
                'errors': errors[-10:],  # Последние 10 ошибок
                'first_activity': user_events[0].timestamp.isoformat(),
                'last_activity': user_events[-1].timestamp.isoformat(),
                'activity_by_day': self._get_daily_activity(user_events)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения аналитики пользователя {user_id}: {e}")
            return {'error': str(e)}
    
    def get_global_analytics(self, hours: int = 24) -> Dict[str, Any]:
        """Получить глобальную аналитику"""
        try:
            cutoff_date = datetime.now() - timedelta(hours=hours)
            
            # Фильтруем события
            recent_events = [
                event for event in self.events
                if event.timestamp >= cutoff_date
            ]
            
            recent_metrics = [
                metric for metric in self.performance_metrics
                if metric.timestamp >= cutoff_date
            ]
            
            # Уникальные пользователи
            unique_users = len(set(event.user_id for event in recent_events))
            
            # Команды
            command_stats = Counter()
            error_stats = Counter()
            
            for event in recent_events:
                if event.event_type == 'command_used':
                    command = event.properties.get('command', 'unknown')
                    command_stats[command] += 1
                elif event.event_type == 'error_occurred':
                    error_type = event.properties.get('error_type', 'unknown')
                    error_stats[error_type] += 1
            
            # Производительность
            if recent_metrics:
                avg_response_time = sum(m.duration_ms for m in recent_metrics) / len(recent_metrics)
                success_rate = len([m for m in recent_metrics if m.success]) / len(recent_metrics)
            else:
                avg_response_time = 0
                success_rate = 1.0
            
            return {
                'period_hours': hours,
                'timestamp': datetime.now().isoformat(),
                'total_events': len(recent_events),
                'unique_users': unique_users,
                'most_used_commands': dict(command_stats.most_common(10)),
                'error_types': dict(error_stats.most_common(5)),
                'performance': {
                    'avg_response_time_ms': round(avg_response_time, 2),
                    'success_rate': round(success_rate, 3),
                    'total_operations': len(recent_metrics)
                },
                'hourly_activity': self._get_hourly_activity(recent_events)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения глобальной аналитики: {e}")
            return {'error': str(e)}
    
    def get_top_users(self, limit: int = 10, days: int = 7) -> List[Dict[str, Any]]:
        """Получить топ активных пользователей"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            user_activity = defaultdict(int)
            
            for event in self.events:
                if event.timestamp >= cutoff_date:
                    user_activity[event.user_id] += 1
            
            # Сортируем по активности
            sorted_users = sorted(user_activity.items(), key=lambda x: x[1], reverse=True)
            
            result = []
            for user_id, activity_count in sorted_users[:limit]:
                user_data = self.user_stats.get(user_id, {})
                result.append({
                    'user_id': user_id,
                    'activity_count': activity_count,
                    'first_seen': user_data.get('first_seen'),
                    'last_seen': user_data.get('last_seen'),
                    'total_requests': user_data.get('total_requests', 0),
                    'errors_count': user_data.get('errors_count', 0)
                })
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения топ пользователей: {e}")
            return []
    
    def get_correlation_events(self, correlation_id: str) -> List[Dict[str, Any]]:
        """Получить все события с определенным correlation_id"""
        try:
            related_events = [
                event for event in self.events
                if event.correlation_id == correlation_id
            ]
            
            return [
                {
                    'timestamp': event.timestamp.isoformat(),
                    'event_type': event.event_type,
                    'user_id': event.user_id,
                    'properties': event.properties,
                    'success': event.success,
                    'error_message': event.error_message
                }
                for event in sorted(related_events, key=lambda x: x.timestamp)
            ]
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения событий correlation {correlation_id}: {e}")
            return []
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Очистка старых данных аналитики"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Очищаем события
            old_events_count = len(self.events)
            self.events = [
                event for event in self.events
                if event.timestamp >= cutoff_date
            ]
            events_cleaned = old_events_count - len(self.events)
            
            # Очищаем метрики производительности
            old_metrics_count = len(self.performance_metrics)
            self.performance_metrics = [
                metric for metric in self.performance_metrics
                if metric.timestamp >= cutoff_date
            ]
            metrics_cleaned = old_metrics_count - len(self.performance_metrics)
            
            total_cleaned = events_cleaned + metrics_cleaned
            
            if total_cleaned > 0:
                logger.info(f"🧹 Очищено {total_cleaned} старых записей аналитики")
            
            return total_cleaned
            
        except Exception as e:
            logger.error(f"❌ Ошибка очистки данных аналитики: {e}")
            return 0
    
    def export_analytics_data(self, format: str = 'dict') -> Any:
        """Экспорт данных аналитики"""
        try:
            if format == 'dict':
                return {
                    'events': [asdict(event) for event in self.events[-1000:]],  # Последние 1000
                    'performance_metrics': [asdict(metric) for metric in self.performance_metrics[-500:]],
                    'global_stats': self.get_global_analytics(24),
                    'export_timestamp': datetime.now().isoformat()
                }
            # Можно добавить другие форматы (JSON, CSV)
            
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта данных аналитики: {e}")
            return {}
    
    # Приватные методы
    
    def _update_stats_cache(self, event: AnalyticsEvent):
        """Обновление кешей статистики"""
        try:
            user_id = event.user_id
            now = datetime.now()
            
            # Обновляем статистику пользователя
            if user_id not in self.user_stats:
                self.user_stats[user_id]['first_seen'] = now.isoformat()
            
            self.user_stats[user_id]['last_seen'] = now.isoformat()
            self.user_stats[user_id]['total_requests'] += 1
            
            if event.event_type == 'command_used':
                command = event.properties.get('command', 'unknown')
                self.command_stats[command] += 1
                self.user_stats[user_id]['commands_used'][command] += 1
            elif event.event_type == 'error_occurred':
                self.user_stats[user_id]['errors_count'] += 1
                
        except Exception as e:
            logger.debug(f"Ошибка обновления кеша статистики: {e}")
    
    def _update_performance_stats(self, metric: PerformanceMetric):
        """Обновление статистики производительности"""
        try:
            self.performance_stats['total_operations'] += 1
            
            # Скользящее среднее времени отклика
            current_avg = self.performance_stats['avg_response_time']
            total_ops = self.performance_stats['total_operations']
            
            new_avg = ((current_avg * (total_ops - 1)) + metric.duration_ms) / total_ops
            self.performance_stats['avg_response_time'] = new_avg
            
            # Коэффициент ошибок
            if not metric.success:
                error_count = self.performance_stats.get('error_count', 0) + 1
                self.performance_stats['error_count'] = error_count
                self.performance_stats['error_rate'] = error_count / total_ops
            
            # Топ медленных операций
            if metric.duration_ms > 1000:  # Более 1 секунды
                slow_ops = self.performance_stats.setdefault('slowest_operations', [])
                slow_ops.append({
                    'operation': metric.operation,
                    'duration_ms': metric.duration_ms,
                    'timestamp': metric.timestamp.isoformat()
                })
                
                # Оставляем только 20 самых медленных
                slow_ops.sort(key=lambda x: x['duration_ms'], reverse=True)
                self.performance_stats['slowest_operations'] = slow_ops[:20]
                
        except Exception as e:
            logger.debug(f"Ошибка обновления статистики производительности: {e}")
    
    def _get_daily_activity(self, events: List[AnalyticsEvent]) -> Dict[str, int]:
        """Получить активность по дням"""
        daily_activity = defaultdict(int)
        for event in events:
            day = event.timestamp.strftime('%Y-%m-%d')
            daily_activity[day] += 1
        return dict(daily_activity)
    
    def _get_hourly_activity(self, events: List[AnalyticsEvent]) -> Dict[str, int]:
        """Получить активность по часам"""
        hourly_activity = defaultdict(int)
        for event in events:
            hour = event.timestamp.strftime('%H:00')
            hourly_activity[hour] += 1
        return dict(hourly_activity)

class OperationTracker:
    """Context manager для отслеживания времени выполнения операций"""
    
    def __init__(self, analytics_manager: AnalyticsManager, operation_name: str):
        self.analytics = analytics_manager
        self.operation_name = operation_name
        self.start_time = None
        self.correlation_id = str(uuid.uuid4())[:8]
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration_ms = (time.time() - self.start_time) * 1000
            success = exc_type is None
            
            details = {}
            if exc_type:
                details['error_type'] = exc_type.__name__
                details['error_message'] = str(exc_val)[:200]
            
            self.analytics.track_performance(
                self.operation_name,
                duration_ms,
                success,
                details,
                self.correlation_id
            )

# Глобальный экземпляр менеджера аналитики
analytics = AnalyticsManager()

# Функции для быстрого доступа
def track_event(event_type: str, user_id: int, properties: Dict[str, Any] = None) -> str:
    """Быстрое отслеживание события"""
    return analytics.track_event(event_type, user_id, properties)

def track_command(user_id: int, command: str, **kwargs) -> str:
    """Быстрое отслеживание команды"""
    return analytics.track_command(user_id, command, **kwargs)

def track_error(user_id: int, error_type: str, error_message: str = "") -> str:
    """Быстрое отслеживание ошибки"""
    return analytics.track_error(user_id, error_type, error_message)

def track_performance(operation: str, duration_ms: float, success: bool = True) -> str:
    """Быстрое отслеживание производительности"""
    return analytics.track_performance(operation, duration_ms, success)

def start_operation(operation_name: str) -> OperationTracker:
    """Начать отслеживание операции"""
    return analytics.start_operation(operation_name)