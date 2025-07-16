#!/usr/bin/env python3
"""
Система безопасности и лимитов для Get ID Bot by Mister DMS
Включает: проверку лимитов, blacklist/whitelist, защиту от злоупотреблений
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict

from config import (
    BLACKLIST_USERS, TRUSTED_USERS, WHITELIST_ONLY_MODE,
    MAX_REQUESTS_PER_USER_DAY, COOLDOWN_BETWEEN_USERS,
    MAX_USERS_PER_HOUR, MAX_DAILY_REQUESTS, 
    DEVELOPMENT_MODE, ALERT_ADMIN_IDS
)

logger = logging.getLogger(__name__)

@dataclass
class SecurityEvent:
    """Событие безопасности"""
    user_id: int
    event_type: str
    timestamp: datetime
    details: str
    severity: str = 'info'  # info, warning, critical

@dataclass
class RateLimitInfo:
    """Информация о лимитах пользователя"""
    user_id: int
    requests_today: int
    last_request: datetime
    cooldown_until: Optional[datetime]
    daily_limit: int
    is_trusted: bool

class SecurityManager:
    """Менеджер безопасности и лимитов"""
    
    def __init__(self):
        # Счетчики запросов по пользователям
        self.user_requests: Dict[int, List[datetime]] = defaultdict(list)
        self.last_request_time: Dict[int, datetime] = {}
        
        # Блокировки и ограничения
        self.blocked_users: Set[int] = set()
        self.temporary_blocks: Dict[int, datetime] = {}
        
        # События безопасности
        self.security_events: List[SecurityEvent] = []
        self.max_events = 1000
        
        # Статистика
        self.stats = {
            'total_requests': 0,
            'blocked_requests': 0,
            'rate_limited_requests': 0,
            'security_violations': 0,
            'new_users_today': 0
        }
        
        # Инициализация blacklist
        self._init_blacklist()
        
        logger.info("🛡️ SecurityManager инициализирован")
    
    def _init_blacklist(self):
        """Инициализация списка заблокированных пользователей"""
        try:
            for user_str in BLACKLIST_USERS:
                if user_str.strip().isdigit():
                    user_id = int(user_str.strip())
                    self.blocked_users.add(user_id)
                    logger.info(f"🚫 Пользователь {user_id} добавлен в blacklist")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации blacklist: {e}")
    
    def is_user_allowed(self, user_id: int) -> Tuple[bool, str]:
        """
        Главная проверка: разрешено ли пользователю делать запросы
        
        Returns:
            (is_allowed: bool, reason: str)
        """
        try:
            self.stats['total_requests'] += 1
            
            # 1. Проверка режима разработки
            if DEVELOPMENT_MODE and not self.is_trusted_user(user_id):
                self._log_security_event(user_id, 'dev_mode_block', 
                                        'Заблокирован в режиме разработки', 'info')
                return False, "🔧 **Режим разработки**\n\nБот временно недоступен для тестирования новых функций."
            
            # 2. Проверка blacklist
            if self._is_blacklisted(user_id):
                self.stats['blocked_requests'] += 1
                self._log_security_event(user_id, 'blacklist_hit', 
                                        'Пользователь в blacklist', 'warning')
                return False, "❌ **Доступ запрещен**\n\nВы заблокированы."
            
            # 3. Проверка временной блокировки
            if self._is_temporarily_blocked(user_id):
                self.stats['rate_limited_requests'] += 1
                return False, "⏰ **Временная блокировка**\n\nПопробуйте позже."
            
            # 4. Проверка whitelist режима
            if WHITELIST_ONLY_MODE and not self.is_trusted_user(user_id):
                self._log_security_event(user_id, 'whitelist_mode_block', 
                                        'Не в whitelist при whitelist режиме', 'info')
                return False, "⚠️ **Ограниченный доступ**\n\nБот в режиме whitelist."
            
            # 5. Проверка глобальных лимитов
            if not self._check_global_limits():
                self.stats['rate_limited_requests'] += 1
                return False, "⏰ **Высокая нагрузка**\n\nПопробуйте через несколько минут."
            
            # 6. Проверка дневного лимита пользователя
            if not self._check_daily_limit(user_id):
                self.stats['rate_limited_requests'] += 1
                limit = self._get_user_daily_limit(user_id)
                return False, f"⏰ **Дневной лимит**\n\nВы превысили лимит ({limit} запросов/день).\nЛимит обновится завтра в 00:00."
            
            # 7. Проверка cooldown
            if not self._check_cooldown(user_id):
                self.stats['rate_limited_requests'] += 1
                remaining = self._get_cooldown_remaining(user_id)
                return False, f"🕐 **Пауза между запросами**\n\nПодождите {remaining} секунд."
            
            # Все проверки пройдены
            return True, "✅ Доступ разрешен"
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки безопасности для {user_id}: {e}")
            return False, "❌ Ошибка проверки безопасности"
    
    def record_request(self, user_id: int, command: str = "", chat_type: str = "private"):
        """Записать запрос пользователя"""
        try:
            now = datetime.now()
            
            # Добавляем запрос в историю
            self.user_requests[user_id].append(now)
            self.last_request_time[user_id] = now
            
            # Очищаем старые запросы (более суток)
            day_ago = now - timedelta(days=1)
            self.user_requests[user_id] = [
                req_time for req_time in self.user_requests[user_id] 
                if req_time >= day_ago
            ]
            
            # Логируем подозрительную активность
            requests_last_hour = len([
                req for req in self.user_requests[user_id] 
                if req >= now - timedelta(hours=1)
            ])
            
            if requests_last_hour > 50 and not self.is_trusted_user(user_id):
                self._log_security_event(user_id, 'suspicious_activity', 
                                        f'{requests_last_hour} запросов за час', 'warning')
                # Временная блокировка на 1 час
                self.temporary_blocks[user_id] = now + timedelta(hours=1)
            
            logger.debug(f"📊 Запрос {command} от пользователя {user_id} записан ({chat_type})")
            
        except Exception as e:
            logger.error(f"❌ Ошибка записи запроса для {user_id}: {e}")
    
    def is_trusted_user(self, user_id: int) -> bool:
        """Проверка доверенного пользователя"""
        try:
            return str(user_id) in TRUSTED_USERS
        except:
            return False
    
    def get_user_limits_info(self, user_id: int) -> RateLimitInfo:
        """Получить информацию о лимитах пользователя"""
        now = datetime.now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Считаем запросы за сегодня
        requests_today = len([
            req for req in self.user_requests.get(user_id, [])
            if req >= day_start
        ])
        
        # Определяем дневной лимит
        daily_limit = self._get_user_daily_limit(user_id)
        
        # Время последнего запроса
        last_request = self.last_request_time.get(user_id)
        
        # Время окончания cooldown
        cooldown_until = None
        if last_request:
            cooldown_end = last_request + timedelta(seconds=COOLDOWN_BETWEEN_USERS)
            if cooldown_end > now:
                cooldown_until = cooldown_end
        
        return RateLimitInfo(
            user_id=user_id,
            requests_today=requests_today,
            last_request=last_request,
            cooldown_until=cooldown_until,
            daily_limit=daily_limit,
            is_trusted=self.is_trusted_user(user_id)
        )
    
    def get_security_status(self, user_id: Optional[int] = None) -> Dict:
        """Получить статус безопасности"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'global_stats': self.stats.copy(),
            'active_blocks': len(self.temporary_blocks),
            'blacklisted_users': len(self.blocked_users),
            'trusted_users': len(TRUSTED_USERS),
            'development_mode': DEVELOPMENT_MODE,
            'whitelist_only_mode': WHITELIST_ONLY_MODE
        }
        
        if user_id:
            user_limits = self.get_user_limits_info(user_id)
            status['user_limits'] = {
                'requests_today': user_limits.requests_today,
                'daily_limit': user_limits.daily_limit,
                'is_trusted': user_limits.is_trusted,
                'cooldown_remaining': (
                    (user_limits.cooldown_until - datetime.now()).total_seconds()
                    if user_limits.cooldown_until else 0
                ),
                'is_blocked': self._is_blacklisted(user_id),
                'is_temp_blocked': self._is_temporarily_blocked(user_id)
            }
        
        return status
    
    def get_recent_security_events(self, limit: int = 50, user_id: Optional[int] = None) -> List[Dict]:
        """Получить последние события безопасности"""
        events = self.security_events[-limit:] if not user_id else [
            event for event in self.security_events[-limit*2:] 
            if event.user_id == user_id
        ]
        
        return [
            {
                'user_id': event.user_id,
                'event_type': event.event_type,
                'timestamp': event.timestamp.isoformat(),
                'details': event.details,
                'severity': event.severity
            }
            for event in events[-limit:]
        ]
    
    # Приватные методы
    
    def _is_blacklisted(self, user_id: int) -> bool:
        """Проверка blacklist"""
        return user_id in self.blocked_users
    
    def _is_temporarily_blocked(self, user_id: int) -> bool:
        """Проверка временной блокировки"""
        if user_id in self.temporary_blocks:
            if datetime.now() >= self.temporary_blocks[user_id]:
                # Время блокировки истекло
                del self.temporary_blocks[user_id]
                return False
            return True
        return False
    
    def _check_global_limits(self) -> bool:
        """Проверка глобальных лимитов системы"""
        now = datetime.now()
        
        # Проверяем общее количество запросов за день
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        total_requests_today = sum(
            len([req for req in requests if req >= day_start])
            for requests in self.user_requests.values()
        )
        
        if total_requests_today > MAX_DAILY_REQUESTS:
            self._log_security_event(0, 'global_daily_limit', 
                                   f'{total_requests_today} запросов за день', 'warning')
            return False
        
        # Проверяем количество новых пользователей за час
        hour_ago = now - timedelta(hours=1)
        new_users_hour = len([
            user_id for user_id, requests in self.user_requests.items()
            if requests and requests[0] >= hour_ago  # Первый запрос в последний час
        ])
        
        if new_users_hour > MAX_USERS_PER_HOUR:
            self._log_security_event(0, 'new_users_limit', 
                                   f'{new_users_hour} новых пользователей за час', 'warning')
            return False
        
        return True
    
    def _check_daily_limit(self, user_id: int) -> bool:
        """Проверка дневного лимита пользователя"""
        if self.is_trusted_user(user_id):
            return True  # Доверенные пользователи без лимитов
        
        now = datetime.now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        requests_today = len([
            req for req in self.user_requests.get(user_id, [])
            if req >= day_start
        ])
        
        daily_limit = self._get_user_daily_limit(user_id)
        return requests_today < daily_limit
    
    def _check_cooldown(self, user_id: int) -> bool:
        """Проверка cooldown между запросами"""
        if self.is_trusted_user(user_id):
            return True  # Доверенные пользователи без cooldown
        
        if user_id in self.last_request_time:
            time_diff = (datetime.now() - self.last_request_time[user_id]).total_seconds()
            return time_diff >= COOLDOWN_BETWEEN_USERS
        return True
    
    def _get_user_daily_limit(self, user_id: int) -> int:
        """Получить дневной лимит для пользователя"""
        if self.is_trusted_user(user_id):
            return 999999  # Практически неограниченно
        
        # Можно добавить логику для разных типов пользователей
        # Например, новые пользователи - меньший лимит
        return MAX_REQUESTS_PER_USER_DAY
    
    def _get_cooldown_remaining(self, user_id: int) -> int:
        """Получить оставшееся время cooldown в секундах"""
        if user_id in self.last_request_time:
            elapsed = (datetime.now() - self.last_request_time[user_id]).total_seconds()
            remaining = max(0, COOLDOWN_BETWEEN_USERS - elapsed)
            return int(remaining)
        return 0
    
    def _log_security_event(self, user_id: int, event_type: str, details: str, severity: str = 'info'):
        """Логирование события безопасности"""
        event = SecurityEvent(
            user_id=user_id,
            event_type=event_type,
            timestamp=datetime.now(),
            details=details,
            severity=severity
        )
        
        self.security_events.append(event)
        
        # Ограничиваем размер списка событий
        if len(self.security_events) > self.max_events:
            self.security_events = self.security_events[-self.max_events//2:]
        
        # Логируем в зависимости от серьезности
        if severity == 'critical':
            logger.error(f"🚨 SECURITY: {event_type} | User {user_id} | {details}")
            self.stats['security_violations'] += 1
        elif severity == 'warning':
            logger.warning(f"⚠️ SECURITY: {event_type} | User {user_id} | {details}")
        else:
            logger.debug(f"🛡️ SECURITY: {event_type} | User {user_id} | {details}")
    
    def add_to_blacklist(self, user_id: int, reason: str = "Manual block") -> bool:
        """Добавить пользователя в blacklist"""
        try:
            self.blocked_users.add(user_id)
            self._log_security_event(user_id, 'manual_blacklist', reason, 'warning')
            logger.info(f"🚫 Пользователь {user_id} добавлен в blacklist: {reason}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка добавления в blacklist {user_id}: {e}")
            return False
    
    def remove_from_blacklist(self, user_id: int, reason: str = "Manual unblock") -> bool:
        """Удалить пользователя из blacklist"""
        try:
            if user_id in self.blocked_users:
                self.blocked_users.remove(user_id)
                self._log_security_event(user_id, 'manual_unblock', reason, 'info')
                logger.info(f"✅ Пользователь {user_id} удален из blacklist: {reason}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка удаления из blacklist {user_id}: {e}")
            return False
    
    def cleanup_old_data(self) -> int:
        """Очистка старых данных"""
        cleaned = 0
        now = datetime.now()
        
        try:
            # Очищаем старые запросы (более 7 дней)
            week_ago = now - timedelta(days=7)
            for user_id in list(self.user_requests.keys()):
                old_count = len(self.user_requests[user_id])
                self.user_requests[user_id] = [
                    req for req in self.user_requests[user_id] 
                    if req >= week_ago
                ]
                cleaned += old_count - len(self.user_requests[user_id])
                
                # Удаляем пустые записи
                if not self.user_requests[user_id]:
                    del self.user_requests[user_id]
            
            # Очищаем истекшие временные блокировки
            expired_blocks = [
                user_id for user_id, block_until in self.temporary_blocks.items()
                if now >= block_until
            ]
            for user_id in expired_blocks:
                del self.temporary_blocks[user_id]
                cleaned += 1
            
            # Очищаем старые события безопасности (более 30 дней)
            month_ago = now - timedelta(days=30)
            old_events_count = len(self.security_events)
            self.security_events = [
                event for event in self.security_events
                if event.timestamp >= month_ago
            ]
            cleaned += old_events_count - len(self.security_events)
            
            if cleaned > 0:
                logger.info(f"🧹 Очищено {cleaned} старых записей безопасности")
            
            return cleaned
            
        except Exception as e:
            logger.error(f"❌ Ошибка очистки данных безопасности: {e}")
            return 0

# Глобальный экземпляр менеджера безопасности
security_manager = SecurityManager()

# Функции для быстрого доступа
def is_user_allowed(user_id: int) -> Tuple[bool, str]:
    """Быстрая проверка доступа пользователя"""
    return security_manager.is_user_allowed(user_id)

def record_user_request(user_id: int, command: str = "", chat_type: str = "private"):
    """Быстрая запись запроса пользователя"""
    security_manager.record_request(user_id, command, chat_type)

def is_trusted_user(user_id: int) -> bool:
    """Быстрая проверка доверенного пользователя"""
    return security_manager.is_trusted_user(user_id)

def get_user_limits(user_id: int) -> RateLimitInfo:
    """Быстрое получение информации о лимитах"""
    return security_manager.get_user_limits_info(user_id)