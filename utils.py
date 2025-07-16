#!/usr/bin/env python3
"""
Вспомогательные функции Get ID Bot by Mister DMS
ОБНОВЛЕНО v4.1.1: улучшенные утилиты + безопасность
"""

import logging
import json
import re
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Union
from cryptography.fernet import Fernet
import base64
import hashlib

from config import ENCRYPTION_KEY, SALT, LOG_LEVEL, DEVELOPMENT_MODE

# === НАСТРОЙКА ЛОГИРОВАНИЯ ===

def setup_logging() -> logging.Logger:
    """Настройка системы логирования"""
    
    # Создаем главный логгер
    logger = logging.getLogger('get_id_bot')
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    
    # Избегаем дублирования обработчиков
    if logger.handlers:
        return logger
    
    # Создаем обработчик для консоли
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Формат логов
    if DEVELOPMENT_MODE:
        # Подробный формат для разработки
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%H:%M:%S'
        )
    else:
        # Компактный формат для production
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%H:%M:%S'
        )
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Настраиваем логгеры телеграм библиотек
    logging.getLogger('telethon').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    
    return logger

# === УТИЛИТЫ ДЛЯ СООБЩЕНИЙ ===

class MessageUtils:
    """Утилиты для работы с сообщениями Telegram"""
    
    @staticmethod
    async def smart_reply(event, text: str, buttons=None, parse_mode='md') -> Any:
        """Умная отправка ответа с обработкой ошибок"""
        try:
            # Ограничиваем длину сообщения
            if len(text) > 4096:
                text = text[:4090] + "..."
            
            # Отправляем сообщение
            if buttons:
                return await event.respond(text, buttons=buttons, parse_mode=parse_mode)
            else:
                return await event.respond(text, parse_mode=parse_mode)
                
        except Exception as e:
            logging.getLogger(__name__).error(f"❌ Ошибка отправки сообщения: {e}")
            
            # Fallback - отправляем без форматирования
            try:
                fallback_text = "❌ Ошибка отправки сообщения. Попробуйте еще раз."
                return await event.respond(fallback_text)
            except:
                pass  # Если и это не работает, молча игнорируем
    
    @staticmethod
    def escape_markdown(text: str) -> str:
        """Экранирование специальных символов Markdown"""
        escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        
        for char in escape_chars:
            text = text.replace(char, f'\\{char}')
        
        return text
    
    @staticmethod
    def format_code_block(code: str, language: str = '') -> str:
        """Форматирование блока кода"""
        return f"```{language}\n{code}\n```"
    
    @staticmethod
    def format_inline_code(code: str) -> str:
        """Форматирование inline кода"""
        return f"`{code}`"
    
    @staticmethod
    def truncate_text(text: str, max_length: int = 4000, suffix: str = "...") -> str:
        """Обрезка текста с сохранением структуры"""
        if len(text) <= max_length:
            return text
        
        # Обрезаем по словам если возможно
        words = text[:max_length - len(suffix)].split()
        if len(words) > 1:
            words.pop()  # Убираем последнее неполное слово
            return ' '.join(words) + suffix
        
        return text[:max_length - len(suffix)] + suffix

# === УТИЛИТЫ ШИФРОВАНИЯ ===

class EncryptionUtils:
    """Утилиты для шифрования данных"""
    
    _fernet = None
    
    @classmethod
    def get_cipher(cls):
        """Получение объекта шифрования"""
        if cls._fernet is None:
            # Создаем ключ из ENCRYPTION_KEY
            key = base64.urlsafe_b64encode(
                hashlib.sha256(ENCRYPTION_KEY.encode()).digest()
            )
            cls._fernet = Fernet(key)
        return cls._fernet
    
    @classmethod
    def encrypt(cls, data: str) -> str:
        """Шифрование строки"""
        try:
            if not data:
                return ''
            
            cipher = cls.get_cipher()
            # Добавляем соль для дополнительной безопасности
            salted_data = f"{SALT}:{data}"
            encrypted_bytes = cipher.encrypt(salted_data.encode())
            return base64.urlsafe_b64encode(encrypted_bytes).decode()
            
        except Exception as e:
            logging.getLogger(__name__).error(f"❌ Ошибка шифрования: {e}")
            return ''
    
    @classmethod
    def decrypt(cls, encrypted_data: str) -> str:
        """Расшифровка строки"""
        try:
            if not encrypted_data:
                return ''
            
            cipher = cls.get_cipher()
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode())
            decrypted_data = cipher.decrypt(encrypted_bytes).decode()
            
            # Убираем соль
            if decrypted_data.startswith(f"{SALT}:"):
                return decrypted_data[len(SALT) + 1:]
            
            return decrypted_data
            
        except Exception as e:
            logging.getLogger(__name__).error(f"❌ Ошибка расшифровки: {e}")
            return ''
    
    @classmethod
    def hash_password(cls, password: str) -> str:
        """Хеширование пароля"""
        return hashlib.sha256(f"{SALT}:{password}".encode()).hexdigest()

# === УТИЛИТЫ ВАЛИДАЦИИ ===

class ValidationUtils:
    """Утилиты для валидации данных"""
    
    @staticmethod
    def validate_api_credentials(api_id: str, api_hash: str) -> bool:
        """Валидация API credentials"""
        try:
            # Проверяем API_ID
            api_id_int = int(api_id)
            if api_id_int <= 0 or api_id_int > 999999999:
                return False
            
            # Проверяем API_HASH
            if not api_hash or len(api_hash) < 20:
                return False
            
            # Проверяем что это hex строка
            int(api_hash, 16)
            
            return True
            
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_user_id(user_id: Union[str, int]) -> bool:
        """Валидация Telegram User ID"""
        try:
            user_id_int = int(user_id)
            return 0 < user_id_int < 10**12  # Разумные пределы для Telegram ID
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_chat_id(chat_id: Union[str, int]) -> bool:
        """Валидация Telegram Chat ID"""
        try:
            chat_id_int = int(chat_id)
            # Chat ID может быть отрицательным для групп
            return abs(chat_id_int) < 10**15
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """Валидация URL"""
        url_pattern = re.compile(
            r'^https?://'  # http:// или https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...или IP
            r'(?::\d+)?'  # опциональный порт
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        return url_pattern.match(url) is not None
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Очистка имени файла от опасных символов"""
        # Убираем опасные символы
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Ограничиваем длину
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        
        return sanitized

# === УТИЛИТЫ ФОРМАТИРОВАНИЯ ===

def format_user_info(user_data: Dict[str, Any]) -> str:
    """Форматирование информации о пользователе"""
    username = user_data.get('telegram_username', 'N/A')
    first_name = user_data.get('first_name', 'N/A')
    mode = user_data.get('mode', 'bot')
    status = user_data.get('status', 'active')
    
    return f"👤 **{first_name}** (@{username})\n🤖 Режим: {mode}\n📊 Статус: {status}"

def format_timespan(timestamp: Optional[datetime]) -> str:
    """Форматирование временного промежутка"""
    if not timestamp:
        return "Неизвестно"
    
    if isinstance(timestamp, str):
        try:
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except ValueError:
            return "Неизвестно"
    
    now = datetime.now()
    if timestamp.tzinfo:
        # Если timestamp с timezone, приводим now к UTC
        from datetime import timezone
        now = now.replace(tzinfo=timezone.utc)
    
    delta = now - timestamp
    
    if delta.days > 0:
        return f"{delta.days} дн. назад"
    elif delta.seconds > 3600:
        hours = delta.seconds // 3600
        return f"{hours} ч. назад"
    elif delta.seconds > 60:
        minutes = delta.seconds // 60
        return f"{minutes} мин. назад"
    else:
        return "Только что"

def format_file_size(bytes_count: int) -> str:
    """Форматирование размера файла"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_count < 1024:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024
    return f"{bytes_count:.1f} TB"

def format_number(number: int) -> str:
    """Форматирование чисел с разделителями"""
    return f"{number:,}".replace(',', ' ')

# === УТИЛИТЫ ДЛЯ РАБОТЫ С ЧАТАМИ ===

def is_group_message(event) -> bool:
    """Проверка является ли сообщение из группы"""
    try:
        # Проверяем тип чата
        if hasattr(event, 'is_group') and event.is_group:
            return True
        
        # Проверяем по ID чата (отрицательные для групп)
        if hasattr(event, 'chat_id') and event.chat_id < 0:
            return True
        
        # Дополнительная проверка через объект чата
        if hasattr(event, 'chat'):
            chat = event.chat
            if hasattr(chat, 'megagroup') and chat.megagroup:
                return True
            if hasattr(chat, 'broadcast') and not chat.broadcast:
                return True
        
        return False
        
    except Exception:
        return False

def extract_chat_info(event) -> Dict[str, Any]:
    """Извлечение информации о чате"""
    info = {
        'chat_id': None,
        'chat_type': 'unknown',
        'chat_title': None,
        'is_group': False,
        'is_supergroup': False,
        'is_channel': False,
        'is_private': False
    }
    
    try:
        if hasattr(event, 'chat_id'):
            info['chat_id'] = event.chat_id
        
        if hasattr(event, 'chat'):
            chat = event.chat
            
            if hasattr(chat, 'title'):
                info['chat_title'] = chat.title
            
            if hasattr(chat, 'megagroup'):
                info['is_supergroup'] = chat.megagroup
                info['is_group'] = True
                info['chat_type'] = 'supergroup'
            elif hasattr(chat, 'broadcast'):
                info['is_channel'] = chat.broadcast
                info['chat_type'] = 'channel'
            elif event.chat_id > 0:
                info['is_private'] = True
                info['chat_type'] = 'private'
            else:
                info['is_group'] = True
                info['chat_type'] = 'group'
        
        elif event.chat_id:
            if event.chat_id > 0:
                info['is_private'] = True
                info['chat_type'] = 'private'
            else:
                info['is_group'] = True
                info['chat_type'] = 'group'
    
    except Exception as e:
        logging.getLogger(__name__).debug(f"Ошибка извлечения информации о чате: {e}")
    
    return info

# === УТИЛИТЫ ДЛЯ РАБОТЫ С JSON ===

class JSONUtils:
    """Утилиты для работы с JSON"""
    
    @staticmethod
    def safe_json_loads(json_str: str, default=None) -> Any:
        """Безопасная загрузка JSON"""
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return default
    
    @staticmethod
    def safe_json_dumps(data: Any, default=None) -> str:
        """Безопасная сериализация в JSON"""
        try:
            return json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        except (TypeError, ValueError):
            return json.dumps(default) if default is not None else '{}'
    
    @staticmethod
    def pretty_json(data: Any) -> str:
        """Красивое форматирование JSON"""
        try:
            return json.dumps(data, ensure_ascii=False, indent=2)
        except (TypeError, ValueError):
            return str(data)

# === УТИЛИТЫ ДЛЯ РАБОТЫ С ASYNC ===

class AsyncUtils:
    """Утилиты для асинхронной работы"""
    
    @staticmethod
    async def safe_execute(coro, timeout: int = 30, default=None):
        """Безопасное выполнение корутины с таймаутом"""
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            logging.getLogger(__name__).warning(f"Таймаут выполнения корутины: {timeout}s")
            return default
        except Exception as e:
            logging.getLogger(__name__).error(f"Ошибка выполнения корутины: {e}")
            return default
    
    @staticmethod
    async def gather_with_errors(*coros, return_exceptions=True):
        """Выполнение нескольких корутин с обработкой ошибок"""
        try:
            results = await asyncio.gather(*coros, return_exceptions=return_exceptions)
            
            # Логируем ошибки
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logging.getLogger(__name__).error(f"Ошибка в корутине {i}: {result}")
            
            return results
        except Exception as e:
            logging.getLogger(__name__).error(f"Критическая ошибка в gather: {e}")
            return [e] * len(coros)

# === УТИЛИТЫ ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ ===

class PerformanceUtils:
    """Утилиты для мониторинга производительности"""
    
    @staticmethod
    def measure_time(func):
        """Декоратор для измерения времени выполнения"""
        async def async_wrapper(*args, **kwargs):
            start_time = datetime.now()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logging.getLogger(__name__).debug(
                    f"⏱️ {func.__name__} выполнен за {duration:.3f}s"
                )
        
        def sync_wrapper(*args, **kwargs):
            start_time = datetime.now()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logging.getLogger(__name__).debug(
                    f"⏱️ {func.__name__} выполнен за {duration:.3f}s"
                )
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

# === ЭКСПОРТ ===

__all__ = [
    'setup_logging',
    'MessageUtils',
    'EncryptionUtils', 
    'ValidationUtils',
    'JSONUtils',
    'AsyncUtils',
    'PerformanceUtils',
    'format_user_info',
    'format_timespan',
    'format_file_size',
    'format_number',
    'is_group_message',
    'extract_chat_info'
]