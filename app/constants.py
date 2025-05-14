"""
Constants used throughout the application.
"""
from typing import Dict, List, Final

# API Constants
API_TIMEOUT: Final[int] = 15  # seconds
API_RETRY_ATTEMPTS: Final[int] = 3
API_RETRIES: Final[int] = 3  # For test compatibility
API_BACKOFF_FACTOR: Final[float] = 0.5  # For test compatibility

# Database Constants
DB_POOL_SIZE: Final[int] = 5
DB_MAX_OVERFLOW: Final[int] = 10
DB_POOL_TIMEOUT: Final[int] = 30
DB_POOL_RECYCLE: Final[int] = 3600  # For test compatibility

# Time Constants
TIMEZONE: Final[str] = 'America/Santiago'
DATE_FORMAT: Final[str] = '%Y-%m-%d'
TIME_FORMAT: Final[str] = '%H:%M:%S'
DATETIME_FORMAT: Final[str] = f'{DATE_FORMAT} {TIME_FORMAT}'

# UI Constants
PAGE_TITLE: Final[str] = 'Chilean Electricity Market Dashboard'
PAGE_ICON: Final[str] = '⚡'
LAYOUT: Final[str] = 'wide'

# Barra Constants
BARRAS: Final[Dict[str, str]] = {
    'CHARRUA_220': 'Charrua066',
    'QUILLOTA_220': 'LVegas110'
}

# Status Constants
STATUS_ACTIVE: Final[str] = 'Activo'
STATUS_INACTIVE: Final[str] = 'No Activo'
STATUS_ERROR: Final[str] = 'Error'  # For test compatibility

# Error Messages
ERROR_MESSAGES: Final[Dict[str, str]] = {
    'API_ERROR': 'Error al conectar con la API',
    'DB_ERROR': 'Error al conectar con la base de datos',
    'VALIDATION_ERROR': 'Error de validación',
    'CONFIG_ERROR': 'Error de configuración'
}

# Success Messages
SUCCESS_MESSAGES: Final[Dict[str, str]] = {
    'DATA_UPDATED': 'Datos actualizados correctamente',
    'OPERATION_SUCCESS': 'Operación completada con éxito'
}

# Cache Constants
CACHE_TTL: Final[int] = 300  # 5 minutes
CACHE_MAX_ENTRIES: Final[int] = 100

# Validation Constants
MIN_CMG_VALUE: Final[float] = 0.0
MAX_CMG_VALUE: Final[float] = 1000.0
MIN_COST_VALUE: Final[float] = 0.0
MAX_COST_VALUE: Final[float] = 1000000.0

# Chart Constants
CHART_HEIGHT: Final[int] = 400
CHART_WIDTH: Final[int] = 800
CHART_THEME: Final[str] = 'streamlit'

# Table Constants
TABLE_HEIGHT: Final[int] = 400
MAX_ROWS_PER_PAGE: Final[int] = 100 