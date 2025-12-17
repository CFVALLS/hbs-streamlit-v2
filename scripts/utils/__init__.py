"""
Módulo para utilidades y funciones comunes.
Contiene funciones de uso general para todos los scripts.
"""

# Utils imports are optional to avoid hard failures when dependencies (e.g., yaml)
# are missing in lightweight environments. Helpers can still be used without them.
try:
    from .utils import (
        get_date,
        load_config,
        setup_logging,
        list_files,
        timestamp_to_unix,
        unix_to_datetime,
        ensure_dir_exists
    )

    __all__ = [
        'get_date',
        'load_config',
        'setup_logging',
        'list_files',
        'timestamp_to_unix',
        'unix_to_datetime',
        'ensure_dir_exists'
    ]
except ImportError:
    __all__ = []
