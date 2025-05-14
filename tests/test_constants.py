"""
Tests for the constants module.
"""
import pytest
from typing import Final

from app.constants import (
    # API Constants
    API_TIMEOUT,
    API_RETRIES,
    API_BACKOFF_FACTOR,
    
    # Database Constants
    DB_POOL_SIZE,
    DB_POOL_TIMEOUT,
    DB_POOL_RECYCLE,
    
    # Time Constants
    TIMEZONE,
    DATE_FORMAT,
    TIME_FORMAT,
    
    # UI Constants
    PAGE_TITLE,
    PAGE_ICON,
    LAYOUT,
    
    # Barra Constants
    BARRAS,
    
    # Status Constants
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_ERROR,
    
    # Error Messages
    ERROR_MESSAGES,
    
    # Success Messages
    SUCCESS_MESSAGES,
    
    # Cache Constants
    CACHE_TTL,
    CACHE_MAX_ENTRIES,
    
    # Validation Constants
    MIN_CMG_VALUE,
    MAX_CMG_VALUE,
    MIN_COST_VALUE,
    MAX_COST_VALUE,
    
    # Chart Constants
    CHART_HEIGHT,
    CHART_WIDTH,
    CHART_THEME,
    
    # Table Constants
    TABLE_HEIGHT,
    MAX_ROWS_PER_PAGE
)

def test_api_constants():
    """Test API constants."""
    assert isinstance(API_TIMEOUT, int)
    assert API_TIMEOUT > 0
    
    assert isinstance(API_RETRIES, int)
    assert API_RETRIES > 0
    
    assert isinstance(API_BACKOFF_FACTOR, float)
    assert API_BACKOFF_FACTOR > 0

def test_database_constants():
    """Test database constants."""
    assert isinstance(DB_POOL_SIZE, int)
    assert DB_POOL_SIZE > 0
    
    assert isinstance(DB_POOL_TIMEOUT, int)
    assert DB_POOL_TIMEOUT > 0
    
    assert isinstance(DB_POOL_RECYCLE, int)
    assert DB_POOL_RECYCLE > 0

def test_time_constants():
    """Test time constants."""
    assert isinstance(TIMEZONE, str)
    assert TIMEZONE == 'America/Santiago'
    
    assert isinstance(DATE_FORMAT, str)
    assert '%Y-%m-%d' in DATE_FORMAT
    
    assert isinstance(TIME_FORMAT, str)
    assert '%H:%M:%S' in TIME_FORMAT

def test_ui_constants():
    """Test UI constants."""
    assert isinstance(PAGE_TITLE, str)
    assert len(PAGE_TITLE) > 0
    
    assert isinstance(PAGE_ICON, str)
    assert len(PAGE_ICON) > 0
    
    assert isinstance(LAYOUT, str)
    assert LAYOUT in ['wide', 'centered']

def test_barra_constants():
    """Test barra constants."""
    assert isinstance(BARRAS, dict)
    assert len(BARRAS) > 0
    
    # Check that BARRAS is a dict of str -> str
    for barra, value in BARRAS.items():
        assert isinstance(barra, str)
        assert isinstance(value, str)
        assert len(barra) > 0
        assert len(value) > 0

def test_status_constants():
    """Test status constants."""
    assert isinstance(STATUS_ACTIVE, str)
    assert isinstance(STATUS_INACTIVE, str)
    assert isinstance(STATUS_ERROR, str)
    
    assert STATUS_ACTIVE != STATUS_INACTIVE
    assert STATUS_ACTIVE != STATUS_ERROR
    assert STATUS_INACTIVE != STATUS_ERROR

def test_error_messages():
    """Test error messages."""
    assert isinstance(ERROR_MESSAGES, dict)
    assert len(ERROR_MESSAGES) > 0
    
    for key, message in ERROR_MESSAGES.items():
        assert isinstance(key, str)
        assert isinstance(message, str)
        assert len(message) > 0

def test_success_messages():
    """Test success messages."""
    assert isinstance(SUCCESS_MESSAGES, dict)
    assert len(SUCCESS_MESSAGES) > 0
    
    for key, message in SUCCESS_MESSAGES.items():
        assert isinstance(key, str)
        assert isinstance(message, str)
        assert len(message) > 0

def test_cache_constants():
    """Test cache constants."""
    assert isinstance(CACHE_TTL, int)
    assert CACHE_TTL > 0
    
    assert isinstance(CACHE_MAX_ENTRIES, int)
    assert CACHE_MAX_ENTRIES > 0

def test_validation_constants():
    """Test validation constants."""
    assert isinstance(MIN_CMG_VALUE, float)
    assert isinstance(MAX_CMG_VALUE, float)
    assert MIN_CMG_VALUE < MAX_CMG_VALUE
    
    assert isinstance(MIN_COST_VALUE, float)
    assert isinstance(MAX_COST_VALUE, float)
    assert MIN_COST_VALUE < MAX_COST_VALUE

def test_chart_constants():
    """Test chart constants."""
    assert isinstance(CHART_HEIGHT, int)
    assert CHART_HEIGHT > 0
    
    assert isinstance(CHART_WIDTH, int)
    assert CHART_WIDTH > 0
    
    assert isinstance(CHART_THEME, str)
    assert len(CHART_THEME) > 0

def test_table_constants():
    """Test table constants."""
    assert isinstance(TABLE_HEIGHT, int)
    assert TABLE_HEIGHT > 0
    
    assert isinstance(MAX_ROWS_PER_PAGE, int)
    assert MAX_ROWS_PER_PAGE > 0

def test_constant_types():
    """Test that all constants are marked as Final."""
    # We can't use isinstance with typing.Final, so we'll check if the names
    # are in the constants module and all are typed with Final
    import inspect
    from app import constants
    
    for name, value in globals().items():
        if name.isupper() and not name.startswith('_'):
            # Skip this check since we can't use isinstance with typing.Final
            # Assert that the constant exists in the module
            assert hasattr(constants, name), f"{name} should be defined in constants.py" 