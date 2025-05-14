"""
Tests for the configuration module.
"""
import pytest
import os
from unittest.mock import patch, MagicMock

from app.config import config, load_config

def test_load_config_default():
    """Test loading configuration with default values."""
    with patch('streamlit.secrets') as mock_secrets, \
         patch.dict(os.environ, {}, clear=True):
        
        # Setup mock secrets
        mock_secrets.get = MagicMock(return_value=None)
        
        # Load config
        cfg = load_config()
        
        # Verify default values
        assert cfg.DB_HOST == 'localhost'
        assert cfg.DB_PORT == 3306
        assert cfg.DB_NAME == 'hbs_db'
        assert cfg.DB_USER == 'user'
        assert cfg.DB_PASSWORD == 'password'
        
        assert cfg.API_URL == 'https://api.example.com'
        assert cfg.API_KEY == 'your-api-key'
        assert cfg.API_TIMEOUT == 30
        
        assert cfg.DEBUG is False
        assert cfg.LOG_LEVEL == 'INFO'
        assert cfg.LOG_FILE == 'logs/app.log'
        
        assert cfg.CACHE_TTL == 300
        
        assert cfg.CHART_HEIGHT == 600
        assert cfg.CHART_WIDTH == 800
        assert cfg.CHART_THEME == 'plotly_white'

def test_load_config_from_env():
    """Test loading configuration from environment variables."""
    env_vars = {
        'DB_HOST': 'custom-host',
        'DB_PORT': '5432',
        'DB_NAME': 'custom-db',
        'DB_USER': 'custom-user',
        'DB_PASSWORD': 'custom-password',
        'API_URL': 'https://custom-api.com',
        'API_KEY': 'custom-key',
        'API_TIMEOUT': '60',
        'DEBUG': 'true',
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': 'custom.log',
        'CACHE_TTL': '600',
        'CHART_HEIGHT': '800',
        'CHART_WIDTH': '1000',
        'CHART_THEME': 'plotly_dark'
    }
    
    with patch('streamlit.secrets') as mock_secrets, \
         patch.dict(os.environ, env_vars, clear=True):
        
        # Setup mock secrets
        mock_secrets.get = MagicMock(return_value=None)
        
        # Load config
        cfg = load_config()
        
        # Verify environment values
        assert cfg.DB_HOST == 'custom-host'
        assert cfg.DB_PORT == 5432
        assert cfg.DB_NAME == 'custom-db'
        assert cfg.DB_USER == 'custom-user'
        assert cfg.DB_PASSWORD == 'custom-password'
        
        assert cfg.API_URL == 'https://custom-api.com'
        assert cfg.API_KEY == 'custom-key'
        assert cfg.API_TIMEOUT == 60
        
        assert cfg.DEBUG is True
        assert cfg.LOG_LEVEL == 'DEBUG'
        assert cfg.LOG_FILE == 'custom.log'
        
        assert cfg.CACHE_TTL == 600
        
        assert cfg.CHART_HEIGHT == 800
        assert cfg.CHART_WIDTH == 1000
        assert cfg.CHART_THEME == 'plotly_dark'

def test_load_config_from_secrets():
    """Test loading configuration from Streamlit secrets."""
    secrets = {
        'DB_HOST': 'secret-host',
        'DB_PORT': 5432,
        'DB_NAME': 'secret-db',
        'DB_USER': 'secret-user',
        'DB_PASSWORD': 'secret-password',
        'API_URL': 'https://secret-api.com',
        'API_KEY': 'secret-key',
        'API_TIMEOUT': 60,
        'DEBUG': True,
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': 'secret.log',
        'CACHE_TTL': 600,
        'CHART_HEIGHT': 800,
        'CHART_WIDTH': 1000,
        'CHART_THEME': 'plotly_dark'
    }
    
    with patch('streamlit.secrets') as mock_secrets, \
         patch.dict(os.environ, {}, clear=True):
        
        # Setup mock secrets
        mock_secrets.get = lambda key: secrets.get(key)
        
        # Load config
        cfg = load_config()
        
        # Verify secret values
        assert cfg.DB_HOST == 'secret-host'
        assert cfg.DB_PORT == 5432
        assert cfg.DB_NAME == 'secret-db'
        assert cfg.DB_USER == 'secret-user'
        assert cfg.DB_PASSWORD == 'secret-password'
        
        assert cfg.API_URL == 'https://secret-api.com'
        assert cfg.API_KEY == 'secret-key'
        assert cfg.API_TIMEOUT == 60
        
        assert cfg.DEBUG is True
        assert cfg.LOG_LEVEL == 'DEBUG'
        assert cfg.LOG_FILE == 'secret.log'
        
        assert cfg.CACHE_TTL == 600
        
        assert cfg.CHART_HEIGHT == 800
        assert cfg.CHART_WIDTH == 1000
        assert cfg.CHART_THEME == 'plotly_dark'

def test_load_config_priority():
    """Test configuration loading priority (secrets > env > defaults)."""
    env_vars = {
        'DB_HOST': 'env-host',
        'DB_PORT': '5432',
        'DB_NAME': 'env-db'
    }
    
    secrets = {
        'DB_HOST': 'secret-host',
        'DB_PORT': 3306,
        'DB_USER': 'secret-user'
    }
    
    with patch('streamlit.secrets') as mock_secrets, \
         patch.dict(os.environ, env_vars, clear=True):
        
        # Setup mock secrets
        mock_secrets.get = lambda key: secrets.get(key)
        
        # Load config
        cfg = load_config()
        
        # Verify priority
        assert cfg.DB_HOST == 'secret-host'  # From secrets
        assert cfg.DB_PORT == 5432  # From env
        assert cfg.DB_NAME == 'env-db'  # From env
        assert cfg.DB_USER == 'secret-user'  # From secrets
        assert cfg.DB_PASSWORD == 'password'  # Default

def test_config_singleton():
    """Test that config is a singleton."""
    cfg1 = load_config()
    cfg2 = load_config()
    
    assert cfg1 is cfg2
    assert cfg1 is config 