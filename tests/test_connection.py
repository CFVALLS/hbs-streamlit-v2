"""
Tests for the database connection module.
"""
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.connection_db import (
    get_engine,
    get_session,
    init_db,
    close_db
)

@pytest.fixture
def mock_config():
    """Mock configuration."""
    database_config = type('DatabaseConfig', (), {
        'host': 'localhost',
        'port': 3306,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    })()
    
    return type('MockConfig', (), {
        'database': database_config,
        'DB_POOL_SIZE': 5,
        'DB_POOL_TIMEOUT': 30,
        'DB_POOL_RECYCLE': 3600
    })()

def test_get_engine(mock_config):
    """Test engine creation."""
    with patch('app.db.connection_db.config', mock_config), \
         patch('app.db.connection_db.create_engine') as mock_create_engine:
        
        # Setup mock
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Call function
        engine = get_engine()
        
        # Verify calls
        mock_create_engine.assert_called_once()
        assert engine == mock_engine

def test_get_session(mock_config):
    """Test session creation."""
    with patch('app.db.connection_db.get_engine') as mock_get_engine:
        # Setup mock
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        
        # Call function
        with get_session() as session:
            assert session is not None
        
        # Verify calls
        mock_get_engine.assert_called_once()

def test_init_db(mock_config):
    """Test database initialization."""
    with patch('app.db.connection_db.get_engine') as mock_get_engine, \
         patch('app.db.connection_db.Base.metadata.create_all') as mock_create_all:
        
        # Setup mock
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        
        # Call function
        init_db()
        
        # Verify calls
        mock_get_engine.assert_called_once()
        mock_create_all.assert_called_once_with(mock_engine)

def test_close_db(mock_config):
    """Test database closure."""
    with patch('app.db.connection_db.get_engine') as mock_get_engine:
        # Setup mock
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        
        # Call function
        close_db()
        
        # Verify calls
        mock_get_engine.assert_called_once()
        mock_engine.dispose.assert_called_once()

def test_session_context_manager(mock_config):
    """Test session context manager."""
    session = MagicMock()
    SessionMock = MagicMock(return_value=session)
    
    with patch('app.db.connection_db.get_engine') as mock_get_engine, \
         patch('app.db.connection_db.sessionmaker', return_value=SessionMock):
        
        # Setup mocks
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        
        # Test successful transaction
        with get_session() as sess:
            assert sess is not None
        
        # Verify calls
        session.commit.assert_called_once()
        session.close.assert_called_once()

def test_session_context_manager_error(mock_config):
    """Test session context manager with error."""
    session = MagicMock()
    SessionMock = MagicMock(return_value=session)
    
    with patch('app.db.connection_db.get_engine') as mock_get_engine, \
         patch('app.db.connection_db.sessionmaker', return_value=SessionMock):
        
        # Setup mocks
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        
        # Test error handling
        with pytest.raises(Exception):
            with get_session() as sess:
                assert sess is not None
                raise Exception("Test error")
        
        # Verify calls
        session.rollback.assert_called_once()
        session.close.assert_called_once()

def test_engine_connection_pool(mock_config):
    """Test engine connection pool settings."""
    with patch('app.db.connection_db.config', mock_config), \
         patch('app.db.connection_db.create_engine') as mock_create_engine:
        
        # Call function
        get_engine()
        
        # Verify pool settings
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        
        assert call_args['pool_size'] == 5
        assert call_args['pool_timeout'] == 30
        assert call_args['pool_recycle'] == 3600

def test_engine_connection_string(mock_config):
    """Test engine connection string format."""
    with patch('app.db.connection_db.config', mock_config), \
         patch('app.db.connection_db.create_engine') as mock_create_engine:
        
        # Call function
        get_engine()
        
        # Verify connection string
        mock_create_engine.assert_called_once()
        connection_string = mock_create_engine.call_args[0][0]
        
        assert f"mysql+pymysql://{mock_config.database.user}:{mock_config.database.password}@" in connection_string
        assert f"{mock_config.database.host}:{mock_config.database.port}/{mock_config.database.database}" in connection_string 