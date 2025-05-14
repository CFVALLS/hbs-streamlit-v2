"""
Tests for the database service.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from app.services.database import db_service
from app.constants import ERROR_MESSAGES

@pytest.fixture
def mock_session():
    """Mock database session."""
    session = MagicMock()
    session.execute.return_value = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    return session

@pytest.fixture
def mock_tracking_data():
    """Mock tracking data."""
    return (
        1,  # id
        datetime.now(),  # timestamp
        "user",  # user
        "modification",  # modification
        "status"  # status
    )

@pytest.fixture
def mock_desacople_data():
    """Mock desacople data."""
    return {
        "central_referencia": "Test Central",
        "desacople_bool": True,
        "cmg": 100.0
    }

def test_get_tracking_coordinador_success(mock_session, mock_tracking_data):
    """Test successful tracking coordinador retrieval."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar_one.return_value = mock_tracking_data
        
        result = db_service.get_tracking_coordinador()
        
        assert result == mock_tracking_data
        mock_session.execute.assert_called_once()

def test_get_tracking_coordinador_not_found(mock_session):
    """Test tracking coordinador retrieval with no data."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar_one.side_effect = Exception("No data")
        
        result = db_service.get_tracking_coordinador()
        
        assert result is None

def test_get_desacople_status_success(mock_session, mock_desacople_data):
    """Test successful desacople status retrieval."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.mappings.return_value = [mock_desacople_data]
        
        result = db_service.get_desacople_status("Test Barra")
        
        assert result == mock_desacople_data
        mock_session.execute.assert_called_once()

def test_get_desacople_status_not_found(mock_session):
    """Test desacople status retrieval with no data."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.mappings.return_value = []
        
        result = db_service.get_desacople_status("NonExistentBarra")
        
        assert result is None

def test_get_cmg_ponderado_success(mock_session):
    """Test successful CMG ponderado retrieval."""
    mock_data = [
        {"timestamp": datetime.now(), "cmg_ponderado": 100.0},
        {"timestamp": datetime.now() + timedelta(hours=1), "cmg_ponderado": 110.0}
    ]
    
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.mappings.return_value = mock_data
        
        result = db_service.get_cmg_ponderado(
            int(datetime.now().timestamp()),
            delta_hours=24
        )
        
        assert result == mock_data
        mock_session.execute.assert_called_once()

def test_get_cmg_ponderado_empty(mock_session):
    """Test CMG ponderado retrieval with no data."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.mappings.return_value = []
        
        result = db_service.get_cmg_ponderado(
            int(datetime.now().timestamp()),
            delta_hours=24
        )
        
        assert result == []

def test_get_central_info_success(mock_session):
    """Test successful central info retrieval."""
    mock_data = (
        1,  # id
        "Test Central",  # nombre
        "Active",  # estado
        100.0,  # potencia
        "Hidro",  # tipo
        "Region",  # region
        "Comuna",  # comuna
        "Owner",  # propietario
        1000.0,  # costo_operacional
        100.0,  # ajuste
        900.0  # costo_operacional_base
    )
    
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar_one.return_value = mock_data
        
        result = db_service.get_central_info("Test Central")
        
        assert result == mock_data
        mock_session.execute.assert_called_once()

def test_get_central_info_not_found(mock_session):
    """Test central info retrieval with non-existent central."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        mock_session.execute.return_value.scalar_one.side_effect = Exception("No data")
        
        result = db_service.get_central_info("NonExistentCentral")
        
        assert result is None

def test_get_central_status_success(mock_session):
    """Test successful central status retrieval."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        
        # Mock the get_latest_status_central function
        with patch('app.db.operaciones_db.get_latest_status_central') as mock_get_status:
            mock_get_status.return_value = 'ON'
            
            result = db_service.get_central_status("Test Central")
            
            assert result == 'ON'
            mock_get_status.assert_called_once_with(mock_session, "Test Central")

def test_get_central_status_not_found(mock_session):
    """Test central status retrieval with no status found."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        
        # Mock the get_latest_status_central function
        with patch('app.db.operaciones_db.get_latest_status_central') as mock_get_status:
            mock_get_status.return_value = None
            
            result = db_service.get_central_status("NonExistentCentral")
            
            assert result is None
            mock_get_status.assert_called_once_with(mock_session, "NonExistentCentral")

def test_get_central_status_error(mock_session):
    """Test central status retrieval with error."""
    with patch('app.services.database.db_service.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        
        # Mock the get_latest_status_central function
        with patch('app.db.operaciones_db.get_latest_status_central') as mock_get_status:
            mock_get_status.side_effect = Exception("Test error")
            
            result = db_service.get_central_status("Test Central")
            
            assert result is None
            mock_get_status.assert_called_once_with(mock_session, "Test Central") 