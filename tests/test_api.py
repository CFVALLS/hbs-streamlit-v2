"""
Tests for the API service.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from app.services.api import api_service
from app.constants import ERROR_MESSAGES

@pytest.fixture
def mock_response():
    """Mock API response."""
    return {
        "status": "success",
        "data": {
            "cmg": 100.0,
            "timestamp": "2024-02-20T12:00:00Z"
        }
    }

def test_get_costo_marginal_online_success(mock_response):
    """Test successful CMG retrieval."""
    with patch('requests.Session.get') as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.status_code = 200
        
        result = api_service.get_costo_marginal_online("2024-02-20")
        
        assert result == mock_response
        mock_get.assert_called_once()

def test_get_costo_marginal_online_invalid_date():
    """Test CMG retrieval with invalid date."""
    with pytest.raises(ValueError) as exc_info:
        api_service.get_costo_marginal_online("invalid-date")
    
    assert str(exc_info.value) == ERROR_MESSAGES['INVALID_DATE']

def test_get_costo_marginal_online_api_error():
    """Test CMG retrieval with API error."""
    with patch('requests.Session.get') as mock_get:
        mock_get.return_value.status_code = 500
        
        with pytest.raises(Exception) as exc_info:
            api_service.get_costo_marginal_online("2024-02-20")
        
        assert str(exc_info.value) == ERROR_MESSAGES['API_ERROR']

def test_get_central_success(mock_response):
    """Test successful central info retrieval."""
    with patch('requests.Session.get') as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.status_code = 200
        
        result = api_service.get_central("Los Angeles")
        
        assert result == mock_response
        mock_get.assert_called_once()

def test_get_central_not_found():
    """Test central info retrieval with non-existent central."""
    with patch('requests.Session.get') as mock_get:
        mock_get.return_value.status_code = 404
        
        with pytest.raises(Exception) as exc_info:
            api_service.get_central("NonExistentCentral")
        
        assert str(exc_info.value) == ERROR_MESSAGES['CENTRAL_NOT_FOUND']

def test_insert_central_success():
    """Test successful central insertion."""
    central_data = {
        "nombre": "Test Central",
        "tipo": "Hidro",
        "potencia": 100.0
    }
    
    with patch('requests.Session.post') as mock_post:
        mock_post.return_value.status_code = 201
        
        result = api_service.insert_central(central_data)
        
        assert result is True
        mock_post.assert_called_once()

def test_insert_central_validation_error():
    """Test central insertion with invalid data."""
    invalid_data = {
        "nombre": "",  # Empty name
        "tipo": "Invalid",
        "potencia": -100.0  # Negative power
    }
    
    with pytest.raises(ValueError) as exc_info:
        api_service.insert_central(invalid_data)
    
    assert str(exc_info.value) == ERROR_MESSAGES['INVALID_CENTRAL_DATA'] 