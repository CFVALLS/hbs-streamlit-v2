"""
Tests for the validators.
"""
import pytest
from datetime import datetime

from app.utils.validators import (
    validate_date,
    validate_time,
    validate_barra,
    validate_cmg_value,
    validate_cost_value,
    validate_email,
    validate_api_response,
    validate_db_connection
)
from app.constants import ERROR_MESSAGES

def test_validate_date_success():
    """Test successful date validation."""
    assert validate_date("2024-02-20")
    assert validate_date("2024-02-20", format="%Y-%m-%d")
    assert validate_date("20/02/2024", format="%d/%m/%Y")

def test_validate_date_invalid():
    """Test date validation with invalid date."""
    assert not validate_date("invalid-date")
    assert not validate_date("2024-13-45")  # Invalid month and day
    assert not validate_date("2024-02-30")  # Invalid day for February

def test_validate_time_success():
    """Test successful time validation."""
    assert validate_time("12:00:00")
    assert validate_time("12:00", format="%H:%M")
    assert validate_time("12:00:00.000", format="%H:%M:%S.%f")

def test_validate_time_invalid():
    """Test time validation with invalid time."""
    assert not validate_time("invalid-time")
    assert not validate_time("25:00:00")  # Invalid hour
    assert not validate_time("12:60:00")  # Invalid minute
    assert not validate_time("12:00:61")  # Invalid second

def test_validate_barra_success():
    """Test successful barra validation."""
    assert validate_barra("Los Angeles")
    assert validate_barra("Chillan")
    assert validate_barra("Temuco")

def test_validate_barra_invalid():
    """Test barra validation with invalid barra."""
    assert not validate_barra("")
    assert not validate_barra("NonExistentBarra")
    assert not validate_barra(None)

def test_validate_cmg_value_success():
    """Test successful CMG value validation."""
    assert validate_cmg_value(100.0)
    assert validate_cmg_value(0.0)
    assert validate_cmg_value(1000.0)

def test_validate_cmg_value_invalid():
    """Test CMG value validation with invalid values."""
    assert not validate_cmg_value(-100.0)
    assert not validate_cmg_value(None)
    assert not validate_cmg_value("invalid")

def test_validate_cost_value_success():
    """Test successful cost value validation."""
    assert validate_cost_value(100.0)
    assert validate_cost_value(0.0)
    assert validate_cost_value(1000.0)

def test_validate_cost_value_invalid():
    """Test cost value validation with invalid values."""
    assert not validate_cost_value(-100.0)
    assert not validate_cost_value(None)
    assert not validate_cost_value("invalid")

def test_validate_email_success():
    """Test successful email validation."""
    assert validate_email("test@example.com")
    assert validate_email("user.name@domain.co.uk")
    assert validate_email("user+tag@example.com")

def test_validate_email_invalid():
    """Test email validation with invalid emails."""
    assert not validate_email("")
    assert not validate_email("invalid-email")
    assert not validate_email("user@")
    assert not validate_email("@domain.com")
    assert not validate_email("user@.com")

def test_validate_api_response_success():
    """Test successful API response validation."""
    valid_response = {
        "status": "success",
        "data": {
            "cmg": 100.0,
            "timestamp": "2024-02-20T12:00:00Z"
        }
    }
    assert validate_api_response(valid_response)

def test_validate_api_response_invalid():
    """Test API response validation with invalid responses."""
    assert not validate_api_response({})
    assert not validate_api_response({"status": "error"})
    assert not validate_api_response({"data": {}})
    assert not validate_api_response(None)

def test_validate_db_connection_success():
    """Test successful database connection validation."""
    mock_connection = type('MockConnection', (), {
        'execute': lambda x: type('MockResult', (), {'scalar': lambda: 1})()
    })
    assert validate_db_connection(mock_connection)

def test_validate_db_connection_invalid():
    """Test database connection validation with invalid connection."""
    assert not validate_db_connection(None)
    assert not validate_db_connection({})
    assert not validate_db_connection(type('MockConnection', (), {'execute': lambda x: None})) 