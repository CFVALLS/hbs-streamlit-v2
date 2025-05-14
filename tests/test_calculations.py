"""
Tests for the calculations service.
"""
import pytest
from datetime import datetime
import pytz

from app.services.calculations import calc_service
from app.constants import ERROR_MESSAGES

def test_get_current_time():
    """Test current time retrieval."""
    result = calc_service.get_current_time()
    
    assert isinstance(result, dict)
    assert 'date' in result
    assert 'time' in result
    assert 'rounded_hour' in result
    assert 'unixtime' in result
    
    # Verify timezone
    chile_tz = pytz.timezone('America/Santiago')
    current_time = datetime.now(chile_tz)
    
    assert result['date'] == current_time.strftime('%Y-%m-%d')
    assert result['time'] == current_time.strftime('%H:%M:%S')
    assert result['rounded_hour'] == current_time.strftime('%H:00:00')
    assert isinstance(result['unixtime'], int)

def test_calculate_cmg_hora_success():
    """Test successful CMG calculation."""
    data_points = [
        {"timestamp": 1708444800, "cmg": 100.0},  # 2024-02-20 12:00:00
        {"timestamp": 1708448400, "cmg": 110.0},  # 2024-02-20 13:00:00
        {"timestamp": 1708452000, "cmg": 120.0}   # 2024-02-20 14:00:00
    ]
    
    result = calc_service.calculate_cmg_hora(
        data_points=data_points,
        unixtime=1708444800,
        duration=3600
    )
    
    assert isinstance(result, float)
    assert result == 100.0  # Should match the first data point

def test_calculate_cmg_hora_empty_data():
    """Test CMG calculation with empty data."""
    with pytest.raises(ValueError) as exc_info:
        calc_service.calculate_cmg_hora(
            data_points=[],
            unixtime=1708444800,
            duration=3600
        )
    
    assert str(exc_info.value) == ERROR_MESSAGES['INVALID_DATA']

def test_calculate_cmg_hora_invalid_timestamp():
    """Test CMG calculation with invalid timestamp."""
    data_points = [
        {"timestamp": 1708444800, "cmg": 100.0}
    ]
    
    with pytest.raises(ValueError) as exc_info:
        calc_service.calculate_cmg_hora(
            data_points=data_points,
            unixtime=0,  # Invalid timestamp
            duration=3600
        )
    
    assert str(exc_info.value) == ERROR_MESSAGES['INVALID_TIMESTAMP']

def test_calculate_operational_cost_success():
    """Test successful operational cost calculation."""
    base_cost = 1000.0
    adjustments = 100.0
    
    result = calc_service.calculate_operational_cost(
        base_cost=base_cost,
        adjustments=adjustments
    )
    
    assert isinstance(result, float)
    assert result == 1100.0  # base_cost + adjustments

def test_calculate_operational_cost_negative():
    """Test operational cost calculation with negative values."""
    with pytest.raises(ValueError) as exc_info:
        calc_service.calculate_operational_cost(
            base_cost=-1000.0,
            adjustments=100.0
        )
    
    assert str(exc_info.value) == ERROR_MESSAGES['INVALID_COST']

def test_calculate_profitability_success():
    """Test successful profitability calculation."""
    cmg = 100.0
    operational_cost = 80.0
    penalization_factor = 1.1
    
    result = calc_service.calculate_profitability(
        cmg=cmg,
        operational_cost=operational_cost,
        penalization_factor=penalization_factor
    )
    
    assert isinstance(result, dict)
    assert 'profit' in result
    assert 'profit_margin' in result
    assert 'roi' in result
    
    expected_profit = (cmg * penalization_factor) - operational_cost
    expected_margin = (expected_profit / operational_cost) * 100
    expected_roi = (expected_profit / operational_cost) * 100
    
    assert result['profit'] == expected_profit
    assert result['profit_margin'] == expected_margin
    assert result['roi'] == expected_roi

def test_calculate_profitability_negative_cmg():
    """Test profitability calculation with negative CMG."""
    with pytest.raises(ValueError) as exc_info:
        calc_service.calculate_profitability(
            cmg=-100.0,
            operational_cost=80.0,
            penalization_factor=1.1
        )
    
    assert str(exc_info.value) == ERROR_MESSAGES['INVALID_CMG']

def test_calculate_profitability_invalid_factor():
    """Test profitability calculation with invalid penalization factor."""
    with pytest.raises(ValueError) as exc_info:
        calc_service.calculate_profitability(
            cmg=100.0,
            operational_cost=80.0,
            penalization_factor=0.0  # Invalid factor
        )
    
    assert str(exc_info.value) == ERROR_MESSAGES['INVALID_FACTOR'] 