"""
Tests for the main Streamlit application.
"""
import pytest
from unittest.mock import patch, MagicMock
import streamlit as st

from app.main import (
    setup_page,
    display_header,
    display_sidebar,
    display_current_status,
    display_cmg_chart,
    display_central_info,
    main
)

@pytest.fixture
def mock_streamlit():
    """Mock Streamlit functions."""
    with patch('streamlit.set_page_config') as mock_set_page_config, \
         patch('streamlit.title') as mock_title, \
         patch('streamlit.markdown') as mock_markdown, \
         patch('streamlit.sidebar') as mock_sidebar, \
         patch('streamlit.columns') as mock_columns, \
         patch('streamlit.plotly_chart') as mock_plotly_chart, \
         patch('streamlit.error') as mock_error, \
         patch('streamlit.success') as mock_success, \
         patch('streamlit.cache_data') as mock_cache_data:
        
        # Setup sidebar mock
        mock_sidebar.header = MagicMock()
        mock_sidebar.date_input = MagicMock(return_value=('2024-02-20', '2024-02-21'))
        mock_sidebar.selectbox = MagicMock(return_value='Test Barra')
        mock_sidebar.button = MagicMock(return_value=False)
        
        # Setup columns mock
        mock_col1 = MagicMock()
        mock_col2 = MagicMock()
        mock_columns.return_value = (mock_col1, mock_col2)
        
        yield {
            'set_page_config': mock_set_page_config,
            'title': mock_title,
            'markdown': mock_markdown,
            'sidebar': mock_sidebar,
            'columns': mock_columns,
            'plotly_chart': mock_plotly_chart,
            'error': mock_error,
            'success': mock_success,
            'cache_data': mock_cache_data,
            'col1': mock_col1,
            'col2': mock_col2
        }

def test_setup_page(mock_streamlit):
    """Test page setup."""
    setup_page()
    
    mock_streamlit['set_page_config'].assert_called_once()

def test_display_header(mock_streamlit):
    """Test header display."""
    display_header()
    
    mock_streamlit['title'].assert_called_once()
    mock_streamlit['markdown'].assert_called_once()

def test_display_sidebar(mock_streamlit):
    """Test sidebar display."""
    display_sidebar()
    
    mock_streamlit['sidebar'].header.assert_called_once()
    mock_streamlit['sidebar'].date_input.assert_called_once()
    mock_streamlit['sidebar'].selectbox.assert_called_once()
    mock_streamlit['sidebar'].button.assert_called_once()

def test_display_current_status_success(mock_streamlit):
    """Test current status display with successful data retrieval."""
    with patch('app.services.calculations.calc_service.get_current_time') as mock_get_time, \
         patch('app.services.database.db_service.get_tracking_coordinador') as mock_get_tracking, \
         patch('app.services.database.db_service.get_desacople_status') as mock_get_desacople:
        
        # Mock successful responses
        mock_get_time.return_value = {
            'date': '2024-02-20',
            'time': '12:00:00',
            'rounded_hour': '12:00:00',
            'unixtime': 1708444800
        }
        mock_get_tracking.return_value = (1, '2024-02-20 12:00:00', 'user', 'mod', 'status')
        mock_get_desacople.return_value = {
            'central_referencia': 'Test Central',
            'desacople_bool': True,
            'cmg': 100.0
        }
        
        display_current_status()
        
        mock_get_time.assert_called_once()
        mock_get_tracking.assert_called_once()
        mock_get_desacople.assert_called_once()

def test_display_current_status_error(mock_streamlit):
    """Test current status display with error."""
    with patch('app.services.calculations.calc_service.get_current_time') as mock_get_time:
        mock_get_time.side_effect = Exception("Test error")
        
        display_current_status()
        
        mock_streamlit['error'].assert_called_once()

def test_display_cmg_chart_success(mock_streamlit):
    """Test CMG chart display with successful data retrieval."""
    with patch('app.services.database.db_service.get_cmg_ponderado') as mock_get_cmg:
        # Mock successful response
        mock_get_cmg.return_value = [
            {'timestamp': '2024-02-20 12:00:00', 'cmg_ponderado': 100.0},
            {'timestamp': '2024-02-20 13:00:00', 'cmg_ponderado': 110.0}
        ]
        
        display_cmg_chart()
        
        mock_get_cmg.assert_called_once()
        mock_streamlit['plotly_chart'].assert_called_once()

def test_display_cmg_chart_error(mock_streamlit):
    """Test CMG chart display with error."""
    with patch('app.services.database.db_service.get_cmg_ponderado') as mock_get_cmg:
        mock_get_cmg.side_effect = Exception("Test error")
        
        display_cmg_chart()
        
        mock_streamlit['error'].assert_called_once()

def test_display_central_info_success(mock_streamlit):
    """Test central info display with successful data retrieval."""
    with patch('app.services.database.db_service.get_central_info') as mock_get_central, \
         patch('app.services.database.db_service.get_central_status') as mock_get_status, \
         patch('app.services.database.db_service.get_desacople_status') as mock_get_desacople, \
         patch('app.services.database.db_service.check_desacople_status') as mock_check_desacople:
        
        # Mock successful responses
        mock_get_central.return_value = (
            1, 'Test Central', 'Active', 'BARRA_TEST', 'Hidro',
            'Region', 'Comuna', 'Owner', 1000.0, 100.0, 900.0
        )
        mock_get_status.return_value = 'ON'  # Mock central status as ON
        mock_get_desacople.return_value = {'central_referencia': 'Ref Central'}
        mock_check_desacople.return_value = False
        
        display_central_info()
        
        mock_get_central.assert_called_once()
        mock_get_status.assert_called_once()

def test_display_central_info_error(mock_streamlit):
    """Test central info display with error."""
    with patch('app.services.database.db_service.get_central_info') as mock_get_central:
        mock_get_central.side_effect = Exception("Test error")
        
        display_central_info()
        
        mock_streamlit['error'].assert_called_once()

def test_main_success(mock_streamlit):
    """Test main function with successful execution."""
    with patch('app.main.setup_page') as mock_setup, \
         patch('app.main.display_header') as mock_header, \
         patch('app.main.display_sidebar') as mock_sidebar, \
         patch('app.main.display_current_status') as mock_status, \
         patch('app.main.display_central_info') as mock_info, \
         patch('app.main.display_cmg_chart') as mock_chart:
        
        main()
        
        mock_setup.assert_called_once()
        mock_header.assert_called_once()
        mock_sidebar.assert_called_once()
        mock_status.assert_called_once()
        mock_info.assert_called_once()
        mock_chart.assert_called_once()

def test_main_error(mock_streamlit):
    """Test main function with error."""
    with patch('app.main.setup_page') as mock_setup:
        mock_setup.side_effect = Exception("Test error")
        
        main()
        
        mock_streamlit['error'].assert_called_once() 