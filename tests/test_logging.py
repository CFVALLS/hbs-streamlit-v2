"""
Tests for the logging utility.
"""
import pytest
import logging
import os
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile
import shutil

from app.utils.logging import setup_logging, get_logger

def test_setup_logging_default():
    """Test logging setup with default parameters."""
    with patch('logging.getLogger') as mock_get_logger, \
         patch('logging.StreamHandler') as mock_stream_handler, \
         patch('pathlib.Path.mkdir') as mock_mkdir, \
         patch('logging.FileHandler') as mock_file_handler:
        
        # Setup mocks
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_stream = MagicMock()
        mock_stream_handler.return_value = mock_stream
        
        # Call function with no log_file parameter
        logger = setup_logging()
        
        # Verify calls
        mock_get_logger.assert_called_once()
        mock_stream_handler.assert_called_once()
        
        # FileHandler should not be called when log_file is None
        mock_file_handler.assert_not_called()
        
        mock_logger.setLevel.assert_called_once_with(logging.INFO)
        
        assert logger == mock_logger

def test_setup_logging_custom():
    """Test logging setup with custom parameters."""
    with patch('logging.getLogger') as mock_get_logger, \
         patch('logging.StreamHandler') as mock_stream_handler, \
         patch('pathlib.Path.mkdir') as mock_mkdir, \
         patch('logging.FileHandler') as mock_file_handler, \
         patch('logging.Formatter') as mock_formatter:
        
        # Setup mocks
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_stream = MagicMock()
        mock_stream_handler.return_value = mock_stream
        
        mock_file = MagicMock()
        mock_file_handler.return_value = mock_file
        
        mock_formatter_instance = MagicMock()
        mock_formatter.return_value = mock_formatter_instance
        
        # Call function with custom parameters
        logger = setup_logging(
            level=logging.DEBUG,
            log_file='custom.log',
            log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Verify calls
        mock_get_logger.assert_called_once()
        mock_stream_handler.assert_called_once()
        
        # Verify mkdir was called
        mock_mkdir.assert_called_once()
        
        # Verify FileHandler was called, but don't specify the exact path
        # which includes a timestamp
        assert mock_file_handler.called
        
        mock_logger.setLevel.assert_called_once_with(logging.DEBUG)
        mock_logger.addHandler.assert_called()
        
        assert logger == mock_logger

def test_get_logger():
    """Test getting a logger instance."""
    with patch('logging.getLogger') as mock_get_logger:
        # Setup mock
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Call function
        logger = get_logger('test_module')
        
        # Verify calls
        mock_get_logger.assert_called_once_with('test_module')
        assert logger == mock_logger

def test_logger_creation():
    """Test actual logger creation and file handling."""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Setup logging with patches to prevent actual file creation
        with patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch('logging.FileHandler') as mock_file_handler:
            
            # Create a proper mock handler with a level attribute
            mock_handler = MagicMock()
            mock_handler.level = logging.DEBUG
            mock_file_handler.return_value = mock_handler
            
            # Setup logging
            logger = setup_logging(
                name="test_logger",
                level=logging.DEBUG,
                log_file='test.log',
                log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            
            # Verify that the logging configuration worked
            assert mock_mkdir.called
            assert mock_file_handler.called
    finally:
        # Clean up
        shutil.rmtree(temp_dir)

def test_logger_levels():
    """Test logger level filtering."""
    # Create a real logger with mocked handler
    with patch('logging.Handler') as MockHandler:
        # Set up the mock handler
        mock_handler = MagicMock()
        mock_handler.level = logging.INFO  # Set the level attribute
        MockHandler.return_value = mock_handler
        
        # Create a logger
        logger = logging.getLogger('test_filters')
        logger.setLevel(logging.INFO)
        
        # Replace handlers with our mock
        logger.handlers = []
        logger.addHandler(MockHandler())
        
        # Log messages at different levels
        logger.debug('Debug message')  # Should be filtered out
        logger.info('Info message')    # Should be logged
        logger.warning('Warning message')  # Should be logged
        logger.error('Error message')  # Should be logged
        
        # Check the handle method was called 3 times (not for debug)
        assert mock_handler.handle.call_count == 3
        
        # We can't easily check the content of the messages in this approach
        # So we'll just verify the call count 