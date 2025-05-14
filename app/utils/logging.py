"""
Logging configuration for the application.
"""
import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime

def setup_logging(
    name: str = 'streamlit_app',
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level: Optional[int] = None  # Added for backward compatibility with tests
) -> logging.Logger:
    """
    Set up logging configuration for the application.
    
    Args:
        name: Name of the logger (default: 'streamlit_app')
        log_level: Logging level (default: INFO)
        log_file: Path to log file (optional)
        log_format: Format for log messages
        level: Alternative parameter for log_level (for compatibility with tests)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Use level if provided (for compatibility with tests)
    if level is not None:
        log_level = level
        
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if log_file is provided
    if log_file:
        # Create logs directory if it doesn't exist
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # Add timestamp to log filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_path = log_dir / f"{log_file}_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the given name.
    
    Args:
        name: Name of the logger
        
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)

# Create default logger
logger = setup_logging('streamlit_app', log_file='app') 