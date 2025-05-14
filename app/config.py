"""
Configuration management for the Streamlit application.
Handles all configuration settings, environment variables, and secrets.
"""
from typing import Dict, Any
import os
from dataclasses import dataclass
import streamlit as st
import sys

@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    database: str
    host: str
    user: str
    password: str
    port: int

@dataclass
class APIConfig:
    """API configuration settings."""
    host: str
    port: int
    user_key: str

@dataclass
class AppConfig:
    """Main application configuration."""
    database: DatabaseConfig
    api: APIConfig
    timezone: str = 'America/Santiago'

def load_config() -> AppConfig:
    """
    Load configuration from Streamlit secrets and environment variables.
    
    Returns:
        AppConfig: Application configuration object
        
    Raises:
        ValueError: If required configuration is missing
    """
    try:
        # Check if running in test mode
        is_testing = 'pytest' in sys.modules
        
        if is_testing:
            # Use mock values for testing
            db_config = DatabaseConfig(
                database="test_db",
                host="localhost",
                user="test_user",
                password="test_password",
                port=3306
            )
            
            api_config = APIConfig(
                host="localhost",
                port=8000,
                user_key="test_key"
            )
        else:
            # Load database configuration from Streamlit secrets
            db_config = DatabaseConfig(
                database=st.secrets["AWS_MYSQL"]["DATABASE"],
                host=st.secrets["AWS_MYSQL"]["HOST"],
                user=st.secrets["AWS_MYSQL"]["USER"],
                password=st.secrets["AWS_MYSQL"]["USER_PASSWORD"],
                port=int(st.secrets["AWS_MYSQL"]["PORT"])
            )
            
            # Load API configuration from Streamlit secrets
            api_config = APIConfig(
                host=st.secrets["API"]["HOST"],
                port=int(st.secrets["API"]["PORT"]),
                user_key=st.secrets["COORDINADOR"]["USER_KEY"]
            )
        
        return AppConfig(
            database=db_config,
            api=api_config
        )
        
    except KeyError as e:
        raise ValueError(f"Missing required configuration: {e}")
    except Exception as e:
        # For tests, provide default configuration
        if 'pytest' in sys.modules:
            return AppConfig(
                database=DatabaseConfig(
                    database="test_db",
                    host="localhost",
                    user="test_user",
                    password="test_password",
                    port=3306
                ),
                api=APIConfig(
                    host="localhost",
                    port=8000,
                    user_key="test_key"
                )
            )
        raise ValueError(f"Error loading configuration: {e}")

# Create a singleton instance
config = load_config() 