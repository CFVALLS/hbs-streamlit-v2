"""
Database service module.
"""
import os
import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import streamlit as st
# Import the utilities you've defined
from app.services.database_utils import establecer_engine, establecer_session, session_scope

class DatabaseService:
    """Database service for handling database operations."""
    
    def __init__(self):
        """Initialize the database service with connection to the database."""
        try:
            # Get database configuration from environment variables
            db_name = st.secrets["AWS_MYSQL"]["DATABASE"]
            db_user = st.secrets["AWS_MYSQL"]["USER"]
            db_password = st.secrets["AWS_MYSQL"]["USER_PASSWORD"]
            db_host = st.secrets["AWS_MYSQL"]["HOST"]
            db_port = st.secrets["AWS_MYSQL"]["PORT"]
            
            # Establish connection to the database
            self.engine, self.metadata = establecer_engine(
                database=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
                verbose=True
            )
            
            if self.engine is None:
                raise SQLAlchemyError("Failed to create database engine")
            
            # Create session factory
            self.Session = establecer_session(self.engine)
            
            if self.Session is None:
                raise SQLAlchemyError("Failed to create session factory")
                
            # Validate connection
            self._validate_connection()
            
            logging.info("Database service initialized successfully")
            
        except Exception as e:
            logging.error(f"Error initializing database service: {str(e)}")
            raise SQLAlchemyError(f"Failed to validate database connection: {str(e)}")
    
    def _validate_connection(self):
        """Validate database connection."""
        try:
            # Using SQLAlchemy 2.0 compatible approach
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                conn.commit()
        except Exception as e:
            raise SQLAlchemyError(f"Invalid database connection: {str(e)}")
    
    def execute_query(self, query, params=None):
        """
        Execute a SQL query and return results.
        
        Args:
            query (str): SQL query to execute
            params (dict, optional): Query parameters
            
        Returns:
            pd.DataFrame or None: Query results as DataFrame or None on error
        """
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                conn.commit()
                
                if result.returns_rows:
                    return pd.DataFrame(result.fetchall(), columns=result.keys())
                return None
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            return None
    
    def fetch_dataframe(self, query, params=None):
        """
        Fetch data as a pandas DataFrame.
        
        Args:
            query (str): SQL query
            params (dict, optional): Query parameters
            
        Returns:
            pd.DataFrame or None: Query results or None on error
        """
        return self.execute_query(query, params)
    
    def execute_with_session(self, operation_func):
        """
        Execute operations within a session context.
        
        Args:
            operation_func (callable): Function that takes a session as argument
            
        Returns:
            Any: Result of the operation function or None on error
        """
        try:
            with session_scope(self.Session) as session:
                return operation_func(session)
        except Exception as e:
            logging.error(f"Error executing operation with session: {str(e)}")
            return None
    
    # Add more database-related methods as needed

# Create singleton instance
db_service = DatabaseService()