"""
Database service for handling database operations.
"""
from typing import Any, Dict, List, Optional, Union
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
import streamlit as st
from ..config import config
from ..constants import DB_POOL_SIZE, DB_MAX_OVERFLOW, DB_POOL_TIMEOUT
from ..utils.logging import get_logger
from ..utils.validators import validate_db_connection

logger = get_logger(__name__)

class DatabaseService:
    """Service for handling database operations."""
    
    def __init__(self):
        """Initialize database service."""
        self.engine = None
        self.metadata = None
        self.Session = None
        self._initialize()
    
    def _initialize(self):
        """Initialize database connection and session."""
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                f"mysql+pymysql://{config.database.user}:{config.database.password}@"
                f"{config.database.host}:{config.database.port}/{config.database.database}",
                pool_size=DB_POOL_SIZE,
                max_overflow=DB_MAX_OVERFLOW,
                pool_timeout=DB_POOL_TIMEOUT,
                pool_recycle=3600
            )
            
            # Create metadata
            self.metadata = MetaData()
            
            # Create session factory
            self.Session = sessionmaker(bind=self.engine)
            
            # Validate connection
            if not validate_db_connection(self.engine):
                raise SQLAlchemyError("Failed to validate database connection")
                
            logger.info("Database service initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database service: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Session:
        """
        Get database session.
        
        Yields:
            Session: Database session
            
        Raises:
            SQLAlchemyError: If session creation fails
        """
        session = None
        try:
            session = self.Session()
            yield session
            session.commit()
        except Exception as e:
            if session:
                session.rollback()
            logger.error(f"Error in database session: {e}")
            raise
        finally:
            if session:
                session.close()
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_tracking_coordinador(self) -> Optional[List[Any]]:
        """
        Get last tracking coordinador entry.
        
        Returns:
            Optional[List[Any]]: Last tracking coordinador entry
            
        Raises:
            SQLAlchemyError: If query fails
        """
        try:
            with self.get_session() as session:
                from ..db.operaciones_db import retrieve_tracking_coordinador
                result = retrieve_tracking_coordinador(session)
                return result
        except Exception as e:
            logger.error(f"Error getting tracking coordinador: {e}")
            raise
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_desacople_status(self, barra_transmision: str) -> Optional[Dict[str, Any]]:
        """
        Get desacople status for a barra.
        
        Args:
            barra_transmision: Barra name
            
        Returns:
            Optional[Dict[str, Any]]: Desacople status
            
        Raises:
            SQLAlchemyError: If query fails
        """
        try:
            with self.get_session() as session:
                from ..db.operaciones_db import query_values_last_desacople_bool
                result = query_values_last_desacople_bool(session, barra_transmision)
                if result:
                    central_referencia, desacople_bool, cmg = result
                    # Only return a dictionary if we have a valid central_referencia
                    if central_referencia is not None:
                        return {
                            'central_referencia': central_referencia,
                            'desacople_bool': desacople_bool if desacople_bool is not None else False,
                            'cmg': float(cmg) if cmg is not None else 0.0
                        }
                return None
        except Exception as e:
            logger.error(f"Error getting desacople status: {e}")
            return None
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_cmg_ponderado(self, unixtime: int, delta_hours: int = 48) -> Optional[List[Dict[str, Any]]]:
        """
        Get CMG ponderado for a time range.
        
        Args:
            unixtime: Unix timestamp
            delta_hours: Hours to look back
            
        Returns:
            Optional[List[Dict[str, Any]]]: CMG ponderado data
            
        Raises:
            SQLAlchemyError: If query fails
        """
        try:
            with self.get_session() as session:
                from ..db.operaciones_db import query_cmg_ponderado_by_time
                result = query_cmg_ponderado_by_time(session, unixtime, delta_hours)
                return result
        except Exception as e:
            logger.error(f"Error getting CMG ponderado: {e}")
            raise
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_central_info(self, name_central: str) -> Optional[List[Any]]:
        """
        Get central information.
        
        Args:
            name_central: Central name
            
        Returns:
            Optional[List[Any]]: Central information
            
        Raises:
            SQLAlchemyError: If query fails
        """
        try:
            with self.get_session() as session:
                from ..db.operaciones_db import query_last_row_central
                result = query_last_row_central(session, name_central)
                return result
        except Exception as e:
            logger.error(f"Error getting central info: {e}")
            raise

    @st.cache_data(ttl=30)  # Cache for 30 seconds - shorter because status changes frequently
    def get_central_status(self, central_name: str) -> Optional[str]:
        """
        Get latest operational status for a central.
        
        Args:
            central_name: Central name
            
        Returns:
            Optional[str]: Status ('ON', 'OFF', or 'HOLD') or None if not found
            
        Raises:
            SQLAlchemyError: If query fails
        """
        try:
            with self.get_session() as session:
                from ..db.operaciones_db import get_latest_status_central
                return get_latest_status_central(session, central_name)
        except Exception as e:
            logger.error(f"Error getting central status: {e}")
            return None

# Create singleton instance
db_service = DatabaseService() 