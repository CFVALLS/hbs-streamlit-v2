"""
Database connection utilities for the application.
"""
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError

def establecer_engine(database, user, password, host, port, verbose=False):
    """
    Establishes a connection to the database and returns the engine and metadata.
    
    Args:
        database (str): Database name
        user (str): Database user
        password (str): Database password
        host (str): Database host
        port (str): Database port
        verbose (bool): Whether to print verbose output
        
    Returns:
        tuple: (engine, metadata) if successful, (None, None) if connection fails
    """
    try:
        # Create SQLAlchemy engine
        engine_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        
        if verbose:
            logging.info(f"Connecting to database: {host}:{port}/{database}")
        
        engine = create_engine(
            engine_string,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,
            echo=False
        )
        
        # Test connection
        with engine.connect() as conn:
            if verbose:
                logging.info("Database connection successful")
        
        # Create metadata
        metadata = MetaData()
        metadata.reflect(bind=engine)
        
        return engine, metadata
    
    except SQLAlchemyError as e:
        logging.error(f"Database connection error: {str(e)}")
        return None, None
    
    except Exception as e:
        logging.error(f"Unexpected error connecting to database: {str(e)}")
        return None, None

def establecer_session(engine):
    """
    Creates a session factory for the given engine.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        function: Session factory function or None if engine is None
    """
    if engine is None:
        logging.error("Cannot create session with null engine")
        return None
    
    try:
        # Create session factory
        session_factory = sessionmaker(bind=engine)
        Session = scoped_session(session_factory)
        
        return Session
    
    except SQLAlchemyError as e:
        logging.error(f"Error creating session: {str(e)}")
        return None
    
    except Exception as e:
        logging.error(f"Unexpected error creating session: {str(e)}")
        return None

@contextmanager
def session_scope(session_factory):
    """
    Provides a transactional scope for a series of operations.
    
    Args:
        session_factory: SQLAlchemy session factory
        
    Yields:
        session: Active SQLAlchemy session
    """
    session = session_factory()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database transaction error: {str(e)}")
        raise
    finally:
        session.close()