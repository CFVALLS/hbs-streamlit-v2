"""
Módulo para establecer la conexión a la base de datos.
"""
import os
import sys
from contextlib import contextmanager
# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker, Session
import logging
from scripts.utils.utils import setup_logging, get_date

# Configurar logger
logger = setup_logging(log_name='connection_db', log_level=logging.INFO, log_filename='connection_db.log')

def establecer_engine(database_url=None):
    """
    Establece y retorna un engine de SQLAlchemy.
    
    Args:
        database_url (str): URL de conexión a la base de datos. Si es None, 
                           se usa la variable de entorno DB_CONNECTION_STRING
                           o una URL por defecto.
    
    Returns:
        Engine: Engine de SQLAlchemy
    """
    try:
        # Obtener URL de conexión
        if database_url is None:
            # Try to get from environment variable first
            database_url = os.getenv("DB_CONNECTION_STRING")
            
            # If not set, try to construct from individual environment variables
            if database_url is None:
                # Prefer explicit DB_* vars, then MYSQL_*, then AWS_MYSQL_*.
                db_user = (
                    os.getenv("DB_USER")
                    or os.getenv("MYSQL_USER")
                    or os.getenv("AWS_MYSQL_USER")
                    or "admin"
                )
                db_password = (
                    os.getenv("DB_USER_PASSWORD")
                    or os.getenv("MYSQL_USER_PASSWORD")
                    or os.getenv("AWS_MYSQL_USER_PASSWORD")
                    or ""
                )
                db_host = (
                    os.getenv("DB_HOST")
                    or os.getenv("MYSQL_HOST")
                    or os.getenv("AWS_MYSQL_HOST")
                    or "172.27.144.1"
                )
                db_port = (
                    os.getenv("DB_PORT")
                    or os.getenv("MYSQL_PORT")
                    or os.getenv("AWS_MYSQL_PORT")
                    or "3306"
                )
                db_name = (
                    os.getenv("DB_DATABASE")
                    or os.getenv("MYSQL_DATABASE")
                    or os.getenv("AWS_MYSQL_DATABASE")
                    or "hbsv2"
                )
                
                # Construct MySQL connection string
                database_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
                
                # If still not set, use SQLite as fallback
                if not all([db_user, db_host, db_name]):
                    database_url = "sqlite:///cmg_db.sqlite"
                    logger.warning("Using SQLite as fallback database")
        
        # Crear engine
        engine = create_engine(database_url)
        try:
            parsed_url = make_url(database_url)
            safe_target = (
                f"{parsed_url.drivername}://{parsed_url.host}:{parsed_url.port}/"
                f"{parsed_url.database}"
            )
        except Exception:
            safe_target = "<unparsed_db_target>"
        logger.info(f"Engine establecido con éxito: {safe_target}")
        return engine
    
    except Exception as e:
        logger.error(f"Error al establecer engine: {e}")
        raise

def establecer_session(engine):
    """
    Establece y retorna una sesión de SQLAlchemy.
    
    Args:
        engine (Engine): Engine de SQLAlchemy
    
    Returns:
        Session: Sesión de SQLAlchemy
    """
    try:
        # Crear sessionmaker
        SessionMaker = sessionmaker(bind=engine)
        
        # Crear sesión
        session = SessionMaker()
        logger.info("Sesión establecida con éxito")
        return session
    
    except Exception as e:
        logger.error(f"Error al establecer sesión: {e}")
        raise

@contextmanager
def session_scope(session: Session):
    """
    Context manager to provide a transactional scope around a series of operations.
    Accepts an existing session and ensures proper close/rollback semantics.
    """
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
