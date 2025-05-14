"""
Tests for the database models.
"""
import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models_orm import (
    Base,
    TrackingCoordinador,
    Desacople,
    CMGPonderado,
    Central
)

@pytest.fixture
def engine():
    """Create a test database engine."""
    return create_engine('sqlite:///:memory:')

@pytest.fixture
def tables(engine):
    """Create test tables."""
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)

@pytest.fixture
def session(engine, tables):
    """Create a test session."""
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()

def test_tracking_coordinador(session):
    """Test TrackingCoordinador model."""
    # Create test data
    tracking = TrackingCoordinador(
        timestamp=datetime.now(),
        user='test_user',
        modification='test_modification',
        status='test_status'
    )
    
    # Add to session
    session.add(tracking)
    session.commit()
    
    # Query and verify
    result = session.query(TrackingCoordinador).first()
    assert result is not None
    assert result.user == 'test_user'
    assert result.modification == 'test_modification'
    assert result.status == 'test_status'

def test_desacople(session):
    """Test Desacople model."""
    # Create test data
    desacople = Desacople(
        barra='Test Barra',
        central_referencia='Test Central',
        desacople_bool=True,
        cmg=100.0,
        timestamp=datetime.now()
    )
    
    # Add to session
    session.add(desacople)
    session.commit()
    
    # Query and verify
    result = session.query(Desacople).first()
    assert result is not None
    assert result.barra == 'Test Barra'
    assert result.central_referencia == 'Test Central'
    assert result.desacople_bool is True
    assert result.cmg == 100.0

def test_cmg_ponderado(session):
    """Test CMGPonderado model."""
    # Create test data
    cmg = CMGPonderado(
        timestamp=datetime.now(),
        cmg_ponderado=100.0
    )
    
    # Add to session
    session.add(cmg)
    session.commit()
    
    # Query and verify
    result = session.query(CMGPonderado).first()
    assert result is not None
    assert result.cmg_ponderado == 100.0

def test_central(session):
    """Test Central model."""
    # Create test data
    central = Central(
        nombre='Test Central',
        estado='Active',
        potencia=100.0,
        tipo='Hidro',
        region='Test Region',
        comuna='Test Comuna',
        propietario='Test Owner',
        costo_operacional=1000.0,
        ajuste=100.0,
        costo_operacional_base=900.0
    )
    
    # Add to session
    session.add(central)
    session.commit()
    
    # Query and verify
    result = session.query(Central).first()
    assert result is not None
    assert result.nombre == 'Test Central'
    assert result.estado == 'Active'
    assert result.potencia == 100.0
    assert result.tipo == 'Hidro'
    assert result.region == 'Test Region'
    assert result.comuna == 'Test Comuna'
    assert result.propietario == 'Test Owner'
    assert result.costo_operacional == 1000.0
    assert result.ajuste == 100.0
    assert result.costo_operacional_base == 900.0

def test_central_relationships(session):
    """Test Central model relationships."""
    # Create test data
    central = Central(
        nombre='Test Central',
        estado='Active',
        potencia=100.0,
        tipo='Hidro',
        region='Test Region',
        comuna='Test Comuna',
        propietario='Test Owner',
        costo_operacional=1000.0,
        ajuste=100.0,
        costo_operacional_base=900.0
    )
    
    desacople = Desacople(
        barra='Test Barra',
        central_referencia='Test Central',
        desacople_bool=True,
        cmg=100.0,
        timestamp=datetime.now()
    )
    
    # Add to session
    session.add(central)
    session.add(desacople)
    session.commit()
    
    # Query and verify relationships
    result = session.query(Central).first()
    assert result is not None
    assert result.nombre == desacople.central_referencia

def test_model_constraints(session):
    """Test model constraints."""
    # Test unique constraint
    central1 = Central(
        nombre='Test Central',
        estado='Active',
        potencia=100.0,
        tipo='Hidro',
        region='Test Region',
        comuna='Test Comuna',
        propietario='Test Owner',
        costo_operacional=1000.0,
        ajuste=100.0,
        costo_operacional_base=900.0
    )
    
    central2 = Central(
        nombre='Test Central',  # Same name
        estado='Active',
        potencia=100.0,
        tipo='Hidro',
        region='Test Region',
        comuna='Test Comuna',
        propietario='Test Owner',
        costo_operacional=1000.0,
        ajuste=100.0,
        costo_operacional_base=900.0
    )
    
    # Add first central
    session.add(central1)
    session.commit()
    
    # Try to add second central with same name
    session.add(central2)
    with pytest.raises(Exception):
        session.commit()
    
    # Rollback for next test
    session.rollback()
    
    # Test not null constraint
    central3 = Central(
        nombre=None,  # Missing required field
        estado='Active',
        potencia=100.0,
        tipo='Hidro',
        region='Test Region',
        comuna='Test Comuna',
        propietario='Test Owner',
        costo_operacional=1000.0,
        ajuste=100.0,
        costo_operacional_base=900.0
    )
    
    session.add(central3)
    with pytest.raises(Exception):
        session.commit()

def test_model_defaults(session):
    """Test model default values."""
    # Create test data with minimal required fields
    central = Central(
        nombre='Test Central',
        estado='Active',
        potencia=100.0,
        tipo='Hidro',
        region='Test Region',
        comuna='Test Comuna',
        propietario='Test Owner',
        costo_operacional=1000.0
    )
    
    # Add to session
    session.add(central)
    session.commit()
    
    # Query and verify defaults
    result = session.query(Central).first()
    assert result is not None
    assert result.ajuste == 0.0  # Default value
    assert result.costo_operacional_base == 1000.0  # Same as costo_operacional 