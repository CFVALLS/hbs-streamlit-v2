"""
SQLAlchemy ORM models for database tables.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Text, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
import logging

# Configure logger
logger = logging.getLogger(__name__)

# Try to import MySQL connector, but provide fallback if not available
try:
    import mysql.connector
    from mysql.connector import Error
    MYSQL_AVAILABLE = True
except ImportError:
    logger.warning("MySQL connector not available. Some functionality may be limited.")
    MYSQL_AVAILABLE = False

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, select, MetaData, desc, asc
from sqlalchemy import Column, Integer, String, Boolean, Text, DECIMAL, ForeignKey, DateTime, UniqueConstraint, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import relationship, Session

Base = declarative_base()

class TrackingCoordinador(Base):
    """
    Representa la tabla 'tracking_coordinador' en la base de datos.
    """
    __tablename__ = 'tracking_coordinador'

    id = Column(Integer, primary_key=True)
    timestamp = Column(Text)
    last_modification = Column(Text)
    rio_mod = Column(Boolean)

    def as_dict(self):
        "return a dictionary representation of the object"
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]

class RioRawData(Base):
    """
    Representa la tabla 'rio_raw_data' en la base de datos.
    """
    __tablename__ = 'rio_raw_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(String(10), nullable=True)  # YYYY-MM-DD
    hora = Column(String(8), nullable=True)    # HH:MM:SS
    nombre_configuracion = Column(Text, nullable=True)
    unidad_generadora = Column(Text, nullable=True)
    potencia_maxima = Column(DECIMAL(10, 2), nullable=True)
    potencia_minima = Column(DECIMAL(10, 2), nullable=True)
    potencia_instruida = Column(DECIMAL(10, 2), nullable=True)
    estado_operacional = Column(String(10), nullable=True)
    estado_operacional_combustible = Column(String(20), nullable=True)
    consignas = Column(String(10), nullable=True)
    consigna_limitacion = Column(String(10), nullable=True)
    motivo = Column(Text, nullable=True)
    comentario = Column(Text, nullable=True)
    zona_desacople = Column(Text, nullable=True)
    sentido_flujo = Column(Text, nullable=True)
    estado_de_embalse = Column(Text, nullable=True)
    numero_documento = Column(Text, nullable=True)
    centro_de_control = Column(Text, nullable=True)
    crucero_22o = Column(Text, nullable=True)
    d_almagro_22o = Column(Text, nullable=True)
    cardones_22o = Column(Text, nullable=True)
    p_azucar_22o = Column(Text, nullable=True)
    l_palmas_22o = Column(Text, nullable=True)
    quillota_22o = Column(Text, nullable=True)
    a_jahuel_22o = Column(Text, nullable=True)
    charrua_22o = Column(Text, nullable=True)
    p_montt_22o = Column(Text, nullable=True)
    fecha_edicion_registro = Column(DateTime, nullable=True)  # ISO timestamp
    date_str = Column(String(25), nullable=True)              # '25.03.25 00:00:00'
    motivo_zona = Column(Text, nullable=True)

    def as_list(self):
        "Devuelve la fila como lista"
        return [getattr(self, c.name) for c in self.__table__.columns]  

class CmgTiempoReal(Base):
    """
    Representa la tabla 'cmg_tiempo_real' en la base de datos.

    Atributos:
        id_tracking (int): Es la clave primaria de la tabla.
        barra_transmision (str): Nombre de la barra de transmisión. En MySQL se utiliza 'tinytext' que puede representarse como un String en SQLAlchemy.
        año (int): Representa el año.
        mes (int): Representa el mes.
        dia (int): Representa el día.
        hora (str): Representa la hora. En MySQL se utiliza 'tinytext' que puede representarse como un String en SQLAlchemy.
        unix_time (int): Representa el tiempo unix.
        desacople_bool (bool): Un valor booleano para el desacople.
        cmg (DECIMAL(7,3)): Representa el valor cmg con precisión decimal de 7 dígitos en total, de los cuales 3 son decimales.
        central_referencia (str): Referencia de la central. En MySQL se utiliza 'text' que puede representarse como Text en SQLAlchemy.
    """
    __tablename__ = 'cmg_tiempo_real'

    id_tracking = Column(Integer, primary_key=True, autoincrement=True)
    # tinytext puede ser representado como un String
    barra_transmision = Column(String(255))
    año = Column(Integer)
    mes = Column(Integer)
    dia = Column(Integer)
    # tinytext puede ser representado como un String
    hora = Column(String(255))
    unix_time = Column(Integer)
    desacople_bool = Column(Boolean)
    cmg = Column(DECIMAL(7, 3))
    central_referencia = Column(Text)

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]


class CmgPonderado(Base):
    """
    Representa la tabla 'cmg_ponderado' en la base de datos.
    """
    __tablename__ = 'cmg_ponderado'

    id = Column(Integer, primary_key=True)
    # tinytext puede ser representado como un String
    barra_transmision = Column(String(255))
    # tinytext puede ser representado como un String
    timestamp = Column(String(255))
    unix_time = Column(Integer)
    cmg_ponderado = Column(DECIMAL(7, 4))

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]

class CmgProgramados(Base):
    """
    Representa la tabla 'cmg_programados' en la base de datos.

    Atributos:
        id (int): Es la clave primaria de la tabla.
        central (str): Nombre de la central. En MySQL se utiliza 'tinytext' que puede representarse como un String en SQLAlchemy.
        central_referencia (str): Referencia de la central. En MySQL se utiliza 'tinytext' que puede representarse como un String en SQLAlchemy.
        fecha_programado (str): Fecha programada. En MySQL se utiliza 'text' que puede representarse como un Text en SQLAlchemy.
        '00:00' - '23:00' (float): Valores ponderados para cada hora del día. En MySQL se utiliza 'decimal(7,4)' que puede representarse como DECIMAL(7,4) en SQLAlchemy.
    """
    __tablename__ = 'cmg_programados'

    id = Column(Integer, primary_key=True)
    central = Column(String(255))
    central_referencia = Column(String(255))
    fecha_programado = Column(Text)
    _00_00 = Column(DECIMAL(7, 4), name='00:00')
    _01_00 = Column(DECIMAL(7, 4), name='01:00')
    _02_00 = Column(DECIMAL(7, 4), name='02:00')
    _03_00 = Column(DECIMAL(7, 4), name='03:00')
    _04_00 = Column(DECIMAL(7, 4), name='04:00')
    _05_00 = Column(DECIMAL(7, 4), name='05:00')
    _06_00 = Column(DECIMAL(7, 4), name='06:00')
    _07_00 = Column(DECIMAL(7, 4), name='07:00')
    _08_00 = Column(DECIMAL(7, 4), name='08:00')
    _09_00 = Column(DECIMAL(7, 4), name='09:00')
    _10_00 = Column(DECIMAL(7, 4), name='10:00')
    _11_00 = Column(DECIMAL(7, 4), name='11:00')
    _12_00 = Column(DECIMAL(7, 4), name='12:00')
    _13_00 = Column(DECIMAL(7, 4), name='13:00')
    _14_00 = Column(DECIMAL(7, 4), name='14:00')
    _15_00 = Column(DECIMAL(7, 4), name='15:00')
    _16_00 = Column(DECIMAL(7, 4), name='16:00')
    _17_00 = Column(DECIMAL(7, 4), name='17:00')
    _18_00 = Column(DECIMAL(7, 4), name='18:00')
    _19_00 = Column(DECIMAL(7, 4), name='19:00')
    _20_00 = Column(DECIMAL(7, 4), name='20:00')
    _21_00 = Column(DECIMAL(7, 4), name='21:00')
    _22_00 = Column(DECIMAL(7, 4), name='22:00')
    _23_00 = Column(DECIMAL(7, 4), name='23:00')

############# OBJECTOS CENTRAL #############

class CentralTable(Base):
    __tablename__ = 'central'
    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(255))
    barra_transmision = Column(String(255), nullable=True)  # Added field to link central with transmission bar
    tasa_proveedor = Column(DECIMAL(7, 4))
    porcentaje_brent = Column(DECIMAL(7, 4))
    tasa_central = Column(DECIMAL(7, 4))
    precio_brent = Column(DECIMAL(7, 3))
    margen_garantia = Column(DECIMAL(7, 3))
    factor_motor = Column(DECIMAL(7, 3))
    fecha_registro = Column(Text)
    external_update = Column(Boolean, default=False)
    editor = Column(String(60), nullable=True)


    __table_args__ = {}

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]

class StatusCentral(Base):
    """
    Registra el estado operacional de una central basado en la comparación entre CMG y costo operacional.
    Cada registro está vinculado directamente al cálculo de costo operacional que lo originó.
    
    Attributes:
        id: Identificador único del registro
        central: Nombre de la central
        barra: Barra de transmisión asociada
        timestamp: Marca de tiempo en formato string
        unix_time: Marca de tiempo en formato unix
        cmg_timestamp: Marca de tiempo del CMG (opcional)
        cmg_ponderado: Valor del CMG ponderado
        status_operacional: Estado de operación ('ON', 'OFF', 'HOLD')
        costo_operacional_id: ID del cálculo de costo operacional que originó este estado
    """
    __tablename__ = 'status_central'
    
    id = Column(Integer, primary_key=True)
    central = Column(String(255), nullable=False, index=True)
    barra = Column(String(255), nullable=False)
    timestamp = Column(String(255), nullable=False)
    unix_time = Column(Integer, nullable=False, index=True)
    cmg_timestamp = Column(String(255), nullable=True)  # optional
    cmg_ponderado = Column(DECIMAL(7, 4), nullable=False)
    status_operacional = Column(String(255), nullable=False) # ON, OFF, HOLD
    
    # Direct relationship to the costo operacional that triggered this status
    costo_operacional_id = Column(Integer, ForeignKey("central_costo_operacional.id"), nullable=False)
    costo_operacional_rel = relationship('CentralCostoOperacional', backref='status_decisions')

    __table_args__ = (
        UniqueConstraint('central', 'unix_time', name='uq_central_unix_time'),
        # Add check constraint for status_operacional values
        CheckConstraint(
            "status_operacional IN ('ON', 'OFF', 'HOLD')",
            name='check_status_operacional'
        ),
    )

    def as_list(self):
        """Return a list representation of the object"""
        return [getattr(self, c.name) for c in self.__table__.columns]

class CentralCostoOperacional(Base):
    """
    Historial de costos operacionales calculados para las centrales.
    Cada registro vincula un costo operacional con la configuración de central utilizada.
    """
    __tablename__ = 'central_costo_operacional'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    central_id = Column(Integer, ForeignKey("central.id"), nullable=False)
    central_nombre = Column(String(255))
    timestamp = Column(String(255))
    unix_time = Column(Integer)
    costo_operacional = Column(DECIMAL(7, 3))
    editor = Column(String(60), nullable=True)

    # Simplify relationship
    central = relationship('CentralTable', backref='costos_historicos')

    __table_args__ = (
        UniqueConstraint('central_id', 'unix_time', name='uq_central_id_unix_time'),
    )

    def as_list(self):
        """Return a list representation of the object"""
        return [getattr(self, c.name) for c in self.__table__.columns]

############# OBJECTOS EMAIL - COMUNICACION #############
    
class TipoEmail(Base):
    """
    Representa la tabla 'tipo_email' en la base de datos.
    """
    __tablename__ = 'tipo_email'

    id_tipo_email = Column(Integer, primary_key=True)
    tipo_email_desc = Column(String(255), nullable=False)


class TrackingEmail(Base):
    """
    Representa la tabla 'tracking_email' en la base de datos.
    """
    __tablename__ = 'tracking_email'

    id = Column(Integer, primary_key=True)
    tipo_email_id = Column(Integer, ForeignKey(
        'tipo_email.id_tipo_email'), nullable=False)
    destinatario = Column(String(255), nullable=False)
    timestamp_envio = Column(String(255), nullable=False)
    unixtime_envio = Column(Integer, nullable=False)

    # Define a relationship to the TipoEmail table
    tipo_email = relationship('TipoEmail', backref='tracking_emails')

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]


############# OBJECTOS TRACKING - DESACOPLE #############

class TrackingDesacople(Base):
    """
    Representa la tabla 'tracking_desacople' en la base de datos.

    Atributos:
        id (int): Es la clave primaria de la tabla con auto-incremento.
        central (str): Nombre de la central. En MySQL se utiliza 'tinytext' que puede representarse como un String en SQLAlchemy.
        zona_en_desacople (bool): Un valor booleano para indicar el estado de la zona de desacople.
        timestamp_mov_zona_desacople (str): Representa el timestamp del cambio de estado de la zona de desacople. En MySQL se utiliza 'tinytext' que puede representarse como un String en SQLAlchemy.
    """
    __tablename__ = 'tracking_desacople'

    id = Column(Integer, primary_key=True, autoincrement=True)
    central = Column(String(40), nullable=False)
    zona_en_desacople = Column(Boolean, nullable=False)
    timestamp_mov_zona_desacople = Column(String(17))

    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]
    
class FactorPenalizacion(Base):
    __tablename__ = 'factor_penalizacion'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(String(40), nullable=False)
    barra = Column(String(150), nullable=False)
    hora = Column(Integer, nullable=False)
    penalizacion = Column(DECIMAL(7, 6))
    
    def as_list(self):
        "return a list representation of the object"
        return [getattr(self, c.name) for c in self.__table__.columns]

class TrackingTco(Base):
   __tablename__ = 'tracking_tco'

   id = Column(Integer, primary_key=True, autoincrement=True)
   fecha = Column(String(17), nullable=False)
   central = Column(String(40), nullable=False) 
   costo_marginal = Column(DECIMAL(7, 4))
   bloque_horario = Column(String(1))
   
   def as_list(self):
       return [getattr(self, c.name) for c in self.__table__.columns]
