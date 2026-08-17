"""
Módulo para operaciones de base de datos incluyendo inserciones y consultas a las tablas del sistema.
"""
# general modules
import logging
from datetime import datetime

import pandas as pd

# mysql
from sqlalchemy import create_engine, desc, func, inspect, or_
from sqlalchemy.orm import joinedload, sessionmaker, Session
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.exc import NoResultFound

# import models
from .models_orm import *
from .db_utils import safe_bool_convert, safe_float_convert
from scripts.utils.utils import setup_logging, get_timestamp_from_unix_time, get_date, get_logger

#########################################################################
##############                Classes                 ###################
#########################################################################

# Configurar logger
logger = get_logger('operaciones_db', logging.INFO, 'operaciones_db.log')

Base = declarative_base()

def inject_tracking_coordinador(session, timestamp, last_modification, rio_mod):
    """
    Inserta un nuevo registro en la tabla tracking_coordinador.
    
    Args:
        session: Sesión SQLAlchemy activa
        timestamp: Timestamp del registro
        last_modification: Última modificación
        rio_mod: Indicador de modificación RIO
        
    Returns:
        El objeto TrackingCoordinador insertado
    """
    tracking = TrackingCoordinador(
        timestamp=timestamp,
        last_modification=last_modification,
        rio_mod=rio_mod
    )
    session.add(tracking)
    session.commit()
    return tracking

########## FUNCTIONS FOR ETL RIO ##########
def retrieve_tracking_coordinador(session, id=None, limit=None):
    """
    Recupera registros de la tabla tracking_coordinador.
    
    Args:
        session: Sesión SQLAlchemy activa
        id: ID específico a recuperar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        El último objeto TrackingCoordinador o un objeto específico si se proporciona id
    """
    try:
        query = session.query(TrackingCoordinador)

        if id is not None:
            query = query.filter(TrackingCoordinador.id == id)

        query = query.order_by(desc(TrackingCoordinador.id))

        if limit is not None:
            query = query.limit(limit)

        result = query.first()

        if result:
            return [
                result.id,
                result.timestamp,
                result.last_modification,
                result.rio_mod,
            ]

        return [None, None, None, None]
    except Exception as e:
        logger.error(f"Error retrieving tracking coordinador: {e}")
        raise

def retrieve_ultima_modificacion_rio_file(session, fecha=None, hora=None):
    """
    Recupera la última modificación del archivo RIO.
    
    Args:
        session (Session): Sesión de SQLAlchemy
        fecha (str, optional): Fecha en formato YYYY-MM-DD
        hora (int, optional): Hora
        
    Returns:
        tuple: (last_modification, rio_mod) or (None, None) if not found
    """
    try:
        # Get the most recent record, ordered by id descending
        tracking = session.query(TrackingCoordinador).order_by(
            TrackingCoordinador.id.desc()
        ).first()
        
        if tracking:
            last_mod = tracking.last_modification if hasattr(tracking, 'last_modification') else None
            rio_mod = tracking.rio_mod if hasattr(tracking, 'rio_mod') else None
            logger.info(f"Found tracking record: id={tracking.id}, last_mod={last_mod}, rio_mod={rio_mod}")
            return last_mod, rio_mod
        else:
            logger.warning("No se encontró registro de última modificación del archivo RIO")
            return None, None
            
    except Exception as e:
        logger.error(f"Error al recuperar última modificación del archivo RIO: {e}")
        return None, None 

def inject_rio_raw_data(session, **kwargs):
    """
    Inserta un nuevo registro en la tabla rio_raw_data.
    
    Args:
        session: Sesión SQLAlchemy activa
        **kwargs: Argumentos con nombres correspondientes a las columnas de la tabla
        
    Returns:
        El objeto RioRawData insertado
    """
    rio_data = RioRawData(**kwargs)
    session.add(rio_data)
    session.commit()
    return rio_data

def retrieve_rio_raw_data(session, id=None, fecha=None, unidad_generadora=None, limit=None):
    """
    Recupera registros de la tabla rio_raw_data.
    
    Args:
        session: Sesión SQLAlchemy activa
        id: ID mínimo para filtrar (recupera registros con ID > id) (opcional)
        fecha: Fecha para filtrar (opcional)
        unidad_generadora: Unidad generadora para filtrar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos RioRawData
    """
    query = session.query(RioRawData)
    
    if id is not None:
        query = query.filter(RioRawData.id > id)
    
    if fecha is not None:
        query = query.filter(RioRawData.fecha == fecha)
        
    if unidad_generadora is not None:
        query = query.filter(RioRawData.unidad_generadora == unidad_generadora)
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()

def retrieve_last_entry_from_rio_raw_data(session, limit=1):
    """
    Recupera el último registro de la tabla rio_raw_data para una barra de transmisión específica.
    
    Args:
        session: Sesión SQLAlchemy activa
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        dict: Diccionario con los datos del último registro o None si no hay registros
    """
    query = session.query(RioRawData)
    query = query.order_by(RioRawData.id.desc())
    if limit is not None:
        query = query.limit(limit)
    result = query.first()
    
    if result:
        # Convert the model to a dictionary
        return {c.name: getattr(result, c.name) for c in result.__table__.columns}
    return None


########## FUNCTIONS FOR DESACOPLE HISTORY ##########

def _normalize_desacople_timestamp(timestamp):
    """
    Normaliza timestamps usados en consultas de desacople.
    """
    if timestamp is None:
        return None

    if isinstance(timestamp, datetime):
        return timestamp.replace(tzinfo=None) if timestamp.tzinfo else timestamp

    timestamp_str = str(timestamp).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%d.%m.%y %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(timestamp_str)
    except ValueError:
        logger.warning(f"No se pudo normalizar timestamp de desacople: {timestamp}")
        return None


def normalize_barra(barra: str) -> str:
    """Normaliza nombres de barras para cubrir variantes históricas."""
    if not barra:
        return barra

    barra_upper = barra.upper()
    return {
        'CHARRUA_22O': 'CHARRUA__220',
        'QUILLOTA_22O': 'QUILLOTA__220',
        'CHARRUA_220': 'CHARRUA__220',
        'QUILLOTA_220': 'QUILLOTA__220',
        'CHARRUA': 'CHARRUA__220',
        'QUILLOTA': 'QUILLOTA__220',
    }.get(barra_upper, barra_upper)


def _barra_variants(barra_transmision: str):
    """Retorna variantes conocidas de una barra para consultas tolerantes."""
    barra_normalizada = normalize_barra(barra_transmision)
    variants_map = {
        'CHARRUA__220': {'CHARRUA__220', 'CHARRUA_22O', 'CHARRUA_220', 'CHARRUA'},
        'QUILLOTA__220': {'QUILLOTA__220', 'QUILLOTA_22O', 'QUILLOTA_220', 'QUILLOTA'},
    }

    variants = set()
    if barra_transmision:
        variants.add(barra_transmision.upper())
    if barra_normalizada:
        variants.add(barra_normalizada.upper())
        variants.update(variants_map.get(barra_normalizada, set()))
    return variants


def retrieve_latest_desacople_event(session, barra_transmision=None, timestamp=None, include_equal=True):
    """
    Recupera el evento más reciente de desacople_history para una barra.
    """
    try:
        inspector = inspect(session.bind)
        if DesacopleHistory.__tablename__ not in inspector.get_table_names():
            raise RuntimeError(
                f"La tabla requerida {DesacopleHistory.__tablename__} no existe"
            )

        query = session.query(DesacopleHistory)
        if barra_transmision is not None:
            variants = _barra_variants(barra_transmision)
            if variants:
                query = query.filter(func.upper(DesacopleHistory.barra_transmision).in_(list(variants)))

        timestamp_norm = _normalize_desacople_timestamp(timestamp)
        if timestamp_norm is not None:
            if include_equal:
                query = query.filter(DesacopleHistory.detected_at <= timestamp_norm)
            else:
                query = query.filter(DesacopleHistory.detected_at < timestamp_norm)

        return query.order_by(DesacopleHistory.detected_at.desc(), DesacopleHistory.id.desc()).first()
    except Exception as e:
        logger.error(f"Error en retrieve_latest_desacople_event: {e}")
        raise


def was_notification_sent(session, canal, destino, plantilla, timestamp_envio, contenido=None):
    """
    Verifica si ya existe un envío exitoso para la misma notificación.
    """
    try:
        inspector = inspect(session.bind)
        if 'tracking_comunicacion' not in inspector.get_table_names():
            logger.warning("La tabla tracking_comunicacion no existe. No se puede validar idempotencia de notificaciones.")
            return False

        query = session.query(TrackingComunicacion).filter(
            TrackingComunicacion.canal == canal,
            TrackingComunicacion.destino == destino,
            TrackingComunicacion.plantilla == plantilla,
            TrackingComunicacion.timestamp_envio == timestamp_envio,
            TrackingComunicacion.exito.is_(True),
        )
        if contenido is not None:
            query = query.filter(TrackingComunicacion.contenido == contenido)

        return query.first() is not None
    except Exception as e:
        logger.error(f"Error en was_notification_sent: {e}")
        return False


def sync_tracking_desacople_from_history(session, barra_transmision):
    """
    Sincroniza tracking_desacople con el último evento vigente en desacople_history.
    """
    try:
        inspector = inspect(session.bind)
        if 'tracking_desacople' not in inspector.get_table_names():
            logger.warning("La tabla tracking_desacople no existe. Se omite sincronización.")
            return None

        ultimo_evento = retrieve_latest_desacople_event(session, barra_transmision=barra_transmision)
        if ultimo_evento is None:
            return None

        ultimo_tracking = (
            session.query(TrackingDesacople)
            .filter(TrackingDesacople.barra_transmision == barra_transmision)
            .order_by(TrackingDesacople.id.desc())
            .first()
        )

        timestamp_str = ultimo_evento.detected_at.strftime('%Y-%m-%d %H:%M:%S')
        zona_en_desacople = ultimo_evento.estado == 'desacople'

        if (
            ultimo_tracking
            and ultimo_tracking.zona_en_desacople == zona_en_desacople
            and ultimo_tracking.tramo_desacople == ultimo_evento.tramo
            and ultimo_tracking.timestamp_mov_zona_desacople == timestamp_str
        ):
            return ultimo_tracking

        tracking_desacople = TrackingDesacople(
            barra_transmision=barra_transmision,
            zona_en_desacople=zona_en_desacople,
            timestamp_mov_zona_desacople=timestamp_str,
            tramo_desacople=ultimo_evento.tramo,
        )
        session.add(tracking_desacople)
        session.flush()
        return tracking_desacople
    except Exception as e:
        logger.error(f"Error en sync_tracking_desacople_from_history: {e}")
        raise

def upsert_desacople_history(session, barra_transmision: str, estado: str, detected_at: datetime,
                             tramo: str = None, comentario: str = None, fuente: str = None):
    """
    Inserta un evento en desacople_history si representa una transición nueva.
    """
    try:
        detected_at = _normalize_desacople_timestamp(detected_at)
        if detected_at is None:
            raise ValueError("detected_at inválido para desacople_history")

        existente = session.query(DesacopleHistory).filter(
            DesacopleHistory.barra_transmision == barra_transmision,
            DesacopleHistory.estado == estado,
            DesacopleHistory.detected_at == detected_at,
            DesacopleHistory.tramo == tramo,
        ).first()
        if existente:
            logger.info(f"Evento exacto ya existe para {barra_transmision} en {detected_at}")
            return existente

        evento_previo = retrieve_latest_desacople_event(
            session,
            barra_transmision=barra_transmision,
            timestamp=detected_at,
            include_equal=False,
        )
        if evento_previo and evento_previo.estado == estado:
            logger.info(
                f"Se omite evento redundante de {barra_transmision}: {estado} en {detected_at}"
            )
            return evento_previo

        nuevo = DesacopleHistory(
            barra_transmision=barra_transmision,
            estado=estado,
            detected_at=detected_at,
            tramo=tramo,
            comentario=comentario,
            fuente=fuente
        )
        session.add(nuevo)
        sync_tracking_desacople_from_history(session, barra_transmision)
        session.commit()
        logger.info(f"Insertado evento de {barra_transmision} -> {estado} en {detected_at}")
        return nuevo
    except Exception as e:
        session.rollback()
        logger.error(f"Error en upsert_desacople_history para {barra_transmision}: {e}")
        raise

########## FUNCTIONS FOR ETL CMG ##########
def inject_cmg_tiempo_real(session, barra_transmision, año, mes, dia, hora, unix_time, desacople_bool, cmg, central_referencia):
    """
    Inserta un nuevo registro en la tabla cmg_tiempo_real.
    
    Args:
        session: Sesión SQLAlchemy activa
        barra_transmision: Nombre de la barra de transmisión
        año: Año del registro
        mes: Mes del registro
        dia: Día del registro
        hora: Hora del registro
        unix_time: Tiempo unix
        desacople_bool: Indicador de desacople
        cmg: Valor del CMG
        central_referencia: Referencia de la central
        
    Returns:
        El objeto CmgTiempoReal insertado
    """
    cmg_tr = CmgTiempoReal(
        barra_transmision=barra_transmision,
        año=año,
        mes=mes,
        dia=dia,
        hora=hora,
        unix_time=unix_time,
        desacople_bool=desacople_bool,
        cmg=cmg,
        central_referencia=central_referencia
    )
    session.add(cmg_tr)
    session.commit()
    return cmg_tr


def retrieve_cmg_tiempo_real(session, id_tracking=None, barra=None, fecha=None, limit=None):
    """
    Recupera registros de la tabla cmg_tiempo_real.
    
    Args:
        session: Sesión SQLAlchemy activa
        id_tracking: ID específico a recuperar (opcional)
        barra: Barra de transmisión para filtrar (opcional)
        fecha: Diccionario con año, mes, dia para filtrar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos CmgTiempoReal o un solo objeto si se especifica id_tracking
    """
    query = session.query(CmgTiempoReal)
    
    if id_tracking is not None:
        return query.filter(CmgTiempoReal.id_tracking == id_tracking).first()
    
    if barra is not None:
        query = query.filter(CmgTiempoReal.barra_transmision == barra)
        
    if fecha is not None:
        if 'año' in fecha:
            query = query.filter(CmgTiempoReal.año == fecha['año'])
        if 'mes' in fecha:
            query = query.filter(CmgTiempoReal.mes == fecha['mes'])
        if 'dia' in fecha:
            query = query.filter(CmgTiempoReal.dia == fecha['dia'])
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()

def query_ultimo_cmg_antes_de_unixtime(session_in: Session, unixtime: int, barra_transmision: str):
    """
    Recupera la entrada más reciente de CMG antes del tiempo Unix especificado para una barra de transmisión.

    Parámetros:
    - session_in (Session): La sesión para realizar consultas en la base de datos.
    - unixtime (int): El tiempo Unix que se utiliza como referencia para la consulta.
    - barra_transmision (str): La barra de transmisión para buscar en la tabla "cmg_tiempo_real".

    Devuelve:
    - Tuple[str, bool, Decimal]: Una tupla que contiene la referencia de la central, si está afectada por el desacople,
      y el valor del CMG, o None si no se encuentra ningún resultado.
    """
    try:
        resultado = (session_in.query(CmgTiempoReal.central_referencia,
                                      CmgTiempoReal.desacople_bool,
                                      CmgTiempoReal.cmg)
                     .filter(CmgTiempoReal.barra_transmision == barra_transmision,
                             CmgTiempoReal.unix_time < unixtime)
                     .order_by(desc(CmgTiempoReal.unix_time))
                     .limit(1)
                     .one_or_none())

        if resultado:
            central_referencia, afecto_desacople, cmg = resultado
            return central_referencia, afecto_desacople, cmg

        return None

    except Exception as excepcion:
        logger.error(f"Error al recuperar el último CMG antes del tiempo Unix {unixtime} para {barra_transmision}: {excepcion}")
        return None


def get_cmg_tiempo_real_by_interval(session_in, unixtime_start, unixtime_end, barra_transmision_in):
    """
    Recupera registros de CMG en tiempo real dentro de un intervalo de tiempo para una barra de transmisión específica.
    
    Args:
        session_in: Sesión SQLAlchemy activa
        unixtime_start: Tiempo unix de inicio del intervalo
        unixtime_end: Tiempo unix de fin del intervalo
        barra_transmision_in: Barra de transmisión a consultar
        
    Returns:
        Lista de objetos CmgTiempoReal que cumplen con los criterios
    """
    try:
        return session_in.query(CmgTiempoReal).filter(
                CmgTiempoReal.unix_time >= unixtime_start,
                CmgTiempoReal.unix_time <= unixtime_end,
                CmgTiempoReal.barra_transmision == barra_transmision_in
            ).all()
    
    except Exception as excepcion:
        logger.error(f"Error al recuperar los registros de CMG en tiempo real dentro de un intervalo de tiempo para {barra_transmision_in}: {excepcion}")
        return None

def insert_or_replace_row_cmg_ponderado(session, barra_transmision,  cmg_ponderado, timestamp = None, unix_time = None):
    """
    Inserta una fila en la tabla cmg_ponderado si la fila no existe, o reemplaza una fila existente con los mismos
    valores de central y unix_time.

    Args:
        session (sqlalchemy.orm.Session): SQLAlchemy Session object.
        barra_transmision (str): Nombre de la barra de transmision para la que se desea obtener la información.
        cmg_ponderado (float): El cmg ponderado que se desea insertar en la tabla 'cmg_ponderado'.
        timestamp (str): El timestamp que se desea buscar en la tabla 'cmg_ponderado'.
        unix_time (int): El tiempo unix que se desea buscar en la tabla 'cmg_tiempo_real'.

    Raises:
        TypeError: Si alguno de los argumentos no es del tipo esperado.
        ValueError: Si alguno de los argumentos no tiene el valor esperado.
    """
    if timestamp is None:
        timestamp = get_timestamp_from_unix_time(float(unix_time))

    try:
        try:
            # Try to get the existing row
            existing_row = session.query(CmgPonderado).filter_by(barra_transmision=barra_transmision, unix_time=unix_time).one()
            # Update the row
            existing_row.timestamp = timestamp
            existing_row.cmg_ponderado = cmg_ponderado
        except NoResultFound:
            # The row does not exist, insert a new row
            new_row = CmgPonderado(barra_transmision=barra_transmision,
                                   timestamp=timestamp, unix_time=unix_time, cmg_ponderado=cmg_ponderado)
            session.add(new_row)

    except TypeError as typee:
        logger.error(f"Invalid argument types: {typee}")
        session.rollback()

    except ValueError as valuee:
        logger.error(f"Invalid argument values: {valuee}")
        session.rollback()

    except Exception as othererror:
        logger.error(f"Error while inserting row into table: {othererror}")
        session.rollback()

def inject_cmg_ponderado(session, barra_transmision, timestamp, unix_time, cmg_ponderado):
    """
    Inserta un nuevo registro en la tabla cmg_ponderado.
    
    Args:
        session: Sesión SQLAlchemy activa
        barra_transmision: Nombre de la barra de transmisión
        timestamp: Timestamp del registro
        unix_time: Tiempo unix
        cmg_ponderado: Valor del CMG ponderado
        
    Returns:
        El objeto CmgPonderado insertado
    """
    cmg_pond = CmgPonderado(
        barra_transmision=barra_transmision,
        timestamp=timestamp,
        unix_time=unix_time,
        cmg_ponderado=cmg_ponderado
    )
    session.add(cmg_pond)
    session.commit()
    return cmg_pond


def retrieve_cmg_ponderado(session, id=None, barra=None, timestamp=None, limit=None):
    """
    Recupera registros de la tabla cmg_ponderado.
    
    Args:
        session: Sesión SQLAlchemy activa
        id: ID específico a recuperar (opcional)
        barra: Barra de transmisión para filtrar (opcional)
        timestamp: Timestamp para filtrar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos CmgPonderado o un solo objeto si se especifica id
    """
    query = session.query(CmgPonderado)
    
    if id is not None:
        return query.filter(CmgPonderado.id == id).first()
    
    if barra is not None:
        query = query.filter(CmgPonderado.barra_transmision == barra)
        
    if timestamp is not None:
        query = query.filter(CmgPonderado.timestamp == timestamp)
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()
    
def inject_cmg_programados(session, central, central_referencia, fecha_programado, valores_horarios):
    """
    Inserta un nuevo registro en la tabla cmg_programados.
    
    Args:
        session: Sesión SQLAlchemy activa
        central: Nombre de la central
        central_referencia: Referencia de la central
        fecha_programado: Fecha programada
        valores_horarios: Diccionario con valores para cada hora (00:00 a 23:00)
        
    Returns:
        El objeto CmgProgramados insertado
    """
    cmg_prog = CmgProgramados(
        central=central,
        central_referencia=central_referencia,
        fecha_programado=fecha_programado
    )
    
    # Asignar valores horarios
    for hora, valor in valores_horarios.items():
        hora_formateada = hora.replace(':', '_')
        setattr(cmg_prog, f"_{hora_formateada}", valor)
    
    session.add(cmg_prog)
    session.commit()
    return cmg_prog

def insert_row_cmg_programados(session_in, row_in):
    """
    Inserta una fila en la tabla cmg_programados.
    
    Args:
        session_in: Sesión SQLAlchemy
        row_in: Lista con los datos a insertar
        
    Returns:
        int: ID de la fila insertada
    """
    try:
        # Create a new CmgProgramados instance
        cmg_programado = CmgProgramados(
            central=row_in[0],
            central_referencia=row_in[1],
            fecha_programado=row_in[2],
            _00_00=row_in[3],
            _01_00=row_in[4],
            _02_00=row_in[5],
            _03_00=row_in[6],
            _04_00=row_in[7],
            _05_00=row_in[8],
            _06_00=row_in[9],
            _07_00=row_in[10],
            _08_00=row_in[11],
            _09_00=row_in[12],
            _10_00=row_in[13],
            _11_00=row_in[14],
            _12_00=row_in[15],
            _13_00=row_in[16],
            _14_00=row_in[17],
            _15_00=row_in[18],
            _16_00=row_in[19],
            _17_00=row_in[20],
            _18_00=row_in[21],
            _19_00=row_in[22],
            _20_00=row_in[23],
            _21_00=row_in[24],
            _22_00=row_in[25],
            _23_00=row_in[26]
        )
        
        session_in.add(cmg_programado)
        session_in.commit()
        return cmg_programado.id
        
    except Exception as e:
        session_in.rollback()
        logger.error(f"Error inserting row into cmg_programados: {e}")
        return None


def retrieve_cmg_programados(session, id=None, central=None, fecha=None, limit=None):
    """
    Recupera registros de la tabla cmg_programados.
    
    Args:
        session: Sesión SQLAlchemy activa
        id: ID específico a recuperar (opcional)
        central: Central para filtrar (opcional)
        fecha: Fecha programada para filtrar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos CmgProgramados o un solo objeto si se especifica id
    """
    query = session.query(CmgProgramados)
    
    if id is not None:
        return query.filter(CmgProgramados.id == id).first()
    
    if central is not None:
        query = query.filter(CmgProgramados.central == central)
        
    if fecha is not None:
        query = query.filter(CmgProgramados.fecha_programado == fecha)
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()

########## FUNCTIONS FOR ETL CENTRAL ##########
def inject_central(session, **kwargs):
    """
    Inserta un nuevo registro en la tabla central.
    
    Args:
        session: Sesión SQLAlchemy activa
        **kwargs: Argumentos con nombres correspondientes a las columnas de la tabla
        
    Returns:
        El objeto CentralTable insertado
    """
    central = CentralTable(**kwargs)
    session.add(central)
    session.commit()
    return central


def retrieve_central(session, id=None, nombre=None, barra_transmision=None, limit=None):
    """
    Recupera registros de la tabla central.
    
    Args:
        session: Sesión SQLAlchemy activa
        id: ID específico a recuperar (opcional)
        nombre: Nombre de la central para filtrar (opcional)
        barra_transmision: Barra de transmisión para filtrar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos CentralTable o un solo objeto si se especifica id
    """
    query = session.query(CentralTable)
    
    if id is not None:
        return query.filter(CentralTable.id == id).first()
    
    if nombre is not None:
        query = query.filter(CentralTable.nombre == nombre)
    
    if barra_transmision is not None:
        query = query.filter(CentralTable.barra_transmision == barra_transmision)
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()

########## FUNCTIONS FOR ETL EMAIL ##########
def inject_tipo_email(session, tipo_email_desc):
    """
    Inserta un nuevo registro en la tabla tipo_email.
    
    Args:
        session: Sesión SQLAlchemy activa
        tipo_email_desc: Descripción del tipo de email
        
    Returns:
        El objeto TipoEmail insertado
    """
    tipo_email = TipoEmail(tipo_email_desc=tipo_email_desc)
    session.add(tipo_email)
    session.commit()
    return tipo_email

def retrieve_tipo_email(session, id_tipo_email=None, limit=None):
    """
    Recupera registros de la tabla tipo_email.
    
    Args:
        session: Sesión SQLAlchemy activa
        id_tipo_email: ID específico a recuperar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos TipoEmail o un solo objeto si se especifica id_tipo_email
    """
    query = session.query(TipoEmail)
    
    if id_tipo_email is not None:
        return query.filter(TipoEmail.id_tipo_email == id_tipo_email).first()
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()

def inject_tracking_email(session, tipo_email_id, destinatario, timestamp_envio, unixtime_envio):
    """
    Inserta un nuevo registro en la tabla tracking_email.
    
    Args:
        session: Sesión SQLAlchemy activa
        tipo_email_id: ID del tipo de email
        destinatario: Destinatario del email
        timestamp_envio: Timestamp de envío
        unixtime_envio: Tiempo unix de envío
        
    Returns:
        El objeto TrackingEmail insertado
    """
    tracking_email = TrackingEmail(
        tipo_email_id=tipo_email_id,
        destinatario=destinatario,
        timestamp_envio=timestamp_envio,
        unixtime_envio=unixtime_envio
    )
    session.add(tracking_email)
    session.commit()
    return tracking_email


def retrieve_tracking_email(session, id=None, tipo_email_id=None, destinatario=None, limit=None):
    """
    Recupera registros de la tabla tracking_email.
    
    Args:
        session: Sesión SQLAlchemy activa
        id: ID específico a recuperar (opcional)
        tipo_email_id: ID del tipo de email para filtrar (opcional)
        destinatario: Destinatario para filtrar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos TrackingEmail o un solo objeto si se especifica id
    """
    query = session.query(TrackingEmail)
    
    if id is not None:
        return query.filter(TrackingEmail.id == id).first()
    
    if tipo_email_id is not None:
        query = query.filter(TrackingEmail.tipo_email_id == tipo_email_id)
        
    if destinatario is not None:
        query = query.filter(TrackingEmail.destinatario == destinatario)
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()
############## FUNCTIONS FOR ETL TRACKING DESACOPLE ##########

def inject_tracking_desacople(session, barra_transmision, zona_en_desacople, timestamp_mov_zona_desacople, tramo_desacople):
    """
    Inserta un nuevo registro en la tabla tracking_desacople.
    
    Args:
        session: Sesión SQLAlchemy activa
        barra_transmision: Nombre de la barra de transmisión
        zona_en_desacople: Estado de la zona de desacople
        tramo_desacople: Tramo de desacople
        timestamp_mov_zona_desacople: Timestamp del movimiento de zona de desacople
        
    Returns:
        El objeto TrackingDesacople insertado
    """
    tracking_desacople = TrackingDesacople(
        barra_transmision=barra_transmision,
        zona_en_desacople=zona_en_desacople,
        timestamp_mov_zona_desacople=timestamp_mov_zona_desacople,
        tramo_desacople=tramo_desacople
    )
    session.add(tracking_desacople)
    session.commit()
    return tracking_desacople

def retrieve_status_desacople(session, barra_transmision_in=None, timestamp=None):
    """
    Recupera el estado de desacople para una barra de transmisión específica.

    Args:
        session: Sesión SQLAlchemy activa
        barra_transmision_in: Nombre de la barra (opcional)
        timestamp: Timestamp específico para filtrar (opcional)

    Returns:
        tuple: (bool, str) si hay desacople, False en caso contrario
    """

    try:
        ultimo_evento = retrieve_latest_desacople_event(
            session,
            barra_transmision=barra_transmision_in,
            timestamp=timestamp,
        )
        if ultimo_evento:
            return ultimo_evento.estado == 'desacople', ultimo_evento.tramo

        logger.info(
            f"Sin evento histórico en {DesacopleHistory.__tablename__} para barra {barra_transmision_in}"
            + (f" al timestamp {timestamp}" if timestamp is not None else "")
        )
        return None, None

    except Exception as e:
        logger.error(f"Error en retrieve_status_desacople: {e}")
        raise
    
########### Funciones Factor Penalizacion y TCO ###################

def retrieve_latest_cmg_ponderado(session, barra, timestamp):
    """
    Devuelve el registro más reciente de CMG para una barra y timestamp.
    """
    return (session.query(CmgPonderado)
            .filter(CmgPonderado.barra_transmision == barra)
            .filter(CmgPonderado.timestamp <= timestamp)
            .order_by(CmgPonderado.unix_time.desc())
            .first())


def insert_factor_penalizacion(session, fecha, barra, hora, penalizacion):
    """
    Inserta un nuevo registro en la tabla factor_penalizacion.
    
    Args:
        session: Sesión SQLAlchemy activa
        fecha: Fecha del factor de penalización
        barra: Nombre de la barra
        hora: Hora del factor de penalización
        penalizacion: Valor de la penalización
        
    Returns:
        El objeto FactorPenalizacion insertado
    """
    factor_penalizacion = FactorPenalizacion(
        fecha=fecha,
        barra=barra,
        hora=hora,
        penalizacion=penalizacion
    )
    session.add(factor_penalizacion)
    session.commit()
    return factor_penalizacion

def query_date_factor_penalizacion(date_str, session):
    """
    Checks if any factor_penalizacion entries exist for the given date.

    Args:
        date_str (str): Date in 'dd.mm.yy' format
        session: SQLAlchemy session

    Returns:
        bool: True if at least one row exists, False otherwise
    """
    return session.query(FactorPenalizacion).filter(
        FactorPenalizacion.fecha == date_str
    ).count() > 0


def retrieve_factor_penalizacion(session, id=None, fecha=None, barra=None, hora=None, limit=None):
    """
    Recupera registros de la tabla factor_penalizacion.
    
    Args:
        session: Sesión SQLAlchemy activa
        id: ID específico a recuperar (opcional)
        fecha: Fecha para filtrar (opcional)
        barra: Barra para filtrar (opcional)
        hora: Hora para filtrar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos FactorPenalizacion o un solo objeto si se especifica id
    """
    query = session.query(FactorPenalizacion)
    
    if id is not None:
        return query.filter(FactorPenalizacion.id == id).first()
    
    if fecha is not None:
        query = query.filter(FactorPenalizacion.fecha == fecha)
        
    if barra is not None:
        query = query.filter(FactorPenalizacion.barra == barra)
        
    if hora is not None:
        query = query.filter(FactorPenalizacion.hora == hora)
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()

def query_valor_factor_penalizacion(barra_in, session_in, date_in = None, request_timestamp=None, date_format='%d.%m.%y', time_format='%H:%M:%S'):
    """
    Consulta en la base de datos para verificar la existencia de un valor de factor de penalización.

    Args:
        date_in (datetime): Objeto datetime que contiene la fecha y hora de la consulta.
        barra_in (str): El nombre de la barra de transmisión para la cual se busca el factor de penalización.
        session_in (Session): Una sesión activa de SQLAlchemy para realizar la consulta en la base de datos.

    Returns:
        factor Penalizacion: True si el valor de factor de penalización existe, False de lo contrario.

    Raises:
        SQLAlchemyError: Si ocurre un error durante la consulta a la base de datos.
    """
        # Diccionario para mapear la entrada a los valores esperados en la base de datos.
    barra_transmision_select = {'QUILLOTA_22O': 'LVegas110',
                                'CHARRUA_22O': 'Charrua066',
                                'quillota_22o': 'LVegas110',
                                'charrua_22o': 'Charrua066'}
    # Convertir a mayúsculas para hacer la búsqueda insensible a mayúsculas/minúsculas
    barra_upper = barra_in.upper() if barra_in else None
    request_barra = barra_transmision_select.get(barra_upper)
         
    if date_in is None:
        date_in = get_date()
   
    fecha = date_in.strftime(date_format)
    hora = int(date_in.hour) + 1


    if not request_barra:
        logger.error(f"No se encontró la barra {barra_in} en el diccionario de transmisión.")
        return False

    # Ajustar la hora según request_timestamp si está presente
    if request_timestamp:
        if len(request_timestamp) > 8:  # Formato completo con fecha y hora
            date_part, time_part = request_timestamp.split()
            hora = int(time_part.split(':')[0]) + 1
        else:  # Solo hora
            hora = int(request_timestamp.split(':')[0]) + 1
    
    # Realizar la consulta en la base de datos.
    try:
        valor = session_in.query(FactorPenalizacion.penalizacion).filter(
            FactorPenalizacion.fecha == fecha,
            FactorPenalizacion.hora == hora,
            FactorPenalizacion.barra == request_barra
        ).scalar()
        
        if valor is None:
            logging.error(f'Valor de factor de penalizacion no encontrado para la fecha {fecha} y hora {hora}')
            return 0
        else:
            return valor
    
    except Exception as e:
        logger.error(f"Error en la consulta a la base de datos: {e}")
        return False


def insert_tracking_tco(session, fecha, central, costo_marginal, bloque_horario):
    """
    Inserta un nuevo registro en la tabla tracking_tco.
    
    Args:
        session: Sesión SQLAlchemy activa
        fecha: Fecha del registro
        central: Nombre de la central
        costo_marginal: Costo marginal
        bloque_horario: Bloque horario
        
    Returns:
        El objeto TrackingTco insertado
    """
    tracking_tco = TrackingTco(
        fecha=fecha,
        central=central,
        costo_marginal=costo_marginal,
        bloque_horario=bloque_horario
    )
    session.add(tracking_tco)
    session.commit()
    return tracking_tco

def retrieve_costo_marginal_tco(bloque, central, session_in, date_in=None):
    """
    Recupera el costo marginal de TCO para una central y bloque específicos.
    
    Args:
        bloque: Bloque horario ('A', 'B' o 'C')
        central: Nombre de la central
        session_in: Sesión SQLAlchemy activa
        date_in: Fecha específica (opcional, por defecto usa la fecha actual)
        
    Returns:
        float: Valor del costo marginal o None si no se encuentra
    """
    try:
        # If date_in is not provided, use current date
        if date_in is None:
            from scripts.utils.utils import get_date
            date_in = get_date()
            
        # Convert date to string format if it's a datetime object
        if hasattr(date_in, 'strftime'):
            # Convert to dd.mm.yy format to match database
            date_str = date_in.strftime('%d.%m.%y')
        else:
            date_str = date_in
            
        # Query the database for TCO data
        query = session_in.query(TrackingTco)
        query = query.filter(TrackingTco.fecha == date_str)
        query = query.filter(TrackingTco.central == central)
        query = query.filter(TrackingTco.bloque_horario == bloque)
        
        result = query.first()
        
        if result:
            return float(result.costo_marginal)
        else:
            if central == "ERNC":
                return 0
            # Fallback: use most recent available date for this central+bloque
            from sqlalchemy import func as _func
            fallback = (session_in.query(TrackingTco)
                        .filter(TrackingTco.central == central,
                                TrackingTco.bloque_horario == bloque)
                        .order_by(desc(TrackingTco.id))
                        .first())
            if fallback:
                logger.warning(
                    f"TCO no encontrado para fecha {date_str} "
                    f"(central={central}, bloque={bloque}). "
                    f"Usando fallback: fecha={fallback.fecha}, costo={fallback.costo_marginal}"
                )
                return float(fallback.costo_marginal)
            logger.warning(f"No se encontró costo marginal para central {central}, bloque {bloque}, fecha {date_str}")
            return None
            
    except Exception as e:
        logger.error(f"Error en retrieve_costo_marginal_tco: {e}")
        return None

def retrieve_tracking_tco(session, id=None, fecha=None, central=None, bloque_horario=None, limit=None):
    """
    Recupera registros de la tabla tracking_tco.
    
    Args:
        session: Sesión SQLAlchemy activa
        id: ID específico a recuperar (opcional)
        fecha: Fecha para filtrar (opcional)
        central: Central para filtrar (opcional)
        bloque_horario: Bloque horario para filtrar (opcional)
        limit: Límite de registros a recuperar (opcional)
        
    Returns:
        Lista de objetos TrackingTco o un solo objeto si se especifica id
    """
    query = session.query(TrackingTco)
    
    if id is not None:
        return query.filter(TrackingTco.id == id).first()
    
    if fecha is not None:
        query = query.filter(TrackingTco.fecha == fecha)
        
    if central is not None:
        query = query.filter(TrackingTco.central == central)
        
    if bloque_horario is not None:
        query = query.filter(TrackingTco.bloque_horario == bloque_horario)
    
    if limit is not None:
        query = query.limit(limit)
        
    return query.all()

def retrieve_valor_factor_penalizacion(session_in, barra_in, request_timestamp=None, date_in=None):
    """
    Recupera el factor de penalización para una barra específica.
    
    Args:
        session_in: Sesión SQLAlchemy activa
        barra_in: Nombre de la barra
        request_timestamp: Timestamp de la solicitud (opcional)
        date_in: Fecha específica (opcional, por defecto usa la fecha actual)
        
    Returns:
        float: Valor del factor de penalización o None si no se encuentra
    """
    # Diccionario para mapear la entrada a los valores esperados en la base de datos.
        # Diccionario para mapear la entrada a los valores esperados en la base de datos.
    barra_transmision_select = {'QUILLOTA_22O': 'LVegas110',
                                'CHARRUA_22O': 'Charrua066',
                                'quillota_22o': 'LVegas110',
                                'charrua_22o': 'Charrua066'}
    
    # Convertir a mayúsculas para hacer la búsqueda insensible a mayúsculas/minúsculas
    barra_upper = barra_in.upper() if barra_in else None
    request_barra = barra_transmision_select.get(barra_upper)

    if not request_barra:
        logger.error(f"No se encontró la barra {barra_in} en el diccionario de transmisión. retrieve_valor_factor_penalizacion")
        return 1.0

    try:
        # If date_in is not provided, use current date
        if date_in is None:
            from scripts.utils.utils import get_date
            date_in = get_date()
            
        # Convert date to string format if it's a datetime object
        if hasattr(date_in, 'strftime'):
            date_str = date_in.strftime('%d.%m.%y')
        else:
            date_str = date_in
            
        # Determine hour from request_timestamp
        if request_timestamp:
            if isinstance(request_timestamp, str):
                if ':' in request_timestamp:
                    # Extract hour from timestamp
                    hour = int(request_timestamp.split(':')[0])
                else:
                    hour = int(request_timestamp)
            else:
                hour = request_timestamp.hour
        else:
            hour = datetime.now().hour
            
        # Add 1 to hour to match database format
        hour = hour + 1
            
        # Query the database for Factor Penalización data
        query = session_in.query(FactorPenalizacion)
        query = query.filter(FactorPenalizacion.fecha == date_str)
        query = query.filter(FactorPenalizacion.barra == request_barra)
        query = query.filter(FactorPenalizacion.hora == hour)
        
        result = query.first()
        
        if result:
            return float(result.penalizacion)
        else:
            # Fallback: use most recent available date for this barra+hora
            fallback = (session_in.query(FactorPenalizacion)
                        .filter(FactorPenalizacion.barra == request_barra,
                                FactorPenalizacion.hora == hour)
                        .order_by(desc(FactorPenalizacion.id))
                        .first())
            if fallback:
                logger.warning(
                    f"FP no encontrado para fecha {date_str} "
                    f"(barra={barra_in}, hora={hour}). "
                    f"Usando fallback: fecha={fallback.fecha}, fp={fallback.penalizacion}"
                )
                return float(fallback.penalizacion)
            logger.warning(f"No se encontró factor de penalización para barra {barra_in}, fecha {date_str}, hora {hour}")
            return 1.0
            
    except Exception as e:
        logger.error(f"Error en retrieve_valor_factor_penalizacion: {e}")
        return 1.0  # Default to 1.0 on error
    
########################################  CENTRAL ########################################


def _record_unix_time(record, timestamp_attribute):
    unix_time = getattr(record, 'unix_time', None)
    if unix_time is not None:
        try:
            return int(unix_time)
        except (TypeError, ValueError):
            return None

    timestamp = _normalize_desacople_timestamp(
        getattr(record, timestamp_attribute, None)
    )
    return int(timestamp.timestamp()) if timestamp is not None else None


def _match_costs_to_central_revisions(session_in, central_rows):
    """Match legacy costs to the revision interval in which they were recorded."""
    if not central_rows:
        return {}

    central_names = {row.nombre for row in central_rows if row.nombre}
    revisions = session_in.query(CentralTable).filter(
        CentralTable.nombre.in_(central_names)
    ).order_by(CentralTable.nombre, CentralTable.id).all()
    revision_ids = [row.id for row in revisions]
    costs = session_in.query(CentralCostoOperacional).filter(
        or_(
            CentralCostoOperacional.central_nombre.in_(central_names),
            CentralCostoOperacional.central_id.in_(revision_ids),
        )
    ).order_by(
        CentralCostoOperacional.unix_time,
        CentralCostoOperacional.id,
    ).all()

    revision_name_by_id = {revision.id: revision.nombre for revision in revisions}
    costs_by_name = {name: [] for name in central_names}
    for cost in costs:
        name = cost.central_nombre or revision_name_by_id.get(cost.central_id)
        if name in costs_by_name:
            costs_by_name[name].append(cost)

    matches = {}
    for name in central_names:
        named_revisions = [revision for revision in revisions if revision.nombre == name]
        revision_times = [
            _record_unix_time(revision, 'fecha_registro')
            for revision in named_revisions
        ]
        named_costs = costs_by_name[name]

        for index, revision in enumerate(named_revisions):
            start_time = revision_times[index]
            next_times = [
                value for value in revision_times[index + 1:]
                if value is not None
            ]
            end_time = next_times[0] if next_times else None
            if start_time is None or (end_time is not None and end_time <= start_time):
                continue

            candidates = []
            for cost in named_costs:
                cost_time = _record_unix_time(cost, 'timestamp')
                if cost_time is None or cost_time < start_time:
                    continue
                if end_time is not None and cost_time >= end_time:
                    continue
                candidates.append(cost)

            if candidates:
                matches[revision.id] = max(
                    candidates,
                    key=lambda cost: (
                        _record_unix_time(cost, 'timestamp'),
                        cost.id,
                    ),
                )

    return matches


def query_last_row_central(session_in, name_central):
    """
    Retrieves the last entry from the 'central' table based on the provided name.

    Args:
        session (sqlalchemy.orm.session.Session): SQLAlchemy Session object.
        name (str): The name to search for in the 'central' table.

    Returns:
        list: The last entry matching the provided name with latest costo operacional merged in.
    """
    try:
        last_entry = session_in.query(CentralTable).filter_by(
            nombre=name_central).order_by(desc(CentralTable.id)).first()
        latest_cost = session_in.query(CentralCostoOperacional).filter_by(
            central_nombre=name_central).order_by(desc(CentralCostoOperacional.id)).first()

        if last_entry is not None:
            columns = [
                'id', 'nombre', 'generando', 'barra_transmision',
                'tasa_proveedor', 'porcentaje_brent', 'tasa_central',
                'precio_brent', 'margen_garantia', 'costo_operacional',
                'factor_motor', 'fecha_referencia_brent', 'fecha_registro',
                'external_update', 'editor',
            ]

            result = []
            for col in columns:
                if col == 'costo_operacional' and latest_cost is not None:
                    result.append(latest_cost.costo_operacional)
                elif col == 'generando':
                    result.append(getattr(last_entry, col, False))
                elif col in ['tasa_proveedor', 'porcentaje_brent', 'tasa_central', 'precio_brent', 'margen_garantia', 'factor_motor']:
                    result.append(getattr(last_entry, col, 0.0))
                elif col == 'external_update':
                    result.append(getattr(last_entry, col, False))
                else:
                    result.append(getattr(last_entry, col, None))

            return result

        return None
    except Exception as e:
        logger.error(f"Error while getting last entry by name: {e}")
        raise

       
def retrieve_all_centrales(session, centrales = ["Los Angeles", "Quillota"]):
    """
    Devuelve las últimas 2 entradas de centrales especificadas como diccionarios.
    
    Args:
        session: Sesión SQLAlchemy activa
        centrales: Lista de nombres de centrales a filtrar
        
    Returns:
        Lista de diccionarios con los datos de las últimas 2 entradas para cada central especificada
    """
    query = session.query(CentralTable)
    
    if centrales:
        query = query.filter(CentralTable.nombre.in_(centrales))
    
    query = query.order_by(CentralTable.id.desc()).limit(2)
    
    results = query.all()
    
    # Convertir objetos ORM a diccionarios
    data = []
    for central in results:
        central_dict = {}
        for column in central.__table__.columns:
            column_name = column.name
            if column_name not in central_dict:
                central_dict[column_name] = getattr(central, column_name)
        
        data.append(central_dict)
    
    return data

########################################  COSTO OPERACIONAL ########################################

def inject_costo_operacional(session, central_nombre, costo_operacional, timestamp=None, unix_time=None, central_id=None, editor="sistema"):
    """
    Inserta un nuevo registro de costo operacional para una central específica.
    
    Args:
        session: Sesión SQLAlchemy activa
        central_nombre: Nombre de la central
        costo_operacional: Valor del costo operacional
        timestamp: Timestamp del registro (opcional, por defecto fecha/hora actual)
        unix_time: Tiempo unix (opcional, calculado desde timestamp si None)
        central_id: ID de la central (opcional)
        editor: Usuario o sistema que realiza la inserción (opcional)
    
    Returns:
        CentralCostoOperacional: Objeto insertado o None si hay error
    """
    try:
        # Prepare timestamp if not provided
        if timestamp is None:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Usando timestamp generado: {timestamp}")
        else:
            logger.info(f"Usando timestamp proporcionado: {timestamp}")
        
        # Calculate unix_time if not provided
        if unix_time is None:
            from scripts.utils.utils import get_unix_time
            try:
                # Formato esperado por get_unix_time: DD.MM.YY HH:MM:SS
                # Intentamos convertir el timestamp si tiene formato YYYY-MM-DD HH:MM:SS
                if "-" in timestamp:
                    dt = get_date().strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                    formatted_timestamp = dt.strftime("%d.%m.%y %H:%M:%S")
                    logger.info(f"Convertido timestamp a formato esperado: {formatted_timestamp}")
                    unix_time = get_unix_time(formatted_timestamp)
                else:
                    unix_time = get_unix_time(timestamp)
                logger.info(f"Unix time calculado: {unix_time}")
            except Exception as e:
                logger.error(f"Error al calcular unix_time desde timestamp '{timestamp}': {e}")
                logger.error("Usando timestamp actual para unix_time")
                import time
                unix_time = int(time.time())
        else:
            logger.info(f"Usando unix_time proporcionado: {unix_time}")
            
        # Get central_id if not provided
        if central_id is None and central_nombre is not None:
            central = session.query(CentralTable).filter_by(
                nombre=central_nombre
            ).order_by(desc(CentralTable.id)).first()
            if central:
                central_id = central.id
                logger.info(f"Obtenido central_id: {central_id}")
        
        # Create the object
        costo_op = CentralCostoOperacional(
            central_id=central_id,
            central_nombre=central_nombre,
            timestamp=timestamp,
            unix_time=unix_time,
            costo_operacional=costo_operacional,
            editor=editor
        )
        
        # Add and commit
        session.add(costo_op)
        session.commit()
        logger.info(f"Costo operacional insertado para central '{central_nombre}': {costo_operacional}")
        return costo_op
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error al insertar costo operacional para central '{central_nombre}': {e}")
        return None

########## FUNCTIONS FOR ETL costos operacionales ##########

def retrieve_costo_operacional(session, central_nombre):
    """
    Devuelve el último costo operacional registrado para una central específica.
    
    Args:
        session: Sesión SQLAlchemy activa
        central_nombre: Nombre de la central
        
    Returns:
        CentralCostoOperacional: El último registro de costo operacional o None si no existe
    """
    try:
        return session.query(CentralCostoOperacional)\
            .filter(CentralCostoOperacional.central_nombre == central_nombre)\
            .order_by(CentralCostoOperacional.unix_time.desc(), CentralCostoOperacional.id.desc())\
            .first()
    except Exception as e:
        logger.error(f"Error al recuperar costo operacional para la central '{central_nombre}': {e}")
        return None


########################################  DECISIONES ########################################

def insert_status_central(session, central_nombre, barra, timestamp, unix_time, cmg_ponderado, 
                         status_operacional, cmg_timestamp=None):
    """
    Inserta una nueva decisión para una central en la tabla StatusCentral.
    
    Args:
        session: Sesión SQLAlchemy activa
        central_nombre: Nombre de la central
        barra: Barra de transmisión asociada
        timestamp: Timestamp en formato string
        unix_time: Timestamp en formato unix
        cmg_ponderado: Valor del CMG ponderado
        status_operacional: Estado de operación ('ON', 'OFF', 'HOLD')
        cmg_timestamp: Timestamp del CMG (opcional)
        
    Returns:
        StatusCentral: El objeto insertado o None si hay error
    """
    try:
        # Obtener el último costo operacional para esta central
        costo_op = retrieve_costo_operacional(session, central_nombre)
        if not costo_op:
            logger.error(f"No se encontró costo operacional para la central '{central_nombre}'")
            return None
            
        status_row = StatusCentral(
            central=central_nombre,
            barra=barra,
            timestamp=timestamp,
            unix_time=unix_time,
            cmg_timestamp=cmg_timestamp,
            cmg_ponderado=cmg_ponderado,
            status_operacional=status_operacional,
            costo_operacional_id=costo_op.id
        )
        
        session.add(status_row)
        session.commit()
        logger.info(f"Decisión insertada para central '{central_nombre}': {status_operacional}")
        return status_row
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error al insertar decisión para central '{central_nombre}': {e}")
        return None

def get_last_status_central(session, central_nombre):
    """
    Devuelve la última decisión de una central específica, ordenada por unix_time descendente.
    
    Args:
        session: Sesión SQLAlchemy activa
        central_nombre: Nombre de la central
        
    Returns:
        StatusCentral: El objeto de la última decisión o None si no existe
    """
    try:
        return (
            session.query(StatusCentral)
            .filter(StatusCentral.central == central_nombre)
            .order_by(StatusCentral.unix_time.desc())
            .first()
        )
    except Exception as e:
        logger.error(f"Error al recuperar la última decisión para la central '{central_nombre}': {e}")
        return None


def query_central_table(session_in, num_entries=6):
    """
    Retrieves multiple entries from the central table.
    """
    try:
        centrals = session_in.query(CentralTable).order_by(
            desc(CentralTable.id)
        ).limit(num_entries).all()

        if centrals:
            costs_by_revision = _match_costs_to_central_revisions(session_in, centrals)
            data = []
            for central in centrals:
                latest_cost = costs_by_revision.get(central.id)
                entry = {
                    'id': central.id,
                    'nombre': central.nombre,
                    'barra_transmision': getattr(central, 'barra_transmision', None),
                    'tasa_proveedor': getattr(central, 'tasa_proveedor', 0.0),
                    'porcentaje_brent': getattr(central, 'porcentaje_brent', 0.0),
                    'tasa_central': getattr(central, 'tasa_central', 0.0),
                    'precio_brent': getattr(central, 'precio_brent', 0.0),
                    'margen_garantia': getattr(central, 'margen_garantia', 0.0),
                    'factor_motor': getattr(central, 'factor_motor', 0.0),
                    'fecha_registro': getattr(central, 'fecha_registro', ''),
                    'external_update': getattr(central, 'external_update', False),
                    'editor': getattr(central, 'editor', ''),
                    'generando': getattr(central, 'generando', False),
                    'costo_operacional': latest_cost.costo_operacional if latest_cost else None,
                    'fecha_referencia_brent': getattr(central, 'fecha_referencia_brent', None),
                }
                data.append(entry)

            return pd.DataFrame(data)

        return pd.DataFrame(columns=[
            'id', 'nombre', 'generando', 'barra_transmision', 'tasa_proveedor',
            'porcentaje_brent', 'tasa_central', 'precio_brent', 'margen_garantia',
            'costo_operacional', 'factor_motor', 'fecha_referencia_brent',
            'fecha_registro', 'external_update', 'editor',
        ])
    except Exception as e:
        logger.error(f"Error in query_central_table: {e}")
        raise


def query_central_table_modifications(session_in, num_entries=10):
    """
    Retrieves central table entries with modifications.
    """
    try:
        centrals = session_in.query(CentralTable).filter(
            CentralTable.external_update.is_(True)
        ).order_by(
            desc(CentralTable.id)
        ).limit(num_entries).all()

        if centrals:
            costs_by_revision = _match_costs_to_central_revisions(session_in, centrals)
            data = []
            for central in centrals:
                latest_cost = costs_by_revision.get(central.id)
                entry = {
                    'id': central.id,
                    'nombre': central.nombre,
                    'barra_transmision': getattr(central, 'barra_transmision', None),
                    'tasa_proveedor': getattr(central, 'tasa_proveedor', 0.0),
                    'porcentaje_brent': getattr(central, 'porcentaje_brent', 0.0),
                    'tasa_central': getattr(central, 'tasa_central', 0.0),
                    'precio_brent': getattr(central, 'precio_brent', 0.0),
                    'margen_garantia': getattr(central, 'margen_garantia', 0.0),
                    'factor_motor': getattr(central, 'factor_motor', 0.0),
                    'fecha_registro': getattr(central, 'fecha_registro', ''),
                    'external_update': True,
                    'editor': getattr(central, 'editor', ''),
                    'generando': getattr(central, 'generando', False),
                    'costo_operacional': latest_cost.costo_operacional if latest_cost else None,
                    'fecha_referencia_brent': getattr(central, 'fecha_referencia_brent', None),
                }
                data.append(entry)

            return pd.DataFrame(data)

        df = pd.DataFrame(columns=[
            'id', 'nombre', 'generando', 'barra_transmision', 'tasa_proveedor',
            'porcentaje_brent', 'tasa_central', 'precio_brent', 'margen_garantia',
            'costo_operacional', 'factor_motor', 'fecha_referencia_brent',
            'fecha_registro', 'external_update', 'editor',
        ])
        df['external_update'] = True
        return df
    except Exception as e:
        logger.error(f"Error in query_central_table_modifications: {e}")
        raise


def query_cmg_ponderado_by_time(session_in, unixtime, delta_hours=48):
    """
    Retrieves CMG ponderado values for a specific time range.
    """
    try:
        start_time = unixtime - (delta_hours * 3600)
        results = session_in.query(CmgPonderado).filter(
            CmgPonderado.unix_time >= start_time,
            CmgPonderado.unix_time <= unixtime,
        ).order_by(CmgPonderado.unix_time).all()

        if results:
            return [{
                'barra_transmision': record.barra_transmision,
                'timestamp': record.timestamp,
                'unix_time': record.unix_time,
                'cmg_ponderado': record.cmg_ponderado,
            } for record in results]

        return []
    except Exception as e:
        logger.error(f"Error in query_cmg_ponderado_by_time: {e}")
        raise


def get_cmg_tiempo_real(session_in, unix_time_in):
    """
    Retrieves CMG tiempo real values for a specific time range.
    """
    try:
        results = session_in.query(CmgTiempoReal).filter(
            CmgTiempoReal.unix_time >= unix_time_in
        ).order_by(CmgTiempoReal.unix_time).all()

        if results:
            data = []
            for record in results:
                try:
                    data.append({
                        'id_tracking': record.id_tracking,
                        'barra_transmision': record.barra_transmision,
                        'año': record.año,
                        'mes': record.mes,
                        'dia': record.dia,
                        'hora': record.hora,
                        'unix_time': record.unix_time,
                        'desacople_bool': safe_bool_convert(record.desacople_bool),
                        'cmg': safe_float_convert(record.cmg),
                        'central_referencia': record.central_referencia,
                    })
                except Exception as inner_e:
                    logger.error(f"Error processing record in get_cmg_tiempo_real: {inner_e}")

            if data:
                return data

        return []
    except Exception as e:
        logger.error(f"Error in get_cmg_tiempo_real: {e}")
        raise


def get_cmg_programados(session_in, name_central, date_in):
    """
    Retrieves the programmed marginal cost (CMG) data for a specific central and date.
    """
    try:
        date_str = date_in.strftime('%Y-%m-%d') if isinstance(date_in, datetime) else date_in

        result = session_in.query(CmgProgramados).filter(
            CmgProgramados.central == name_central,
            CmgProgramados.fecha_programado == date_str,
        ).first()

        if result:
            data = {}
            for hour in range(24):
                hour_key = f"{hour:02d}:00"
                value = getattr(result, f"_{hour:02d}_00", None)
                if value is not None:
                    data[hour_key] = float(value)
            return data

        return {}
    except Exception as e:
        logger.error(f"Error retrieving CMG programados for {name_central} on {date_in}: {e}")
        raise


def get_latest_status_central(session_in, central_name):
    """
    Retrieves the latest operational status for a central from status_central table.
    """
    try:
        latest_status = session_in.query(StatusCentral).filter_by(
            central=central_name
        ).order_by(desc(StatusCentral.unix_time)).first()

        if latest_status:
            return latest_status.status_operacional

        logger.warning(f"No status entries found for central: {central_name}")
        return None
    except Exception as e:
        logger.error(f"Error fetching latest status for {central_name}: {e}")
        raise


def get_status_central_history(session_in, limit=50, centrals=None, since_unix=None):
    """
    Retrieves the status history for centrals from the status_central table.
    """
    try:
        if session_in is None:
            logger.error("Cannot query status_central: session is None")
            return pd.DataFrame()

        base_query = session_in.query(StatusCentral).options(
            joinedload(StatusCentral.costo_operacional_rel)
        )
        if centrals and isinstance(centrals, list):
            results = []
            for central_name in centrals:
                central_query = base_query.filter(StatusCentral.central == central_name)
                if since_unix is not None:
                    recent = central_query.filter(
                        StatusCentral.unix_time >= since_unix
                    ).order_by(desc(StatusCentral.unix_time)).all()
                    predecessor = central_query.filter(
                        StatusCentral.unix_time < since_unix
                    ).order_by(desc(StatusCentral.unix_time)).first()
                    results.extend(recent)
                    if predecessor is not None:
                        results.append(predecessor)
                else:
                    central_query = central_query.order_by(desc(StatusCentral.unix_time))
                    if limit:
                        central_query = central_query.limit(limit)
                    results.extend(central_query.all())
            results.sort(key=lambda row: row.unix_time, reverse=True)
        else:
            query = base_query
            if since_unix is not None:
                query = query.filter(StatusCentral.unix_time >= since_unix)
            query = query.order_by(desc(StatusCentral.unix_time))
            if limit and since_unix is None:
                query = query.limit(limit)
            results = query.all()
        if not results:
            logger.warning("No status history found")
            return pd.DataFrame()

        data = []
        for status in results:
            costo_op = status.costo_operacional_rel

            try:
                fecha = pd.to_datetime(status.timestamp).strftime('%Y-%m-%d')
                hora = pd.to_datetime(status.timestamp).strftime('%H:%M:%S')
            except Exception:
                fecha = status.timestamp
                hora = ''

            data.append({
                'id': status.id,
                'central': status.central,
                'barra': status.barra,
                'timestamp': status.timestamp,
                'fecha': fecha,
                'hora': hora,
                'unix_time': status.unix_time,
                'cmg_timestamp': status.cmg_timestamp,
                'cmg_ponderado': float(status.cmg_ponderado),
                'status_operacional': status.status_operacional,
                'generando': status.status_operacional == 'ON',
                'costo_operacional': float(costo_op.costo_operacional) if costo_op else None,
            })

        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error fetching status history: {e}")
        raise


def get_latest_desacople_event(session, barra_transmision: str):
    """
    Retrieves the most recent desacople history event for a barra.

    Returns:
        dict with keys: estado, detected_at, tramo, comentario, fuente; or None if not found.
    """
    try:
        ultimo = retrieve_latest_desacople_event(session, barra_transmision=barra_transmision)
        if ultimo is None:
            return None

        return {
            'estado': ultimo.estado,
            'detected_at': ultimo.detected_at,
            'tramo': ultimo.tramo,
            'comentario': ultimo.comentario,
            'fuente': ultimo.fuente,
        }
    except Exception as e:
        logger.error(f"Error al obtener último evento de desacople para {barra_transmision}: {e}")
        raise


def query_values_last_desacople_bool(session_in, barra_transmision):
    """
    Recupera central_referencia y cmg desde cmg_tiempo_real, pero usa desacople_history
    como fuente de verdad para el estado de desacople.
    """
    try:
        variants = _barra_variants(barra_transmision)

        result = session_in.query(
            CmgTiempoReal.central_referencia,
            CmgTiempoReal.cmg,
        ).filter(
            func.upper(CmgTiempoReal.barra_transmision).in_(list(variants))
        ).order_by(
            desc(CmgTiempoReal.unix_time)
        ).first()

        if not result:
            logger.warning(f"No match found in cmg_tiempo_real for barra {barra_transmision}")
            return None, None, None

        desacople_bool, _ = retrieve_status_desacople(session_in, barra_transmision_in=barra_transmision)
        return result.central_referencia, desacople_bool, result.cmg
    except Exception as e:
        logger.error(f"Error in query_values_last_desacople_bool: {e}")
        raise
    
