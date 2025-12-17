"""
Este módulo contiene funciones de utilidad para todo el proyecto.
"""
import os
import sys
import logging
from typing import Any, Dict, Optional
import yaml
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
from datetime import time
import pytz
import time

load_dotenv()



########################################################################################
######################### Funciones de setting #########################################
########################################################################################


def load_config(file_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Carga un archivo de configuración YAML y devuelve un diccionario.

    Args:
    - file_path (str): Ruta del archivo de configuración YAML.

    Returns:
    - dict: Diccionario con la configuración cargada.
    """
    try:
        # Get the current directory of this module
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Travel up two levels to reach the project root
        project_root = os.path.abspath(os.path.join(current_dir, '../..'))
        
        # First try in project root
        file_route = os.path.join(project_root, file_path)
        if os.path.exists(file_route):
            with open(file_route, 'r') as file:
                return yaml.safe_load(file)
                
        # If not found, try in the scripts directory
        scripts_dir = os.path.join(project_root, 'scripts')
        file_route = os.path.join(scripts_dir, file_path)
        if os.path.exists(file_route):
            with open(file_route, 'r') as file:
                return yaml.safe_load(file)
                
        # If still not found, try looking relative to the current module
        file_route = os.path.join(current_dir, '..', file_path)
        if os.path.exists(file_route):
            with open(file_route, 'r') as file:
                return yaml.safe_load(file)
        
        # If we get here, the file wasn't found
        logging.error(f"Archivo de configuración {file_path} no encontrado.")
        logging.error(f"Rutas revisadas: {os.path.join(project_root, file_path)}, {os.path.join(scripts_dir, file_path)}, {os.path.join(current_dir, '..', file_path)}")
        return {}
    
    except FileNotFoundError:
        logging.error(f"Archivo de configuración {file_path} no encontrado.")
    
    except yaml.YAMLError as exc:
        logging.error(
            f"Error al parsear el archivo de configuración {file_path}. Detalle: {exc}")
    return {}

def setup_logging(log_name: str = 'hbs', log_level: int = logging.INFO, log_filename: str = 'hbs.log', depth: int = 2) -> logging.Logger:
    """
    Configura y devuelve un objeto logger para la aplicación.

    Args:
    - log_name (str): Nombre del logger.
    - log_level (int): Nivel de log.
    - log_filename (str): Nombre del archivo de log.
    - depth (int): Nivel de directorios hacia arriba para encontrar el directorio de log.

    Returns:
    - logger: Objeto logger configurado.
    """
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    
    for _ in range(depth):
        parent_dir = os.path.dirname(parent_dir)

    log_dir = os.path.join(parent_dir, 'log')
    log_path = os.path.join(log_dir, log_filename)

    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    logger.propagate = False

    # Avoid duplicate handlers for the same file
    existing = [
        h for h in logger.handlers
        if isinstance(h, logging.FileHandler)
        and getattr(h, 'baseFilename', None) == os.path.abspath(log_path)
    ]
    if not existing:
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(lineno)d')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

################################ settings #########################################################

config = load_config()
# Provide default values if 'logging' key is not present
if 'logging' not in config:
    config['logging'] = {'utils': 'INFO'}
    
log_level = config['logging'].get('utils', 'INFO')

if isinstance(log_level, str):
    log_level = getattr(logging, log_level.upper())

_LOGGER_CACHE = {}


def get_logger(name: str = 'utils', level: int = None, filename: str = 'utils.log') -> logging.Logger:
    """
    Obtener logger singleton por nombre para evitar handlers duplicados.
    """
    global _LOGGER_CACHE
    key = (name, filename)
    if key in _LOGGER_CACHE:
        return _LOGGER_CACHE[key]

    resolved_level = level if level is not None else log_level
    logger_obj = setup_logging(log_name=name, log_level=resolved_level, log_filename=filename)
    _LOGGER_CACHE[key] = logger_obj
    return logger_obj


logger = get_logger('utils', log_level, 'utils.log')


# Simple cache settings (disabled by default)
CACHE_DOWNLOADS = os.getenv("CACHE_DOWNLOADS", "0") == "1"
RIO_CACHE_TTL_HOURS = int(os.getenv("RIO_CACHE_TTL_HOURS", "6"))
FP_TCO_CACHE_TTL_HOURS = int(os.getenv("FP_TCO_CACHE_TTL_HOURS", "24"))

####################################################################################################
################################        funciones de tiempo         ################################
####################################################################################################

# Constantes a nivel de módulo para la zona horaria y el formato de fecha
TIMEZONE = pytz.timezone('America/Santiago')
DATE_FORMAT = "%d.%m.%y %H:%M:%S"

def current_datetime(offset_days=0):
    """Devuelve la fecha y hora actual con un desplazamiento opcional en días.
    Para offset_days positivos, devuelve fechas futuras.
    Para offset_days negativos, devuelve fechas pasadas.
    """
    return datetime.now(TIMEZONE) + timedelta(days=offset_days)

def get_date():
    """ get fecha y hora del momento en que se ejectua la funcion
    se puede obtener hora, dia, mes , etc obteniendo el atributo de fecha_actual: ej get_date().hour
    Status: Cuando se traspase a servidor hay que ajustar por zona horaria"""
    return current_datetime()

def get_date_tomorrow():
    """ get fecha y hora del momento en que se ejectua la funcion
    se puede obtener hora, dia, mes , etc obteniendo el atributo de fecha_actual: ej get_date().hour
    Status: Cuando se traspase a servidor hay que ajustar por zona horaria
    """
    return current_datetime(offset_days=1)

def get_date_yesterday():
    """ get fecha y hora del momento en que se ejectua la funcion
    se puede obtener hora, dia, mes , etc obteniendo el atributo de fecha_actual: ej get_date().hour
    Status: Cuando se traspase a servidor hay que ajustar por zona horaria
    """
    return current_datetime(offset_days=-1)

def round_down_timestamp(current_date):
    """
    Redondea hacia el inicio de hora el string de hora con formato "%d.%m.%y %H:%M:%S"
    """
    try:
        current_datetime = datetime.strptime(current_date, "%d.%m.%y %H:%M:%S")
        current_datetime = current_datetime.replace(minute=0, second=0, microsecond=0)
        return current_datetime.strftime("%d.%m.%y %H:%M:%S")
    
    except ValueError:
        logger.error(f"Invalid date format for {current_date}")
        return None

def timestamp_decomp(str_timestamp_in):
    ''' Descomponer '23.02.23 14:50:22' dia, año, mes, hora y  unixtime'''
    # Convertir str timestamp a objecto datetime
    input_format = "%d.%m.%y %H:%M:%S"
    datetime_object = datetime.strptime(str_timestamp_in, input_format)

    # Extraer time de datetime object
    str_time = datetime_object.time()

    # Convertir obj:datetime a Unix timestamp
    int_unix_time = datetime_object.timestamp()

    return (datetime_object.year, datetime_object.month, datetime_object.day, str(str_time), int_unix_time)

def get_unix_time(input_str):
    """
    Convierte un string de fecha y hora en formato 'dd.mm.yy hh:mm:ss' a
    un timestamp Unix.

    Parámetros:
        input_str (str): Una cadena que contiene una fecha y hora en el formato
            'dd.mm.yy hh:mm:ss', por ejemplo '14.03.23 13:30:45'.

    Retorna:
        int: El timestamp Unix de la fecha y hora de entrada como entero.
    """
    date = datetime.strptime(input_str, "%d.%m.%y %H:%M:%S")
    return int(date.timestamp())

def timestamp_to_datetime(timestamp_in: str):
    """
    converts input string in format %d.%m.%y %H:%M:%S to a datetime object
    """
    date = datetime.strptime(timestamp_in, "%d.%m.%y %H:%M:%S")
    return date 

def get_timestamp_from_unix_time(unix_time):
    """
    Convierte un timestamp Unix a una cadena de texto con el formato %H:%M:%S.

    Parámetros:
        unix_time (int): El timestamp Unix a convertir.

    Retorna:
        str: Una cadena de texto que representa la hora en el formato %H:%M:%S.
    """
    
    try:
        date = datetime.fromtimestamp(unix_time)

    except OSError:
        logger.error(f"Error converting Unix timestamp in get_timestamp_from_unix_time: {unix_time}")
        return None
    
    return date.strftime("%d.%m.%y %H:%M:%S")


def get_unixtime_init_hour(fecha_actual):
    """
    Calcula el timestamp Unix del inicio de la hora actual.

    Parámetros:
        fecha_actual (date objt): El timestamp Unix a convertir.

    Retorna:
        Unix time del inicio de la hora pasada
    """
    # Get the current time and round down to the nearest hour
    now = fecha_actual.replace(microsecond=0, second=0, minute=0)
    # Get the start of the past hour
    start_unix_time = int(now.timestamp())

    return start_unix_time

def comparacion_minutos(timestamp_inicio, timestamp_fin, minutos):
    """
    Comprueba si ha transcurrido más de cierta cantidad de minutos entre dos timestamps.

    Parámetros:
        timestamp_inicio (str): Timestamp de inicio en formato 'dd.mm.yy HH:MM:SS'.
        timestamp_fin (str): Timestamp de fin en formato 'dd.mm.yy HH:MM:SS'.
        minutos (int): Cantidad de minutos para comparar.

    Retorna:
        bool: True si han transcurrido más minutos que el valor de entrada, False en caso contrario.
    """

    formato_fecha_hora = "%d.%m.%y %H:%M:%S"
    
    try:
        fecha_hora_inicio = datetime.strptime(timestamp_inicio, formato_fecha_hora)
    except ValueError:
        logger.error(f"Error parsing start timestamp in comparacion_minutos: {timestamp_inicio}")
        return None

    
    try:
        fecha_hora_fin = datetime.strptime(timestamp_fin, formato_fecha_hora)
    except ValueError:
        logger.error(f"Error parsing end timestamp in comparacion_minutos: {timestamp_fin}")
        return None


    diferencia_tiempo = fecha_hora_fin - fecha_hora_inicio
    minutos_diferencia = diferencia_tiempo.total_seconds() / 60

    return minutos_diferencia > minutos


def son_mismo_dia(timestamp1, timestamp2, formato='%y.%m.%d %H:%M:%S'):
    """
    Comprueba si dos marcas de tiempo (timestamps) corresponden al mismo día.

    Argumentos:
    - timestamp1: Un string que representa la primera marca de tiempo en el formato dado.
    - timestamp2: Un string que representa la segunda marca de tiempo en el formato dado.
    - formato: El formato utilizado en los strings de marca de tiempo. Por defecto, es '%y.%m.%d %H:%M:%S'.

    Retorna:
    - True si las marcas de tiempo corresponden al mismo día.
    - False si las marcas de tiempo corresponden a días diferentes.
    """
    # Convertir los strings de marca de tiempo a objetos datetime
    
    try:
        dt1 = datetime.strptime(timestamp1, formato)
    except ValueError:
        logger.error(f"Error parsing first timestamp in son_mismo_dia: {timestamp1}")
        return False

        
    try:
        dt2 = datetime.strptime(timestamp2, formato)
    except ValueError:
        logger.error(f"Error parsing second timestamp in son_mismo_dia: {timestamp2}")
        return False
    
    # Comparar los atributos 'date' para verificar si son del mismo día
    return dt1.date() == dt2.date()


####################################################################################################
#################################          funciones sys           #################################
####################################################################################################


def list_files(directory):
    """ lista archivos en el directorio inputado"""
    return [filename for filename in os.listdir(directory) if os.path.isfile(os.path.join(directory, filename))]


def delete_temp_file(file_name):
    """
    funcion elimina archivo de carpeta temporal
    """
    temp_dir = 'temp_dir/'
    file_path = os.path.join(temp_dir, file_name)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
        else:
            logger.error(f"{file_name} does not exist in {temp_dir}.")

    except Exception as exception:
        logger.error(f"Error deleting file: {file_name}. {exception}")

def validate_aws_env_vars(local=False):
    '''
    validar variables de aws antes de empezar el proceso
    '''
    if local:
        required_vars = ['MYSQL_DATABASE', 'MYSQL_HOST',
                         'MYSQL_USER', 'MYSQL_USER_PASSWORD', 'MYSQL_PORT']
    else:
        required_vars = ['AWS_MYSQL_DATABASE', 'AWS_MYSQL_HOST',
                         'AWS_MYSQL_USER', 'AWS_MYSQL_USER_PASSWORD', 'AWS_MYSQL_PORT']

    for var in required_vars:
        value = os.environ.get(var)
        if value is None or value == "":
            logging.critical(
                "Variables de AWS incorrectas, No se puede proceder")
            sys.exit(1)


def get_santiago_timezone():
    """
    Obtiene la zona horaria de Santiago.
    
    Returns:
        pytz.timezone: Objeto de zona horaria para America/Santiago.
    """
    return pytz.timezone('America/Santiago')

def timestamp_to_unix(timestamp_str, format_str="%Y-%m-%d %H:%M:%S"):
    """
    Convierte una cadena de timestamp a tiempo Unix.
    
    Args:
        timestamp_str (str): Cadena con formato de timestamp.
        format_str (str, optional): Formato de la cadena timestamp.
        
    Returns:
        int: Tiempo Unix (segundos desde la época) o None si hay error.
    """
    try:
        dt = datetime.strptime(timestamp_str, format_str)
        # Localizar en zona horaria de Santiago
        dt = get_santiago_timezone().localize(dt)
        return int(dt.timestamp())
    except Exception as e:
        logger.error(f"Error convirtiendo timestamp a unix: {e}")
        return None

def unix_to_datetime(unix_time):
    """
    Convierte tiempo Unix a objeto datetime.
    
    Args:
        unix_time (int): Tiempo Unix en segundos.
        
    Returns:
        datetime: Objeto datetime localizado en zona horaria de Santiago.
    """
    try:
        dt = datetime.fromtimestamp(unix_time)
        # Localizar en zona horaria de Santiago
        return get_santiago_timezone().localize(dt)
    except Exception as e:
        logger.error(f"Error convirtiendo unix a datetime: {e}")
        return None

def ensure_dir_exists(directory):
    """
    Asegura que un directorio exista, creándolo si es necesario.
    
    Args:
        directory (str): Ruta del directorio a verificar/crear.
        
    Returns:
        bool: True si el directorio existe o se creó correctamente, False en caso contrario.
    """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Directorio creado: {directory}")
        return True
    except Exception as e:
        logger.error(f"Error creando directorio {directory}: {e}")
        return False 

def get_current_timestamp():
    """
    Obtiene el timestamp actual en formato ISO.
    
    Returns:
        str: Timestamp actual en formato ISO.
    """
    return datetime.now().isoformat()
