import streamlit as st
import pandas as pd
import numpy as np
import datetime
import os
import sys
import time
import calendar
import json
import base64
import io
import requests
from datetime import datetime, timedelta
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
import logging
import pytz
from urllib.parse import quote

# Configure logging
# Configure root logger to not impact Streamlit's UI
try:
    log_file = "app.log"
    if not os.path.exists(os.path.dirname(log_file)) and os.path.dirname(log_file):
        os.makedirs(os.path.dirname(log_file))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),  # Log to file
            # No StreamHandler to avoid printing to stdout/stderr which might interfere with Streamlit
        ]
    )
except (IOError, PermissionError) as e:
    # Fallback to basic configuration without file handler if we can't write to log file
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    print(f"Warning: Could not configure file logging: {e}")

# Create a custom logger for this app
logger = logging.getLogger("streamlit_app")
logger.setLevel(logging.INFO)  # Set to INFO level for normal operation, can be changed to DEBUG for troubleshooting

# Ensure the app directory is on the path for local imports
APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

# Import our own modules
from db.operaciones_db import (
    query_cmg_ponderado_by_time,
    get_cmg_tiempo_real,
    get_cmg_programados,
    retrieve_tracking_coordinador,
    query_last_row_central,
    retrieve_status_desacople,
    query_values_last_desacople_bool,
    query_central_table,
    query_central_table_modifications,
    get_latest_status_central,
    get_status_central_history,
    get_latest_desacople_event
)

from db.connection_db import establecer_engine, establecer_session, session_scope
from utils.helpers import (
    tooltip,
    add_notification,
    show_notifications,
    auto_refresh,
    is_mobile,
    get_json_costo_marginal_online,
    get_costo_marginal_online_hora,
    get_central,
    get_cmg_programados,
    tooltip_explanations,
    insert_central,
    reformat_to_iso,
    create_status_piechart  # Add this line
)

#############################################################
################### CONFIGURATION ###########################
#############################################################
# Esconder e importa de manera segura las creedenciales

# Initialize session state for persistent settings across reruns
if 'auto_refresh' not in st.session_state:
    st.session_state['auto_refresh'] = False
    
if 'refresh_interval' not in st.session_state:
    st.session_state['refresh_interval'] = 5  # minutes
    
if 'time_range' not in st.session_state:
    st.session_state['time_range'] = 48  # hours

if 'chart_type' not in st.session_state:
    st.session_state['chart_type'] = 'line'

if 'show_charrua' not in st.session_state:
    st.session_state['show_charrua'] = True
    
if 'show_quillota' not in st.session_state:
    st.session_state['show_quillota'] = True

if 'show_operational_costs' not in st.session_state:
    st.session_state['show_operational_costs'] = True

if 'dark_mode' not in st.session_state:
    st.session_state['dark_mode'] = False
    
if 'notifications' not in st.session_state:
    st.session_state['notifications'] = []
    
if 'mobile_warning_shown' not in st.session_state:
    st.session_state['mobile_warning_shown'] = False

# Use a wider layout and add custom theming
st.set_page_config(
    layout="wide", 
    page_title="HBS-CMg",
    page_icon="⚡",
    initial_sidebar_state="expanded" if st.session_state.get('dark_mode', False) else "collapsed"
)

# Custom CSS styles for a cleaner, more modern look
st.markdown("""
<style>
    :root {
        --bg: #f5f7fb;
        --panel: #ffffff;
        --panel-2: #f0f4ff;
        --accent: #2563eb;
        --accent-2: #22c55e;
        --muted: #6b7280;
        --text: #0f172a;
        --border: #e5e7eb;
        --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
    }
    body {
        background: radial-gradient(120% 120% at 10% 20%, #f6f8ff 0%, #eef2ff 40%, #f5f7fb 100%);
        font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
        color: var(--text);
    }
    .main {
        padding: 1rem 2rem;
        background: transparent;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
        border-bottom: 1px solid var(--border);
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 1rem;
        font-weight: 700;
        color: var(--muted);
        padding: 0.75rem 1rem;
    }
    .stTabs [aria-selected="true"] {
        color: var(--text);
        border-bottom: 2px solid var(--accent);
    }
    .metric-card {
        background: linear-gradient(145deg, var(--panel), var(--panel-2));
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1rem;
        box-shadow: var(--shadow);
    }
    .metric-value {
        font-size: 1.6rem;
        font-weight: 800;
        margin-bottom: 0.15rem;
        color: var(--text);
    }
    .metric-label {
        font-size: 0.85rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .status-active {
        color: #22c55e;
        font-weight: 800;
        font-size: 1.5rem;
    }
    .status-inactive {
        color: #ef4444;
        font-weight: 800;
        font-size: 1.5rem;
    }
    .section-title {
        font-size: 1.45rem;
        font-weight: 800;
        margin-bottom: 1rem;
        color: #0f172a;
        letter-spacing: -0.01em;
    }
    .card-container {
        background: linear-gradient(160deg, var(--panel), #f7f9fe);
        border-radius: 14px;
        padding: 1.35rem;
        border: 1px solid var(--border);
        box-shadow: var(--shadow);
        margin-bottom: 1.35rem;
    }
    .divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, #e5e7eb, transparent);
        margin: 1.5rem 0;
    }
    /* Fix for empty containers */
    div.element-container:empty {
        display: none !important;
        min-height: 0 !important;
        height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    /* Tooltip styling - improved version */
    .tooltip {
        position: relative;
        display: inline-block;
        cursor: help;
    }
    .tooltip::after {
        content: "ⓘ";
        font-size: 0.8rem;
        color: var(--accent-2);
        margin-left: 0.25rem;
    }
    .tooltip:hover::before {
        content: attr(title);
        position: absolute;
        bottom: 100%;
        left: 50%;
        transform: translateX(-50%);
        padding: 0.5rem 0.75rem;
        background-color: #0f172a;
        color: white;
        border-radius: 0.35rem;
        white-space: nowrap;
        z-index: 1000;
        font-size: 0.78rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.35);
        border: 1px solid #1f2937;
    }
    /* Mobile optimizations */
    @media (max-width: 768px) {
        .metric-card {
            padding: 0.75rem;
            margin-bottom: 0.5rem;
        }
        .metric-value {
            font-size: 1.3rem;
        }
        .card-container {
            padding: 1rem;
        }
        .section-title {
            font-size: 1.2rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# Get date in format YYYY-MM-DD and current hour
# Specify the timezone for Chile
chile_tz = pytz.timezone('America/Santiago')

# Create a datetime object in Chile's timezone
chile_datetime = datetime.now(chile_tz)

fecha = chile_datetime.strftime("%Y-%m-%d")
hora = chile_datetime.strftime("%H:%M:%S")

# round hora to nearest hour
hora = hora.split(':')
hora_redondeada = f'{hora[0]}:00:00'
hora_redondeada_cmg_programados = f'{hora[0]}:00'

naive_datetime = chile_datetime.astimezone().replace(tzinfo=None)
unixtime = int(time.mktime(naive_datetime.timetuple()))

# Safe access to secrets (avoid crash when secrets.toml is missing)
def get_secret_section(section_name: str) -> dict:
    try:
        return dict(st.secrets[section_name])
    except Exception:
        # Fallback to local secrets file if Streamlit secrets are unavailable
        return load_local_secrets().get(section_name, {})

def load_local_secrets():
    """
    Load secrets from a local .streamlit/secrets.toml if present (useful for local dev).
    """
    paths = [
        Path(__file__).resolve().parent / ".streamlit" / "secrets.toml",
        Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml",
    ]
    for p in paths:
        if p.exists():
            try:
                try:
                    import tomllib  # Python 3.11+
                    return tomllib.loads(p.read_text())
                except ImportError:
                    import toml  # type: ignore
                    return toml.load(str(p))
            except Exception:
                return {}
    return {}

# Credenciales mysql remoto con fallback local para desarrollo
def load_db_config():
    # Prefer Streamlit secrets if available
    secrets = get_secret_section("AWS_MYSQL")
    if secrets:
        return {
            "DATABASE": secrets.get("DATABASE"),
            "HOST": secrets.get("HOST"),
            "USER": secrets.get("USER"),
            "PASSWORD": secrets.get("USER_PASSWORD", ""),
            "PORT": secrets.get("PORT", "3306"),
        }
    
    # Fallback: environment variables or local Laragon defaults
    return {
        "DATABASE": os.getenv("DB_DATABASE", "hbsv2"),
        "HOST": os.getenv("DB_HOST", "172.27.144.1"),
        "USER": os.getenv("DB_USER", "admin"),
        "PASSWORD": os.getenv("DB_USER_PASSWORD", ""),
        "PORT": os.getenv("DB_PORT", "3306"),
    }

db_conf = load_db_config()
DATABASE = db_conf["DATABASE"]
HOST = db_conf["HOST"]
USER = db_conf["USER"]
PASSWORD = db_conf["PASSWORD"]
PORT = db_conf["PORT"]

# API key (still optional)
USER_KEY = get_secret_section("COORDINADOR").get("USER_KEY", os.getenv("COORDINADOR_USER_KEY", ""))

#Informacion API flask (optional; fallback to env/default localhost)
API_HOST = get_secret_section("API").get("HOST", os.getenv("API_HOST", "localhost"))
API_PORT = get_secret_section("API").get("PORT", os.getenv("API_PORT", "8000"))


# Establecer motor de base de datos (cacheado para evitar recrearlo en cada rerun)
@st.cache_resource(show_spinner=False)
def get_engine():
    db_url = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    # establecer_engine de este proyecto acepta un string de conexión; construimos aquí para compatibilidad
    return establecer_engine(db_url)

engine = get_engine()
session = establecer_session(engine)


CONN_STATUS = engine is not None

# Initialize dataframes that will be populated by database queries
# These will remain empty if the database connection fails
df_central = pd.DataFrame()
df_central_mod = pd.DataFrame()
df_central_mod_co = pd.DataFrame()
cmg_ponderado_96h = pd.DataFrame()
merged_df = pd.DataFrame()
filtered_df = pd.DataFrame()  # Initialize filtered_df with empty DataFrame

# Initialize variables with default values in case DB connection fails
ultimo_tracking = "N/A"
ultimo_mod_rio = "N/A"
central_referencia_charrua = "No Activo"
afecto_desacople_charrua = "No Activo"
cmg_charrua = 0.0
central_referencia_quillota = "No Activo"
afecto_desacople_quillota = "No Activo"
cmg_quillota = 0.0
estado_generacion_la = False
estado_generacion_q = False
costo_operacional_la = 0.0
costo_operacional_la_base = 0.0
costo_operacional_q = 0.0
costo_operacional_q_base = 0.0
row_cmg_la = 0.0
row_cmg_quillota = 0.0

# Cacheable fetchers to reduce repeated DB reads on reruns
@st.cache_data(show_spinner=False, ttl=60)
def get_cmg_ponderado_cached(unixtime_in: int, time_range_hours: int):
    """Fetch cmg ponderado data for a time window; cached for 60s to avoid re-querying every rerun."""
    local_engine = get_engine()
    SessionLocal = establecer_session(local_engine)
    with session_scope(SessionLocal) as session:
        df = pd.DataFrame(query_cmg_ponderado_by_time(session, unixtime_in, time_range_hours))
    return df.copy()

@st.cache_data(show_spinner=False, ttl=60)
def get_cmg_programados_cached(name_central: str, date_in: str, conn_status: bool, api_host: str, api_port: str):
    """Cache CMg programados per central/date for 60s to avoid repeated calls."""
    if conn_status:
        SessionLocal = establecer_session(get_engine())
        with session_scope(SessionLocal) as session:
            result = get_cmg_programados(name_central, date_in=date_in, session=session)
    else:
        result = get_cmg_programados(name_central, date_in=date_in, host=api_host, port=api_port)
    return result.copy() if isinstance(result, dict) else result

#############################################################
###################  Consultas    ###########################
#############################################################

if engine is not None:
    Session = establecer_session(engine)
    if Session is not None:
        try:
            with session_scope(Session) as session:
                # Latest desacople events per barra
                desacople_event_charrua = get_latest_desacople_event(session, 'CHARRUA__220') or {}
                desacople_event_quillota = get_latest_desacople_event(session, 'QUILLOTA__220') or {}
                # last row tracking_cmg
                tracking_cmg_last_row = retrieve_tracking_coordinador(session)
                ultimo_tracking = tracking_cmg_last_row[1]
                ultimo_mod_rio = tracking_cmg_last_row[3]

                # get last entry cmg_tiempo_real , afecto_desacople, central_referencia
                central_referencia_charrua, desacople_charrua, cmg_charrua = query_values_last_desacople_bool(
                    session, barra_transmision='CHARRUA__220')

                if desacople_charrua:
                    afecto_desacople_charrua = 'Activo'
                else:
                    afecto_desacople_charrua = 'No Activo'

                central_referencia_quillota, desacople_quillota, cmg_quillota = query_values_last_desacople_bool(
                    session, barra_transmision='QUILLOTA__220')

                if desacople_quillota:
                    afecto_desacople_quillota = 'Activo'
                else:
                    afecto_desacople_quillota = 'No Activo'

                cmg_charrua = round(float(cmg_charrua), 2)
                cmg_quillota = round(float(cmg_quillota), 2)
                
                # consulta de datos cmg_ponderado 48 horas previas
                cmg_ponderado_96h = get_cmg_ponderado_cached(unixtime, st.session_state['time_range'])
                # Try multiple date formats to handle mixed formats
                if not cmg_ponderado_96h.empty and 'timestamp' in cmg_ponderado_96h.columns:
                    # Ensure timestamp column is string type
                    cmg_ponderado_96h['timestamp'] = cmg_ponderado_96h['timestamp'].astype(str)
                    
                    # Try to convert with multiple formats in sequence
                    try:
                        cmg_ponderado_96h['timestamp'] = pd.to_datetime(cmg_ponderado_96h["timestamp"], format="%d.%m.%y %H:%M:%S", errors='coerce')
                    except ValueError:
                        pass
                        
                    # Fill NaT values with next format attempt
                    mask = cmg_ponderado_96h['timestamp'].isna()
                    if mask.any():
                        try:
                            cmg_ponderado_96h.loc[mask, 'timestamp'] = pd.to_datetime(
                                cmg_ponderado_96h.loc[mask, "timestamp"], 
                                format="%Y-%m-%d %H:%M:%S", 
                                errors='coerce'
                            )
                        except ValueError:
                            pass
                    
                    # Last attempt with auto-detection for any remaining NaT values
                    mask = cmg_ponderado_96h['timestamp'].isna()
                    if mask.any():
                        try:
                            cmg_ponderado_96h.loc[mask, 'timestamp'] = pd.to_datetime(
                                cmg_ponderado_96h.loc[mask, "timestamp"],
                                errors='coerce'
                            )
                        except Exception as e:
                            st.warning(f"Some timestamp values could not be parsed: {e}")
                    
                    # For any remaining NaT values, use current datetime
                    mask = cmg_ponderado_96h['timestamp'].isna()
                    if mask.any():
                        # Convert chile_datetime to pandas datetime64 compatible format first
                        chile_dt_pandas = pd.to_datetime(chile_datetime.strftime('%Y-%m-%d %H:%M:%S'))
                        cmg_ponderado_96h.loc[mask, 'timestamp'] = chile_dt_pandas
                        
                    # Ensure timestamp is actually datetime type before using .dt accessor
                    if not pd.api.types.is_datetime64_any_dtype(cmg_ponderado_96h['timestamp']):
                        try:
                            # Re-convert to make sure it's actually datetime type
                            cmg_ponderado_96h['timestamp'] = pd.to_datetime(cmg_ponderado_96h['timestamp'], errors='coerce')
                        except Exception as e:
                            st.warning(f"Failed to ensure timestamp is datetime type: {e}")
                            # Create default timestamp as fallback
                            cmg_ponderado_96h['timestamp'] = chile_datetime
                    
                    # Add fecha and hora columns explicitly with safe checks
                    try:
                        cmg_ponderado_96h['fecha'] = cmg_ponderado_96h['timestamp'].dt.strftime('%Y-%m-%d')
                        cmg_ponderado_96h['hora'] = cmg_ponderado_96h['timestamp'].dt.strftime('%H:%M:%S')
                    except Exception as e:
                        st.warning(f"Error using dt accessor: {e}. Using string methods instead.")
                        # Fallback to string representation if dt accessor fails
                        cmg_ponderado_96h['fecha'] = chile_datetime.strftime('%Y-%m-%d')
                        cmg_ponderado_96h['hora'] = chile_datetime.strftime('%H:%M:%S')
                
                # Drop unix_time column if it exists
                if 'unix_time' in cmg_ponderado_96h.columns:
                    cmg_ponderado_96h.drop(['unix_time'], axis=1, inplace=True)

                # Add central column for mapping to central data
                if 'barra_transmision' in cmg_ponderado_96h.columns:
                    cmg_ponderado_96h['central'] = cmg_ponderado_96h['barra_transmision'].replace({
                        'CHARRUA__220': 'Los Angeles', 
                        'QUILLOTA__220': 'Quillota',
                        'charrua__220': 'Los Angeles', 
                        'quillota__220': 'Quillota',
                        'CHARRUA_22O': 'Los Angeles',
                        'QUILLOTA_22O': 'Quillota',
                        'charrua_22o': 'Los Angeles',
                        'quillota_22o': 'Quillota'
                    })
                
                # consulta estado central 
                last_row_la = query_last_row_central(session, 'Los Angeles') 
                last_row_q = query_last_row_central(session, 'Quillota')

                # Consultar ultimas entradas de table Central: 
                df_central = query_central_table(session, num_entries=20)
                if not df_central.empty and 'margen_garantia' in df_central.columns:
                    df_central['margen_garantia'] = df_central['margen_garantia'].astype(float)
                
                df_central_mod = query_central_table_modifications(session, num_entries=20)
                if not df_central_mod.empty and 'margen_garantia' in df_central_mod.columns:
                    df_central_mod['margen_garantia'] = df_central_mod['margen_garantia'].astype(float)

                # Extract required columns safely
                required_cols = ['nombre', 'costo_operacional', 'fecha_registro']
                if not df_central_mod.empty and all(col in df_central_mod.columns for col in required_cols):
                    df_central_mod_co = df_central_mod.loc[:, required_cols]
                else:
                    # Create empty dataframe with required columns if missing
                    df_central_mod_co = pd.DataFrame(columns=required_cols)
                
                # Handle possible different date formats in fecha_registro
                def safe_reformat_to_iso(date_string):
                    if pd.isna(date_string):
                        return None
                    try:
                        # First try the expected format
                        dt_object = datetime.strptime(date_string, '%d.%m.%y %H:%M:%S')
                        return dt_object.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            # Check if it's already in ISO format
                            dt_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
                            return date_string
                        except ValueError:
                            # If all else fails, return the original string
                            return date_string
                
                # Apply the safe conversion function if dataframe is not empty
                if not df_central_mod_co.empty and 'fecha_registro' in df_central_mod_co.columns:
                    df_central_mod_co['fecha_registro'] = df_central_mod_co['fecha_registro'].apply(safe_reformat_to_iso)
                
                    # Eliminar todas las entradas que tenga mas de 96 horas.
                    try:
                        df_central_mod_co['fecha_registro'] = pd.to_datetime(df_central_mod_co['fecha_registro'], errors='coerce')
                    except Exception as e:
                        st.warning(f"Error converting dates: {e}")
                        # Provide a default dataframe if conversion fails
                        if not df_central_mod_co.empty:
                            # Create a Series of datetime values with the same length as the DataFrame
                            df_central_mod_co['fecha_registro'] = pd.Series([chile_datetime] * len(df_central_mod_co), index=df_central_mod_co.index)
                        else:
                            # If DataFrame is empty, just create an empty Series
                            df_central_mod_co['fecha_registro'] = pd.Series(dtype='datetime64[ns]')
                
                # Filter out rows where the date is more than 4 days ago - with proper safety checks
                four_days_ago = chile_datetime - timedelta(days=4)
                four_days_ago = four_days_ago.replace(tzinfo=None)    

                # Initialize filtered_df with safety checks
                if not df_central_mod_co.empty and 'fecha_registro' in df_central_mod_co.columns:
                    # Make sure all fecha_registro values are datetime objects
                    if not pd.api.types.is_datetime64_any_dtype(df_central_mod_co['fecha_registro']):
                        # Try to convert to datetime
                        try:
                            df_central_mod_co['fecha_registro'] = pd.to_datetime(df_central_mod_co['fecha_registro'], errors='coerce')
                        except Exception as e:
                            st.warning(f"Error converting fecha_registro to datetime: {e}")
                    
                    # Only use rows with valid datetime values for filtering
                    valid_mask = ~df_central_mod_co['fecha_registro'].isna()
                    if valid_mask.any():
                        filtered_df = df_central_mod_co[valid_mask & (df_central_mod_co['fecha_registro'] > four_days_ago)]
                    else:
                        filtered_df = pd.DataFrame(columns=df_central_mod_co.columns)
                        st.warning("No valid dates found in fecha_registro. Using empty filtered dataframe.")
                else:
                    # Create empty DataFrame with same columns
                    filtered_df = pd.DataFrame(columns=df_central_mod_co.columns)

                # Safe access to last_row_la and last_row_q
                try:
                    # Get the latest operational status from status_central table
                    status_la = get_latest_status_central(session, 'Los Angeles')
                    status_q = get_latest_status_central(session, 'Quillota')
                    
                    # Set generation status based on the status_central table values
                    estado_generacion_la = status_la == 'ON' if status_la else False
                    estado_generacion_q = status_q == 'ON' if status_q else False
                    
                    def safe_float(value):
                        try:
                            return float(value)
                        except (TypeError, ValueError):
                            return 0.0

                    # Access costo_operacional directly from index 9 where it's stored in the result
                    costo_operacional_la = round(safe_float(last_row_la[9]), 2) if last_row_la and len(last_row_la) > 9 else 0.0
                    # Validate that we have factor_motor at index 10
                    factor_motor_la = round(safe_float(last_row_la[10]), 2) if last_row_la and len(last_row_la) > 10 else 0.0
                    costo_operacional_la_base = costo_operacional_la - factor_motor_la if costo_operacional_la > 0 else 0.0
                    
                    costo_operacional_q = round(safe_float(last_row_q[9]), 2) if last_row_q and len(last_row_q) > 9 else 0.0
                    factor_motor_q = round(safe_float(last_row_q[10]), 2) if last_row_q and len(last_row_q) > 10 else 0.0
                    costo_operacional_q_base = costo_operacional_q - factor_motor_q if costo_operacional_q > 0 else 0.0
                    
                    # Log the retrieved values
                    logging.info(f"Retrieved costs - LA: {costo_operacional_la}, Quillota: {costo_operacional_q}")
                except (IndexError, TypeError, ValueError) as e:
                    st.warning(f"Error processing central data: {e}")
                    # Set defaults
                    estado_generacion_la = False
                    estado_generacion_q = False
                    costo_operacional_la = 0.0
                    costo_operacional_la_base = 0.0
                    costo_operacional_q = 0.0
                    costo_operacional_q_base = 0.0
                
                # Create the minimal DataFrame we need if cmg_ponderado_96h is empty
                if cmg_ponderado_96h.empty:
                    cmg_ponderado = pd.DataFrame({
                        'timestamp': [chile_datetime.strftime('%Y-%m-%d %H:%M:%S')],
                        'barra_transmision': ['CHARRUA__220'],
                        'fecha': [chile_datetime.strftime('%Y-%m-%d')],
                        'hora': [chile_datetime.strftime('%H:%M:%S')],
                        'central': ['Los Angeles'],
                        'cmg_ponderado': [0.0]
                    })
                else:
                    # Use the actual cmg_ponderado_96h data
                    cmg_ponderado = cmg_ponderado_96h.copy()
                    
                    # Normalize the barra_transmision values for consistent filtering
                    if 'barra_transmision' in cmg_ponderado.columns:
                        cmg_ponderado['barra_transmision'] = cmg_ponderado['barra_transmision'].str.upper()
                        # Map different bar name formats to consistent values for filtering
                        cmg_ponderado['barra_transmision'] = cmg_ponderado['barra_transmision'].replace({
                            'CHARRUA_22O': 'CHARRUA__220',
                            'QUILLOTA_22O': 'QUILLOTA__220',
                            'CHARRUA_220': 'CHARRUA__220',
                            'QUILLOTA_220': 'QUILLOTA__220',
                            'CHARRUA': 'CHARRUA__220',
                            'QUILLOTA': 'QUILLOTA__220'
                        })

                # Filter the data for each specific area
                charrua_filters = ['CHARRUA__220', 'CHARRUA_220', 'CHARRUA_22O', 'CHARRUA', 'charrua_22o']
                quillota_filters = ['QUILLOTA__220', 'QUILLOTA_220', 'QUILLOTA_22O', 'QUILLOTA', 'quillota_22o']
                
                # Filter rows for Los Angeles/Charrua
                cmg_ponderado_la = cmg_ponderado[
                    cmg_ponderado['barra_transmision'].str.upper().isin([f.upper() for f in charrua_filters])
                ]
                
                # Filter rows for Quillota
                cmg_ponderado_quillota = cmg_ponderado[
                    cmg_ponderado['barra_transmision'].str.upper().isin([f.upper() for f in quillota_filters])
                ]
                
                # Check if dataframes are not empty before accessing elements
                if not cmg_ponderado_quillota.empty:
                    try:
                        row_cmg_quillota = round(float(cmg_ponderado_quillota.iloc[-1]['cmg_ponderado']), 2)
                    except (IndexError, ValueError, TypeError) as e:
                        logging.error(f"Error accessing Quillota CMG value: {e}")
                        row_cmg_quillota = 0.0
                else:
                    row_cmg_quillota = 0.0
                    
                if not cmg_ponderado_la.empty:
                    try:
                        row_cmg_la = round(float(cmg_ponderado_la.iloc[-1]['cmg_ponderado']), 2)
                    except (IndexError, ValueError, TypeError) as e:
                        logging.error(f"Error accessing Los Angeles CMG value: {e}")
                        row_cmg_la = 0.0
                else:
                    row_cmg_la = 0.0

                # Get status history from StatusCentral table
                # This will directly provide the data we need for the "Últimos Movimientos Encendido/Apagado" table
                try:
                    if session is not None:
                        merged_df = get_status_central_history(
                            session_in=session, 
                            limit=50, 
                            centrals=['Los Angeles', 'Quillota']
                        )
                    else:
                        logging.error("Cannot get status history: session is None")
                        merged_df = pd.DataFrame()
                except Exception as e:
                    logging.error(f"Error getting status history: {e}")
                    merged_df = pd.DataFrame()
                
                # If merged_df is empty (no status history yet), create a minimal DataFrame
                if merged_df.empty:
                    merged_df = pd.DataFrame({
                        'central': ['Los Angeles', 'Quillota'],
                        'timestamp': [chile_datetime.strftime('%Y-%m-%d %H:%M:%S')] * 2,
                        'fecha': [chile_datetime.strftime('%Y-%m-%d')] * 2,
                        'hora': [chile_datetime.strftime('%H:%M:%S')] * 2,
                        'cmg_ponderado': [row_cmg_la, row_cmg_quillota],
                        'costo_operacional': [costo_operacional_la, costo_operacional_q],
                        'generando': [estado_generacion_la, estado_generacion_q],
                        'status_operacional': [
                            'ON' if estado_generacion_la else 'OFF',
                            'ON' if estado_generacion_q else 'OFF'
                        ]
                    })

                # Data for "Últimos Movimientos Encendido/Apagado" comes directly from StatusCentral table
                # We've removed the unnecessary code that was overwriting this data with complex merging logic

        except Exception as e:
            st.error(f"Error accessing database: {str(e)}")
            # Log the error
            logging.error(f"Database query error: {str(e)}")
    else:
        st.error("Failed to create database session")
else:
    st.error("Database connection failed. Using fallback data.")

############# Queries externas #############
# Use direct database access by passing the session parameter
cmg_programados_quillota = get_cmg_programados_cached('Quillota', fecha, CONN_STATUS, API_HOST, API_PORT)
cmg_programados_la = get_cmg_programados_cached('Los Angeles', fecha, CONN_STATUS, API_HOST, API_PORT)
cmg_online = get_costo_marginal_online_hora(fecha_gte=fecha, fecha_lte=fecha, barras=['Quillota', 'Charrua'], hora_in=hora_redondeada, user_key=USER_KEY)

# check if cmg_online is empty
if not cmg_online:
    cmg_online = {'Charrua': 'Not Available', 'Quillota': 'Not Available'}
else:
    cmg_online = {key : round(cmg_online[key], 2) for key in cmg_online}

#########################################################
################### WEBSITE DESIGN ######################
#########################################################
tab1, tab2, tab3 = st.tabs(["Monitoreo", "Atributos", "Descarga Archivos"])

with tab1:
    # st.header("Monitoreo")
    # First thing, check connection and show appropriate notification
    if not CONN_STATUS:
        add_notification("No se pudo conectar a la base de datos. Usando datos de prueba.", type="warning", duration=10)
    
    # Check for mobile device and show warning if needed
    if is_mobile() and not st.session_state['mobile_warning_shown']:
        add_notification("Esta aplicación está optimizada para pantallas más grandes. Algunas funcionalidades pueden verse afectadas en dispositivos móviles.", type="info", duration=15)
        st.session_state['mobile_warning_shown'] = True
    
    # Update data querying to respect the selected time range
    Session = establecer_session(engine)
    if Session is not None:
        try:
            with session_scope(Session) as session:
                # Modify this to respect the time range setting
                cmg_ponderado_96h_update = get_cmg_ponderado_cached(
                    unixtime,
                    st.session_state['time_range']  # Use the selected time range instead of fixed 96
                )
                
                # Only update the global variable if we got valid data
                if not cmg_ponderado_96h_update.empty:
                    cmg_ponderado_96h = cmg_ponderado_96h_update
        except Exception as e:
            st.error(f"Error querying data: {str(e)}")
    else:
        st.warning("Could not create database session. Using cached data.")

    ################## DATOS Centrales ##############################################
    # Create a unified card template for both locations
    def friendly_delta(dt):
        """Return a short human delta like 'hace 2h'."""
        if not hasattr(dt, 'timestamp'):
            return "N/D"
        delta = datetime.now() - dt
        minutes = int(delta.total_seconds() // 60)
        if minutes < 1:
            return "hace instantes"
        if minutes < 60:
            return f"hace {minutes}m"
        hours = minutes // 60
        if hours < 48:
            return f"hace {hours}h"
        days = hours // 24
        return f"hace {days}d"

    def display_central_card(name, estado_generacion, cmg_calculado, costo_operacional, cmg_online, 
                           cmg_programado, central_referencia, afecto_desacople, hora_redondeada, desacople_event):
        """Create a unified card for central data display"""
        # Card container with consistent styling
        st.markdown(f'<h2 class="section-title" style="text-align: center;">{name}</h2>', unsafe_allow_html=True)

        # Generation status with icon - make text bold and use stronger colors
        if estado_generacion:
            status_color = "#00b300"  # Stronger green
            status_text = "ENCENDIDO"
        else:
            status_color = "#cc0000"  # Stronger red
            status_text = "APAGADO"
            
        st.markdown(f'<div style="text-align: center; margin-bottom: 1rem;"><span style="color:{status_color}; font-size: 1.5em; font-weight: bold;">● {status_text}</span></div>', unsafe_allow_html=True)

        # Main metrics in a 2-column grid
        cols = st.columns(2)
        
        # Define a simple helper for consistent metric cards
        def metric_card(col, label, value, tooltip_key=None):
            tooltip_html = f'<div class="metric-label">{tooltip(label, tooltip_key) if tooltip_key else label}</div>'
            col.markdown(f'''
            <div class="metric-card">
                {tooltip_html}
                <div class="metric-value">{value}</div>
            </div>
            ''', unsafe_allow_html=True)
        
        # Row 1: Main metrics
        metric_card(cols[0], "CMg Calculado", cmg_calculado, "cmg_calculado")
        metric_card(cols[1], "Costo Operacional", costo_operacional, "costo_operacional")
        
        # Row 2: CMg metrics
        metric_card(cols[0], f"CMg Online - {hora_redondeada}", cmg_online, "cmg_online")
        
        if hora_redondeada_cmg_programados in cmg_programado:
            prog_value = round(float(cmg_programado[hora_redondeada_cmg_programados]), 2)
        else:
            prog_value = "N/D"
        metric_card(cols[1], f"CMg Programado - {hora_redondeada}", prog_value, "cmg_programado")
        
        # Row 3: Status metrics
        metric_card(cols[0], "Central referencia", central_referencia, "central_referencia")
        
        # Use stronger colors and bold text for zone status
        if afecto_desacople == "Activo":
            status_color = "#ff8c00"  # Stronger orange for active
            status_text = "Activo"
        else:
            status_color = "#00b300"  # Stronger green
            status_text = "No Activo"
            
        cols[1].markdown(f'''
        <div class="metric-card">
            <div class="metric-label">{tooltip("Zona en desacople", "zona_desacople")}</div>
            <div class="metric-value"><span style="color:{status_color}; font-weight: bold;">{status_text}</span></div>
        </div>
        ''', unsafe_allow_html=True)

        # Desacople history summary
        if desacople_event:
            evento_estado = (desacople_event.get("estado") or "N/D").upper()
            evento_fecha = desacople_event.get("detected_at")
            fecha_txt = evento_fecha.strftime('%Y-%m-%d %H:%M') if hasattr(evento_fecha, 'strftime') else str(evento_fecha) or "N/D"
            delta_txt = friendly_delta(evento_fecha) if hasattr(evento_fecha, 'timestamp') else "N/D"
            cols_history = st.columns(2)
            metric_card(cols_history[0], "Último evento desacople", evento_estado)
            metric_card(cols_history[1], "Cuándo", f"{fecha_txt} · {delta_txt}")
        else:
            cols_history = st.columns(1)
            metric_card(cols_history[0], "Último evento desacople", "Sin registros")

    # Display the two central cards in a two-column layout
    central_cols = st.columns(2)
    
    with central_cols[0]:
        display_central_card(
            name="Los Angeles",
            estado_generacion=estado_generacion_la,
            cmg_calculado=row_cmg_la,
            costo_operacional=costo_operacional_la,
            cmg_online=cmg_online['Charrua'],
            cmg_programado=cmg_programados_la,
            central_referencia=central_referencia_charrua,
            afecto_desacople=afecto_desacople_charrua,
            hora_redondeada=hora_redondeada,
            desacople_event=desacople_event_charrua
        )
    
    with central_cols[1]:
        display_central_card(
            name="Quillota",
            estado_generacion=estado_generacion_q,
            cmg_calculado=row_cmg_quillota,
            costo_operacional=costo_operacional_q,
            cmg_online=cmg_online['Quillota'],
            cmg_programado=cmg_programados_quillota,
            central_referencia=central_referencia_quillota,
            afecto_desacople=afecto_desacople_quillota,
            hora_redondeada=hora_redondeada,
            desacople_event=desacople_event_quillota
        )

    ################## GRAFICO ##################
    with st.container():
        st.markdown('<h3 class="section-title">Gráfico de CMg Ponderado</h3>', unsafe_allow_html=True)

        costo_operacional_plot_lineas_la = False
        costo_operacional_plot_lineas_quillota = False
        if filtered_df.empty:
            costo_operacional_plot_lineas_quillota = True
            costo_operacional_plot_lineas_la = True
        else:
            cmg_ponderado_96h = pd.concat([cmg_ponderado_96h, filtered_df], axis=1)
            if filtered_df[filtered_df['nombre'] == 'Quillota'].empty:
                costo_operacional_plot_lineas_quillota = True
            if filtered_df[filtered_df['nombre'] == 'Los Angeles'].empty:   
                costo_operacional_plot_lineas_la = True

        # Filter data based on user selection
        plot_data = cmg_ponderado_96h.copy()
        if not st.session_state['show_charrua']:
            plot_data = plot_data[plot_data['barra_transmision'] != 'CHARRUA__220']
        if not st.session_state['show_quillota']:
            plot_data = plot_data[plot_data['barra_transmision'] != 'QUILLOTA__220']
        
        # Ensure we have data to plot
        if plot_data.empty:
            st.warning("No hay datos para mostrar con los filtros seleccionados. Por favor, active al menos una barra.")
        else:
            # Normalize the barra_transmision column values to ensure consistent casing
            if 'barra_transmision' in plot_data.columns:
                # Make a new column with normalized values
                plot_data['barra_transmision_original'] = plot_data['barra_transmision']
                # Convert to uppercase for consistency
                plot_data['barra_transmision'] = plot_data['barra_transmision'].str.upper()
                # Fix formatting if needed
                plot_data['barra_transmision'] = plot_data['barra_transmision'].replace({
                    'CHARRUA_22O': 'CHARRUA__220',
                    'QUILLOTA_22O': 'QUILLOTA__220'
                })
            
            # Create a custom color palette with more professional colors
            palette = {
                "CHARRUA__220": "#2C7BB6", 
                "QUILLOTA__220": "#D7301F",
                "charrua__220": "#2C7BB6", 
                "quillota__220": "#D7301F",
                "charrua_22o": "#2C7BB6",
                "quillota_22o": "#D7301F"
            }
            
            # Plot based on selected chart type
            if st.session_state['chart_type'] == 'line':
                # Create Plotly line chart
                fig = px.line(
                    plot_data, 
                    x="timestamp", 
                    y="cmg_ponderado", 
                    color="barra_transmision",
                    color_discrete_map=palette,
                    title=None,  # Remove title, we use section header instead
                    labels={"timestamp": "Fecha y Hora", "cmg_ponderado": "Costo Marginal (CMg)"}
                )
                
            elif st.session_state['chart_type'] == 'area':
                # Create Plotly area chart
                fig = px.area(
                    plot_data, 
                    x="timestamp", 
                    y="cmg_ponderado", 
                    color="barra_transmision",
                    color_discrete_map=palette,
                    title=None,  # Remove title
                    labels={"timestamp": "Fecha y Hora", "cmg_ponderado": "Costo Marginal (CMg)"}
                )
                
            else:  # bar chart
                # For bar chart, use Plotly bar chart
                fig = px.bar(
                    plot_data, 
                    x="timestamp", 
                    y="cmg_ponderado", 
                    color="barra_transmision",
                    color_discrete_map=palette,
                    title=None,  # Remove title
                    labels={"timestamp": "Fecha y Hora", "cmg_ponderado": "Costo Marginal (CMg)"}
                )
                
            # Improve the layout with better styling
            fig.update_layout(
                legend_title="Barras de Transmisión",
                xaxis_title="Fecha y Hora",
                yaxis_title="Costo Marginal (CMg)",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                margin=dict(l=20, r=20, t=20, b=20),
                plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
                xaxis=dict(
                    gridcolor='rgba(211,211,211,0.3)',  # Lighter grid
                    showgrid=True
                ),
                yaxis=dict(
                    gridcolor='rgba(211,211,211,0.3)',  # Lighter grid
                    showgrid=True
                )
            )
            
            # Improve line chart appearance
            if st.session_state['chart_type'] == 'line':
                fig.update_traces(
                    line=dict(width=3),  # Thicker lines
                    mode='lines'  # Remove markers
                )
            
            # Add horizontal lines for operational costs if enabled
            if st.session_state['show_operational_costs']:
                if costo_operacional_plot_lineas_quillota and st.session_state['show_quillota']:
                    fig.add_hline(
                        y=costo_operacional_q,
                        line_dash="dash",
                        line_color="#D7301F",
                        annotation_text="Costo Operacional - Quillota",
                        annotation_position="top right"
                    )
                if costo_operacional_plot_lineas_la and st.session_state['show_charrua']:
                    fig.add_hline(
                        y=costo_operacional_la,
                        line_dash="dash",
                        line_color="#2C7BB6",
                        annotation_text="Costo Operacional - Los Angeles",
                        annotation_position="top right"
                    )
            
            # Show the Plotly figure in Streamlit with a consistent container width
            st.plotly_chart(fig, use_container_width=True)
            
            # Add a caption with data summary
            if not plot_data.empty and 'timestamp' in plot_data.columns:
                min_date = pd.to_datetime(plot_data['timestamp']).min()
                max_date = pd.to_datetime(plot_data['timestamp']).max()
                st.caption(f"Visualizando datos desde {min_date.strftime('%Y-%m-%d %H:%M')} hasta {max_date.strftime('%Y-%m-%d %H:%M')}")
            
        # Create a two-column layout for the data tables with better styling
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        st.markdown('<h3 class="section-title">Datos detallados</h3>', unsafe_allow_html=True)
        
        col_data_1, col_data_2 = st.columns(2)

        with col_data_1:
            st.markdown('<h4 style="font-size: 1.1rem; font-weight: 600;">Costos Marginales Ponderados</h4>', unsafe_allow_html=True)
            cmg_ponderado_96h['cmg_ponderado'] = cmg_ponderado_96h['cmg_ponderado'].round(2)
            cmg_ponderado_96h['Central'] = cmg_ponderado_96h['barra_transmision'].replace({'CHARRUA__220':'Los Angeles', 'QUILLOTA__220': 'Quillota'})
            
            # Prepare data with better column names and sorting
            display_df = cmg_ponderado_96h.rename(columns={
                'barra_transmision': 'Alimentador', 
                'timestamp': 'Fecha y Hora', 
                'cmg_ponderado': 'CMg Ponderado'
            }).sort_values('Fecha y Hora', ascending=False).head(10)
            
            # Add search and filter capability
            with st.expander("Filtrar datos"):
                filter_value = st.text_input("Buscar por texto", placeholder="Ingrese texto para filtrar")
                if filter_value:
                    displayed_data = display_df[display_df.astype(str).apply(lambda row: row.str.contains(filter_value, case=False).any(), axis=1)]
                else:
                    displayed_data = display_df
            
            # Show the dataframe with better column configuration
            st.dataframe(
                displayed_data, 
                use_container_width=True, 
                height=300,
                column_config={
                    "Fecha y Hora": st.column_config.DatetimeColumn(
                        "Fecha y Hora",
                        format="DD/MM/YYYY HH:mm"
                    ),
                    "CMg Ponderado": st.column_config.NumberColumn(
                        "CMg Ponderado",
                        format="%.2f"
                    )
                }
            )

        with col_data_2:
            st.markdown('<h4 style="font-size: 1.1rem; font-weight: 600;">Últimos Movimientos Encendido/Apagado</h4>', unsafe_allow_html=True)
            
            # Add export options for the data
            export_cols = st.columns(2)
            with export_cols[0]:
                if st.button("📊 Exportar a CSV", use_container_width=True):
                    csv = merged_df.to_csv().encode('utf-8')
                    st.download_button(
                        label="Descargar CSV",
                        data=csv,
                        file_name="movimientos_encendido_apagado.csv",
                        mime="text/csv",
                        key="download_csv_btn",
                        use_container_width=True
                    )
            with export_cols[1]:
                if st.button("📈 Ver estadísticas", use_container_width=True):
                    with st.expander("Estadísticas", expanded=True):
                        st.write("Estadísticas básicas:")
                        st.write(merged_df.describe())
            
            # Show the dataframe with better column configuration
            st.dataframe(
                merged_df.sort_values('fecha', ascending=False) if 'fecha' in merged_df.columns else merged_df, 
                use_container_width=True, 
                height=300,
                column_config={
                    "central": "Central",
                    "costo_operacional": st.column_config.NumberColumn(
                        "Costo Operacional",
                        format="%.2f"
                    ),
                    "cmg_ponderado": st.column_config.NumberColumn(
                        "CMg Ponderado",
                        format="%.2f"
                    ),
                    "generando": st.column_config.CheckboxColumn(
                        "Generando"
                    ),
                    "status_operacional": st.column_config.TextColumn(
                        "Estado"
                    )
                }
            )
        
        st.markdown('</div>', unsafe_allow_html=True)

    # After the main chart, add a new section for status pie charts
    st.markdown('<h3 class="section-title">Distribución de Estados</h3>', unsafe_allow_html=True)
    
    # Create two columns for the pie charts
    pie_cols = st.columns(2)
    
    # Create session for querying status data
    status_session = None
    if CONN_STATUS:
        try:
            Session = establecer_session(engine)
            if Session is not None:
                with session_scope(Session) as session:
                    status_session = session
        except Exception as e:
            logging.error(f"Error creating session for status charts: {e}")
    
    # Create and display the pie charts
    merged_df.head()
    with pie_cols[0]:
        # Pass the dataframe with Los Angeles data
        la_df = merged_df[merged_df['central'] == 'Los Angeles']
        la_chart = create_status_piechart(la_df, 'Los Angeles', st.session_state['time_range'])
        st.plotly_chart(la_chart, use_container_width=True)
        
    with pie_cols[1]:
        # Pass the dataframe with Quillota data
        q_df = merged_df[merged_df['central'] == 'Quillota']
        q_chart = create_status_piechart(q_df, 'Quillota', st.session_state['time_range'])
        st.plotly_chart(q_chart, use_container_width=True)
    
    # Add explanatory text
    st.caption(f"Los gráficos muestran la distribución del tiempo en ENCENDIDO/APAGADO durante las últimas {st.session_state['time_range']} horas.")

    # Continue with the original data tables section
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.markdown('<h3 class="section-title">Datos detallados</h3>', unsafe_allow_html=True)

################## Modificación de parametros ##################

with tab2:
    st.header("Modificación de Parámetros")
    
    with st.container():        
        col_a, col_b = st.columns((1, 2))
        
        with col_a:
            st.markdown('<h3 class="section-title">Fórmula de Costo Operacional</h3>', unsafe_allow_html=True)
            
            # Formula styling with MathJax
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 0.5rem; margin-bottom: 1.5rem;">
                <p style="text-align: center; font-size: 0.9rem;">
                    $$Costo\,Operacional = ((Porcentaje\,Brent \times Precio\,Brent) + Tasa\,Proveedor) \times Factor\,Motor + Tasa\,Central + Margen\,de\,Garantía$$
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # Form with better styling
            st.markdown('<div style="background-color: #f8f9fa; padding: 1.5rem; border-radius: 0.5rem;">', unsafe_allow_html=True)
            
            editor = st.text_input('Nombre del editor', 'Cristian Valls', 
                                  placeholder="Ingrese su nombre")
            
            central_seleccion = st.radio(
                "Seleccionar central a modificar:",
                ('Los Angeles', 'Quillota'),
                horizontal=True
            )
            
            # Add description for the central selection
            if central_seleccion == 'Los Angeles':
                st.markdown('<p style="color: #666; font-size: 0.9rem;">Central térmica Los Angeles</p>', unsafe_allow_html=True)
            else:
                st.markdown('<p style="color: #666; font-size: 0.9rem;">Central térmica Quillota</p>', unsafe_allow_html=True)
            
            st.markdown('<p style="font-weight: 600; margin-top: 1rem;">Parámetros a modificar:</p>', unsafe_allow_html=True)
            
            options = st.multiselect(
                'Seleccionar atributos',
                ['Porcentaje Brent', 'Tasa Proveedor', 'Factor Motor', 'Tasa Central', 'Margen Garantia'],
                ['Margen Garantia']
            )

            dict_data = {}
            
            # Create a clean parameter input layout
            if options:
                st.markdown('<div style="margin-top: 1rem;">', unsafe_allow_html=True)
                
                if 'Porcentaje Brent' in options:
                    porcentaje_brent = st.number_input('Porcentaje Brent:', 
                                                      value=0.0, 
                                                      format="%.4f",
                                                      help="Ejemplo: 0.14")
                    dict_data['porcentaje_brent'] = porcentaje_brent
                    
                if 'Tasa Proveedor' in options:
                    tasa_proveedor = st.number_input('Tasa Proveedor:', 
                                                    value=0.0, 
                                                    format="%.2f",
                                                    help="Ejemplo: 4.12")
                    dict_data['tasa_proveedor'] = tasa_proveedor
                    
                if 'Factor Motor' in options:
                    factor_motor = st.number_input('Factor Motor:', 
                                                  value=0.0, 
                                                  format="%.2f",
                                                  help="Ejemplo: 10.12")
                    dict_data['factor_motor'] = factor_motor
                    
                if 'Tasa Central' in options:
                    tasa_central = st.number_input('Tasa Central:', 
                                                 value=0.0, 
                                                 format="%.2f",
                                                 help="Ejemplo: 8.8")
                    dict_data['tasa_central'] = tasa_central
                    
                if 'Margen Garantia' in options:
                    margen_garantia = st.number_input('Margen Garantía:', 
                                                    value=0.0, 
                                                    format="%.2f",
                                                    help="Ejemplo: -25.0")
                    dict_data['margen_garantia'] = margen_garantia
                
                st.markdown('</div>', unsafe_allow_html=True)
                
                submit_button = st.button('Actualizar Parámetros', type="primary")
                
                if submit_button:
                    try:
                        result = insert_central(central_seleccion, editor, dict_data, host=API_HOST, port=API_PORT)
                        add_notification(f"Atributos de central {central_seleccion} actualizados correctamente", type="success")
                        st.success(f'Atributos de central {central_seleccion} actualizados correctamente')
                        st.json(result)
                    except Exception as error:
                        add_notification(f"Error al actualizar parámetros: {error}", type="error", duration=10)
                        st.error(f'Error al actualizar parámetros: {error}')
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col_b:
            st.markdown('<h3 class="section-title">Información de Costos</h3>', unsafe_allow_html=True)
            
            # Display cost information with better styling
            st.markdown(f'''
            <div class="metric-card" style="margin-bottom: 1rem;">
                <div class="metric-label">Los Angeles - Costo Operacional Basal</div>
                <div class="metric-value">{costo_operacional_la_base}</div>
            </div>
            ''', unsafe_allow_html=True)
            
            st.markdown(f'''
            <div class="metric-card" style="margin-bottom: 1.5rem;">
                <div class="metric-label">Quillota - Costo Operacional Basal</div>
                <div class="metric-value">{costo_operacional_q_base}</div>
            </div>
            ''', unsafe_allow_html=True)
            
            # Change history table
            st.markdown('<h4 style="font-size: 1.1rem; font-weight: 600; margin-top: 1.5rem;">Historial de Cambios</h4>', unsafe_allow_html=True)
            
            # Add filter options
            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                filter_central = st.selectbox(
                    "Filtrar por central", 
                    ["Todas", "Los Angeles", "Quillota"]
                )
            
            # Apply filtering if needed
            if filter_central != "Todas":
                filtered_df_mod = df_central_mod[df_central_mod["nombre"] == filter_central]
            else:
                filtered_df_mod = df_central_mod
                
            # Display the table with better styling
            st.dataframe(
                filtered_df_mod, 
                use_container_width=True,
                height=400,
                column_config={
                    "nombre": "Central",
                    "fecha_registro": "Fecha y hora",
                    "costo_operacional": st.column_config.NumberColumn(
                        "Costo Operacional",
                        format="%.2f"
                    ),
                    "margen_garantia": st.column_config.NumberColumn(
                        "Margen Garantía",
                        format="%.2f"
                    )
                }
            )
            
        st.markdown('</div>', unsafe_allow_html=True)


################## Descarga de Datos ##################

with tab3:
    st.header("Descarga de Datos")
    
    with st.container():        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown('<h3 class="section-title">Selección de Datos</h3>', unsafe_allow_html=True)
            
            # Selection options with better UI
            central_seleccion = st.radio(
                "Seleccionar central para descargar datos", 
                ('Los Angeles', 'Quillota'),
                horizontal=True
            )
            
            if central_seleccion == 'Los Angeles':
                SELECCIONAR = 'charrua_22o'
                st.markdown('<p style="color: #666; font-size: 0.9rem;">Datos asociados a barra Charrúa</p>', unsafe_allow_html=True)
            else:
                SELECCIONAR = 'quillota_22o'
                st.markdown('<p style="color: #666; font-size: 0.9rem;">Datos asociados a barra Quillota</p>', unsafe_allow_html=True)
            
            st.markdown('<p style="margin-top: 1.5rem;"></p>', unsafe_allow_html=True)
            
            # Date picker with better styling
            st.markdown("""
            <style>
                .stDateInput > div > div > input {
                    border-radius: 0.5rem;
                }
            </style>
            """, unsafe_allow_html=True)
            
            st.markdown('<p style="font-weight: 600;">Seleccionar período para la descarga:</p>', unsafe_allow_html=True)
            date_calculate = st.date_input(
                "Fecha para descarga de datos",
                value=datetime(2023, 6, 6).date(),
                min_value=datetime(2023, 5, 1).date(),
                max_value=datetime.now().date(),
                label_visibility="collapsed"
            )
            
            # Convert date_calculate to a Unix timestamp
            datetime_obj = datetime.combine(date_calculate, datetime.min.time())
            # Add timezone info to match chile_datetime
            datetime_obj = datetime_obj.replace(tzinfo=chile_tz)
            unix_timestamp = int(datetime_obj.timestamp())
            
            # Calculate how many hours to query (from selected date to now)
            current_unix = int(chile_datetime.timestamp())
            unix_time_delta = current_unix - unix_timestamp
            horas_delta = max(1, int(unix_time_delta / 3600))  # Ensure at least 1 hour
            
            # Show data time range in a more visible way
            days_diff = (chile_datetime.date() - date_calculate).days
            st.markdown(f'<div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px; margin-bottom: 10px;"><p><strong>Período seleccionado:</strong> {days_diff} días desde {date_calculate.strftime("%d/%m/%Y")} hasta {chile_datetime.strftime("%d/%m/%Y")}</p><p>Total horas: {horas_delta}</p></div>', unsafe_allow_html=True)
            
        with col2:
            st.markdown('<h3 class="section-title">Descargar Archivos</h3>', unsafe_allow_html=True)

            # Initialize cached download data in session state
            if 'cmg_ponderado_descarga' not in st.session_state:
                st.session_state['cmg_ponderado_descarga'] = pd.DataFrame()
            if 'cmg_tiempo_real_descarga' not in st.session_state:
                st.session_state['cmg_tiempo_real_descarga'] = pd.DataFrame()

            # Run the expensive queries only when requested
            fetch_data = st.button("Consultar datos", type="primary", use_container_width=True)

            if fetch_data:
                Session = establecer_session(engine)
                if Session is not None:
                    try:
                        with session_scope(Session) as session:
                            st.info(f"Consultando datos desde {datetime_obj.strftime('%Y-%m-%d')} hasta hoy")
                            st.markdown(f"<p style='font-size:0.8rem; color:gray;'>Solicitando datos para unixtime: {unixtime}, rango: {horas_delta} horas</p>", unsafe_allow_html=True)
                            
                            cmg_ponderado_descarga = get_cmg_ponderado_cached(unixtime, horas_delta)
                            
                            cmg_tiempo_real_data = get_cmg_tiempo_real(session, unix_time_delta)
                            cmg_tiempo_real_descarga = pd.DataFrame(cmg_tiempo_real_data)
                            
                            st.session_state['cmg_ponderado_descarga'] = cmg_ponderado_descarga
                            st.session_state['cmg_tiempo_real_descarga'] = cmg_tiempo_real_descarga
                            
                            if cmg_ponderado_descarga.empty:
                                st.warning("No se encontraron datos de CMg Ponderado para la fecha seleccionada")
                            else:
                                st.success(f"Se encontraron {len(cmg_ponderado_descarga)} registros de CMg Ponderado")
                            
                            if cmg_tiempo_real_descarga.empty:
                                st.warning("No se encontraron datos de CMg Tiempo Real para la fecha seleccionada")
                            else:
                                st.success(f"Se encontraron {len(cmg_tiempo_real_descarga)} registros de CMg Tiempo Real")
                    except Exception as e:
                        st.error(f"Error querying data for download: {str(e)}")
                        st.session_state['cmg_ponderado_descarga'] = pd.DataFrame()
                        st.session_state['cmg_tiempo_real_descarga'] = pd.DataFrame()
                else:
                    st.warning("No se pudo crear sesión de base de datos para la descarga.")
                    st.session_state['cmg_ponderado_descarga'] = pd.DataFrame()
                    st.session_state['cmg_tiempo_real_descarga'] = pd.DataFrame()

            cmg_ponderado_descarga = st.session_state['cmg_ponderado_descarga']
            cmg_tiempo_real_descarga = st.session_state['cmg_tiempo_real_descarga']

            # Preview data
            if not cmg_ponderado_descarga.empty:
                filtered_data = cmg_ponderado_descarga[cmg_ponderado_descarga['barra_transmision'] == SELECCIONAR]
                st.markdown(f"<p>Vista previa ({len(filtered_data)} registros):</p>", unsafe_allow_html=True)
                st.dataframe(
                    filtered_data.head(5),
                    use_container_width=True,
                    height=150
                )
            else:
                st.info("Presiona \"Consultar datos\" para obtener y previsualizar resultados.")
            
            # Style the download buttons
            st.markdown("""
            <style>
                div[data-testid="stDownloadButton"] button {
                    background-color: #4CAF50;
                    color: white;
                    padding: 0.5rem 1rem;
                    border-radius: 0.5rem;
                    border: none;
                    transition: all 0.3s;
                    margin-bottom: 1rem;
                    width: 100%;
                }
                div[data-testid="stDownloadButton"] button:hover {
                    background-color: #45a049;
                    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.2);
                }
            </style>
            """, unsafe_allow_html=True)
            
            # Download buttons with icons
            st.markdown('<div style="margin-top: 2rem;">', unsafe_allow_html=True)
            
            @st.cache_data
            def convert_df(df):
                'seleccionar central a descargar y convertir a csv'
                # IMPORTANT: Cache the conversion to prevent computation on every rerun
                if df.empty:
                    st.warning(f"No hay datos disponibles para {central_seleccion} en la fecha seleccionada.")
                    return "".encode('utf-8')
                
                # Check if barra_transmision column exists
                if 'barra_transmision' not in df.columns:
                    st.warning("Los datos no contienen la columna 'barra_transmision'.")
                    return df.to_csv().encode('utf-8')
                
                # Filter by the selected barra
                filtered_df = df[df['barra_transmision'] == SELECCIONAR]
                
                # Check if any rows match the filter
                if filtered_df.empty:
                    st.warning(f"No hay datos para {SELECCIONAR} en la fecha seleccionada.")
                    # Return all data instead of empty
                    return df.to_csv().encode('utf-8')
                
                return filtered_df.to_csv().encode('utf-8')
            
            # Add debug information
            if cmg_ponderado_descarga.empty:
                st.error("No se encontraron datos de costos marginales ponderados para la fecha seleccionada.")
            
            csv = convert_df(cmg_ponderado_descarga)
            
            st.download_button(
                label="📈 Descargar Costos Marginales Ponderados",
                data=csv,
                file_name=f'cmg_ponderados_{central_seleccion}_{date_calculate.strftime("%Y%m%d")}.csv',
                mime='text/csv',
                on_click=lambda: add_notification(f"Archivo de costos marginales ponderados para {central_seleccion} descargado", type="success")
            )
            
            # Add debug information
            if cmg_tiempo_real_descarga.empty:
                st.error("No se encontraron datos de costos marginales en tiempo real para la fecha seleccionada.")
            
            csv_2 = convert_df(cmg_tiempo_real_descarga)
            
            st.download_button(
                label="⚡ Descargar Costos Marginales en Tiempo Real",
                data=csv_2,
                file_name=f'cmg_tiempo_real_{central_seleccion}_{date_calculate.strftime("%Y%m%d")}.csv',
                mime='text/csv',
                on_click=lambda: add_notification(f"Archivo de costos marginales en tiempo real para {central_seleccion} descargado", type="success")
            )
            
            st.markdown('</div>', unsafe_allow_html=True)
            
        st.markdown('</div>', unsafe_allow_html=True)

################## footer ##################

with st.container():
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div style="display: flex; justify-content: space-between; align-items: center; padding: 1rem 0;">
        <div>
            <p style="font-weight: 600; margin-bottom: 0.5rem;">Costos Eléctricos Chile</p>
            <p style="color: #666; font-size: 0.8rem;">Desarrollado por <a href="https://github.com/CFVALLS" style="color: #1E88E5; text-decoration: none;">Cristian Valls</a></p>
        </div>
        <div>
            <p style="color: #666; font-size: 0.8rem;">Última actualización: {chile_datetime.strftime('%d/%m/%Y %H:%M')}</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

# Add a help section below the header
with tab1:
    # First, add a help toggle in the existing settings container
    # Find the settings container
    with st.container():
        help_expander = st.expander("🛟 Ayuda y explicaciones")
        with help_expander:
            st.markdown("### Glosario de términos")
            st.markdown("Este panel muestra información sobre los costos marginales y operacionales para centrales eléctricas en Chile.")
            
            st.markdown("#### Términos importantes:")
            for term, explanation in tooltip_explanations.items():
                st.markdown(f"**{term.replace('_', ' ').title()}:** {explanation}")
            
            st.markdown("#### Cómo usar esta aplicación:")
            st.markdown("""
            1. **Panel de Monitoreo**: Muestra información en tiempo real de las centrales.
            2. **Panel de Atributos**: Permite modificar parámetros de las centrales.
            3. **Panel de Descarga**: Facilita la descarga de datos históricos.
            
            En el panel de monitoreo puede:
            - Cambiar el tipo de gráfico
            - Filtrar la información por central
            - Cambiar el rango de tiempo mostrado
            - Activar la actualización automática
            """)
            
            # Add a FAQ section - avoid nesting expanders
            st.markdown("#### Preguntas frecuentes:")
            st.markdown("""
            
            **¿Cómo se calcula el costo operacional?**
            
            Mediante la fórmula: ((Porcentaje Brent × Precio Brent) + Tasa Proveedor) × Factor Motor + Tasa Central + Margen de Garantía
            """)

# Add dark mode toggle in a sidebar
with st.sidebar:
    st.title("Configuración")
    
    # Dark mode toggle
    dark_mode = st.toggle("Modo oscuro", value=st.session_state['dark_mode'])
    if dark_mode != st.session_state['dark_mode']:
        st.session_state['dark_mode'] = dark_mode
        add_notification("Modo de visualización cambiado", type="info")
        st.experimental_rerun()
    
    # Language selection for future multi-language support
    st.selectbox("Idioma", ["Español", "English"], index=0, disabled=True)
    
    # Add status information box
    st.markdown("---")
    st.markdown("### Monitoreo")
    
    # Status message that shows what data is being displayed
    st.markdown(f"""
    <p style='font-size: 0.9rem;'>
        Mostrando datos de las últimas {st.session_state['time_range']} horas 
        en formato de gráfico de {"línea" if st.session_state['chart_type'] == "line" else "área" if st.session_state['chart_type'] == "area" else "barra"} para 
        {', '.join(filter(None, [
            'Charrúa' if st.session_state['show_charrua'] else None, 
            'Quillota' if st.session_state['show_quillota'] else None
        ]))}
        {' con costos operacionales' if st.session_state['show_operational_costs'] else ''}
    </p>
    """, unsafe_allow_html=True)
    
    # Status information about database connection and update times
    st.markdown(f"**Última Actualización:** {ultimo_tracking}")
    
    if CONN_STATUS:
        connection_status = '<span style="color:#00b300; font-weight:bold;">Conectado</span>'
    else:
        connection_status = '<span style="color:#cc0000; font-weight:bold;">Desconectado</span>'
    
    st.markdown(f"**Estado DB:** {connection_status}", unsafe_allow_html=True)
    st.markdown(f"**Modificación CEN:** {ultimo_mod_rio}")
    
    # Add divider
    st.markdown("---")
    
    # Move visualization settings from tab1 to sidebar
    st.markdown("### Visualización")
    
    # Auto-refresh toggle moved from tab1
    auto_refresh_enabled = st.toggle("Actualización automática", value=st.session_state['auto_refresh'])
    if auto_refresh_enabled != st.session_state['auto_refresh']:
        st.session_state['auto_refresh'] = auto_refresh_enabled
        st.session_state['last_refresh_time'] = time.time()
    
    if auto_refresh_enabled:
        refresh_interval = st.select_slider(
            "Intervalo (minutos)", 
            options=[1, 2, 5, 10, 15, 30, 60],
            value=st.session_state['refresh_interval']
        )
        if refresh_interval != st.session_state['refresh_interval']:
            st.session_state['refresh_interval'] = refresh_interval
            st.session_state['last_refresh_time'] = time.time()
        
        # Display countdown
        remaining = auto_refresh()
        if remaining is not None:
            st.markdown(f"<p style='font-size: 0.8rem; color: #666;'>Próxima actualización en: {remaining//60}m {remaining%60}s</p>", unsafe_allow_html=True)
    
    # Time range selector moved from tab1
    st.markdown("### Rango de tiempo")
    time_range_options = {
        "12h": 12,
        "24h": 24,
        "48h": 48, 
        "72h": 72,
        "7d": 168
    }
    
    time_range = st.radio(
        "Seleccione rango:",
        options=list(time_range_options.keys()),
        horizontal=True,
        index=list(time_range_options.values()).index(st.session_state['time_range']) if st.session_state['time_range'] in time_range_options.values() else 2
    )
    
    # Update time range based on selection
    if time_range in time_range_options:
        selected_hours = time_range_options[time_range]
        if selected_hours != st.session_state['time_range']:
            st.session_state['time_range'] = selected_hours
    
    # Chart type selection moved from tab1
    st.markdown("### Tipo de gráfico")
    chart_type = st.radio(
        "Seleccione tipo:",
        options=["Línea", "Área", "Barra"],
        horizontal=True,
        index=0 if st.session_state['chart_type'] == 'line' else 1 if st.session_state['chart_type'] == 'area' else 2
    )
    
    if chart_type == "Línea" and st.session_state['chart_type'] != 'line':
        st.session_state['chart_type'] = 'line'
    elif chart_type == "Área" and st.session_state['chart_type'] != 'area':
        st.session_state['chart_type'] = 'area'
    elif chart_type == "Barra" and st.session_state['chart_type'] != 'bar':
        st.session_state['chart_type'] = 'bar'
    
    # Data display toggles moved from tab1
    st.markdown("### Datos a mostrar")
    show_charrua = st.checkbox("Charrúa (Los Angeles)", value=st.session_state['show_charrua'])
    if show_charrua != st.session_state['show_charrua']:
        st.session_state['show_charrua'] = show_charrua
        
    show_quillota = st.checkbox("Quillota", value=st.session_state['show_quillota'])
    if show_quillota != st.session_state['show_quillota']:
        st.session_state['show_quillota'] = show_quillota
        
    show_costs = st.checkbox("Costos Operacionales", value=st.session_state['show_operational_costs'])
    if show_costs != st.session_state['show_operational_costs']:
        st.session_state['show_operational_costs'] = show_costs
    
    # About section
    st.markdown("---")
    st.markdown("### Acerca de")
    st.markdown("""
    **Costos Eléctricos Chile**
    
    Versión 2.0
    
    Desarrollado por [Cristian Valls](https://github.com/CFVALLS)
    
    © 2024 - Todos los derechos reservados
    """)

# Apply dark mode if enabled
if st.session_state['dark_mode']:
    st.markdown("""
    <script>
        document.body.classList.add('dark-mode');
    </script>
    """, unsafe_allow_html=True)

# Add responsive design detection
st.markdown("""
<script>
    // Detect if device is mobile
    function isMobile() {
        return window.innerWidth <= 768;
    }
    
    // Add a class to the body for mobile devices
    if (isMobile()) {
        document.body.classList.add('mobile');
    }
</script>
""", unsafe_allow_html=True)

# Show any active notifications
show_notifications()
