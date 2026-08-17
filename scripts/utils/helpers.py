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
import plotly.express as px
import plotly.graph_objects as go
import logging
import pytz
import unicodedata
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

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    get_latest_status_central
    # Removed get_central_status_history as it doesn't exist
)

from db.connection_db import establecer_engine, establecer_session, session_scope

#############################################################
################### HELPER FUNCTIONS ########################
#############################################################

# Define tooltip explanations as a dictionary
tooltip_explanations = {
    "cmg_calculado": "Costo Marginal Calculado en base a datos históricos y condiciones actuales del sistema.",
    "cmg_online": "Costo Marginal en tiempo real obtenido desde el Coordinador Eléctrico Nacional.",
    "cmg_programado": "Costo Marginal programado para las próximas horas.",
    "costo_operacional": "Costo total de operación de la central, incluido margen de garantía.",
    "central_referencia": "Central eléctrica utilizada como referencia para la barra de transmisión.",
    "zona_desacople": "Indica si la zona está en desacople (operando con precios diferentes al resto del sistema).",
    "margen_garantia": "Margen adicional que asegura la rentabilidad mínima de la central.",
    "porcentaje_brent": "Porcentaje del precio del petróleo Brent que impacta en el costo operacional.",
    "tasa_proveedor": "Tasa cobrada por el proveedor de combustible.",
    "tasa_central": "Tasa específica de operación de la central.",
    "factor_motor": "Factor multiplicador relacionado con la eficiencia de los motores.",
    "desacople": "Un sistema en desacople significa que diferentes zonas del sistema eléctrico operan con precios distintos debido a restricciones técnicas."
}

# Function to create tooltips
def tooltip(label, key):
    return f'<span class="tooltip" title="{tooltip_explanations.get(key, "")}">{label}</span>'

# Function to add notifications
def add_notification(message, type="info", duration=5):
    notification = {
        "message": message,
        "type": type,  # info, success, warning, error
        "time": time.time(),
        "duration": duration  # seconds to display
    }
    st.session_state['notifications'].append(notification)

# Function to display notifications
def show_notifications():
    current_time = time.time()
    if st.session_state['notifications']:
        with st.container():
            # Create a fixed position notification area with CSS
            st.markdown("""
            <style>
                .notification-container {
                    position: fixed;
                    top: 10px;
                    right: 10px;
                    z-index: 9999;
                    max-width: 300px;
                }
                .notification {
                    padding: 0.75rem 1rem;
                    margin-bottom: 0.5rem;
                    border-radius: 0.25rem;
                    animation: fadeInOut 0.3s ease-in-out;
                    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
                }
                .notification.info {
                    background-color: #cce5ff;
                    color: #004085;
                    border-left: 4px solid #004085;
                }
                .notification.success {
                    background-color: #d4edda;
                    color: #155724;
                    border-left: 4px solid #155724;
                }
                .notification.warning {
                    background-color: #fff3cd;
                    color: #856404;
                    border-left: 4px solid #856404;
                }
                .notification.error {
                    background-color: #f8d7da;
                    color: #721c24;
                    border-left: 4px solid #721c24;
                }
                @keyframes fadeInOut {
                    0% { opacity: 0; transform: translateX(20px); }
                    100% { opacity: 1; transform: translateX(0); }
                }
                .dark-mode .notification.info {
                    background-color: #0d47a1;
                    color: #e3f2fd;
                }
                .dark-mode .notification.success {
                    background-color: #2e7d32;
                    color: #e8f5e9;
                }
                .dark-mode .notification.warning {
                    background-color: #f57f17;
                    color: #fffde7;
                }
                .dark-mode .notification.error {
                    background-color: #c62828;
                    color: #ffebee;
                }
            </style>
            <div class="notification-container">
            """, unsafe_allow_html=True)
            
            # Display each notification that's still within its display duration
            active_notifications = []
            for notification in st.session_state['notifications']:
                if current_time - notification['time'] < notification['duration']:
                    st.markdown(f"""
                    <div class="notification {notification['type']}">
                        {notification['message']}
                    </div>
                    """, unsafe_allow_html=True)
                    active_notifications.append(notification)
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Update notifications list to only include active ones
            st.session_state['notifications'] = active_notifications

# Function to trigger page refresh based on session state
def auto_refresh():
    if st.session_state['auto_refresh']:
        # Display countdown timer for next refresh
        remaining_time = st.session_state.get('last_refresh_time', time.time()) + (st.session_state['refresh_interval'] * 60) - time.time()
        if remaining_time <= 0:
            st.session_state['last_refresh_time'] = time.time()
            add_notification("Datos actualizados", type="success", duration=3)
            st.experimental_rerun()
        return int(remaining_time)
    return None

# Use this in places where we need to adjust layout for mobile
def is_mobile():
    # This is a best-guess approach since we can't directly detect
    # device type from Python. Instead, we'll make decisions based
    # on window size.
    return False  # Default assumption - can be refined in future versions

def _normalize_bar_label(value):
    normalized = unicodedata.normalize('NFKD', str(value))
    return ''.join(char for char in normalized if not unicodedata.combining(char)).casefold()


def get_json_costo_marginal_online(
    fecha_gte,
    fecha_lte,
    barras,
    user_key=None,
    verbose=False,
    api_url=None,
):
    """
    Fetch data from Coordinador API or returns empty list if fails.
    
    Args:
        fecha_gte: Start date in YYYY-MM-DD format
        fecha_lte: End date in YYYY-MM-DD format
        barras: List of bars to filter by
        user_key: API key (optional)
        verbose: Whether to print verbose output
        
    Returns:
        list: Filtered data for the specified bars
    """
    # Build URL and headers
    url = api_url or os.getenv(
        "COORDINADOR_API_URL",
        "https://api.coordinador.cl/v2/costos-marginales/reales",
    )
    headers = {
        "Content-Type": "application/json",
        "User-Api-Key": user_key if user_key else ""
    }
    
    # Build payload
    payload = {
        "fechaGte": fecha_gte,
        "fechaLte": fecha_lte
    }
    
    # Log the request
    if verbose:
        logging.info(f"GET {url} with payload {payload}")
    
    try:
        # Make request with timeout
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        
        # Check status code
        if response.status_code != 200:
            logging.warning(f"API returned status code {response.status_code}: {response.text}")
            return []
            
        # Check if response text is empty
        if not response.text or response.text.isspace():
            logging.warning("API returned empty response text")
            return []
            
        # Try to parse JSON
        try:
            json_data = json.loads(response.text)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}. Response: {response.text[:100]}...")
            return []
            
        if isinstance(json_data, dict):
            json_data = json_data.get("data") or json_data.get("items") or json_data.get("results") or []
        if not isinstance(json_data, list) or not json_data:
            logging.warning("API returned empty JSON data")
            return []

        requested_bars = {_normalize_bar_label(barra) for barra in barras}
        filtered_data = [
            row for row in json_data
            if isinstance(row, dict)
            and _normalize_bar_label(row.get('barra', '')) in requested_bars
        ]
        return filtered_data
        
    except requests.exceptions.RequestException as error:
        logging.error(f"Request error: {error}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error in get_json_costo_marginal_online: {e}")
        return []

# Function to get costo marginal online hora
def get_costo_marginal_online_hora(
    fecha_gte,
    fecha_lte,
    barras,
    hora_in,
    user_key=None,
    api_url=None,
):
    """
    Obtiene los valores del costo marginal de las barras en una hora específica.

    Args:
        fecha_gte (str): Fecha de inicio en formato "YYYY-MM-DD".
        fecha_lte (str): Fecha de fin en formato "YYYY-MM-DD".
        user_key (str): Clave de usuario para acceder a la API.
        barras (list): Lista de las barras cuyos valores de costo marginal se desean obtener.
        hora (str, optional): Hora en formato "HH:MM:SS". El valor por defecto es "17:00:00".

    Returns:
        dict: Diccionario con las barras como llaves y los valores de costo marginal como valores.
    """
    json_raw = get_json_costo_marginal_online(
        fecha_gte, fecha_lte, barras, user_key, api_url=api_url)
    if not json_raw:
        logging.warning("get_costo_marginal_online_hora: No data returned from API")
        return {}
    
    try:
        fecha_cutoff = pd.Timestamp(f'{fecha_lte} {hora_in}').floor('h')
        selected = {}
        for row in json_raw:
            row_date = pd.to_datetime(row.get('fecha'), errors='coerce')
            if not pd.isna(row_date) and row_date.tzinfo is not None:
                row_date = row_date.tz_convert('America/Santiago').tz_localize(None)
            if pd.isna(row_date) or row_date.floor('h') != fecha_cutoff:
                continue

            normalized_bar = _normalize_bar_label(row.get('barra', ''))
            if normalized_bar == 'charrua':
                bar = 'Charrua'
            elif normalized_bar == 'quillota':
                bar = 'Quillota'
            else:
                continue
            try:
                cmg_value = float(row['cmg'])
                if bar not in selected or row_date > selected[bar][0]:
                    selected[bar] = (row_date, cmg_value)
            except (KeyError, TypeError, ValueError):
                logging.warning(f"Invalid CMg value for {bar}: {row.get('cmg')}")
        return {bar: value for bar, (_, value) in selected.items()}
    except Exception as e:
        logging.error(f"Error processing data in get_costo_marginal_online_hora: {e}")
        return {}

# Function to get central
def get_central(name_central, host=None, port=None, session=None):
    '''
    Get data for a central either via API or directly from database if session is provided
    
    Args:
        name_central: Name of the central
        host: API host (optional)
        port: API port (optional)
        session: SQLAlchemy session (optional, if provided will use direct DB access)
        
    Returns:
        dict: Central data
    '''
    # If session is provided, use direct database access
    if session is not None:
        from db.operaciones_db import query_last_row_central
        
        try:
            # Get central data directly from database
            result = query_last_row_central(session, name_central)
            
            if result:
                # Convert list to dictionary with column names
                columns = ['id', 'nombre', 'generando', 'barra_transmision', 
                          'tasa_proveedor', 'porcentaje_brent', 'tasa_central', 
                          'precio_brent', 'margen_garantia', 'costo_operacional', 
                          'factor_motor', 'fecha_referencia_brent', 'fecha_registro', 
                          'external_update', 'editor']
                
                # Create dictionary from result list
                data = {columns[i]: result[i] for i in range(len(columns)) if i < len(result)}
                return data
            else:
                return {"error": "No central entries found"}
        except Exception as e:
            return {"error": f"Database query failed: {e}"}
    
    # Fall back to API if no session or API is specifically requested
    url = f"http://{host}:{port}/central/{name_central}"
   
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return {"error": "No central entries found"}
        else:
            return {"error": "Failed to retrieve central entry"}
            
    except requests.RequestException as e:
        return {"error": f"Request failed: {e}"}

# Function to get cmg programados
def get_cmg_programados(name_central, date_in, host=None, port=None, session=None):
    """
    Retrieves the entry for the central in the 'cmg_programados' table for the given date.

    Args:
        name_central (str): The name of the central.
        date_in (str): The date in the format "YYYY-MM-DD".
        host: API host (optional)
        port: API port (optional)
        session: SQLAlchemy session (optional, if provided will use direct DB access)

    Returns:
        dict: A dictionary containing the hourly CMG values.
              If no entry is found, fallback data is returned.
    """
    # If session is provided, use direct database access
    if session is not None:
        from db.operaciones_db import get_cmg_programados as db_get_cmg_programados
        
        try:
            # Get CMG programados data directly from database
            result = db_get_cmg_programados(session, name_central, date_in)
            return result
        except Exception as e:
            logging.error(f"Error retrieving CMG programados from database: {e}")
            raise

    if not host or not port:
        return {}
    
    # Normal API behavior when not in local development and not using direct DB access
    url = f"http://{host}:{port}/cmg_programados/{name_central}/{date_in}"

    try:
        response = requests.get(url, timeout=10)
        response_data = json.loads(response.text)

        if response.status_code == 200:
            return response_data
        else:
            return {"error": "Failed to retrieve central entry"}
    except Exception as e:
        logging.error(f"Error retrieving CMG programados from API: {e}")
        return {}

# Function to insert central
def insert_central(name_central, editor, data, host=None, port=None):
    if not host or not port:
        raise ValueError("El servicio de actualización no está configurado")
    url = f"http://{host}:{port}/central/insert/{quote(name_central)}/{quote(editor)}"
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.put(url, headers=headers, json=data, timeout=15)
        
        if response.status_code == 200:
            return response.json()
        raise RuntimeError(
            f"El servicio de actualización respondió {response.status_code}: {response.text}"
        )
            
    except requests.RequestException as e:
        raise RuntimeError(f"No fue posible contactar el servicio de actualización: {e}") from e

# Function to reformat to iso
def reformat_to_iso(date_string):
    # Parse the date_string using strptime with the given format
    dt_object = datetime.strptime(date_string, '%d.%m.%y %H:%M:%S')
    
    # Return the reformatted string using strftime
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')

def create_status_piechart(merged_df, central_name, time_range=None):
    """
    Creates a pie chart showing ON/OFF time distribution for a central.
    
    Args:
        merged_df: DataFrame containing 'Estado' and 'unix_time' columns
        central_name: Name of the central to display in title
        time_range: Optional time range description for title
        
    Returns:
        plotly figure object with pie chart
    """
    required_columns = {"unix_time", "status_operacional"}
    if merged_df.empty or not required_columns.issubset(merged_df.columns):
        # Return empty chart if no data
        fig = go.Figure()
        fig.update_layout(title=f"No status data available for {central_name}")
        return fig
    
    df = merged_df.copy()
    df['unix_time'] = pd.to_numeric(df['unix_time'], errors='coerce')
    df = df.dropna(subset=['unix_time', 'status_operacional']).sort_values('unix_time')
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title=f"No status data available for {central_name}")
        return fig

    now = int(time.time())
    if time_range:
        cutoff = now - int(time_range) * 3600
        previous = df[df['unix_time'] < cutoff].tail(1)
        df = pd.concat([previous, df[df['unix_time'] >= cutoff]]).drop_duplicates()
        df['unix_time'] = df['unix_time'].clip(lower=cutoff)

    df['next_time'] = df['unix_time'].shift(-1).fillna(now)
    df['duration'] = df['next_time'] - df['unix_time']
    df = df[df['duration'] > 0]
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title=f"No status data available for {central_name}")
        return fig
    
    # Group by state and sum durations
    status_summary = df.groupby('status_operacional')['duration'].sum().reset_index()
    
    # Convert to hours for better readability
    status_summary['hours'] = status_summary['duration'] / 3600
    
    # Create pie chart
    colors = {'ON': '#28a745', 'OFF': '#dc3545'}
    
    fig = px.pie(
        status_summary, 
        values='hours', 
        names='status_operacional', 
        title=f'Operational Status for {central_name} {time_range or ""}',
        color='status_operacional',
        color_discrete_map=colors
    )
    
    # Improve layout
    fig.update_traces(
        textposition='inside', 
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Hours: %{value:.1f}<br>Percentage: %{percent:.1f}%'
    )
    
    fig.update_layout(
        legend_title='Status',
        height=400
    )
    
    return fig
