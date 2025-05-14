"""
Main Streamlit application.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import sys
import os

# Add the parent directory to the path so we can import modules from the app package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    # Try absolute imports first - these will work when running the file directly
    from app.config import config
    from app.constants import (
        PAGE_TITLE, PAGE_ICON, LAYOUT, CHART_HEIGHT, CHART_WIDTH,
        CHART_THEME, TABLE_HEIGHT, MAX_ROWS_PER_PAGE,
        ERROR_MESSAGES, SUCCESS_MESSAGES
    )
    from app.services.api import api_service
    from streamlit_app.app.services.database import db_service
    from app.services.calculations import calc_service
    from app.utils.logging import get_logger
except ImportError:
    # Fall back to relative imports if absolute imports fail
    from .config import config
    from .constants import (
        PAGE_TITLE, PAGE_ICON, LAYOUT, CHART_HEIGHT, CHART_WIDTH,
        CHART_THEME, TABLE_HEIGHT, MAX_ROWS_PER_PAGE,
        ERROR_MESSAGES, SUCCESS_MESSAGES
    )
    from .services.api import api_service
    from .services.database import db_service
    from .services.calculations import calc_service
    from .utils.logging import get_logger

logger = get_logger(__name__)

def setup_page():
    """Set up Streamlit page configuration."""
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon=PAGE_ICON,
        layout=LAYOUT
    )

def display_header():
    """Display application header."""
    st.title(PAGE_TITLE)
    st.markdown("---")

def display_sidebar():
    """Display sidebar with filters and controls."""
    with st.sidebar:
        st.header("Filtros")
        
        # Date range selector
        today = datetime.now()
        date_range = st.date_input(
            "Rango de fechas",
            value=(today - timedelta(days=7), today),
            help="Seleccione el rango de fechas para visualizar"
        )
        
        # Convert date objects to datetime with time component
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
            # Add time component to make start_date start at 00:00:00
            start_datetime = datetime.combine(start_date, datetime.min.time())
            # Add time component to make end_date end at 23:59:59
            end_datetime = datetime.combine(end_date, datetime.max.time())
            # Store in session state for other functions to use
            st.session_state['date_range'] = (start_datetime, end_datetime)
        else:
            # Handle case where date_range might be a single date
            date = date_range if not isinstance(date_range, tuple) else date_range[0]
            start_datetime = datetime.combine(date, datetime.min.time())
            end_datetime = datetime.combine(date, datetime.max.time())
            st.session_state['date_range'] = (start_datetime, end_datetime)
        
        # Barra selector
        barra = st.selectbox(
            "Barra",
            options=list(config.BARRAS.keys()),
            help="Seleccione la barra de transmisión"
        )
        
        # Store selected barra in session state
        st.session_state['selected_barra'] = barra
        
        # Refresh button
        if st.button("Actualizar datos", help="Actualizar datos desde la API"):
            st.cache_data.clear()
            st.success(SUCCESS_MESSAGES['DATA_UPDATED'])

def display_current_status():
    """Display current status information."""
    try:
        # Get current time
        current_time = calc_service.get_current_time()
        
        # Get tracking coordinador
        tracking = db_service.get_tracking_coordinador()
        if tracking:
            st.subheader("Última actualización")
            st.write(f"Timestamp: {tracking[1]}")
            st.write(f"Modificación RIO: {tracking[3]}")
        
        # Get desacople status
        barra = st.session_state.get('selected_barra', 'CHARRUA_220')
        desacople_status = db_service.get_desacople_status(barra)
        if desacople_status:
            st.subheader("Estado de desacople")
            st.write(f"Central referencia: {desacople_status['central_referencia']}")
            st.write(f"Desacople: {desacople_status['desacople_bool']}")
            st.write(f"CMG: {desacople_status['cmg']:.2f}")
            
    except Exception as e:
        logger.error(f"Error displaying current status: {e}")
        st.error(ERROR_MESSAGES['DB_ERROR'])

def display_cmg_chart():
    """Display CMG chart."""
    try:
        # Get current time
        current_time = calc_service.get_current_time()
        
        # Get date range from session state
        date_range = st.session_state.get('date_range')
        if date_range:
            start_datetime, end_datetime = date_range
            # Convert to unix timestamp
            start_unix = int(start_datetime.timestamp())
            end_unix = int(end_datetime.timestamp())
            
            # Calculate hours between start and end date
            delta_hours = int((end_unix - start_unix) / 3600)
            
            # Get CMG data
            cmg_data = db_service.get_cmg_ponderado(
                end_unix,
                delta_hours=delta_hours
            )
        else:
            # Fallback to 96 hours if no date range in session state
            cmg_data = db_service.get_cmg_ponderado(
                current_time['unixtime'],
                delta_hours=96
            )
        
        if cmg_data:
            # Convert to DataFrame
            df = pd.DataFrame(cmg_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Filter by date range if available
            if date_range:
                start_datetime, end_datetime = date_range
                df = df[(df['timestamp'] >= start_datetime) & (df['timestamp'] <= end_datetime)]
            
            # Create chart
            fig = px.line(
                df,
                x='timestamp',
                y='cmg_ponderado',
                title='CMG Ponderado',
                height=CHART_HEIGHT,
                width=CHART_WIDTH,
                template=CHART_THEME
            )
            
            # Update layout
            fig.update_layout(
                xaxis_title="Fecha",
                yaxis_title="CMG ($/MWh)",
                showlegend=True
            )
            
            # Display chart
            st.plotly_chart(fig, use_container_width=True)
            
    except Exception as e:
        logger.error(f"Error displaying CMG chart: {e}")
        st.error(ERROR_MESSAGES['DB_ERROR'])

def display_central_info():
    """Display central information."""
    try:
        # Get central info based on selected barra
        barra_name = st.session_state.get('selected_barra', 'CHARRUA_220')
        
        # Map barra to central name
        if barra_name == 'CHARRUA_220':
            central_name = 'Los Angeles'
        elif barra_name == 'QUILLOTA_220':
            central_name = 'Quillota'
        else:
            # Default to Los Angeles if barra doesn't match
            central_name = 'Los Angeles'
            
        # Get the central information from database
        central_info = db_service.get_central_info(central_name)
        
        if central_info:
            st.subheader(f"Información Central {central_name}")
            
            # Get latest status from status_central table
            status = db_service.get_central_status(central_name)
            
            # Display operation status based on status_central table
            if status == 'ON':
                estado = "ENCENDIDO"
            elif status == 'OFF':
                estado = "APAGADO"
            elif status == 'HOLD':
                estado = "EN ESPERA"
            else:
                # Fallback to legacy 'generando' attribute if status is not available
                generando = central_info[2] if len(central_info) > 2 and central_info[2] is not None else False
                estado = "ENCENDIDO" if generando else "APAGADO"
            
            st.write(f"● {estado}")
            
            # Extract required values from central_info
            costo_operacional = central_info[9] if len(central_info) > 9 and central_info[9] is not None else 0.0
            barra_transmision = central_info[3] if len(central_info) > 3 and central_info[3] is not None else ""
            
            # Display calculated and operational costs
            st.write(f"CMg Calculado")
            st.write(f"{st.session_state.get('cmg_calculated', 0.0)}")
            
            st.write(f"Costo Operacional")
            st.write(f"{round(float(costo_operacional), 2)}")
            
            # Get current time
            current_hour = datetime.now().strftime("%H:%M:%S")
            
            # Display online and programmed CMg
            st.write(f"CMg Online - {current_hour}")
            st.write(f"{st.session_state.get('cmg_online', 'Not Available')}")
            
            st.write(f"CMg Programado - {current_hour}")
            st.write(f"{st.session_state.get('cmg_programado', 0.0)}")
            
            # Display reference central and decoupling zone
            st.write(f"Central referencia")
            
            # Get information from cmg_tiempo_real for central_referencia
            barra_for_query = barra_name.replace('_220', '_22O')
            desacople_status = db_service.get_desacople_status(barra_for_query)
            
            if desacople_status and desacople_status.get('central_referencia') is not None:
                # Display value directly from database
                st.write(f"{desacople_status['central_referencia']}")
            else:
                # Query failed or returned None
                st.write("No disponible")
            
            # Check desacople status
            desacople = db_service.check_desacople_status(barra_transmision)
            st.write(f"Zona en desacople")
            st.write(f"{'Activo' if desacople else 'No Activo'}")
        else:
            st.warning(f"No se encontró información para la central {central_name}")
    except Exception as e:
        st.error(f"Error al obtener información de la central: {str(e)}")
        logger.error(f"Error in display_central_info: {str(e)}")

def main():
    """Main application entry point."""
    try:
        # Set up page
        setup_page()
        
        # Display header
        display_header()
        
        # Display sidebar
        display_sidebar()
        
        # Create columns for layout
        col1, col2 = st.columns(2)
        
        with col1:
            # Display current status
            display_current_status()
            
        with col2:
            # Display central info
            display_central_info()
        
        # Display CMG chart
        display_cmg_chart()
        
    except Exception as e:
        logger.error(f"Error in main application: {e}")
        st.error(ERROR_MESSAGES['CONFIG_ERROR'])

if __name__ == "__main__":
    main() 