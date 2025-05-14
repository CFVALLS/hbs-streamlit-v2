"""
API service for interacting with external APIs.
"""
from typing import Dict, List, Optional, Any
import requests
from datetime import datetime
import streamlit as st
from ..config import config
from ..constants import API_TIMEOUT, API_RETRY_ATTEMPTS
from ..utils.logging import get_logger
from ..utils.validators import validate_api_response

logger = get_logger(__name__)

class APIService:
    """Service for handling API requests."""
    
    def __init__(self):
        """Initialize API service."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'StreamlitApp/1.0',
            'Accept': 'application/json'
        })
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_costo_marginal_online(
        self,
        fecha_gte: str,
        fecha_lte: str,
        barras: List[str],
        user_key: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get marginal costs from the API.
        
        Args:
            fecha_gte: Start date
            fecha_lte: End date
            barras: List of bars
            user_key: API user key
            
        Returns:
            List[Dict[str, Any]]: List of marginal costs
            
        Raises:
            ValueError: If dates are invalid
            requests.RequestException: If API request fails
        """
        try:
            # Validate dates
            if not all(validate_date(date) for date in [fecha_gte, fecha_lte]):
                raise ValueError("Invalid date format")
            
            # Use provided user key or default from config
            user_key = user_key or config.api.user_key
            
            # Construct URL
            url = (
                f'https://www.coordinador.cl/wp-json/costo-marginal/v1/data/'
                f'?fecha__gte={fecha_gte}&fecha__lte={fecha_lte}&user_key={user_key}'
            )
            
            # Make request with retries
            for attempt in range(API_RETRY_ATTEMPTS):
                try:
                    response = self.session.get(url, timeout=API_TIMEOUT)
                    response.raise_for_status()
                    data = response.json()
                    
                    # Validate response
                    if not validate_api_response({'status': 'success', 'data': data}):
                        raise ValueError("Invalid API response")
                    
                    # Filter data for specified bars
                    filtered_data = [item for item in data if item['barra'] in barras]
                    return filtered_data
                    
                except requests.RequestException as e:
                    if attempt == API_RETRY_ATTEMPTS - 1:
                        raise
                    logger.warning(f"API request failed, retrying... ({attempt + 1}/{API_RETRY_ATTEMPTS})")
                    continue
                    
        except Exception as e:
            logger.error(f"Error getting marginal costs: {e}")
            raise
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_central(self, name_central: str) -> Dict[str, Any]:
        """
        Get central information from the API.
        
        Args:
            name_central: Name of the central
            
        Returns:
            Dict[str, Any]: Central information
            
        Raises:
            requests.RequestException: If API request fails
        """
        try:
            url = f"http://{config.api.host}:{config.api.port}/central/{name_central}"
            
            response = self.session.get(url, timeout=API_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            if not validate_api_response(data):
                raise ValueError("Invalid API response")
                
            return data
            
        except Exception as e:
            logger.error(f"Error getting central information: {e}")
            raise
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_cmg_programados(
        self,
        name_central: str,
        date_in: str
    ) -> Dict[str, Any]:
        """
        Get programmed marginal costs from the API.
        
        Args:
            name_central: Name of the central
            date_in: Date in YYYY-MM-DD format
            
        Returns:
            Dict[str, Any]: Programmed marginal costs
            
        Raises:
            ValueError: If date is invalid
            requests.RequestException: If API request fails
        """
        try:
            # Validate date
            if not validate_date(date_in):
                raise ValueError("Invalid date format")
            
            url = f"http://{config.api.host}:{config.api.port}/cmg_programados/{name_central}/{date_in}"
            
            response = self.session.get(url, timeout=API_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            if not validate_api_response(data):
                raise ValueError("Invalid API response")
                
            return data
            
        except Exception as e:
            logger.error(f"Error getting programmed marginal costs: {e}")
            raise
    
    def insert_central(
        self,
        name_central: str,
        editor: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Insert central information via API.
        
        Args:
            name_central: Name of the central
            editor: Name of the editor
            data: Central data to insert
            
        Returns:
            Dict[str, Any]: Response from API
            
        Raises:
            requests.RequestException: If API request fails
        """
        try:
            url = f"http://{config.api.host}:{config.api.port}/central/insert/{name_central}/{editor}"
            headers = {"Content-Type": "application/json"}
            
            response = self.session.put(
                url,
                headers=headers,
                json=data,
                timeout=API_TIMEOUT
            )
            response.raise_for_status()
            
            data = response.json()
            if not validate_api_response(data):
                raise ValueError("Invalid API response")
                
            return data
            
        except Exception as e:
            logger.error(f"Error inserting central information: {e}")
            raise

# Create singleton instance
api_service = APIService() 