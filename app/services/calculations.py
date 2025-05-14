"""
Calculations service for business logic.
"""
from typing import Dict, List, Optional, Union
import numpy as np
from datetime import datetime, timedelta
import pytz
from ..utils.logging import get_logger
from ..utils.validators import validate_cmg_value, validate_cost_value
from ..constants import TIMEZONE

logger = get_logger(__name__)

class CalculationsService:
    """Service for handling business calculations."""
    
    def __init__(self):
        """Initialize calculations service."""
        self.timezone = pytz.timezone(TIMEZONE)
    
    def get_current_time(self) -> Dict[str, Union[str, int]]:
        """
        Get current time in Chile timezone.
        
        Returns:
            Dict[str, Union[str, int]]: Current time information
        """
        try:
            # Get current time in Chile timezone
            chile_datetime = datetime.now(self.timezone)
            
            # Format date and time
            fecha = chile_datetime.strftime("%Y-%m-%d")
            hora = chile_datetime.strftime("%H:%M:%S")
            
            # Round hour to nearest hour
            hora_redondeada = f'{hora.split(":")[0]}:00:00'
            hora_redondeada_cmg = f'{hora.split(":")[0]}:00'
            
            # Get unix time
            naive_datetime = chile_datetime.astimezone().replace(tzinfo=None)
            unixtime = int(naive_datetime.timestamp())
            
            return {
                'fecha': fecha,
                'hora': hora,
                'hora_redondeada': hora_redondeada,
                'hora_redondeada_cmg': hora_redondeada_cmg,
                'unixtime': unixtime
            }
            
        except Exception as e:
            logger.error(f"Error getting current time: {e}")
            raise
    
    def calculate_cmg_hora(
        self,
        cmg_data: List[Dict[str, Any]],
        unix_time: int,
        duration: int = 3599
    ) -> float:
        """
        Calculate hourly CMG.
        
        Args:
            cmg_data: List of CMG data points
            unix_time: Unix timestamp
            duration: Duration in seconds
            
        Returns:
            float: Calculated hourly CMG
            
        Raises:
            ValueError: If calculation fails
        """
        try:
            # Filter data points within time range
            filtered_data = [
                point for point in cmg_data
                if unix_time <= point['unix_time'] <= unix_time + duration
            ]
            
            if not filtered_data:
                raise ValueError("No data points found in time range")
            
            # Calculate time differences
            time_diffs = np.array(
                [point['unix_time'] - unix_time for point in filtered_data] + [duration + 1]
            )
            
            # Calculate weights
            weights = np.diff(time_diffs) / (duration + 1)
            
            # Get CMG values
            cmg_values = np.array([float(point['cmg']) for point in filtered_data])
            
            # Calculate weighted average
            cmg_hora = np.sum(np.multiply(weights, cmg_values))
            
            # Validate result
            if not validate_cmg_value(cmg_hora):
                raise ValueError(f"Invalid CMG value calculated: {cmg_hora}")
            
            return float(cmg_hora)
            
        except Exception as e:
            logger.error(f"Error calculating hourly CMG: {e}")
            raise
    
    def calculate_operational_cost(
        self,
        base_cost: float,
        adjustments: List[float]
    ) -> float:
        """
        Calculate operational cost.
        
        Args:
            base_cost: Base operational cost
            adjustments: List of cost adjustments
            
        Returns:
            float: Calculated operational cost
            
        Raises:
            ValueError: If calculation fails
        """
        try:
            # Calculate total cost
            total_cost = base_cost + sum(adjustments)
            
            # Validate result
            if not validate_cost_value(total_cost):
                raise ValueError(f"Invalid cost value calculated: {total_cost}")
            
            return float(total_cost)
            
        except Exception as e:
            logger.error(f"Error calculating operational cost: {e}")
            raise
    
    def calculate_profitability(
        self,
        cmg: float,
        operational_cost: float,
        factor_penalizacion: float = 1.0
    ) -> Dict[str, float]:
        """
        Calculate profitability metrics.
        
        Args:
            cmg: Marginal cost
            operational_cost: Operational cost
            factor_penalizacion: Penalization factor
            
        Returns:
            Dict[str, float]: Profitability metrics
            
        Raises:
            ValueError: If calculation fails
        """
        try:
            # Calculate adjusted CMG
            adjusted_cmg = cmg * factor_penalizacion
            
            # Calculate margin
            margin = adjusted_cmg - operational_cost
            
            # Calculate margin percentage
            margin_percentage = (margin / operational_cost) * 100 if operational_cost > 0 else 0
            
            return {
                'adjusted_cmg': float(adjusted_cmg),
                'margin': float(margin),
                'margin_percentage': float(margin_percentage)
            }
            
        except Exception as e:
            logger.error(f"Error calculating profitability: {e}")
            raise

# Create singleton instance
calc_service = CalculationsService() 