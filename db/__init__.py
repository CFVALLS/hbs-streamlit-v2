"""
Módulo para operaciones de base de datos.
Facilita las operaciones de conexión y consulta a la base de datos.
"""

# Importar modelos ORM
from .models_orm import (
    CmgTiempoReal,
    CmgPonderado,
    CentralTable,
    TrackingCoordinador,
    TrackingTco,
    RioRawData,
    FactorPenalizacion
)

# Importar funciones de operaciones de base de datos
from .operaciones_db import (
    retrieve_costo_marginal_tco,
    retrieve_valor_factor_penalizacion,
    retrieve_last_entry_from_rio_raw_data,
    retrieve_tracking_coordinador,
    retrieve_status_desacople,
    query_values_last_desacople_bool,
    query_last_row_central,
    query_central_table,
    query_central_table_modifications,
    query_cmg_ponderado_by_time,
    get_cmg_tiempo_real,
    generate_minimal_cmg_data
)

# Importar conexión a base de datos
from .connection_db import (
    establecer_engine,
    establecer_session,
    session_scope
)

__all__ = [
    # Modelos ORM
    'CmgTiempoReal',
    'CmgPonderado',
    'CentralTable',
    'TrackingCoordinador',
    'TrackingTco',
    'RioRawData',
    'FactorPenalizacion',
    
    # Operaciones de base de datos
    'retrieve_costo_marginal_tco',
    'retrieve_valor_factor_penalizacion',
    'retrieve_last_entry_from_rio_raw_data',
    'retrieve_tracking_coordinador',
    'retrieve_status_desacople',
    'query_values_last_desacople_bool',
    'query_last_row_central',
    'query_central_table',
    'query_central_table_modifications',
    'query_cmg_ponderado_by_time',
    'get_cmg_tiempo_real',
    'generate_minimal_cmg_data',
    
    # Conexión a base de datos
    'establecer_engine',
    'establecer_session',
    'session_scope'
] 