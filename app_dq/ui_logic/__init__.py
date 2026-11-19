"""
PAQUETE DE CONTROLADORES Y LÓGICA DE INTERFAZ (UI_LOGIC)
=========================================================
Expone de forma modular la lógica de sesión y los controladores específicos
para cada interfaz de la aplicación.
"""
from ui_logic.session import (
    db_backend,
    init_session_state,
    process_login,
    process_logout
)
from ui_logic import alta_individual
from ui_logic import carga_masiva
from ui_logic import catalogo_reglas
from ui_logic import profiling
from ui_logic import configuracion

__all__ = [
    "db_backend",
    "init_session_state",
    "process_login",
    "process_logout",
    "alta_individual",
    "carga_masiva",
    "catalogo_reglas",
    "profiling",
    "configuracion"
]