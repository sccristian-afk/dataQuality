"""
REGISTRO DINÁMICO DE VISTAS (VIEW REGISTRY)
===========================================
Permite añadir nuevas páginas o módulos a la aplicación en una sola línea
sin modificar el enrutador central ni la lógica del menú lateral.
"""
from typing import Callable, Dict, Optional
from dataclasses import dataclass
from views import alta_validaciones, catalogo_reglas, profiling

@dataclass
class ViewDefinition:
    title: str
    icon: str
    render_func: Callable[[], None]
    min_role: Optional[str] = None  # None = Todos, "Owner" = Solo administradores

# Diccionario maestro de vistas registradas
REGISTERED_VIEWS: Dict[str, ViewDefinition] = {
    "Alta de Validaciones": ViewDefinition(
        title="Alta de Validaciones",
        icon="📝",
        render_func=alta_validaciones.render,
        min_role=None
    ),
    "Catálogo de Reglas": ViewDefinition(
        title="Catálogo de Reglas",
        icon="📋",
        render_func=catalogo_reglas.render,
        min_role=None
    ),
    "Profiling": ViewDefinition(
        title="Profiling",
        icon="📊",
        render_func=profiling.render,
        min_role=None
    )
}

def register_view(title: str, icon: str, render_func: Callable[[], None], min_role: Optional[str] = None):
    """
    Función de extensión pública:
    Añade un nuevo módulo o funcionalidad a la app dinámicamente.
    """
    REGISTERED_VIEWS[title] = ViewDefinition(title, icon, render_func, min_role)