from typing import Callable, Dict, List, Optional
from dataclasses import dataclass
from views import alta_validaciones, catalogo_reglas, profiling, configuracion, ejecucion_carga_masiva


@dataclass
class ViewDefinition:
    title: str
    render_func: Callable[[], None]
    icon: str = ""
    allowed_roles: Optional[List[str]] = None
    show_in_sidebar: bool = True


REGISTERED_VIEWS: Dict[str, ViewDefinition] = {
    "Alta de validaciones": ViewDefinition(
        title="Alta de validaciones",
        render_func=alta_validaciones.render,
        allowed_roles=["User", "Owner"],
    ),
    "Ejecución carga masiva": ViewDefinition(
        title="Ejecución carga masiva",
        render_func=ejecucion_carga_masiva.render,
        allowed_roles=["User", "Owner"],
        show_in_sidebar=False,
    ),
    "Catálogo de reglas": ViewDefinition(
        title="Catálogo de reglas",
        render_func=catalogo_reglas.render,
        allowed_roles=["User", "Owner"],
    ),
    "Profiling": ViewDefinition(
        title="Profiling",
        render_func=profiling.render,
        allowed_roles=["User", "Owner"],
    ),
    "Configuración": ViewDefinition(
        title="Configuración",
        render_func=configuracion.render,
        allowed_roles=["Admin"],
    )
}