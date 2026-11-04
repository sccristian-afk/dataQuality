"""
MODELOS DE DATOS Y CONTRATOS (DTOs)
===================================
Define clases fuertemente tipadas para garantizar integridad de datos
entre la interfaz gráfica de Streamlit y el servicio de Databricks.
"""
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ValidationPayload:
    """
    Objeto de transferencia de datos (DTO) para dar de alta una regla.
    Evita el uso de diccionarios planos propensos a KeyError.
    """
    id_validacion: str
    rule_id: str
    technical_rule: str
    table_id: str
    domain: str
    severity: str
    owner: str
    user_email: str
    is_active: bool = True
    perimeter: str = ""
    columns_str: str = ""
    param_val: str = ""
    param_range: str = ""
    param_expr: str = ""
    param_query: str = ""
    param_merge: str = ""
    tags_str: str = "Testing"
    status: str = "Draft"

@dataclass
class UserSession:
    """Modela la identidad y permisos del usuario activo."""
    email: str
    role: str
    full_name: str
