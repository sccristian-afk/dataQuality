"""
MÓDULO DE CONFIGURACIÓN Y PARAMETRIZACIÓN
========================================
Lee las columnas del archivo Excel dinámico (parametrizar_tabla.xlsx o parametros_config.xlsx)
y desacopla los identificadores lógicos del sistema de los nombres comerciales de las hojas de cálculo.
"""
import os
import pandas as pd
from typing import Dict

class ConfigManager:
    """
    Gestor central de configuración y mapeo de parámetros.
    Garantiza que cualquier cambio de nomenclatura en Excel no rompa la aplicación.
    """
    def __init__(self, primary_path="parametrizar_tabla.xlsx", fallback_path="parametros_config.xlsx"):
        # Prioriza el archivo suministrado por el usuario
        self.config_path = primary_path if os.path.exists(primary_path) else fallback_path
        self.column_map: Dict[str, str] = self._load_columns()

    def _load_columns(self) -> Dict[str, str]:
        """Carga el mapeo desde el Excel si existe; de lo contrario, aplica valores predeterminados."""
        try:
            if os.path.exists(self.config_path):
                df = pd.read_excel(self.config_path, sheet_name="Mapeo_Validaciones")
                # Mapea Variable_Interna -> Nombre_Columna_Excel
                return dict(zip(df['Variable_Interna'].astype(str).str.strip(), 
                                df['Nombre_Columna_Excel'].astype(str).str.strip()))
            return self._default_columns()
        except Exception as e:
            print(f"[CONFIG] Aviso: No se pudo leer {self.config_path} ({e}). Usando defaults.")
            return self._default_columns()

    def _default_columns(self) -> Dict[str, str]:
        """Catálogo de respaldo estándar con los 27 campos de gobierno."""
        return {
            "COL_ID_VALIDACION": "Id_validación",
            "COL_ID_REGLA": "Id_regla",
            "COL_TIPO_REGLA": "Tipo_regla",
            "COL_NOMBRE_REGLA": "Nombre_regla",
            "COL_NOMBRE_TECNICO": "Nombre_técnico",
            "COL_DESCRIPCION": "Descripción",
            "COL_DIMENSION_REGLA": "Dimensión_regla",
            "COL_ID_TABLA": "Id_tabla",
            "COL_NOMBRE_TABLA": "Nombre_tabla",
            "COL_DOMINIO_TABLA": "Dominio_tabla",
            "COL_DEFINICION_PERIMETRO": "Definición_perimetro",
            "COL_PARAM_COLUMNAS": "Param_columnas",
            "COL_PARAM_VALOR": "Param_valor",
            "COL_PARAM_VALOR_MIN": "Param_valor_min",
            "COL_PARAM_VALOR_MAX": "Param_valor_max",
            "COL_PARAM_CONJUNTO": "Param_conjunto",
            "COL_PARAM_TIPO_DATO": "Param_tipo_dato",
            "COL_PARAM_EXPRESION": "Param_expresion",
            "COL_PARAM_QUERY_SQL": "Param_query_SQL",
            "COL_PARAM_MERGE_COLUMNAS": "Param_merge_columnas",
            "COL_SEVERIDAD": "Severidad",
            "COL_OWNER": "Owner",
            "COL_ETIQUETAS": "Etiquetas",
            "COL_ACTUALIZADO_POR": "Actualizado_por",
            "COL_FECHA_ACTUALIZACION": "Fecha_actualización",
            "COL_VALIDACION_ACTIVA": "Validación_activa",
            "COL_ESTADO": "Estado"
        }

    def get_col(self, variable_interna: str) -> str:
        """Devuelve el nombre de la columna física a partir de su constante lógica."""
        return self.column_map.get(variable_interna, variable_interna)

# Instancia singleton accesible globalmente
cfg = ConfigManager()
