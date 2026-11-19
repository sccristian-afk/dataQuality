"""
CONTROLADOR DE INTERFAZ: PROFILING DE DATOS
===========================================
Orquesta la obtención de tablas disponibles, la ejecución del perfilado
estadístico en Databricks y el guardado del histórico de métricas.
"""
import streamlit as st
import pandas as pd
from typing import List, Optional, Dict, Any
from ui_logic.session import db_backend


def get_available_tables() -> List[str]:
    """Obtiene el listado de tablas maestras y descubiertas en Unity Catalog."""
    try:
        return db_backend.get_table_list()
    except Exception as ex:
        st.warning(f"No se pudo recuperar el listado completo de tablas: {str(ex)}")
        return []


def run_table_profiling(
    table_name: str,
    sample_size: Optional[int] = None,
    include_correlations: bool = False
) -> bool:
    """Ejecuta el perfilado estadístico y guarda el resultado en el estado de sesión."""
    with st.spinner(f"Consultando Unity Catalog y calculando métricas para `{table_name}`..."):
        try:
            limit_val = sample_size if sample_size and sample_size > 0 else None
            summary_metrics, col_stats, corr_df = db_backend.compute_table_profiling(
                table_name=table_name,
                limit=limit_val,
                correlations=include_correlations,
            )
            st.session_state.profiling_result = {
                "table": table_name,
                "summary": summary_metrics,
                "col_stats": col_stats,
                "corr_df": corr_df,
            }
            return True
        except Exception as ex:
            st.error(f"No se pudo completar el profiling sobre `{table_name}`: {str(ex)}")
            return False


def save_profiling_report(
    table_name: str,
    summary_metrics: Dict[str, Any],
    col_stats: pd.DataFrame,
    executed_by: str
) -> bool:
    """Persiste el reporte de profiling en la tabla histórica de Unity Catalog."""
    target_table = f"{db_backend.catalog}.{db_backend.schema}.dq_profiling_history"
    with st.spinner(f"Guardando métricas en `{target_table}`..."):
        try:
            db_backend.save_profiling_history(
                table_name=table_name,
                summary_metrics=summary_metrics,
                col_stats=col_stats,
                executed_by=executed_by,
            )
            st.success(f"✅ Métricas de `{table_name}` almacenadas correctamente en `{target_table}`.")
            return True
        except Exception as ex:
            st.error(f"Error al guardar el reporte histórico en Unity Catalog: {str(ex)}")
            return False