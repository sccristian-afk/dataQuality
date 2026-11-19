"""
CONTROLADOR DE INTERFAZ: CATÁLOGO DE REGLAS
===========================================
Gestiona la consulta del catálogo, la selección masiva en tabla
y las acciones de gobierno (aprobación y borrado de borradores).
"""
import streamlit as st
import pandas as pd
from typing import List, Optional
from ui_logic.session import db_backend


def get_validations_data() -> pd.DataFrame:
    """Obtiene los datos del catálogo central."""
    try:
        return db_backend.fetch_validations()
    except Exception as err:
        st.warning(f"Aviso de sincronización con el catálogo: {str(err)}")
        return pd.DataFrame()


def process_quick_select(mode: str, df_show: Optional[pd.DataFrame] = None):
    """Controla el modo de selección masiva en la tabla."""
    if mode == "drafts" and df_show is not None:
        draft_count = len(df_show[df_show["status"].fillna("").astype(str).str.strip().str.lower() != "approve"])
        if draft_count == 0:
            st.session_state.feedback_msg = ("info", "ℹ️ Todas las validaciones están aprobadas. No hay borradores pendientes.")
            st.session_state.select_mode = "none"
        else:
            st.session_state.feedback_msg = ("success", f"✅ Marcadas {draft_count} validación(es) en borrador.")
            st.session_state.select_mode = "drafts"
    elif mode == "all":
        st.session_state.select_mode = "all"
        st.session_state.feedback_msg = ("success", "✅ Marcadas todas las validaciones de la tabla.")
    elif mode == "none":
        st.session_state.select_mode = "none"
        st.session_state.feedback_msg = ("info", "Se han desmarcado todas las validaciones.")
    st.session_state.editor_version += 1
    st.rerun()


def process_approve(ids: List[str], df_selected: pd.DataFrame):
    """Aprueba las reglas seleccionadas."""
    if not ids:
        st.warning("Seleccione al menos una validación.")
        return
    pendientes = df_selected[df_selected["status"].str.strip().str.lower() != "approve"]["validation_id"].tolist()
    if not pendientes:
        st.info("Todas las reglas seleccionadas ya están aprobadas ('Approve').")
        return
    try:
        resp = db_backend.update_status(pendientes, "Approve")
        if "SUCCEEDED" in str(resp.status.state):
            st.session_state.feedback_msg = ("success", f"✅ Se han aprobado {len(pendientes)} validación(es).")
            st.session_state.select_mode = "none"
            st.session_state.editor_version += 1
            st.rerun()
        else:
            st.error("No se pudo actualizar el estado en Databricks.")
    except Exception as e:
        st.error(f"Error de ejecución: {str(e)}")


def process_delete(ids: List[str], df_selected: pd.DataFrame):
    """Elimina las reglas seleccionadas verificando que no sean oficiales."""
    if not ids:
        st.warning("Seleccione al menos una validación.")
        return
    aprobadas = df_selected[df_selected["status"].str.strip().str.lower() == "approve"]
    if not aprobadas.empty:
        st.error("❌ Acción bloqueada: No se permite eliminar reglas oficiales ('Approve'). Desmárquelas antes de continuar.")
        return
    try:
        resp = db_backend.delete_drafts(ids)
        if "SUCCEEDED" in str(resp.status.state):
            st.session_state.feedback_msg = ("success", f"✅ Se han eliminado permanentemente {len(ids)} propuesta(s).")
            st.session_state.select_mode = "none"
            st.session_state.editor_version += 1
            st.rerun()
        else:
            st.error("No se pudieron eliminar los registros.")
    except Exception as e:
        st.error(f"Error de ejecución: {str(e)}")