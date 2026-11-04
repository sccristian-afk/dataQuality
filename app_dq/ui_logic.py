"""
CAPA DE CONTROLADOR Y LÓGICA DE INTERFAZ
========================================
Gestiona el estado de sesión de Streamlit, orquesta las validaciones de negocio,
y actúa como puente desacoplado entre las vistas y el backend.
"""
import streamlit as st
import pandas as pd
from typing import List, Optional
from backend import DatabricksBackend, MAESTRO_REGLAS
from models import ValidationPayload

# Instancia singleton del backend
db_backend = DatabricksBackend()

def init_session_state():
    """Inicializa de forma segura las variables de estado en memoria."""
    defaults = {
        "authenticated": False, 
        "user_email": "", 
        "user_role": "", 
        "user_name": "",
        "active_view": "Alta de Validaciones", 
        "editor_version": 0,
        "select_mode": "none", 
        "feedback_msg": None
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

def process_login(email: str, password: str):
    """Maneja el intento de login del usuario."""
    if not email.strip() or not password.strip():
        st.error("Debe ingresar correo y contraseña.")
        return
    try:
        user = db_backend.verify_credentials(email, password)
        if user:
            st.session_state.update(
                authenticated=True, 
                user_email=user.email, 
                user_role=user.role, 
                user_name=user.full_name, 
                active_view="Alta de Validaciones"
            )
            st.rerun()
        else:
            st.error("Credenciales incorrectas o usuario no registrado.")
    except Exception as e:
        st.error(f"Error de acceso al servicio de autenticación: {str(e)}")

def process_logout():
    """Limpia el estado y desconecta al usuario."""
    st.session_state.update(
        authenticated=False, user_email="", user_role="", user_name="", 
        active_view="Alta de Validaciones", select_mode="none", 
        editor_version=0, feedback_msg=None
    )
    st.rerun()

def validate_and_save(payload: ValidationPayload, meta_regla_name: str) -> bool:
    """Aplica reglas de negocio obligatorias antes de persistir en Databricks."""
    meta_regla = MAESTRO_REGLAS.get(meta_regla_name, {})
    errores = []

    if not payload.id_validacion.strip():
        errores.append("El campo 'Id_validación *' es obligatorio.")
    if not payload.owner.strip():
        errores.append("El campo 'Owner / Responsable *' es obligatorio.")
    
    if meta_regla.get("show_cols") and not payload.columns_str.strip():
        errores.append("Para la regla seleccionada es obligatorio indicar 'Param_columnas *'.")
    if meta_regla.get("show_val") and not payload.param_val.strip():
        errores.append(f"Para la regla seleccionada es obligatorio indicar '{meta_regla.get('val_label')}'.")
    if meta_regla.get("show_range") and not payload.param_range.strip():
        errores.append("Para la regla seleccionada es obligatorio definir 'Param_conjunto *'.")
    if meta_regla.get("show_expr") and not payload.param_expr.strip():
        errores.append("Para la regla seleccionada es obligatoria la 'Param_expresion *'.")
    if meta_regla.get("show_query") and not payload.param_query.strip():
        errores.append("Para la regla seleccionada es obligatoria la 'Param_query_SQL *'.")
    if meta_regla.get("show_merge") and not payload.param_merge.strip():
        errores.append("Para la regla seleccionada es obligatorio indicar 'Param_merge_columnas *'.")

    if errores:
        st.error("No se pudo guardar la validación. Complete los campos obligatorios:")
        for err in errores:
            st.markdown(f"- ❌ **{err}**")
        return False

    try:
        resp = db_backend.insert_validation(payload)
        if "SUCCEEDED" in str(resp.status.state):
            st.success(f"Propuesta '{payload.id_validacion}' guardada con éxito en estado 'Draft'.")
            return True
        else:
            err_msg = resp.status.error.message if resp.status and resp.status.error else str(resp.status.state)
            st.error(f"Error en Unity Catalog: {err_msg}")
    except Exception as ex:
        st.error(f"Error de comunicación: {str(ex)}")
    return False

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
