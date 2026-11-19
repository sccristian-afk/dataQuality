"""
CONTROLADOR DE INTERFAZ: ALTA INDIVIDUAL DE VALIDACIONES
========================================================
Aplica reglas de negocio obligatorias sobre el formulario individual
y coordina la persistencia de la propuesta en Unity Catalog.
"""
import streamlit as st
from backend import MAESTRO_REGLAS
from models import ValidationPayload
from ui_logic.session import db_backend


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