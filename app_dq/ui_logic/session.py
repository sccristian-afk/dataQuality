"""
CONTROLADOR DE SESIÓN Y AUTENTICACIÓN
=====================================
Gestiona el estado global de sesión de Streamlit, la instancia compartida
del backend de Databricks y los procesos de login/logout.
"""
import streamlit as st
from backend import DatabricksBackend

# Instancia singleton del backend compartida por todos los controladores
db_backend = DatabricksBackend()


def get_default_view_for_role(role: str) -> str:
    """Determina la vista inicial por defecto según el perfil del usuario."""
    if role == "Admin":
        return "Configuración"
    return "Alta de validaciones"


def init_session_state():
    """Inicializa de forma segura las variables de estado en memoria."""
    defaults = {
        "authenticated": False,
        "user_email": "",
        "user_role": "",
        "user_name": "",
        "active_view": "Alta de validaciones",
        "editor_version": 0,
        "select_mode": "none",
        "feedback_msg": None,
        "profiling_result": None,
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
            default_view = get_default_view_for_role(user.role)
            st.session_state.update(
                authenticated=True,
                user_email=user.email,
                user_role=user.role,
                user_name=user.full_name,
                active_view=default_view,
            )
            st.rerun()
        else:
            st.error("Credenciales incorrectas o usuario no registrado.")
    except Exception as e:
        st.error(f"Error de acceso al servicio de autenticación: {str(e)}")


def process_logout():
    """Limpia el estado y desconecta al usuario."""
    st.session_state.update(
        authenticated=False,
        user_email="",
        user_role="",
        user_name="",
        active_view="Alta de validaciones",
        select_mode="none",
        editor_version=0,
        feedback_msg=None,
        profiling_result=None,
    )
    st.rerun()