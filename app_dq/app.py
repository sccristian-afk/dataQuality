"""
ORQUESTADOR
========================================
Inicializa Streamlit, inyecta los estilos corporativos de Línea Directa
y orquesta el ciclo de vida del login y navegación por vistas registradas.
"""
import streamlit as st
from ui_logic import session as ui_session
from views import REGISTERED_VIEWS

# 1. Configuración de página e identidad visual
st.set_page_config(
    page_title="Línea Directa Aseguradora - Gobierno de Calidad de Datos",
    page_icon="☎️",
    layout="wide"
)

# Estilos CSS Corporativos
st.markdown("""
<style>
    :root { --ld-red: #D3131A; --ld-dark-red: #A50E14; --ld-dark: #212529; --ld-gray-bg: #F8F9FA; --ld-border: #DEE2E6; }
    section[data-testid="stSidebar"] div.stButton > button {
        background: transparent !important; border: none !important; border-radius: 0px !important;
        box-shadow: none !important; padding: 6px 12px !important; margin: 2px 0px !important;
        text-align: left !important; justify-content: flex-start !important; display: flex !important;
        width: 100% !important; transition: color 0.15s ease, border-left 0.15s ease;
    }
    section[data-testid="stSidebar"] div.stButton > button[kind="primary"] {
        color: var(--ld-red) !important; font-weight: 700 !important; font-size: 1.05rem !important; border-left: 3px solid var(--ld-red) !important;
    }
    section[data-testid="stSidebar"] div.stButton > button[kind="secondary"] {
        color: #495057 !important; font-weight: 500 !important; font-size: 1.02rem !important; border-left: 3px solid transparent !important;
    }
    section[data-testid="stSidebar"] div.stButton > button:hover { background-color: rgba(211, 19, 26, 0.04) !important; color: var(--ld-red) !important; }

    div[data-testid="stMain"] div.stButton > button[kind="primary"], div[data-testid="stFormSubmitButton"] > button {
        background-color: var(--ld-red) !important; color: #FFFFFF !important; border: none !important; font-weight: 600 !important; border-radius: 4px !important;
    }
    div[data-testid="stMain"] div.stButton > button[kind="primary"]:hover, div[data-testid="stFormSubmitButton"] > button:hover {
        background-color: var(--ld-dark-red) !important; color: #FFFFFF !important;
    }

    div.st-key-btn_sel_drafts > button, div.st-key-btn_sel_all > button, div.st-key-btn_desel_all > button {
        background-color: #F8F9FA !important; color: #495057 !important; border: 1px solid #CED4DA !important;
        border-radius: 20px !important; font-size: 0.88rem !important; font-weight: 500 !important; padding: 4px 14px !important;
    }
    div.st-key-btn_sel_drafts > button:hover, div.st-key-btn_sel_all > button:hover, div.st-key-btn_desel_all > button:hover {
        background-color: #E9ECEF !important; border-color: #ADB5BD !important; color: #212529 !important;
    }

    div.st-key-btn_aprobar_action > button {
        background-color: var(--ld-red) !important; color: #FFFFFF !important; border: none !important;
        font-weight: 700 !important; font-size: 0.98rem !important; border-radius: 6px !important; padding: 8px 18px !important;
    }
    div.st-key-btn_aprobar_action > button:hover { background-color: var(--ld-dark-red) !important; }

    div.st-key-btn_eliminar_action > button {
        background-color: #FFF5F5 !important; color: var(--ld-red) !important; border: 1.5px solid var(--ld-red) !important;
        font-weight: 700 !important; font-size: 0.98rem !important; border-radius: 6px !important; padding: 8px 18px !important;
    }
    div.st-key-btn_eliminar_action > button:hover { background-color: var(--ld-red) !important; color: #FFFFFF !important; }
    
    .ld-header { border-left: 6px solid var(--ld-red); padding-left: 16px; margin-bottom: 24px; }
    .ld-title { color: var(--ld-dark); font-size: 1.85rem; font-weight: 700; margin-bottom: 2px; }
    .ld-subtitle { color: #6C757D; font-size: 0.95rem; margin-top: 0; }
    .req-mark { color: var(--ld-red); font-weight: 800; font-size: 1.15rem; margin-left: 4px; }
    .req-box { background-color: #FFF5F5; border-left: 4px solid var(--ld-red); padding: 12px 16px; margin-bottom: 20px; border-radius: 3px; font-size: 0.92rem; color: #333333; }
</style>
""", unsafe_allow_html=True)

# 2. Inicializar el estado de sesión
ui_session.init_session_state()

# 3. Flujo de Autenticación
if not st.session_state.authenticated:
    st.markdown("""
        <div class="ld-header">
            <div class="ld-title">LÍNEA DIRECTA ASEGURADORA</div>
            <div class="ld-subtitle">Portal de Gobierno y Calidad de Datos • Autenticación de Acceso</div>
        </div>
    """, unsafe_allow_html=True)
    
    _, col_center, _ = st.columns([1, 1.8, 1])
    with col_center:
        with st.container(border=True):
            st.markdown("### Inicio de Sesión")
            with st.form("form_login", clear_on_submit=False):
                txt_email = st.text_input("Correo electrónico", placeholder="ejemplo@lineadirecta.es")
                txt_pass = st.text_input("Contraseña", type="password", placeholder="••••••••")
                if st.form_submit_button("Iniciar Sesión", type="primary", use_container_width=True):
                    ui_session.process_login(txt_email, txt_pass)
            st.markdown("---")
            st.caption("**Cuentas de prueba disponibles:**")
            st.markdown("""
                    | Perfil | Correo electrónico | Contraseña |
                    | :--- | :--- | :--- |
                    | **User** | `user@lineadirecta.es` | `user123` |
                    | **Owner** | `owner@lineadirecta.es` | `owner123` |
                    | **Admin** | `admin@lineadirecta.es` | `admin123` |
                    """)
    st.stop()

# 4. Barra Lateral Dinámica
st.sidebar.markdown("### LÍNEA DIRECTA")
st.sidebar.caption("Data Quality Governance Platform")
st.sidebar.markdown("---")
st.sidebar.caption(f"{st.session_state.user_email}")

rol = st.session_state.user_role
if rol == "Admin":
    st.sidebar.warning("Perfil asignado: **Admin**")
elif rol == "Owner":
    st.sidebar.success("Perfil asignado: **Owner**")
else:
    st.sidebar.info("Perfil asignado: **User**")

if st.sidebar.button("Cerrar Sesión", use_container_width=True):
    ui_session.process_logout()

st.sidebar.markdown("---")
st.sidebar.markdown("#### Navegación")

# Renderizado automatizado del menú de navegación sin emojis
for view_name, view_meta in REGISTERED_VIEWS.items():
    if not view_meta.show_in_sidebar:
        continue
    if view_meta.allowed_roles is not None and rol not in view_meta.allowed_roles:
        continue
    
    is_active = (st.session_state.active_view == view_name)
    btn_kind = "primary" if is_active else "secondary"
    btn_label = f"{view_meta.icon} {view_meta.title}".strip() if view_meta.icon else view_meta.title
    
    if st.sidebar.button(btn_label, type=btn_kind, use_container_width=True):
        st.session_state.active_view = view_name
        st.session_state.feedback_msg = None
        st.rerun()

# 5. Despacho dinámico de la vista activa con verificación de autorización
vista_activa = st.session_state.active_view
if vista_activa in REGISTERED_VIEWS:
    view_def = REGISTERED_VIEWS[vista_activa]
    if view_def.allowed_roles is not None and rol not in view_def.allowed_roles:
        st.error("⛔ Acceso denegado: Tu perfil no tiene permisos para acceder a esta vista.")
    else:
        view_def.render_func()
else:
    st.warning(f"La vista '{vista_activa}' no está registrada.")