"""
VISTA: ALTA DE VALIDACIONES
===========================
Orquesta la pantalla principal de alta mediante pestañas corporativas
para alternar entre el alta individual y la carga masiva por Excel.
"""
import streamlit as st
from views import carga_masiva, alta_individual


def render():
    st.markdown("""
        <style>
            div[data-testid="stTabs"] button[role="tab"] {
                font-size: 0.98rem;
                font-weight: 600;
                color: #495057;
                padding: 10px 20px;
            }
            div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
                color: #D3131A !important;
                border-bottom-color: #D3131A !important;
            }
            .step-badge {
                display: inline-block;
                background-color: #FFF5F5;
                color: #D3131A;
                border: 1px solid #F5C2C7;
                font-weight: 700;
                font-size: 0.78rem;
                padding: 2px 10px;
                border-radius: 12px;
                margin-bottom: 8px;
                letter-spacing: 0.3px;
                text-transform: uppercase;
            }
        </style>
        <div class="ld-header">
            <div class="ld-title">Alta de validaciones</div>
            <div class="ld-subtitle">Registro de reglas de calidad de datos en el catálogo corporativo</div>
        </div>
    """, unsafe_allow_html=True)

    tab_individual, tab_masiva = st.tabs(["Alta individual", "Carga masiva"])

    with tab_individual:
        st.markdown("<div style='margin-top: 8px;'></div>", unsafe_allow_html=True)
        alta_individual.render()

    with tab_masiva:
        st.markdown("<div style='margin-top: 8px;'></div>", unsafe_allow_html=True)
        carga_masiva.render()