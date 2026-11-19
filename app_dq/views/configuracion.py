"""
VISTA: CONFIGURACIÓN
====================
Pantalla exclusiva para el rol Admin que permite consultar los parámetros
actuales del Excel (solo lectura) y añadir nuevas filas al final.
"""
import streamlit as st
from ui_logic import configuracion as logic


def render():
    if st.session_state.get("user_role") != "Admin":
        st.error("⛔ Acceso restringido: Esta pantalla es exclusiva para el perfil Admin.")
        return

    st.markdown("""
        <div class="ld-header">
            <div class="ld-title">Configuración del sistema</div>
            <div class="ld-subtitle">Panel exclusivo de Administración • Alta de nuevos parámetros en parametrizar_tabla.xlsx</div>
        </div>
    """, unsafe_allow_html=True)

    # Mostrar mensaje de confirmación tras añadir una fila
    if st.session_state.get("config_feedback"):
        tipo, msg = st.session_state["config_feedback"]
        getattr(st, tipo)(msg)
        st.session_state["config_feedback"] = None

    form_v = st.session_state.get("config_form_version", 0)

    # 1. Formulario para insertar exclusivamente nuevas filas
    with st.container(border=True):
        st.markdown("#### ➕ Añadir nueva fila a `parametrizar_tabla.xlsx`")
        st.caption("Los registros existentes están protegidos contra edición. Completa los campos para incorporar una nueva fila.")

        with st.form(f"form_add_config_row_{form_v}", clear_on_submit=False):
            c1, c2 = st.columns(2)
            with c1:
                var_interna = st.text_input(
                    "Variable_Interna *",
                    placeholder="Ej: COL_NUEVO_PARAMETRO",
                    key=f"cfg_var_{form_v}",
                )
            with c2:
                nombre_excel = st.text_input(
                    "Nombre_Columna_Excel *",
                    placeholder="Ej: Nuevo_parametro",
                    key=f"cfg_nom_{form_v}",
                )

            descripcion = st.text_input(
                "Descripcion",
                placeholder="Descripción funcional del nuevo parámetro (opcional)",
                key=f"cfg_desc_{form_v}",
            )

            if st.form_submit_button("Añadir parametro", type="primary"):
                logic.append_config_row(var_interna, nombre_excel, descripcion)

    st.markdown("---")

    # 2. Tabla de registros existentes (estrictamente solo lectura)
    df_current = logic.load_config_data()
    st.markdown(f"#### 📄 Registros actuales en `Mapeo_Validaciones` ({len(df_current)} filas • Solo lectura)")
    st.dataframe(
        df_current,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Variable_Interna": st.column_config.TextColumn("Variable interna (Sistema)"),
            "Nombre_Columna_Excel": st.column_config.TextColumn("Nombre comercial"),
            "Descripcion": st.column_config.TextColumn("Descripción funcional"),
        },
    )