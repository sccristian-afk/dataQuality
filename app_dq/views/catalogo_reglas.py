"""
VISTA: CATÁLOGO DE REGLAS
=========================
Visualización y gestión interactiva según permisos (Owner vs User).
"""
import streamlit as st
import ui_logic
from config import cfg

def render():
    st.markdown("""
        <div class="ld-header">
            <div class="ld-title">Catálogo de Reglas</div>
            <div class="ld-subtitle">Repositorio consolidado de validaciones en Unity Catalog</div>
        </div>
    """, unsafe_allow_html=True)

    df_validaciones = ui_logic.get_validations_data()
    total = len(df_validaciones)
    aprobadas = len(df_validaciones[df_validaciones["status"] == "Approve"]) if not df_validaciones.empty and "status" in df_validaciones.columns else 0
    drafts = len(df_validaciones[df_validaciones["status"] == "Draft"]) if not df_validaciones.empty and "status" in df_validaciones.columns else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Validaciones", total)
    k2.metric("Aprobadas (Approve)", aprobadas)
    k3.metric("Borradores (Draft)", drafts)
    with k4:
        st.write("")
        if st.button("Actualizar Catálogo", use_container_width=True):
            st.session_state.editor_version += 1
            st.session_state.select_mode = "none"
            st.rerun()

    st.markdown("---")

    if df_validaciones.empty:
        st.info("No se registran validaciones en el catálogo central.")
        return

    df_show = df_validaciones.copy()
    if "Param_columns" in df_show.columns:
        df_show["Param_columns"] = df_show["Param_columns"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))

    rol = st.session_state.user_role

    if rol == "Owner":
        st.markdown("##### Gestión de validaciones")
        st.caption("Marque casillas para dictaminar en bloque. Las reglas aprobadas son inmutables.")

        if st.session_state.feedback_msg:
            tipo, msg = st.session_state.feedback_msg
            getattr(st, tipo)(msg)
            st.session_state.feedback_msg = None

        st.markdown("<span style='font-size: 0.88rem; font-weight: 600; color: #495057;'>Selección rápida:</span>", unsafe_allow_html=True)
        s1, s2, s3, _ = st.columns([1.8, 1.4, 1.4, 3.4])
        if s1.button("☑️ Marcar no aprobadas", key="btn_sel_drafts"): ui_logic.process_quick_select("drafts", df_show)
        if s2.button("☑️ Marcar todas", key="btn_sel_all"): ui_logic.process_quick_select("all")
        if s3.button("◻️ Desmarcar todas", key="btn_desel_all"): ui_logic.process_quick_select("none")

        st.markdown("<div style='margin-top: 14px;'></div>", unsafe_allow_html=True)
        a1, a2, _ = st.columns([2.2, 2.2, 3.6])
        btn_aprobar = a1.button("✅ Aprobar selección", key="btn_aprobar_action", use_container_width=True)
        btn_eliminar = a2.button("🗑️ Eliminar selección", key="btn_eliminar_action", use_container_width=True)

        if st.session_state.select_mode == "drafts":
            df_show.insert(0, "Seleccionar", df_show["status"].fillna("").astype(str).str.strip().str.lower() != "approve")
        elif st.session_state.select_mode == "all":
            df_show.insert(0, "Seleccionar", True)
        else:
            df_show.insert(0, "Seleccionar", False)

        cols_display = ["Seleccionar", "validation_id", "rule_id", "table_id", "domain", "Param_columns", "severity", "status", "is_active", "owner", "validation_definition"]
        edited_df = st.data_editor(
            df_show[[c for c in cols_display if c in df_show.columns]],
            column_config={"Seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False)},
            disabled=[c for c in cols_display if c != "Seleccionar" and c in df_show.columns],
            hide_index=True,
            use_container_width=True,
            key=f"editor_owner_{st.session_state.editor_version}"
        )

        filas_sel = edited_df[edited_df["Seleccionar"] == True]
        ids_sel = filas_sel["validation_id"].tolist()

        if ids_sel:
            st.caption(f"📌 {len(ids_sel)} de {total} fila(s) seleccionada(s).")

        if btn_aprobar: ui_logic.process_approve(ids_sel, filas_sel)
        if btn_eliminar: ui_logic.process_delete(ids_sel, filas_sel)

    else:
        # Rol User (Solo lectura)
        cols_user = ["validation_id", "rule_id", "table_id", "domain", "Param_columns", "severity", "status", "is_active", "owner", "validation_definition"]
        st.dataframe(df_show[[c for c in cols_user if c in df_show.columns]], hide_index=True, use_container_width=True)
