"""
VISTA DEDICADA: EJECUCIÓN DE CARGA MASIVA
=========================================
Ventana independiente a la que accede el usuario únicamente tras superar
la fase de autenticidad y comprobación de columnas obligatorias.
Ejecuta toda la inserción en Unity Catalog en una única secuencia SQL.
"""
import streamlit as st
from ui_logic.session import db_backend


def render():
    st.markdown("""
        <div class="ld-header">
            <div class="ld-title">Ejecución de carga masiva</div>
            <div class="ld-subtitle">Inserción atómica en Unity Catalog mediante secuencia SQL única</div>
        </div>
    """, unsafe_allow_html=True)

    batch = st.session_state.get("bulk_validated_batch")
    exec_done = st.session_state.get("bulk_execution_done")

    # Estado posterior a una ejecución exitosa
    if exec_done:
        st.success(
            f"✅ Carga masiva completada en una sola secuencia SQL: "
            f"**{exec_done['count']} validación(es)** insertadas en estado `Draft` desde `{exec_done['file_name']}`."
        )
        c1, c2, _ = st.columns([1.6, 1.6, 3])
        with c1:
            if st.button("Ir al Catálogo de reglas", type="primary", use_container_width=True):
                st.session_state.pop("bulk_execution_done", None)
                st.session_state.active_view = "Catálogo de reglas"
                st.rerun()
        with c2:
            if st.button("Realizar otra carga masiva", use_container_width=True):
                st.session_state.pop("bulk_execution_done", None)
                st.session_state.active_view = "Alta de validaciones"
                st.rerun()
        return

    # Protección si alguien accede sin haber validado previamente una plantilla
    if not batch or not batch.get("payloads"):
        st.warning("No hay ningún lote validado pendiente de ejecución. Sube y comprueba primero una plantilla oficial.")
        if st.button("Volver a Alta de validaciones", type="primary"):
            st.session_state.active_view = "Alta de validaciones"
            st.rerun()
        return

    payloads = batch["payloads"]
    df_diag = batch["df_diag"]
    file_name = batch["file_name"]

    # KPIs resumen del lote verificado
    tablas_unicas = len({p.table_id for p in payloads})
    criticas = sum(1 for p in payloads if p.severity == "CRITICAL")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Reglas a insertar", len(payloads))
    k2.metric("Tablas destino", tablas_unicas)
    k3.metric("Severidad CRITICAL", criticas)
    k4.metric("Estado inicial", "Draft")

    st.info(
        f"Archivo verificado: **{file_name}** • Todas las columnas obligatorias y parámetros por regla "
        "han superado la fase de comprobación."
    )

    st.markdown("#### Resumen de validaciones verificadas")
    st.dataframe(df_diag, hide_index=True, use_container_width=True)

    # Previsualización de la sentencia SQL única que se enviará al Warehouse
    single_sql = db_backend.build_bulk_insert_sql(payloads)
    with st.expander("Previsualizar sentencia SQL única consolidada"):
        st.code(single_sql, language="sql")

    st.markdown("---")
    col_exec, col_back, _ = st.columns([2, 1.5, 2.5])

    with col_exec:
        if st.button(
            f"Ejecutar carga masiva",
            type="primary",
            use_container_width=True,
            key="btn_execute_single_sql_bulk",
        ):
            with st.spinner("Ejecutando sentencia SQL única en Databricks Unity Catalog..."):
                try:
                    resp = db_backend.insert_validations_bulk(payloads)
                    if "SUCCEEDED" in str(resp.status.state):
                        st.session_state["bulk_execution_done"] = {
                            "count": len(payloads),
                            "file_name": file_name,
                        }
                        st.session_state.pop("bulk_validated_batch", None)
                        st.rerun()
                    else:
                        err_msg = (
                            resp.status.error.message
                            if resp.status and resp.status.error
                            else str(resp.status.state)
                        )
                        st.error(f"Error en Unity Catalog al ejecutar la secuencia SQL: {err_msg}")
                except Exception as ex:
                    st.error(f"Error ejecutando la carga masiva: {str(ex)}")

    with col_back:
        if st.button("Cancelar y volver", use_container_width=True, key="btn_cancel_bulk_exec"):
            st.session_state.pop("bulk_validated_batch", None)
            st.session_state.active_view = "Alta de validaciones"
            st.rerun()