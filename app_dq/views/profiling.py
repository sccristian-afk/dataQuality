"""
VISTA: PROFILING DE DATOS
=========================
Calcula y visualiza métricas reales de calidad y perfilado de tablas en Unity Catalog
según permisos (Owner vs User).
"""
import streamlit as st
from typing import Optional
import ui_logic


def render():
    """Punto de entrada estándar invocado por el registro dinámico de vistas."""
    rol = st.session_state.get("user_role", "User")
    render_profiling_view(role=rol)


def render_profiling_view(role: Optional[str] = None):
    if role is None:
        role = st.session_state.get("user_role", "User")

    st.markdown(f"""
        <div class="ld-header">
            <div class="ld-title">Profiling de Datos</div>
            <div class="ld-subtitle">Análisis estadístico en tiempo real sobre Unity Catalog • Modo: {role}</div>
        </div>
    """, unsafe_allow_html=True)

    with st.container(border=True):
        # 1. Selector de tabla / catálogo
        tables = ui_logic.db_backend.get_table_list()
        c_sel, c_custom = st.columns([2, 2])
        with c_sel:
            selected_from_list = st.selectbox(
                "Selecciona una tabla del catálogo:",
                options=tables,
                key="prof_table_sel"
            )
        with c_custom:
            custom_table = st.text_input(
                "O introduce otra tabla de Unity Catalog (opcional):",
                placeholder="Ej: catalog.schema.nombre_tabla",
                key="prof_custom_table"
            )

        selected_table = custom_table.strip() if custom_table.strip() else selected_from_list

        if not selected_table:
            st.info("Selecciona o introduce una tabla disponible para analizar.")
            return

        # Opciones adicionales según el rol
        sample_size = None
        if role == "Owner":
            col1, col2 = st.columns(2)
            with col1:
                sample_size = st.number_input(
                    "Límite de registros para análisis (0 = tabla completa):",
                    min_value=0,
                    value=10000,
                    step=1000,
                    key="prof_sample_size"
                )
            with col2:
                st.write("")
                st.write("")
                include_correlations = st.checkbox(
                    "Calcular correlaciones entre columnas numéricas",
                    value=False,
                    key="prof_corr"
                )
        else:
            include_correlations = False

        if st.button("Ejecutar Profiling", type="primary", key="btn_run_profiling"):
            with st.spinner(f"Consultando Unity Catalog y calculando métricas para `{selected_table}`..."):
                try:
                    summary_metrics, col_stats, corr_df = ui_logic.db_backend.compute_table_profiling(
                        table_name=selected_table,
                        limit=sample_size if sample_size and sample_size > 0 else None,
                        correlations=include_correlations
                    )
                    st.session_state.profiling_result = {
                        "table": selected_table,
                        "summary": summary_metrics,
                        "col_stats": col_stats,
                        "corr_df": corr_df
                    }
                except Exception as ex:
                    st.error(f"No se pudo completar el profiling sobre `{selected_table}`: {str(ex)}")
                    return

    # Mostrar resultados persistidos en sesión
    prof_res = st.session_state.get("profiling_result")
    if prof_res and prof_res.get("table") == selected_table:
        summary_metrics = prof_res["summary"]
        col_stats = prof_res["col_stats"]
        corr_df = prof_res.get("corr_df")

        st.markdown("---")
        # 2. Métricas generales
        st.subheader(f"Resumen General • `{selected_table}`")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Filas Analizadas", f"{summary_metrics.get('total_rows', 0):,}")
        m2.metric("Total Columnas", summary_metrics.get('total_columns', 0))
        m3.metric("% Nulos Global", f"{summary_metrics.get('null_percentage', 0):.2f}%")
        m4.metric("Columnas con Nulos", summary_metrics.get('columns_with_nulls', 0))

        # 3. Detalle por columna
        st.subheader("Métricas por Columna")
        st.dataframe(
            col_stats,
            hide_index=True,
            use_container_width=True,
            column_config={
                "columna": st.column_config.TextColumn("Columna"),
                "tipo": st.column_config.TextColumn("Tipo de Dato"),
                "nulos": st.column_config.NumberColumn("Nulos", format="%d"),
                "null_pct": st.column_config.ProgressColumn(
                    "% Nulos",
                    format="%.2f%%",
                    min_value=0,
                    max_value=100,
                ),
                "unicos": st.column_config.NumberColumn("Valores Únicos (Aprox.)", format="%d"),
            }
        )

        # 4. Correlaciones (si fueron solicitadas por el Owner)
        if corr_df is not None:
            st.subheader("Correlación de Pearson (Columnas Numéricas)")
            if corr_df.empty:
                st.info("No se encontraron suficientes columnas numéricas para calcular correlaciones.")
            else:
                st.dataframe(corr_df, hide_index=True, use_container_width=True)

        # 5. Acciones exclusivas de Owner
        if role == "Owner":
            st.divider()
            st.subheader("Acciones de Administrador")
            if st.button("Guardar reporte en tabla histórica de métricas", key="btn_save_prof"):
                with st.spinner("Guardando métricas en `workspace.default.dq_profiling_history`..."):
                    try:
                        ui_logic.db_backend.save_profiling_history(
                            table_name=selected_table,
                            summary_metrics=summary_metrics,
                            col_stats=col_stats,
                            executed_by=st.session_state.get("user_email", "owner")
                        )
                        st.success(
                            f"✅ Métricas de `{selected_table}` almacenadas correctamente en "
                            f"`{ui_logic.db_backend.catalog}.{ui_logic.db_backend.schema}.dq_profiling_history`."
                        )
                    except Exception as ex:
                        st.error(f"Error al guardar el reporte histórico en Unity Catalog: {str(ex)}")