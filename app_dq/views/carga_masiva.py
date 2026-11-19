"""
SUBVISTA: CARGA MASIVA DE VALIDACIONES
======================================
Paso 1: Descarga de plantilla oficial con confirmación.
Paso 2: Importación estricta de plantilla modificada y fase de comprobación
        de campos obligatorios antes de saltar a la ventana de ejecución SQL.
"""
import streamlit as st
from ui_logic import carga_masiva as logic


def _set_confirm_download(value: bool):
    st.session_state.confirm_tpl_dl = value


def render():
    if "confirm_tpl_dl" not in st.session_state:
        st.session_state.confirm_tpl_dl = False

    col_step1, col_step2 = st.columns(2, gap="large")

    # ==========================================
    # PASO 1: DESCARGA DE PLANTILLA OFICIAL
    # ==========================================
    with col_step1:
        with st.container(border=True):
            st.markdown('<span class="step-badge">Paso 1</span>', unsafe_allow_html=True)
            st.markdown("#### Descargar plantilla oficial")
            st.caption(
                "Obtén el libro Excel configurado con los desplegables vigentes "
                "del catálogo y la guía de parámetros obligatorios por tipo de regla."
            )

            st.markdown("<div style='margin-top: 16px;'></div>", unsafe_allow_html=True)

            if not st.session_state.confirm_tpl_dl:
                st.button(
                    "Descargar plantilla oficial",
                    use_container_width=True,
                    key="btn_request_template",
                    on_click=_set_confirm_download,
                    args=(True,),
                )
            else:
                st.markdown("""
                    <div class="req-box" style="margin-bottom: 12px;">
                        ¿Confirmas que deseas descargar una nueva plantilla oficial? 
                        Asegúrate de no modificar ni eliminar las columnas ni las hojas del libro.
                    </div>
                """, unsafe_allow_html=True)

                c_conf, c_canc = st.columns(2)
                with c_conf:
                    template_bytes = logic.generate_template_bytes()
                    st.download_button(
                        label="Confirmar descarga",
                        data=template_bytes,
                        file_name="plantilla_validaciones_DQ.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                        use_container_width=True,
                        key="btn_confirm_download_tpl",
                        on_click=_set_confirm_download,
                        args=(False,),
                    )
                with c_canc:
                    st.button(
                        "Cancelar",
                        use_container_width=True,
                        key="btn_cancel_download_tpl",
                        on_click=_set_confirm_download,
                        args=(False,),
                    )

    # ==========================================
    # PASO 2: IMPORTACIÓN Y COMPROBACIÓN DE DATOS
    # ==========================================
    with col_step2:
        with st.container(border=True):
            st.markdown('<span class="step-badge">Paso 2</span>', unsafe_allow_html=True)
            st.markdown("#### Importar y comprobar plantilla")
            st.caption(
                "Adjunta únicamente la plantilla oficial cumplimentada. "
                "El sistema verificará su autenticidad y comprobará cada columna obligatoria."
            )

            uploaded_file = st.file_uploader(
                "Seleccionar archivo Excel (.xlsx)",
                type=["xlsx"],
                label_visibility="collapsed",
                key="bulk_excel_uploader",
            )

            st.markdown("<div style='margin-top: 8px;'></div>", unsafe_allow_html=True)

            if st.button(
                "Analizar y comprobar plantilla",
                type="primary",
                use_container_width=True,
                disabled=(uploaded_file is None),
                key="btn_bulk_analyze",
            ):
                # Fase 1: Autenticidad e integridad del Excel modificado
                is_authentic, auth_msg, df_parsed = logic.validate_template_authenticity(uploaded_file)
                if not is_authentic or df_parsed is None:
                    st.session_state.pop("bulk_validation_report", None)
                    st.error(auth_msg)
                    return

                # Fase 2: Comprobación de columnas obligatorias y valores por regla
                all_ok, df_diag, payloads = logic.validate_bulk_business_rules(
                    df_raw=df_parsed,
                    user_email=st.session_state.get("user_email", "unknown"),
                )

                if not all_ok:
                    st.session_state["bulk_validation_report"] = {
                        "file_name": uploaded_file.name,
                        "df_diag": df_diag,
                    }
                else:
                    st.session_state.pop("bulk_validation_report", None)
                    st.session_state["bulk_validated_batch"] = {
                        "file_name": uploaded_file.name,
                        "df_preview": df_parsed.drop(columns=["_excel_row"], errors="ignore"),
                        "df_diag": df_diag,
                        "payloads": payloads,
                    }
                    # Navegación directa a la ventana dedicada de ejecución SQL
                    st.session_state.active_view = "Ejecución carga masiva"
                    st.rerun()

    # Mostrar reporte detallado si hubo errores de comprobación en alguna fila
    report = st.session_state.get("bulk_validation_report")
    if report:
        st.markdown("---")
        df_diag = report["df_diag"]
        err_count = len(df_diag[df_diag["Estado"].str.startswith("❌")])
        st.error(
            f"❌ Comprobación fallida en `{report['file_name']}`: "
            f"se detectaron **{err_count} fila(s) con errores**. Corrige el Excel y vuelve a subirlo."
        )
        st.dataframe(df_diag, hide_index=True, use_container_width=True)