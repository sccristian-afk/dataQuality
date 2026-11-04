"""
VISTA: ALTA DE VALIDACIONES
===========================
Renderiza el formulario guiado de alta según los metadatos institucionales.
"""
import streamlit as st
import ui_logic
from backend import MAESTRO_TABLAS, MAESTRO_REGLAS
from models import ValidationPayload
from config import cfg

def render():
    st.markdown("""
        <div class="ld-header">
            <div class="ld-title">Alta de Validaciones</div>
            <div class="ld-subtitle">Registro y propuesta de validaciones de calidad según el diccionario de metadatos</div>
        </div>
    """, unsafe_allow_html=True)

    with st.container(border=True):
        st.markdown("#### Formulario de registro de validaciones")
        st.markdown("""
            <div class="req-box">
                Los campos identificados con <span class="req-mark">*</span> son de <b>cumplimentación obligatoria</b>.
            </div>
        """, unsafe_allow_html=True)

        # 1. Identificación y tabla
        st.markdown("##### 1. Identificación y tabla de destino")
        c1, c2, c3 = st.columns(3)
        with c1:
            id_val = st.text_input(f"{cfg.get_col('COL_ID_VALIDACION')} *", placeholder="Ej: val-038", key="v_id_val")
        with c2:
            tabla_sel = st.selectbox(f"{cfg.get_col('COL_NOMBRE_TABLA')} *", options=list(MAESTRO_TABLAS.keys()), key="v_tabla_sel")
            meta_tabla = MAESTRO_TABLAS[tabla_sel]
        with c3:
            st.text_input(f"{cfg.get_col('COL_DOMINIO_TABLA')} (Auto)", value=meta_tabla["domain"], disabled=True)

        # 2. Regla técnica y dimensión
        st.markdown("##### 2. Regla técnica y dimensión")
        cb1, cb2, cb3 = st.columns(3)
        with cb1:
            regla_sel = st.selectbox(f"{cfg.get_col('COL_NOMBRE_REGLA')} *", options=list(MAESTRO_REGLAS.keys()), key="v_regla_sel")
            meta_regla = MAESTRO_REGLAS[regla_sel]
        with cb2:
            st.text_input(f"{cfg.get_col('COL_NOMBRE_TECNICO')} (Auto)", value=meta_regla["technical_rule_name"], disabled=True)
        with cb3:
            st.text_input(f"{cfg.get_col('COL_DIMENSION_REGLA')} (Auto)", value=meta_regla["dimension"], disabled=True)

        # 3. Parámetros de la regla
        st.markdown("##### 3. Parámetros técnicos de la regla")
        p_cols, p_val, p_range, p_expr, p_query, p_merge = "", "", "", "", "", ""

        if meta_regla["show_cols"] and not meta_regla["show_merge"]:
            if meta_regla["show_val"]:
                cp1, cp2 = st.columns(2)
                with cp1: p_cols = st.text_input(f"{cfg.get_col('COL_PARAM_COLUMNAS')} *", placeholder="Ej: vat_id", key="vp_col_regex")
                with cp2: p_val = st.text_input(meta_regla["val_label"], placeholder=meta_regla["val_placeholder"], key="vp_val_regex")
            elif meta_regla["show_range"]:
                cp1, cp2 = st.columns(2)
                with cp1: p_cols = st.text_input(f"{cfg.get_col('COL_PARAM_COLUMNAS')} *", placeholder="Ej: currency", key="vp_col_list")
                with cp2: p_range = st.text_input(f"{cfg.get_col('COL_PARAM_CONJUNTO')} *", placeholder="Ej: EUR,USD", key="vp_range_list")
            else:
                p_cols = st.text_input(f"{cfg.get_col('COL_PARAM_COLUMNAS')} *", placeholder="Ej: id_registro", key="vp_col_basic")

        elif meta_regla["show_expr"]:
            p_expr = st.text_area(f"{cfg.get_col('COL_PARAM_EXPRESION')} *", placeholder="Expresión SQL booleana", key="vp_expr_area")
        elif meta_regla["show_query"]:
            p_query = st.text_area(f"{cfg.get_col('COL_PARAM_QUERY_SQL')} *", placeholder="SELECT ... FROM ...", key="vp_query_area")
            p_merge = st.text_input(f"{cfg.get_col('COL_PARAM_MERGE_COLUMNAS')} *", placeholder="Ej: id_registro", key="vp_merge_query")
        elif meta_regla["show_merge"] and meta_regla["show_cols"]:
            cp1, cp2, cp3 = st.columns(3)
            with cp1: p_cols = st.text_input(f"{cfg.get_col('COL_PARAM_COLUMNAS')} (Origen) *", key="vp_col_comp")
            with cp2: p_val = st.text_input(meta_regla["val_label"], key="vp_val_comp")
            with cp3: p_merge = st.text_input(f"{cfg.get_col('COL_PARAM_MERGE_COLUMNAS')} (Destino) *", key="vp_merge_comp")

        p_perimetro = st.text_input(f"{cfg.get_col('COL_DEFINICION_PERIMETRO')} (WHERE opcional)", placeholder="Ej: STATUS='ACTIVO'", key="vp_perim_input")

        # 4. Gobierno
        st.markdown("##### 4. Gobierno y operación")
        cg1, cg2, cg3, cg4 = st.columns(4)
        with cg1: severidad = st.selectbox(f"{cfg.get_col('COL_SEVERIDAD')} *", options=["WARNING", "CRITICAL", "INFO"], key="v_sev")
        with cg2: owner = st.text_input(f"{cfg.get_col('COL_OWNER')} *", value="Línea Directa DQ", key="v_owner")
        with cg3: tags = st.text_input(f"{cfg.get_col('COL_ETIQUETAS')}", value="Testing", key="v_tags")
        with cg4: st.text_input(f"{cfg.get_col('COL_ESTADO')}", value="Draft", disabled=True)

        is_activa = st.checkbox(f"{cfg.get_col('COL_VALIDACION_ACTIVA')}", value=True, key="v_activa")

        if st.button("Guardar Validación en Catálogo", type="primary"):
            payload = ValidationPayload(
                id_validacion=id_val,
                rule_id=meta_regla["rule_id"],
                technical_rule=meta_regla["technical_rule_name"],
                table_id=meta_tabla["table_id"],
                domain=meta_tabla["domain"],
                severity=severidad,
                owner=owner,
                user_email=st.session_state.user_email,
                is_active=is_activa,
                perimeter=p_perimetro,
                columns_str=p_cols,
                param_val=p_val,
                param_range=p_range,
                param_expr=p_expr,
                param_query=p_query,
                param_merge=p_merge,
                tags_str=tags
            )
            ui_logic.validate_and_save(payload, regla_sel)
