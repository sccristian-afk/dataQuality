import streamlit as st
import pandas as pd
import json
from databricks.sdk import WorkspaceClient

# Configuración institucional de la aplicación
st.set_page_config(
    page_title="Línea Directa - Gobierno de Calidad de Datos",
    page_icon="🛡️",
    layout="wide"
)

# Inyección de estilos corporativos (Línea Directa Aseguradora)
st.markdown("""
<style>
    :root {
        --ld-red: #D3131A;
        --ld-dark-red: #A50E14;
        --ld-dark: #212529;
        --ld-gray-bg: #F8F9FA;
        --ld-border: #DEE2E6;
    }
    
    /* =========================================================================
       NAVEGACIÓN LATERAL: TEXTO COMO BOTÓN (SIN CAJAS NI BORDES)
       ========================================================================= */
    section[data-testid="stSidebar"] div.stButton > button {
        background: transparent !important;
        background-color: transparent !important;
        border: none !important;
        border-radius: 0px !important;
        box-shadow: none !important;
        outline: none !important;
        padding: 6px 12px !important;
        margin: 2px 0px !important;
        text-align: left !important;
        justify-content: flex-start !important;
        display: flex !important;
        width: 100% !important;
        cursor: pointer !important;
        transition: color 0.15s ease, border-left 0.15s ease;
    }
    
    section[data-testid="stSidebar"] div.stButton > button[kind="primary"] {
        color: var(--ld-red) !important;
        font-weight: 700 !important;
        font-size: 1.05rem !important;
        border-left: 3px solid var(--ld-red) !important;
        background-color: transparent !important;
    }
    
    section[data-testid="stSidebar"] div.stButton > button[kind="secondary"] {
        color: #495057 !important;
        font-weight: 500 !important;
        font-size: 1.02rem !important;
        border-left: 3px solid transparent !important;
        background-color: transparent !important;
    }
    
    section[data-testid="stSidebar"] div.stButton > button:hover {
        background-color: rgba(211, 19, 26, 0.04) !important;
        color: var(--ld-red) !important;
    }
    section[data-testid="stSidebar"] div.stButton > button:focus {
        outline: none !important;
        box-shadow: none !important;
    }

    /* =========================================================================
       BOTONES GENERALES Y FORMULARIOS
       ========================================================================= */
    div[data-testid="stMain"] div.stButton > button[kind="primary"],
    div[data-testid="stFormSubmitButton"] > button {
        background-color: var(--ld-red) !important;
        color: #FFFFFF !important;
        border: none !important;
        font-weight: 600 !important;
        border-radius: 4px !important;
    }
    div[data-testid="stMain"] div.stButton > button[kind="primary"]:hover,
    div[data-testid="stFormSubmitButton"] > button:hover {
        background-color: var(--ld-dark-red) !important;
        color: #FFFFFF !important;
    }

    /* =========================================================================
       HERRAMIENTAS DE SELECCIÓN RÁPIDA (FORMATO PILL / SECUNDARIO SUAVE)
       ========================================================================= */
    div.st-key-btn_sel_drafts > button,
    div.st-key-btn_sel_all > button,
    div.st-key-btn_desel_all > button {
        background-color: #F8F9FA !important;
        color: #495057 !important;
        border: 1px solid #CED4DA !important;
        border-radius: 20px !important;
        font-size: 0.88rem !important;
        font-weight: 500 !important;
        padding: 4px 14px !important;
        box-shadow: none !important;
        transition: all 0.15s ease !important;
    }
    div.st-key-btn_sel_drafts > button:hover,
    div.st-key-btn_sel_all > button:hover,
    div.st-key-btn_desel_all > button:hover {
        background-color: #E9ECEF !important;
        border-color: #ADB5BD !important;
        color: #212529 !important;
    }

    /* =========================================================================
       BOTONES DE ACCIÓN PRINCIPAL EN EL CATÁLOGO (ALTO IMPACTO Y CLARIDAD)
       ========================================================================= */
    /* Botón Aprobar (Sólido Rojo Corporativo Línea Directa) */
    div.st-key-btn_aprobar_action > button {
        background-color: var(--ld-red) !important;
        color: #FFFFFF !important;
        border: none !important;
        font-weight: 700 !important;
        font-size: 0.98rem !important;
        border-radius: 6px !important;
        padding: 8px 18px !important;
        box-shadow: 0 2px 4px rgba(211, 19, 26, 0.25) !important;
        transition: all 0.2s ease !important;
    }
    div.st-key-btn_aprobar_action > button:hover {
        background-color: var(--ld-dark-red) !important;
        box-shadow: 0 4px 8px rgba(165, 14, 20, 0.35) !important;
    }

    /* Botón Eliminar (Acción Crítica / Peligro Distintivo) */
    div.st-key-btn_eliminar_action > button {
        background-color: #FFF5F5 !important;
        color: var(--ld-red) !important;
        border: 1.5px solid var(--ld-red) !important;
        font-weight: 700 !important;
        font-size: 0.98rem !important;
        border-radius: 6px !important;
        padding: 8px 18px !important;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.04) !important;
        transition: all 0.2s ease !important;
    }
    div.st-key-btn_eliminar_action > button:hover {
        background-color: var(--ld-red) !important;
        color: #FFFFFF !important;
        border-color: var(--ld-dark-red) !important;
        box-shadow: 0 4px 8px rgba(211, 19, 26, 0.3) !important;
    }
    
    /* Encabezados institucionales */
    .ld-header {
        border-left: 6px solid var(--ld-red);
        padding-left: 16px;
        margin-bottom: 24px;
    }
    .ld-title {
        color: var(--ld-dark);
        font-size: 1.85rem;
        font-weight: 700;
        margin-bottom: 2px;
    }
    .ld-subtitle {
        color: #6C757D;
        font-size: 0.95rem;
        margin-top: 0;
    }
    
    /* Indicador visual de obligatoriedad */
    .req-mark {
        color: var(--ld-red);
        font-weight: 800;
        font-size: 1.15rem;
        margin-left: 4px;
    }
    .req-box {
        background-color: #FFF5F5;
        border-left: 4px solid var(--ld-red);
        padding: 12px 16px;
        margin-bottom: 20px;
        border-radius: 3px;
        font-size: 0.92rem;
        color: #333333;
    }
</style>
""", unsafe_allow_html=True)

w = WorkspaceClient()

# -----------------------------------------------------------------------------
# Control de Estado de Sesión (Session State)
# -----------------------------------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.user_email = ""
    st.session_state.user_role = ""
    st.session_state.user_name = ""

if "active_view" not in st.session_state:
    st.session_state.active_view = "Alta de Validaciones"

if "editor_version" not in st.session_state:
    st.session_state.editor_version = 0

if "select_mode" not in st.session_state:
    st.session_state.select_mode = "none"

if "feedback_msg" not in st.session_state:
    st.session_state.feedback_msg = None

# -----------------------------------------------------------------------------
# Metadatos maestros de configuración (Tablas y Reglas)
# -----------------------------------------------------------------------------
MAESTRO_TABLAS = {
    "Sociedades": {
        "table_id": "MD01",
        "domain": "Estrategia Empresarial",
        "technical_name": "`dp-common`.public.masters_companies"
    },
    "Cost Center": {
        "table_id": "MD02",
        "domain": "Gestión Económica Financiera",
        "technical_name": "dp_sap_costcenter.`dp_costcenter_srv:v1`.4tl_cp_gen_cost_center_dbx"
    }
}

MAESTRO_REGLAS = {
    "No vacío": {
        "rule_id": "RE007", "technical_rule_name": "is_not_empty", "dimension": "Completitud",
        "show_cols": True, "show_val": False, "show_range": False, "show_expr": False, "show_query": False, "show_merge": False, "val_label": "", "val_placeholder": ""
    },
    "Duplicados": {
        "rule_id": "RE004", "technical_rule_name": "is_unique", "dimension": "Unicidad",
        "show_cols": True, "show_val": False, "show_range": False, "show_expr": False, "show_query": False, "show_merge": False, "val_label": "", "val_placeholder": ""
    },
    "Formato (regex)": {
        "rule_id": "RE001", "technical_rule_name": "regex_match", "dimension": "Exactitud",
        "show_cols": True, "show_val": True, "show_range": False, "show_expr": False, "show_query": False, "show_merge": False,
        "val_label": "Param_valor (Expresión regular RegEx) *", "val_placeholder": "Ej: ^[A-Za-z]{2}[0-9]{2}$ o ^[0-9]+$"
    },
    "Lista de valores": {
        "rule_id": "RE003", "technical_rule_name": "is_in_list", "dimension": "Validez",
        "show_cols": True, "show_val": False, "show_range": True, "show_expr": False, "show_query": False, "show_merge": False, "val_label": "", "val_placeholder": ""
    },
    "Expresión SQL (por fila)": {
        "rule_id": "RE005", "technical_rule_name": "sql_expression", "dimension": "Consistencia",
        "show_cols": False, "show_val": False, "show_range": False, "show_expr": True, "show_query": False, "show_merge": False, "val_label": "", "val_placeholder": ""
    },
    "Query completa": {
        "rule_id": "RE006", "technical_rule_name": "sql_query", "dimension": "Consistencia",
        "show_cols": False, "show_val": False, "show_range": False, "show_expr": False, "show_query": True, "show_merge": True, "val_label": "", "val_placeholder": ""
    },
    "Comparar tablas": {
        "rule_id": "RE002", "technical_rule_name": "compare_datasets", "dimension": "Integridad",
        "show_cols": True, "show_val": True, "show_range": False, "show_expr": False, "show_query": False, "show_merge": True,
        "val_label": "Param_valor (Nombre técnico de la tabla de destino) *", "val_placeholder": "Ej: `dp-common`.public.masters_companies"
    }
}

# -----------------------------------------------------------------------------
# Funciones auxiliares de conexión con Databricks SQL Warehouse
# -----------------------------------------------------------------------------
def get_warehouse_id():
    warehouses = list(w.warehouses.list())
    if not warehouses:
        st.error("No se detectó ningún SQL Warehouse activo en el entorno.")
        st.stop()
    return warehouses[0].id

def execute_query(sql_query):
    wid = get_warehouse_id()
    return w.statement_execution.execute_statement(
        statement=sql_query,
        warehouse_id=wid,
        catalog="workspace",
        schema="default",
        wait_timeout="30s"
    )

def verify_credentials(email, password):
    try:
        clean_email = email.strip().lower().replace("'", "''")
        clean_pass = password.strip().replace("'", "''")
        sql = f"""
            SELECT email, role, full_name 
            FROM workspace.default.dq_users 
            WHERE lower(email) = '{clean_email}' AND password = '{clean_pass}'
            LIMIT 1
        """
        resp = execute_query(sql)
        if resp.result and resp.result.data_array:
            row = resp.result.data_array[0]
            return {
                "email": row[0],
                "role": row[1],
                "full_name": row[2]
            }
        return None
    except Exception as e:
        st.error(f"Error de acceso al servicio de autenticación: {str(e)}")
        return None

def fetch_validations():
    try:
        sql = """
            SELECT validation_id, rule_id, table_id, domain, severity, 
                   is_active, status, owner, Param_columns, Param_value, 
                   Param_range, Param_expression, Param_query_SQL, 
                   Param_merge_columns, validation_definition
            FROM workspace.default.dq_validations_catalog 
            ORDER BY validation_id ASC
        """
        resp = execute_query(sql)
        if resp.result and resp.result.data_array:
            cols = [col.name for col in resp.manifest.schema.columns]
            return pd.DataFrame(resp.result.data_array, columns=cols)
        return pd.DataFrame()
    except Exception as err:
        st.warning(f"Aviso de sincronización con el catálogo: {str(err)}")
        return pd.DataFrame()

def generate_validation_definition(tech_rule, col_str, val_str, range_str, expr_str, query_str, merge_str):
    cols_list = [c.strip() for c in col_str.split(',') if c.strip()]
    if tech_rule in ['is_not_empty', 'is_unique']:
        if len(cols_list) > 1:
            return json.dumps({"columns": cols_list})
        return json.dumps({"column": cols_list[0]} if cols_list else {})
    elif tech_rule == 'regex_match':
        return json.dumps({"column": cols_list[0] if cols_list else "", "regex": val_str})
    elif tech_rule == 'is_in_list':
        allowed = [x.strip() for x in range_str.split(',') if x.strip()]
        return json.dumps({"column": cols_list[0] if cols_list else "", "allowed": allowed})
    elif tech_rule == 'sql_expression':
        return json.dumps({"expression": expr_str})
    elif tech_rule == 'sql_query':
        return json.dumps({"query": query_str})
    elif tech_rule == 'compare_datasets':
        return json.dumps({
            "columns": cols_list, 
            "target_table": val_str, 
            "target_columns": [c.strip() for c in merge_str.split(',') if c.strip()]
        })
    return "{}"

# =============================================================================
# INICIO DE SESIÓN (LOGIN)
# =============================================================================
if not st.session_state.authenticated:
    st.markdown("""
        <div class="ld-header">
            <div class="ld-title">LÍNEA DIRECTA ASEGURADORA</div>
            <div class="ld-subtitle">Portal de Gobierno y Calidad de Datos • Autenticación de Acceso</div>
        </div>
    """, unsafe_allow_html=True)
    
    col_l1, col_l2, col_l3 = st.columns([1, 1.8, 1])
    
    with col_l2:
        with st.container(border=True):
            st.markdown("### Inicio de Sesión")
            st.caption("Introduzca sus credenciales corporativas para acceder a los servicios de calidad.")
            
            with st.form("form_login", clear_on_submit=False):
                txt_email = st.text_input("Correo electrónico", placeholder="ejemplo@lineadirecta.es")
                txt_password = st.text_input("Contraseña", type="password", placeholder="••••••••")
                btn_login = st.form_submit_button("Iniciar Sesión", type="primary", use_container_width=True)
                
            if btn_login:
                if not txt_email.strip() or not txt_password.strip():
                    st.error("Debe ingresar correo y contraseña.")
                else:
                    user_info = verify_credentials(txt_email, txt_password)
                    if user_info:
                        st.session_state.authenticated = True
                        st.session_state.user_email = user_info["email"]
                        st.session_state.user_role = user_info["role"]
                        st.session_state.user_name = user_info["full_name"]
                        st.session_state.active_view = "Alta de Validaciones"
                        st.rerun()
                    else:
                        st.error("Credenciales incorrectas o usuario no registrado.")
            
            st.markdown("---")
            st.markdown("""
                **Cuentas de prueba preconfiguradas:**
                * **User:** `user@lineadirecta.es` / `user123` *(Propuesta de validaciones en Draft)*
                * **Owner:** `owner@lineadirecta.es` / `owner123` *(Aprobación y homologación en catálogo)*
            """)
            
    st.stop()

# =============================================================================
# APLICACIÓN PRINCIPAL (USUARIO AUTENTICADO)
# =============================================================================
rol = st.session_state.user_role

# Barra Lateral
st.sidebar.markdown("### LÍNEA DIRECTA")
st.sidebar.caption("Data Quality Governance Platform")
st.sidebar.markdown("---")

st.sidebar.markdown(f"**Usuario:** {st.session_state.user_name}")
st.sidebar.caption(f"{st.session_state.user_email}")

if rol == "Owner":
    st.sidebar.success("Perfil asignado: **Owner**\n- Gestión interactiva del catálogo\n- Aprobación y eliminación masiva")
else:
    st.sidebar.info("Perfil asignado: **User**\n- Propuesta de validaciones (Draft)")

if st.sidebar.button("Cerrar Sesión", use_container_width=True):
    st.session_state.authenticated = False
    st.session_state.user_email = ""
    st.session_state.user_role = ""
    st.session_state.user_name = ""
    st.session_state.active_view = "Alta de Validaciones"
    st.session_state.select_mode = "none"
    st.session_state.editor_version = 0
    st.session_state.feedback_msg = None
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("#### Navegación")

# Navegación mediante clic en las palabras
btn_type_alta = "primary" if st.session_state.active_view == "Alta de Validaciones" else "secondary"
btn_type_cat = "primary" if st.session_state.active_view == "Catálogo de Reglas" else "secondary"

if st.sidebar.button("Alta de Validaciones", type=btn_type_alta, use_container_width=True):
    st.session_state.active_view = "Alta de Validaciones"
    st.rerun()

if st.sidebar.button("Catálogo de Reglas", type=btn_type_cat, use_container_width=True):
    st.session_state.active_view = "Catálogo de Reglas"
    st.rerun()

vista_actual = st.session_state.active_view
df_validaciones = fetch_validations()

# =============================================================================
# VISTA: ALTA DE VALIDACIONES
# =============================================================================
if vista_actual == "Alta de Validaciones":
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
                Los parámetros técnicos requeridos en la sección 3 se ajustan automáticamente según la regla seleccionada en la sección 2.
            </div>
        """, unsafe_allow_html=True)

        # SECCIÓN 1: Identificación y tabla de destino
        st.markdown("##### 1. Identificación y tabla de destino")
        col_a1, col_a2, col_a3 = st.columns(3)
        with col_a1:
            id_val_input = st.text_input("Id_validación *", placeholder="Ej: val-038", help="Identificador único (PK)", key="input_id_val")
        with col_a2:
            tabla_sel = st.selectbox("Nombre_tabla *", options=list(MAESTRO_TABLAS.keys()), key="input_tabla_sel")
            meta_tabla = MAESTRO_TABLAS[tabla_sel]
        with col_a3:
            st.text_input("Dominio_tabla (Automático)", value=meta_tabla["domain"], disabled=True)

        # SECCIÓN 2: Regla técnica y dimensión (Reactiva)
        st.markdown("##### 2. Regla técnica y dimensión")
        col_b1, col_b2, col_b3 = st.columns(3)
        with col_b1:
            regla_func_sel = st.selectbox("Nombre_regla *", options=list(MAESTRO_REGLAS.keys()), key="input_regla_sel")
            meta_regla = MAESTRO_REGLAS[regla_func_sel]
        with col_b2:
            st.text_input("Nombre_técnico DQX (Automático)", value=meta_regla["technical_rule_name"], disabled=True)
        with col_b3:
            st.text_input("Dimensión_regla (Automático)", value=meta_regla["dimension"], disabled=True)

        # SECCIÓN 3: Parámetros técnicos de la regla
        st.markdown("##### 3. Parámetros técnicos de la regla")
        
        p_cols = ""
        p_val = ""
        p_range = ""
        p_expr = ""
        p_query = ""
        p_merge = ""
        p_perimetro = ""

        if meta_regla["show_cols"] and not meta_regla["show_merge"]:
            if meta_regla["show_val"]:
                col_p1, col_p2 = st.columns(2)
                with col_p1:
                    p_cols = st.text_input("Param_columnas *", placeholder="Ej: vat_id o id_registro", key="p_col_regex")
                with col_p2:
                    p_val = st.text_input(meta_regla["val_label"], placeholder=meta_regla["val_placeholder"], key="p_val_regex")
            elif meta_regla["show_range"]:
                col_p1, col_p2 = st.columns(2)
                with col_p1:
                    p_cols = st.text_input("Param_columnas *", placeholder="Ej: currency o status", key="p_col_list")
                with col_p2:
                    p_range = st.text_input("Param_conjunto (Valores permitidos separados por comas) *", placeholder="Ej: EUR,USD,GBP o ACTIVO,INACTIVO", key="p_range_list")
            else:
                p_cols = st.text_input("Param_columnas *", placeholder="Ej: id_registro o STATUS", key="p_col_basic")

        elif meta_regla["show_expr"]:
            p_expr = st.text_area(
                "Param_expresion (Expresión SQL booleana por fila) *", 
                placeholder="Ej: geography_two = 'ROW' AND (geography <> 'AMERICA' OR geography IS NULL)", 
                key="p_expr_area"
            )

        elif meta_regla["show_query"]:
            p_query = st.text_area(
                "Param_query_SQL (Consulta SQL completa sobre el dataset) *", 
                placeholder="SELECT ... FROM ... WHERE ...", 
                key="p_query_area"
            )
            p_merge = st.text_input("Param_merge_columnas (Columna(s) de cruce/join separadas por comas) *", placeholder="Ej: id_registro o COST_CENTER, START_DATE", key="p_merge_query")

        elif meta_regla["show_merge"] and meta_regla["show_cols"]:
            col_p1, col_p2, col_p3 = st.columns(3)
            with col_p1:
                p_cols = st.text_input("Param_columnas (Columnas de la tabla origen) *", placeholder="Ej: id_registro", key="p_col_comp")
            with col_p2:
                p_val = st.text_input(meta_regla["val_label"], placeholder=meta_regla["val_placeholder"], key="p_val_comp")
            with col_p3:
                p_merge = st.text_input("Param_merge_columnas (Columnas de comparación de la tabla destino) *", placeholder="Ej: id_registro", key="p_merge_comp")

        p_perimetro = st.text_input("Definición_perimetro (Cláusula WHERE opcional para filtrar el dataset)", placeholder="Ej: COUNTRY='ESP' o STATUS='ACTIVO'", key="p_perim_input")

        # SECCIÓN 4: Gobierno y operación
        st.markdown("##### 4. Gobierno y operación")
        col_g1, col_g2, col_g3, col_g4 = st.columns(4)
        with col_g1:
            severidad_sel = st.selectbox("Severidad *", options=["WARNING", "CRITICAL", "INFO"], key="input_sev")
        with col_g2:
            owner_input = st.text_input("Owner / Responsable *", value="Línea Directa DQ", key="input_owner")
        with col_g3:
            tags_input = st.text_input("Etiquetas (tags)", value="Testing", key="input_tags")
        with col_g4:
            estado_asignado = "Draft"
            st.text_input("Estado asignado", value=estado_asignado, disabled=True, help="Toda validación nueva se crea en estado Draft.")

        val_activa = st.checkbox("Validación_activa (is_active)", value=True, key="input_activa")

        btn_guardar_val = st.button("Guardar Validación en Catálogo", type="primary")

        # Control de Obligatoriedad y Ejecución
        if btn_guardar_val:
            errores = []
            if not id_val_input.strip():
                errores.append("El campo 'Id_validación *' es obligatorio.")
            if not owner_input.strip():
                errores.append("El campo 'Owner / Responsable *' es obligatorio.")
                
            if meta_regla["show_cols"] and not p_cols.strip():
                errores.append(f"Para la regla '{regla_func_sel}' es obligatorio indicar 'Param_columnas *'.")
            if meta_regla["show_val"] and not p_val.strip():
                errores.append(f"Para la regla '{regla_func_sel}' es obligatorio indicar '{meta_regla['val_label']}'.")
            if meta_regla["show_range"] and not p_range.strip():
                errores.append(f"Para la regla '{regla_func_sel}' es obligatorio definir 'Param_conjunto *'.")
            if meta_regla["show_expr"] and not p_expr.strip():
                errores.append(f"Para la regla '{regla_func_sel}' es obligatoria la 'Param_expresion *'.")
            if meta_regla["show_query"] and not p_query.strip():
                errores.append(f"Para la regla '{regla_func_sel}' es obligatoria la 'Param_query_SQL *'.")
            if meta_regla["show_merge"] and not p_merge.strip():
                errores.append(f"Para la regla '{regla_func_sel}' es obligatorio indicar 'Param_merge_columnas *'.")

            if errores:
                st.error("No se pudo guardar la validación. Complete los campos obligatorios señalados:")
                for err in errores:
                    st.markdown(f"- ❌ **{err}**")
            else:
                try:
                    c_id_val = id_val_input.strip().replace("'", "''")
                    c_rule_id = meta_regla["rule_id"]
                    c_table_id = meta_tabla["table_id"]
                    c_domain = meta_tabla["domain"]
                    c_perim = p_perimetro.strip().replace("'", "''")
                    
                    cols_list = [c.strip().replace("'", "''") for c in p_cols.split(',') if c.strip()]
                    sql_param_columns = f"array({', '.join([repr(c) for c in cols_list])})" if cols_list else "array('')"

                    tags_list = [t.strip().replace("'", "''") for t in tags_input.split(',') if t.strip()]
                    sql_tags = f"array({', '.join([repr(t) for t in tags_list])})" if tags_list else "array('Testing')"

                    c_val = p_val.strip().replace("'", "''")
                    c_min = ""
                    c_max = ""
                    c_range = p_range.strip().replace("'", "''")
                    c_data_type = ""
                    c_expr = p_expr.strip().replace("'", "''")
                    c_query = p_query.strip().replace("'", "''")
                    c_merge = p_merge.strip().replace("'", "''")
                    c_sev = severidad_sel
                    c_own = owner_input.strip().replace("'", "''")
                    c_usr = st.session_state.user_email
                    c_active = "true" if val_activa else "false"

                    val_def_json = generate_validation_definition(
                        meta_regla["technical_rule_name"],
                        p_cols, p_val, p_range, p_expr, p_query, p_merge
                    ).replace("'", "''")

                    insert_sql = f"""
                        INSERT INTO workspace.default.dq_validations_catalog 
                        (validation_id, rule_id, table_id, perimeter_definition, is_active, 
                         severity, validation_definition, owner, tags, updated_at, 
                         updated_by, domain, Param_columns, Param_value, Param_value_min, 
                         Param_value_max, Param_range, Param_data_type, Param_expression, 
                         Param_query_SQL, Param_merge_columns, status)
                        VALUES 
                        ('{c_id_val}', '{c_rule_id}', '{c_table_id}', '{c_perim}', {c_active},
                         '{c_sev}', '{val_def_json}', '{c_own}', {sql_tags}, current_timestamp(),
                         '{c_usr}', '{c_domain}', {sql_param_columns}, '{c_val}', '{c_min}',
                         '{c_max}', '{c_range}', '{c_data_type}', '{c_expr}',
                         '{c_query}', '{c_merge}', '{estado_asignado}')
                    """
                    
                    resp = execute_query(insert_sql)
                    if "SUCCEEDED" in str(resp.status.state):
                        st.success(f"Propuesta '{c_id_val}' guardada en estado 'Draft'. Queda pendiente de aprobación por un Owner.")
                        st.rerun()
                    else:
                        err_msg = resp.status.error.message if resp.status and resp.status.error else str(resp.status.state)
                        st.error(f"Error al insertar en Unity Catalog: {err_msg}")
                except Exception as ex:
                    st.error(f"Error en la comunicación con Databricks: {str(ex)}")

# =============================================================================
# VISTA: CATÁLOGO DE REGLAS (GESTIÓN INTERACTIVA PARA OWNER Y VISUAL PARA USER)
# =============================================================================
elif vista_actual == "Catálogo de Reglas":
    st.markdown("""
        <div class="ld-header">
            <div class="ld-title">Catálogo de Reglas</div>
            <div class="ld-subtitle">Repositorio consolidado de validaciones en Unity Catalog (workspace.default.dq_validations_catalog)</div>
        </div>
    """, unsafe_allow_html=True)
    
    total = len(df_validaciones)
    aprobadas = len(df_validaciones[df_validaciones["status"] == "Approve"]) if not df_validaciones.empty and "status" in df_validaciones.columns else 0
    drafts = len(df_validaciones[df_validaciones["status"] == "Draft"]) if not df_validaciones.empty and "status" in df_validaciones.columns else 0

    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns([1, 1, 1, 1])
    with col_kpi1:
        st.metric("Total Validaciones", total)
    with col_kpi2:
        st.metric("Aprobadas (Approve)", aprobadas)
    with col_kpi3:
        st.metric("Borradores (Draft)", drafts)
    with col_kpi4:
        st.write("")
        if st.button("Actualizar Catálogo", use_container_width=True):
            st.session_state.select_mode = "none"
            st.session_state.editor_version += 1
            st.session_state.feedback_msg = None
            st.rerun()

    st.markdown("---")

    if not df_validaciones.empty:
        df_show = df_validaciones.copy()
        if "Param_columns" in df_show.columns:
            df_show["Param_columns"] = df_show["Param_columns"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        
        # ---------------------------------------------------------------------
        # ROL OWNER: TABLA CON CASILLAS Y ACCIONES EN BLOQUE (APROBAR / ELIMINAR)
        # ---------------------------------------------------------------------
        if rol == "Owner":
            st.markdown("##### Gestión de validaciones")
            st.caption("Marque las casillas de las reglas para dictaminar en bloque. Las reglas aprobadas forman parte del catálogo oficial y no pueden eliminarse.")

            # Mostrar mensaje informativo o de éxito previo
            if st.session_state.feedback_msg:
                msg_tipo, msg_texto = st.session_state.feedback_msg
                if msg_tipo == "info":
                    st.info(msg_texto)
                elif msg_tipo == "success":
                    st.success(msg_texto)
                elif msg_tipo == "warning":
                    st.warning(msg_texto)
                st.session_state.feedback_msg = None

            # 1. HERRAMIENTAS DE SELECCIÓN RÁPIDA (Formato secundario tipo pill)
            st.markdown("<span style='font-size: 0.88rem; font-weight: 600; color: #495057;'>Selección rápida:</span>", unsafe_allow_html=True)
            col_s1, col_s2, col_s3, col_s_fill = st.columns([1.8, 1.4, 1.4, 3.4])
            with col_s1:
                btn_sel_drafts = st.button("☑️ Marcar no aprobadas", key="btn_sel_drafts")
            with col_s2:
                btn_sel_all = st.button("☑️ Marcar todas", key="btn_sel_all")
            with col_s3:
                btn_deseleccionar = st.button("◻️ Desmarcar todas", key="btn_desel_all")

            # Manejo de la selección rápida
            if btn_sel_drafts:
                draft_count = len(df_show[df_show["status"].fillna("").astype(str).str.strip().str.lower() != "approve"])
                if draft_count == 0:
                    st.session_state.feedback_msg = ("info", "ℹ️ Todas las validaciones del catálogo están aprobadas ('Approve'). No hay ningún borrador pendiente de selección. Para probar la selección, cree una nueva validación en 'Alta de Validaciones' o use 'Marcar todas'.")
                    st.session_state.select_mode = "none"
                else:
                    st.session_state.feedback_msg = ("success", f"✅ Se han marcado {draft_count} validación(es) pendientes de aprobación (Draft).")
                    st.session_state.select_mode = "drafts"
                st.session_state.editor_version += 1
                st.rerun()

            if btn_sel_all:
                st.session_state.select_mode = "all"
                st.session_state.feedback_msg = ("success", f"✅ Se han marcado todas las validaciones de la tabla ({len(df_show)} en total).")
                st.session_state.editor_version += 1
                st.rerun()

            if btn_deseleccionar:
                st.session_state.select_mode = "none"
                st.session_state.feedback_msg = ("info", "Se han desmarcado todas las validaciones.")
                st.session_state.editor_version += 1
                st.rerun()

            # 2. ACCIONES PRINCIPALES (Aprobar y Eliminar con alto impacto visual)
            st.markdown("<div style='margin-top: 14px; margin-bottom: 4px;'><span style='font-size: 0.88rem; font-weight: 600; color: #495057;'>Acciones sobre la selección:</span></div>", unsafe_allow_html=True)
            col_act1, col_act2, col_act_fill = st.columns([2.2, 2.2, 3.6])
            with col_act1:
                btn_aprobar = st.button("✅ Aprobar selección", key="btn_aprobar_action", use_container_width=True)
            with col_act2:
                btn_eliminar = st.button("🗑️ Eliminar selección", key="btn_eliminar_action", use_container_width=True)

            # Inserción de la columna auxiliar 'Seleccionar' según el modo activo
            if st.session_state.select_mode == "drafts":
                is_draft = df_show["status"].fillna("").astype(str).str.strip().str.lower() != "approve"
                df_show.insert(0, "Seleccionar", is_draft)
            elif st.session_state.select_mode == "all":
                df_show.insert(0, "Seleccionar", True)
            else:
                df_show.insert(0, "Seleccionar", False)

            st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)

            # Tabla interactiva con casilla editable
            edited_df = st.data_editor(
                df_show[[
                    "Seleccionar", "validation_id", "rule_id", "table_id", "domain", 
                    "Param_columns", "severity", "status", "is_active", 
                    "owner", "validation_definition"
                ]],
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn("Seleccionar", help="Marque para dictaminar en bloque", default=False),
                    "validation_id": st.column_config.TextColumn("ID Val.", width="small"),
                    "rule_id": st.column_config.TextColumn("Regla", width="small"),
                    "table_id": st.column_config.TextColumn("Tabla", width="small"),
                    "domain": st.column_config.TextColumn("Dominio", width="medium"),
                    "Param_columns": st.column_config.TextColumn("Columna(s)", width="medium"),
                    "severity": st.column_config.TextColumn("Severidad", width="small"),
                    "status": st.column_config.TextColumn("Estado", width="small"),
                    "is_active": st.column_config.CheckboxColumn("Activa", width="small"),
                    "owner": st.column_config.TextColumn("Owner", width="small"),
                    "validation_definition": st.column_config.TextColumn("Definición DQX (JSON)", width="large")
                },
                disabled=[
                    "validation_id", "rule_id", "table_id", "domain", 
                    "Param_columns", "severity", "status", "is_active", 
                    "owner", "validation_definition"
                ],
                hide_index=True,
                use_container_width=True,
                key=f"editor_owner_{st.session_state.editor_version}"
            )

            # Detección de filas seleccionadas
            filas_seleccionadas = edited_df[edited_df["Seleccionar"] == True]
            ids_seleccionados = filas_seleccionadas["validation_id"].tolist()

            # Contador visual dinámico de selección
            if ids_seleccionados:
                st.markdown(f"<span style='font-size: 0.9rem; color: #495057;'>📌 <b>{len(ids_seleccionados)}</b> de {total} validación(es) seleccionada(s).</span>", unsafe_allow_html=True)
            else:
                st.caption("Ninguna validación seleccionada actualmente.")

            # --- EJECUCIÓN: APROBAR SELECCIÓN ---
            if btn_aprobar:
                if not ids_seleccionados:
                    st.warning("Seleccione al menos una validación de la tabla marcando su casilla.")
                else:
                    reglas_a_aprobar = filas_seleccionadas[filas_seleccionadas["status"].str.strip().str.lower() != "approve"]["validation_id"].tolist()
                    if not reglas_a_aprobar:
                        st.info("Todas las reglas seleccionadas ya se encuentran en estado 'Approve'.")
                    else:
                        try:
                            sql_in = ", ".join([f"'{v}'" for v in reglas_a_aprobar])
                            sql_update = f"UPDATE workspace.default.dq_validations_catalog SET status = 'Approve' WHERE validation_id IN ({sql_in})"
                            resp = execute_query(sql_update)
                            if "SUCCEEDED" in str(resp.status.state):
                                st.session_state.feedback_msg = ("success", f"✅ Se han aprobado e incorporado oficialmente al catálogo {len(reglas_a_aprobar)} validación(es).")
                                st.session_state.select_mode = "none"
                                st.session_state.editor_version += 1
                                st.rerun()
                            else:
                                st.error("No se pudo actualizar el estado en Unity Catalog.")
                        except Exception as e:
                            st.error(f"Error de ejecución: {str(e)}")

            # --- EJECUCIÓN: ELIMINAR SELECCIÓN ---
            if btn_eliminar:
                if not ids_seleccionados:
                    st.warning("Seleccione al menos una validación de la tabla marcando su casilla.")
                else:
                    reglas_aprobadas_marcadas = filas_seleccionadas[filas_seleccionadas["status"].str.strip().str.lower() == "approve"]
                    if not reglas_aprobadas_marcadas.empty:
                        st.error("❌ Operación bloqueada: Ha seleccionado reglas oficiales ('Approve'). Las validaciones aprobadas no pueden eliminarse del catálogo. Desmarque las reglas 'Approve' e inténtelo de nuevo.")
                    else:
                        try:
                            sql_in = ", ".join([f"'{v}'" for v in ids_seleccionados])
                            sql_delete = f"DELETE FROM workspace.default.dq_validations_catalog WHERE validation_id IN ({sql_in}) AND status = 'Draft'"
                            resp = execute_query(sql_delete)
                            if "SUCCEEDED" in str(resp.status.state):
                                st.session_state.feedback_msg = ("success", f"✅ Se han eliminado permanentemente {len(ids_seleccionados)} propuesta(s) en borrador.")
                                st.session_state.select_mode = "none"
                                st.session_state.editor_version += 1
                                st.rerun()
                            else:
                                st.error("No se pudieron eliminar los registros seleccionados.")
                        except Exception as e:
                            st.error(f"Error de ejecución: {str(e)}")

        # ---------------------------------------------------------------------
        # ROL USER: CATÁLOGO DE SOLO LECTURA (SIN MODIFICACIONES)
        # ---------------------------------------------------------------------
        else:
            st.dataframe(
                df_show[[
                    "validation_id", "rule_id", "table_id", "domain", 
                    "Param_columns", "severity", "status", "is_active", 
                    "owner", "validation_definition"
                ]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "validation_id": st.column_config.TextColumn("ID Val.", width="small"),
                    "rule_id": st.column_config.TextColumn("Regla", width="small"),
                    "table_id": st.column_config.TextColumn("Tabla", width="small"),
                    "domain": st.column_config.TextColumn("Dominio", width="medium"),
                    "Param_columns": st.column_config.TextColumn("Columna(s)", width="medium"),
                    "severity": st.column_config.TextColumn("Severidad", width="small"),
                    "status": st.column_config.TextColumn("Estado", width="small"),
                    "is_active": st.column_config.CheckboxColumn("Activa", width="small"),
                    "owner": st.column_config.TextColumn("Owner", width="small"),
                    "validation_definition": st.column_config.TextColumn("Definición DQX (JSON)", width="large")
                }
            )
    else:
        st.info("No se registran validaciones en el catálogo central.")