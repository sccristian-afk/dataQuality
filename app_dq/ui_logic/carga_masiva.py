"""
CONTROLADOR DE INTERFAZ: CARGA MASIVA DE VALIDACIONES
=====================================================
Genera la plantilla oficial Excel con sello criptográfico y control de estado inicial,
verifica que el fichero importado sea exclusivamente dicha plantilla modificada,
valida todos los campos obligatorios por regla y prepara el lote SQL único.
"""
import io
import os
import re
import hmac
import uuid
import hashlib
import zipfile
from datetime import datetime, timezone
from typing import Tuple, Optional, List, Dict, Any
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils.exceptions import InvalidFileException
from backend import MAESTRO_TABLAS, MAESTRO_REGLAS, DatabricksBackend
from models import ValidationPayload
from config import cfg
from ui_logic.session import db_backend

TEMPLATE_SIGNATURE = "app_dq_linea_directa_v2"
_SECRET_SALT = os.environ.get(
    "DQ_TEMPLATE_SECRET",
    "LineaDirecta_DQ_Template_2026_SecretKey"
).encode("utf-8")

SHEET_DATA = "Carga_Masiva"
SHEET_GUIDE = "Guia_Reglas"
SHEET_META = "_sys_meta"


def get_template_columns() -> List[str]:
    """Obtiene los nombres comerciales actualizados desde ConfigManager."""
    return [
        cfg.get_col("COL_ID_VALIDACION"),
        cfg.get_col("COL_NOMBRE_TABLA"),
        cfg.get_col("COL_NOMBRE_REGLA"),
        cfg.get_col("COL_PARAM_COLUMNAS"),
        cfg.get_col("COL_PARAM_VALOR"),
        cfg.get_col("COL_PARAM_CONJUNTO"),
        cfg.get_col("COL_PARAM_EXPRESION"),
        cfg.get_col("COL_PARAM_QUERY_SQL"),
        cfg.get_col("COL_PARAM_MERGE_COLUMNAS"),
        cfg.get_col("COL_DEFINICION_PERIMETRO"),
        cfg.get_col("COL_SEVERIDAD"),
        cfg.get_col("COL_OWNER"),
        cfg.get_col("COL_ETIQUETAS"),
        cfg.get_col("COL_VALIDACION_ACTIVA"),
    ]


def _compute_template_hmac(template_id: str, columns: List[str], initial_hash: str) -> str:
    """Genera un sello criptográfico HMAC-SHA256 ligado al ID único, esquema y hash inicial."""
    payload = f"{TEMPLATE_SIGNATURE}::{template_id}::{'|'.join(columns)}::{initial_hash}".encode("utf-8")
    return hmac.new(_SECRET_SALT, payload, hashlib.sha256).hexdigest()


def _hash_sheet_content(ws) -> str:
    """Calcula la huella SHA-256 determinista del contenido no vacío de la hoja."""
    rows_repr = []
    for row in ws.iter_rows(values_only=True):
        cleaned = tuple("" if v is None else str(v).strip() for v in row)
        if any(cleaned):
            rows_repr.append("|".join(cleaned))
    return hashlib.sha256("\n".join(rows_repr).encode("utf-8")).hexdigest()


def generate_template_bytes() -> bytes:
    """Construye la plantilla Excel oficial en memoria con sello de autenticidad y huella virgen."""
    cols = get_template_columns()
    template_id = str(uuid.uuid4())

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = SHEET_DATA
    ws.append(cols)

    # Estilos corporativos de cabecera
    header_fill = PatternFill(start_color="D3131A", end_color="D3131A", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)
    center_align = Alignment(horizontal="center", vertical="center")

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = center_align
        ws.column_dimensions[cell.column_letter].width = 22

    # Hoja de guía de parámetros obligatorios
    ws_guia = wb.create_sheet(title=SHEET_GUIDE)
    ws_guia.append(["Nombre_regla", "Parámetros Obligatorios", "Ejemplo"])
    guia_rows = [
        ("No vacío", "Param_columnas", "vat_id"),
        ("Duplicados", "Param_columnas", "id_registro"),
        ("Formato (regex)", "Param_columnas, Param_valor", "Param_valor: ^[A-Z]{2}$"),
        ("Lista de valores", "Param_columnas, Param_conjunto", "Param_conjunto: EUR, USD"),
        ("Expresión SQL (por fila)", "Param_expresion", "monto > 0 AND status = 'ACTIVO'"),
        ("Query completa", "Param_query_SQL, Param_merge_columnas", "SELECT id FROM tbl..."),
        ("Comparar tablas", "Param_columnas, Param_valor, Param_merge_columnas", "Param_valor: tabla_destino"),
    ]
    for r in guia_rows:
        ws_guia.append(list(r))

    for cell in ws_guia[1]:
        cell.fill = PatternFill(start_color="495057", end_color="495057", fill_type="solid")
        cell.font = header_font
    ws_guia.column_dimensions["A"].width = 26
    ws_guia.column_dimensions["B"].width = 50
    ws_guia.column_dimensions["C"].width = 40

    # Hoja interna veryHidden con catálogos para desplegables y firma criptográfica
    ws_meta = wb.create_sheet(title=SHEET_META)
    tablas = list(MAESTRO_TABLAS.keys())
    reglas = list(MAESTRO_REGLAS.keys())
    severidades = ["WARNING", "CRITICAL", "INFO"]
    activos = ["True", "False"]

    ws_meta["D1"], ws_meta["E1"], ws_meta["F1"], ws_meta["G1"] = "TABLAS", "REGLAS", "SEVERIDADES", "ACTIVOS"
    for idx, val in enumerate(tablas, start=2):
        ws_meta[f"D{idx}"] = val
    for idx, val in enumerate(reglas, start=2):
        ws_meta[f"E{idx}"] = val
    for idx, val in enumerate(severidades, start=2):
        ws_meta[f"F{idx}"] = val
    for idx, val in enumerate(activos, start=2):
        ws_meta[f"G{idx}"] = val

    # Desplegables referenciando rangos ocultos (evita el límite de 255 caracteres de Excel)
    dv_tabla = DataValidation(type="list", formula1=f"={SHEET_META}!$D$2:$D${len(tablas) + 1}", allow_blank=True)
    dv_regla = DataValidation(type="list", formula1=f"={SHEET_META}!$E$2:$E${len(reglas) + 1}", allow_blank=True)
    dv_sev = DataValidation(type="list", formula1=f"={SHEET_META}!$F$2:$F${len(severidades) + 1}", allow_blank=True)
    dv_act = DataValidation(type="list", formula1=f"={SHEET_META}!$G$2:$G${len(activos) + 1}", allow_blank=True)

    for dv in (dv_tabla, dv_regla, dv_sev, dv_act):
        ws.add_data_validation(dv)

    dv_tabla.add("B2:B500")
    dv_regla.add("C2:C500")
    dv_sev.add("K2:K500")
    dv_act.add("N2:N500")

    # Huella del estado virgen para exigir que el fichero subido haya sido modificado
    initial_hash = _hash_sheet_content(ws)
    schema_hmac = _compute_template_hmac(template_id, cols, initial_hash)

    ws_meta["A1"], ws_meta["B1"] = "SIGNATURE", TEMPLATE_SIGNATURE
    ws_meta["A2"], ws_meta["B2"] = "TEMPLATE_ID", template_id
    ws_meta["A3"], ws_meta["B3"] = "INITIAL_HASH", initial_hash
    ws_meta["A4"], ws_meta["B4"] = "SCHEMA_HMAC", schema_hmac
    ws_meta["A5"], ws_meta["B5"] = "GENERATED_AT", datetime.now(timezone.utc).isoformat()
    ws_meta.sheet_state = "veryHidden"

    wb.properties.keywords = TEMPLATE_SIGNATURE
    wb.properties.category = "DQ_OFFICIAL_TEMPLATE"

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out.getvalue()


def validate_template_authenticity(uploaded_file) -> Tuple[bool, str, Optional[pd.DataFrame]]:
    """
    Valida en 6 capas que el fichero subido sea exclusivamente la plantilla oficial
    descargada desde esta aplicación y que haya sido modificada con datos.
    """
    if uploaded_file is None:
        return False, "No se ha seleccionado ningún archivo.", None

    filename = getattr(uploaded_file, "name", "") or ""
    if not filename.lower().endswith(".xlsx"):
        return (
            False,
            "Formato rechazado: solo se aceptan libros Excel modernos con extensión '.xlsx'.",
            None,
        )

    raw_bytes = uploaded_file.getvalue()
    if not raw_bytes:
        return False, "El archivo subido está vacío.", None

    # CAPA 1: Inspección binaria OpenXML y bloqueo de macros / enlaces externos
    if not zipfile.is_zipfile(io.BytesIO(raw_bytes)):
        return (
            False,
            "Archivo rechazado: el fichero no es un libro Excel (.xlsx) válido (posible CSV o texto renombrado).",
            None,
        )

    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes), "r") as zf:
            zip_entries = zf.namelist()
            if "xl/workbook.xml" not in zip_entries:
                return False, "Archivo rechazado: estructura interna OpenXML no válida.", None
            if any("vbaProject" in entry for entry in zip_entries):
                return False, "Archivo rechazado por seguridad: no se permiten libros con macros (.xlsm).", None
            if any("externalLinks" in entry for entry in zip_entries):
                return False, "Archivo rechazado por seguridad: no se permiten enlaces externos en el libro.", None
    except Exception:
        return False, "Archivo rechazado: no se pudo verificar la estructura interna del fichero.", None

    # CAPA 2: Apertura segura con openpyxl
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), data_only=True)
    except (InvalidFileException, Exception):
        return False, "Archivo rechazado: libro Excel incompatible o dañado.", None

    # CAPA 3: Pestañas exactas (sin hojas añadidas ni eliminadas) y metadatos del libro
    if set(wb.sheetnames) != {SHEET_DATA, SHEET_GUIDE, SHEET_META}:
        return (
            False,
            "Plantilla rechazada: solo se acepta la plantilla oficial sin pestañas añadidas ni eliminadas.",
            None,
        )

    if wb.properties.category != "DQ_OFFICIAL_TEMPLATE" or wb.properties.keywords != TEMPLATE_SIGNATURE:
        return (
            False,
            "Plantilla rechazada: este fichero Excel no pertenece a la plataforma oficial.",
            None,
        )

    # CAPA 4: Firma criptográfica HMAC-SHA256 ligada al TEMPLATE_ID e INITIAL_HASH
    ws_meta = wb[SHEET_META]
    file_sig = str(ws_meta["B1"].value or "").strip()
    template_id = str(ws_meta["B2"].value or "").strip()
    initial_hash = str(ws_meta["B3"].value or "").strip()
    file_hmac = str(ws_meta["B4"].value or "").strip()

    try:
        uuid.UUID(template_id)
    except Exception:
        return False, "Plantilla rechazada: identificador de plantilla inválido o manipulado.", None

    expected_cols = get_template_columns()
    expected_hmac = _compute_template_hmac(template_id, expected_cols, initial_hash)

    if file_sig != TEMPLATE_SIGNATURE or not file_hmac or not hmac.compare_digest(file_hmac, expected_hmac):
        return (
            False,
            "Plantilla rechazada: la firma de autenticidad no coincide con la versión oficial vigente.",
            None,
        )

    # CAPA 5: Cabeceras exactas de la hoja de datos
    ws_data = wb[SHEET_DATA]
    actual_headers = [
        str(cell.value).strip() if cell.value is not None else ""
        for cell in ws_data[1]
    ]
    while actual_headers and actual_headers[-1] == "":
        actual_headers.pop()

    if actual_headers != expected_cols:
        return (
            False,
            "Plantilla rechazada: se han modificado, añadido o eliminado columnas en 'Carga_Masiva'.",
            None,
        )

    # CAPA 6: Exigir que la plantilla haya sido modificada respecto al original descargado
    current_hash = _hash_sheet_content(ws_data)
    if hmac.compare_digest(current_hash, initial_hash):
        return (
            False,
            "Plantilla sin modificar: has subido la plantilla vacía original. Rellena al menos una fila antes de importarla.",
            None,
        )

    # Extraer filas no vacías preservando su número exacto de fila en Excel
    records: List[Dict[str, Any]] = []
    for excel_row_idx, row_vals in enumerate(
        ws_data.iter_rows(min_row=2, max_col=len(expected_cols), values_only=True),
        start=2
    ):
        cleaned = [str(v).strip() if v is not None else "" for v in row_vals]
        if any(cleaned):
            rec = dict(zip(expected_cols, cleaned))
            rec["_excel_row"] = excel_row_idx
            records.append(rec)

    if not records:
        return (
            False,
            "Plantilla sin datos: no se detectaron filas cumplimentadas en 'Carga_Masiva'.",
            None,
        )

    return (
        True,
        f"Plantilla oficial modificada verificada correctamente ({len(records)} fila(s) detectadas).",
        pd.DataFrame(records),
    )


def _parse_bool_strict(raw_val: str) -> Optional[bool]:
    """Convierte valores textuales de Excel a booleano estricto o devuelve None si es inválido."""
    norm = (raw_val or "").strip().lower()
    if norm in ("true", "1", "si", "sí", "verdadero"):
        return True
    if norm in ("false", "0", "no", "falso"):
        return False
    return None


def validate_bulk_business_rules(
    df_raw: pd.DataFrame,
    user_email: str
) -> Tuple[bool, pd.DataFrame, List[ValidationPayload]]:
    """
    Fase de comprobación fila por fila:
    - Verifica que cada columna obligatoria esté rellena y tenga un valor permitido.
    - Verifica parámetros obligatorios específicos según el tipo de regla.
    - Verifica ausencia de IDs duplicados tanto en el Excel como en Unity Catalog.

    Returns:
        (todo_ok, df_diagnostico_filas, lista_payloads_validos)
    """
    c_id = cfg.get_col("COL_ID_VALIDACION")
    c_tabla = cfg.get_col("COL_NOMBRE_TABLA")
    c_regla = cfg.get_col("COL_NOMBRE_REGLA")
    c_cols = cfg.get_col("COL_PARAM_COLUMNAS")
    c_val = cfg.get_col("COL_PARAM_VALOR")
    c_conj = cfg.get_col("COL_PARAM_CONJUNTO")
    c_expr = cfg.get_col("COL_PARAM_EXPRESION")
    c_query = cfg.get_col("COL_PARAM_QUERY_SQL")
    c_merge = cfg.get_col("COL_PARAM_MERGE_COLUMNAS")
    c_perim = cfg.get_col("COL_DEFINICION_PERIMETRO")
    c_sev = cfg.get_col("COL_SEVERIDAD")
    c_owner = cfg.get_col("COL_OWNER")
    c_tags = cfg.get_col("COL_ETIQUETAS")
    c_act = cfg.get_col("COL_VALIDACION_ACTIVA")

    # Consultar IDs existentes en Unity Catalog en una única query
    candidate_ids = [str(x).strip() for x in df_raw[c_id].tolist() if str(x).strip()]
    existing_db_ids = set()
    db_check_warning = ""
    try:
        existing_db_ids = db_backend.get_existing_validation_ids(candidate_ids)
    except Exception as ex:
        db_check_warning = f"Aviso: no se pudo verificar duplicados previos en Unity Catalog ({ex})."

    seen_excel_ids: Dict[str, int] = {}
    diagnostic_rows: List[Dict[str, Any]] = []
    valid_payloads: List[ValidationPayload] = []

    valid_severities = {"WARNING", "CRITICAL", "INFO"}

    for _, row in df_raw.iterrows():
        excel_row = int(row.get("_excel_row", 0))
        row_errors: List[str] = []

        id_val = str(row.get(c_id, "")).strip()
        tabla_name = str(row.get(c_tabla, "")).strip()
        regla_name = str(row.get(c_regla, "")).strip()
        p_cols = str(row.get(c_cols, "")).strip()
        p_val = str(row.get(c_val, "")).strip()
        p_conj = str(row.get(c_conj, "")).strip()
        p_expr = str(row.get(c_expr, "")).strip()
        p_query = str(row.get(c_query, "")).strip()
        p_merge = str(row.get(c_merge, "")).strip()
        p_perim = str(row.get(c_perim, "")).strip()
        sev_raw = str(row.get(c_sev, "")).strip().upper()
        owner_val = str(row.get(c_owner, "")).strip()
        tags_val = str(row.get(c_tags, "")).strip() or "Testing"
        act_raw = str(row.get(c_act, "")).strip()

        # 1. Validación de columnas obligatorias generales
        if not id_val:
            row_errors.append(f"'{c_id}' es obligatorio.")
        elif not re.match(r"^[A-Za-z0-9_-]+$", id_val):
            row_errors.append(f"'{c_id}' ('{id_val}') solo admite letras, números, '-' y '_'.")
        else:
            id_lower = id_val.lower()
            if id_lower in seen_excel_ids:
                row_errors.append(f"'{c_id}' duplicado en el Excel (ya aparece en fila {seen_excel_ids[id_lower]}).")
            else:
                seen_excel_ids[id_lower] = excel_row
            if id_val in existing_db_ids:
                row_errors.append(f"'{c_id}' ('{id_val}') ya existe en el catálogo de Unity Catalog.")

        meta_tabla = MAESTRO_TABLAS.get(tabla_name)
        if not tabla_name:
            row_errors.append(f"'{c_tabla}' es obligatorio.")
        elif not meta_tabla:
            row_errors.append(
                f"'{c_tabla}' ('{tabla_name}') no es válido. Opciones: {', '.join(MAESTRO_TABLAS.keys())}."
            )

        meta_regla = MAESTRO_REGLAS.get(regla_name)
        if not regla_name:
            row_errors.append(f"'{c_regla}' es obligatorio.")
        elif not meta_regla:
            row_errors.append(
                f"'{c_regla}' ('{regla_name}') no es válido. Opciones: {', '.join(MAESTRO_REGLAS.keys())}."
            )

        if not sev_raw:
            row_errors.append(f"'{c_sev}' es obligatorio.")
        elif sev_raw not in valid_severities:
            row_errors.append(f"'{c_sev}' ('{sev_raw}') debe ser WARNING, CRITICAL o INFO.")

        if not owner_val:
            row_errors.append(f"'{c_owner}' es obligatorio.")

        parsed_active = _parse_bool_strict(act_raw)
        if not act_raw:
            row_errors.append(f"'{c_act}' es obligatorio (True o False).")
        elif parsed_active is None:
            row_errors.append(f"'{c_act}' ('{act_raw}') debe ser True o False.")

        # 2. Validación de parámetros obligatorios según el tipo de regla
        if meta_regla:
            if meta_regla.get("show_cols") and not p_cols:
                row_errors.append(f"La regla '{regla_name}' exige rellenar '{c_cols}'.")

            if meta_regla.get("show_val"):
                if not p_val:
                    row_errors.append(f"La regla '{regla_name}' exige rellenar '{c_val}'.")
                elif regla_name == "Formato (regex)":
                    try:
                        re.compile(p_val)
                    except re.error as rx_err:
                        row_errors.append(f"Expresión regular inválida en '{c_val}': {rx_err}.")
                elif regla_name == "Comparar tablas":
                    try:
                        DatabricksBackend._validate_table_identifier(p_val)
                    except ValueError as tbl_err:
                        row_errors.append(f"Tabla destino no válida en '{c_val}': {tbl_err}.")

            if meta_regla.get("show_range"):
                allowed_items = [x.strip() for x in p_conj.split(",") if x.strip()]
                if not allowed_items:
                    row_errors.append(f"La regla '{regla_name}' exige rellenar '{c_conj}' con valores separados por coma.")

            if meta_regla.get("show_expr") and not p_expr:
                row_errors.append(f"La regla '{regla_name}' exige rellenar '{c_expr}'.")

            if meta_regla.get("show_query"):
                if not p_query:
                    row_errors.append(f"La regla '{regla_name}' exige rellenar '{c_query}'.")
                elif not re.match(r"^\s*(SELECT|WITH)\b", p_query, flags=re.IGNORECASE) or ";" in p_query:
                    row_errors.append(f"'{c_query}' debe ser una consulta SELECT/WITH válida sin ';'.")

            if meta_regla.get("show_merge") and not p_merge:
                row_errors.append(f"La regla '{regla_name}' exige rellenar '{c_merge}'.")

        is_row_ok = len(row_errors) == 0
        diagnostic_rows.append({
            "Fila Excel": excel_row,
            "Id_validación": id_val or "(vacío)",
            "Tabla": tabla_name or "(vacío)",
            "Regla": regla_name or "(vacío)",
            "Estado": "✅ Válida" if is_row_ok else "❌ Error",
            "Detalle de comprobación": "Listo para inserción SQL" if is_row_ok else " | ".join(row_errors),
        })

        if is_row_ok and meta_tabla and meta_regla and parsed_active is not None:
            valid_payloads.append(
                ValidationPayload(
                    id_validacion=id_val,
                    rule_id=meta_regla["rule_id"],
                    technical_rule=meta_regla["technical_rule_name"],
                    table_id=meta_tabla["table_id"],
                    domain=meta_tabla["domain"],
                    severity=sev_raw,
                    owner=owner_val,
                    user_email=user_email,
                    is_active=parsed_active,
                    perimeter=p_perim,
                    columns_str=p_cols,
                    param_val=p_val,
                    param_range=p_conj,
                    param_expr=p_expr,
                    param_query=p_query,
                    param_merge=p_merge,
                    tags_str=tags_val,
                    status="Draft",
                )
            )

    df_diag = pd.DataFrame(diagnostic_rows)
    all_ok = (len(valid_payloads) == len(df_raw)) and (len(df_raw) > 0)

    if db_check_warning and all_ok:
        df_diag["Detalle de comprobación"] = df_diag["Detalle de comprobación"] + f" ({db_check_warning})"

    return all_ok, df_diag, valid_payloads