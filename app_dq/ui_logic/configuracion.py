"""
CONTROLADOR DE INTERFAZ: CONFIGURACIÓN
======================================
Gestiona la lectura del archivo Excel de parametrización y la inserción
de nuevas filas sin permitir modificar ni eliminar registros existentes.
"""
import os
import pandas as pd
import streamlit as st
from config import cfg

FILE_PATH = "parametrizar_tabla.xlsx"
SHEET_NAME = "Mapeo_Validaciones"
EXPECTED_COLUMNS = ["Variable_Interna", "Nombre_Columna_Excel", "Descripcion"]


def load_config_data() -> pd.DataFrame:
    """Carga los registros actuales del Excel o genera el respaldo por defecto."""
    if os.path.exists(FILE_PATH):
        try:
            df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME)
            for col in EXPECTED_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            return df[EXPECTED_COLUMNS]
        except Exception as e:
            st.error(f"Error al leer el archivo Excel: {e}")
            return pd.DataFrame(columns=EXPECTED_COLUMNS)

    defaults = cfg._default_columns()
    return pd.DataFrame(
        [(k, v, "") for k, v in defaults.items()],
        columns=EXPECTED_COLUMNS,
    )


def append_config_row(variable_interna: str, nombre_columna_excel: str, descripcion: str) -> bool:
    """Añade una nueva fila al final del Excel sin alterar las filas anteriores."""
    clean_var = variable_interna.strip()
    clean_nombre = nombre_columna_excel.strip()
    clean_desc = descripcion.strip()

    if not clean_var or not clean_nombre:
        st.error("❌ Los campos 'Variable_Interna *' y 'Nombre_Columna_Excel *' son obligatorios.")
        return False

    df_current = load_config_data()

    # Evitar duplicados para garantizar que no se altere ni duplique ningún registro previo
    existing_vars = df_current["Variable_Interna"].fillna("").astype(str).str.strip().str.upper().tolist()
    if clean_var.upper() in existing_vars:
        st.error(f"❌ La variable '{clean_var}' ya existe en el Excel. No se permite modificar ni duplicar filas existentes.")
        return False

    new_row = pd.DataFrame(
        [{
            "Variable_Interna": clean_var,
            "Nombre_Columna_Excel": clean_nombre,
            "Descripcion": clean_desc,
        }]
    )
    df_updated = pd.concat([df_current, new_row], ignore_index=True)

    try:
        df_updated.to_excel(FILE_PATH, sheet_name=SHEET_NAME, index=False)
        # Recargar el diccionario en caliente para toda la aplicación
        cfg.column_map = cfg._load_columns()
        st.session_state["config_feedback"] = (
            "success",
            f"✅ Nueva fila '{clean_var}' añadida correctamente a `{FILE_PATH}`."
        )
        st.session_state["config_form_version"] = st.session_state.get("config_form_version", 0) + 1
        st.rerun()
        return True
    except Exception as e:
        st.error(f"Error al guardar la nueva fila en el Excel: {e}")
        return False