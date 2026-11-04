"""
CAPA DE PERSISTENCIA Y SERVICIOS BACKEND
========================================
Gestiona la comunicación con Databricks SDK, ejecución segura de consultas
y transformaciones de reglas técnicas a formato JSON para DQX.
"""
import json
import pandas as pd
from typing import Optional, List, Dict, Any
from databricks.sdk import WorkspaceClient
from models import ValidationPayload, UserSession

# -----------------------------------------------------------------------------
# Metadatos maestros (Tablas y Reglas soportadas)
# -----------------------------------------------------------------------------
MAESTRO_TABLAS: Dict[str, Dict[str, str]] = {
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

MAESTRO_REGLAS: Dict[str, Dict[str, Any]] = {
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

class DatabricksBackend:
    """Cliente del servicio de Databricks y repositorio de datos."""
    def __init__(self, catalog: str = "workspace", schema: str = "default"):
        self.catalog = catalog
        self.schema = schema
        self.w = WorkspaceClient()

    def get_warehouse_id(self) -> str:
        """Obtiene dinámicamente el primer SQL Warehouse activo."""
        warehouses = list(self.w.warehouses.list())
        if not warehouses:
            raise RuntimeError("No se detectó ningún SQL Warehouse activo en el entorno.")
        return warehouses[0].id

    def execute_query(self, sql_query: str):
        """Ejecuta una consulta SQL en Databricks Statement Execution API."""
        wid = self.get_warehouse_id()
        return self.w.statement_execution.execute_statement(
            statement=sql_query,
            warehouse_id=wid,
            catalog=self.catalog,
            schema=self.schema,
            wait_timeout="30s"
        )

    def verify_credentials(self, email: str, password: str) -> Optional[UserSession]:
        """Valida las credenciales contra la tabla de usuarios."""
        clean_email = email.strip().lower().replace("'", "''")
        clean_pass = password.strip().replace("'", "''")
        sql = f"""
            SELECT email, role, full_name 
            FROM {self.catalog}.{self.schema}.dq_users 
            WHERE lower(email) = '{clean_email}' AND password = '{clean_pass}'
            LIMIT 1
        """
        resp = self.execute_query(sql)
        if resp.result and resp.result.data_array:
            row = resp.result.data_array[0]
            return UserSession(email=row[0], role=row[1], full_name=row[2])
        return None

    def fetch_validations(self) -> pd.DataFrame:
        """Recupera el catálogo completo de reglas de validación."""
        sql = f"""
            SELECT validation_id, rule_id, table_id, domain, severity, 
                   is_active, status, owner, Param_columns, Param_value, 
                   Param_range, Param_expression, Param_query_SQL, 
                   Param_merge_columns, validation_definition
            FROM {self.catalog}.{self.schema}.dq_validations_catalog 
            ORDER BY validation_id ASC
        """
        resp = self.execute_query(sql)
        if resp.result and resp.result.data_array:
            cols = [col.name for col in resp.manifest.schema.columns]
            return pd.DataFrame(resp.result.data_array, columns=cols)
        return pd.DataFrame()

    def insert_validation(self, p: ValidationPayload):
        """Inserta una nueva validación en Unity Catalog a partir de un objeto tipado."""
        cols_list = [c.strip().replace("'", "''") for c in p.columns_str.split(',') if c.strip()]
        sql_param_columns = f"array({', '.join([repr(c) for c in cols_list])})" if cols_list else "array('')"

        tags_list = [t.strip().replace("'", "''") for t in p.tags_str.split(',') if t.strip()]
        sql_tags = f"array({', '.join([repr(t) for t in tags_list])})" if tags_list else "array('Testing')"

        val_def_json = self.generate_validation_definition(
            p.technical_rule, p.columns_str, p.param_val, 
            p.param_range, p.param_expr, p.param_query, p.param_merge
        ).replace("'", "''")

        c_id = p.id_validacion.strip().replace("'", "''")
        c_perim = p.perimeter.strip().replace("'", "''")
        c_own = p.owner.strip().replace("'", "''")
        c_val = p.param_val.strip().replace("'", "''")
        c_range = p.param_range.strip().replace("'", "''")
        c_expr = p.param_expr.strip().replace("'", "''")
        c_query = p.param_query.strip().replace("'", "''")
        c_merge = p.param_merge.strip().replace("'", "''")
        c_active = "true" if p.is_active else "false"

        insert_sql = f"""
            INSERT INTO {self.catalog}.{self.schema}.dq_validations_catalog 
            (validation_id, rule_id, table_id, perimeter_definition, is_active, 
             severity, validation_definition, owner, tags, updated_at, 
             updated_by, domain, Param_columns, Param_value, Param_value_min, 
             Param_value_max, Param_range, Param_data_type, Param_expression, 
             Param_query_SQL, Param_merge_columns, status)
            VALUES 
            ('{c_id}', '{p.rule_id}', '{p.table_id}', '{c_perim}', {c_active},
             '{p.severity}', '{val_def_json}', '{c_own}', {sql_tags}, current_timestamp(),
             '{p.user_email}', '{p.domain}', {sql_param_columns}, '{c_val}', '',
             '', '{c_range}', '', '{c_expr}',
             '{c_query}', '{c_merge}', '{p.status}')
        """
        return self.execute_query(insert_sql)

    def update_status(self, validation_ids: List[str], status: str):
        """Actualiza el estado de homologación (Draft/Approve) para una lista de IDs."""
        sql_in = ", ".join([f"'{v}'" for v in validation_ids])
        sql_update = f"UPDATE {self.catalog}.{self.schema}.dq_validations_catalog SET status = '{status}' WHERE validation_id IN ({sql_in})"
        return self.execute_query(sql_update)

    def delete_drafts(self, validation_ids: List[str]):
        """Elimina de forma segura únicamente registros en estado Draft."""
        sql_in = ", ".join([f"'{v}'" for v in validation_ids])
        sql_delete = f"DELETE FROM {self.catalog}.{self.schema}.dq_validations_catalog WHERE validation_id IN ({sql_in}) AND status = 'Draft'"
        return self.execute_query(sql_delete)

    @staticmethod
    def generate_validation_definition(tech_rule: str, col_str: str, val_str: str, 
                                       range_str: str, expr_str: str, query_str: str, merge_str: str) -> str:
        """Construye el JSON técnico que consume el framework de ejecución DQX."""
        cols_list = [c.strip() for c in col_str.split(',') if c.strip()]
        if tech_rule in ['is_not_empty', 'is_unique']:
            if len(cols_list) > 1: return json.dumps({"columns": cols_list})
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
