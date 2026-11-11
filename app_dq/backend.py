"""
CAPA DE PERSISTENCIA Y SERVICIOS BACKEND
========================================
Gestiona la comunicación con Databricks SDK, ejecución segura de consultas
y transformaciones de reglas técnicas a formato JSON para DQX.
"""
import re
import json
import pandas as pd
from databricks import sql
from typing import Optional, List, Dict, Any, Tuple
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem
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

NUMERIC_PREFIXES = ("INT", "BIGINT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC", "LONG", "SHORT")


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

    def execute_query(
        self,
        sql_query: str,
        parameters: Optional[List[StatementParameterListItem]] = None,
        wait_timeout: str = "30s"
    ):
        """Ejecuta una consulta SQL en Databricks Statement Execution API con soporte de parámetros."""
        wid = self.get_warehouse_id()
        resp = self.w.statement_execution.execute_statement(
            statement=sql_query,
            warehouse_id=wid,
            catalog=self.catalog,
            schema=self.schema,
            parameters=parameters,
            wait_timeout=wait_timeout
        )
        if resp.status and resp.status.error:
            raise RuntimeError(f"Error SQL en Databricks: {resp.status.error.message}")
        return resp

    @staticmethod
    def _validate_table_identifier(table_name: str) -> str:
        """Valida que el identificador de tabla sea seguro antes de interpolarlo en SQL."""
        clean = table_name.strip()
        if not clean or ";" in clean or "--" in clean or "/*" in clean:
            raise ValueError(f"Identificador de tabla no válido: {table_name}")
        if not re.match(r"^[A-Za-z0-9_.\-`:]+$", clean):
            raise ValueError(f"Caracteres no permitidos en el nombre de la tabla: {table_name}")
        return clean

    @staticmethod
    def _quote_col(col_name: str) -> str:
        """Escapa de forma segura el nombre de una columna con backticks."""
        return f"`{col_name.replace('`', '``')}`"

    def get_table_list(self) -> List[str]:
        """
        Retorna las tablas definidas en MAESTRO_TABLAS combinadas con las tablas
        descubiertas dinámicamente en Unity Catalog (SHOW TABLES).
        """
        tables = [meta["technical_name"] for meta in MAESTRO_TABLAS.values()]
        try:
            sql_show = f"SHOW TABLES IN `{self.catalog}`.`{self.schema}`"
            resp = self.execute_query(sql_show)
            if resp.result and resp.result.data_array:
                for row in resp.result.data_array:
                    # En Databricks SHOW TABLES devuelve: [database/namespace, tableName, isTemporary]
                    if len(row) >= 2 and row[1]:
                        full_name = f"{self.catalog}.{self.schema}.{row[1]}"
                        if full_name not in tables:
                            tables.append(full_name)
        except Exception:
            # Si el usuario no tiene permisos de SHOW TABLES en el esquema por defecto,
            # mantiene las tablas del diccionario maestro sin romper la vista.
            pass
        return tables

    def get_table_schema(self, table_name: str) -> List[Tuple[str, str]]:
        """Obtiene la lista real de (nombre_columna, tipo_dato) desde Unity Catalog."""
        safe_table = self._validate_table_identifier(table_name)
        resp = self.execute_query(f"DESCRIBE TABLE {safe_table}")
        columns: List[Tuple[str, str]] = []
        if resp.result and resp.result.data_array:
            for row in resp.result.data_array:
                col_name = str(row[0]).strip() if row[0] is not None else ""
                data_type = str(row[1]).strip().upper() if len(row) > 1 and row[1] is not None else "STRING"
                # Fin de la lista de columnas cuando empiezan las secciones de metadatos/particiones
                if not col_name or col_name.startswith("#"):
                    break
                columns.append((col_name, data_type))
        return columns

    def compute_table_profiling(
        self,
        table_name: str,
        limit: Optional[int] = None,
        correlations: bool = False
    ) -> Tuple[Dict[str, Any], pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Ejecuta el perfilado real de la tabla en Databricks SQL:
        - Obtiene esquema real con DESCRIBE TABLE.
        - Calcula total de filas, nulos por columna, % de nulos y valores únicos en una sola consulta SQL.
        - Si correlations=True, calcula la matriz de correlación entre columnas numéricas.
        """
        safe_table = self._validate_table_identifier(table_name)
        schema_cols = self.get_table_schema(safe_table)

        if not schema_cols:
            raise RuntimeError(f"No se pudieron obtener columnas para la tabla '{safe_table}'.")

        # Subconsulta base (con o sin LIMIT de muestreo)
        if limit and int(limit) > 0:
            source_sql = f"(SELECT * FROM {safe_table} LIMIT {int(limit)}) AS _prof_src"
        else:
            source_sql = f"{safe_table} AS _prof_src"

        # Construcción de expresiones de agregación en una única pasada
        agg_exprs = ["COUNT(*) AS `total_rows`"]
        for idx, (col_name, _) in enumerate(schema_cols):
            q_col = self._quote_col(col_name)
            agg_exprs.append(f"(COUNT(*) - COUNT({q_col})) AS `nulls_{idx}`")
            agg_exprs.append(f"APPROX_COUNT_DISTINCT({q_col}) AS `uniq_{idx}`")

        profile_sql = f"SELECT {', '.join(agg_exprs)} FROM {source_sql}"
        resp = self.execute_query(profile_sql, wait_timeout="50s")

        if not resp.result or not resp.result.data_array:
            raise RuntimeError(f"La consulta de perfilado sobre '{safe_table}' no devolvió resultados.")

        row_data = resp.result.data_array[0]
        total_rows = int(row_data[0]) if row_data[0] is not None else 0
        total_columns = len(schema_cols)

        stats_rows = []
        total_null_cells = 0
        cols_with_nulls = 0

        for idx, (col_name, data_type) in enumerate(schema_cols):
            nulos = int(row_data[1 + (idx * 2)] or 0)
            unicos = int(row_data[2 + (idx * 2)] or 0)
            null_pct = round((nulos / total_rows) * 100.0, 2) if total_rows > 0 else 0.0

            total_null_cells += nulos
            if nulos > 0:
                cols_with_nulls += 1

            stats_rows.append({
                "columna": col_name,
                "tipo": data_type,
                "nulos": nulos,
                "null_pct": null_pct,
                "unicos": unicos
            })

        total_cells = total_rows * total_columns
        global_null_pct = round((total_null_cells / total_cells) * 100.0, 2) if total_cells > 0 else 0.0

        summary_metrics = {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "null_percentage": global_null_pct,
            "columns_with_nulls": cols_with_nulls
        }

        col_stats_df = pd.DataFrame(stats_rows)

        # Cálculo opcional de correlaciones reales en SQL para columnas numéricas
        corr_df: Optional[pd.DataFrame] = None
        if correlations and total_rows > 1:
            num_cols = [
                col_name for col_name, dtype in schema_cols
                if any(dtype.startswith(prefix) for prefix in NUMERIC_PREFIXES)
            ][:10]  # Límite de seguridad de hasta 10 columnas numéricas para evitar explosión combinatoria

            if len(num_cols) >= 2:
                corr_exprs = []
                pairs: List[Tuple[str, str]] = []
                for i in range(len(num_cols)):
                    for j in range(i + 1, len(num_cols)):
                        c1, c2 = num_cols[i], num_cols[j]
                        pairs.append((c1, c2))
                        corr_exprs.append(
                            f"CORR(CAST({self._quote_col(c1)} AS DOUBLE), CAST({self._quote_col(c2)} AS DOUBLE)) AS `corr_{i}_{j}`"
                        )

                corr_sql = f"SELECT {', '.join(corr_exprs)} FROM {source_sql}"
                corr_resp = self.execute_query(corr_sql, wait_timeout="50s")
                if corr_resp.result and corr_resp.result.data_array:
                    corr_row = corr_resp.result.data_array[0]
                    corr_records = []
                    for idx, (c1, c2) in enumerate(pairs):
                        val = corr_row[idx]
                        corr_records.append({
                            "columna_1": c1,
                            "columna_2": c2,
                            "correlacion_pearson": round(float(val), 4) if val is not None else None
                        })
                    corr_df = pd.DataFrame(corr_records)

        return summary_metrics, col_stats_df, corr_df

    def save_profiling_history(
        self,
        table_name: str,
        summary_metrics: Dict[str, Any],
        col_stats: pd.DataFrame,
        executed_by: str
    ):
        """
        Persiste el resultado del profiling en la tabla histórica de Unity Catalog
        ({catalog}.{schema}.dq_profiling_history). Crea la tabla si aún no existe.
        """
        history_table = f"`{self.catalog}`.`{self.schema}`.`dq_profiling_history`"
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {history_table} (
                executed_at TIMESTAMP,
                executed_by STRING,
                table_name STRING,
                total_rows BIGINT,
                total_columns INT,
                global_null_pct DOUBLE,
                columna STRING,
                tipo STRING,
                nulos BIGINT,
                null_pct DOUBLE,
                unicos BIGINT
            )
        """
        self.execute_query(create_sql)

        c_table = table_name.strip().replace("'", "''")
        c_user = (executed_by or "unknown").strip().replace("'", "''")
        t_rows = int(summary_metrics.get("total_rows", 0))
        t_cols = int(summary_metrics.get("total_columns", 0))
        g_null = float(summary_metrics.get("null_percentage", 0.0))

        values_sql = []
        for _, row in col_stats.iterrows():
            col_n = str(row["columna"]).replace("'", "''")
            col_t = str(row["tipo"]).replace("'", "''")
            nulos = int(row["nulos"])
            null_pct = float(row["null_pct"])
            unicos = int(row["unicos"])
            values_sql.append(
                f"(current_timestamp(), '{c_user}', '{c_table}', {t_rows}, {t_cols}, {g_null}, "
                f"'{col_n}', '{col_t}', {nulos}, {null_pct}, {unicos})"
            )

        if values_sql:
            insert_sql = f"INSERT INTO {history_table} VALUES {', '.join(values_sql)}"
            return self.execute_query(insert_sql)
        return None

    def verify_credentials(self, email: str, password: str) -> Optional[UserSession]:
        """Valida las credenciales contra la tabla de usuarios."""
        clean_email = email.strip().lower()
        clean_pass = password.strip()

        sql_stmt = "SELECT email, role, full_name FROM workspace.default.dq_users WHERE lower(email) = :email AND password = :password LIMIT 1"

        params = [
            StatementParameterListItem(name="email", value=clean_email, type="STRING"),
            StatementParameterListItem(name="password", value=clean_pass, type="STRING")
        ]
        resp = self.execute_query(sql_stmt, parameters=params)
        if resp.result and resp.result.data_array:
            row = resp.result.data_array[0]
            return UserSession(email=row[0], role=row[1], full_name=row[2])
        return None

    def fetch_validations(self) -> pd.DataFrame:
        """Recupera el catálogo completo de reglas de validación."""
        sql_stmt = """
            SELECT validation_id, rule_id, table_id, domain, severity, 
                   is_active, status, owner, Param_columns, Param_value, 
                   Param_range, Param_expression, Param_query_SQL, 
                   Param_merge_columns, validation_definition
            FROM workspace.default.dq_validations_catalog 
            ORDER BY validation_id ASC
        """
        resp = self.execute_query(sql_stmt)
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
        """Actualiza el estado de homologación (Draft/Approve) usando parámetros nativos."""
        if not validation_ids:
            return None

        for vid in validation_ids:
            if not vid.replace("-", "").replace("_", "").isalnum():
                raise ValueError(f"Identificador de regla no válido: {vid}")

        params = [StatementParameterListItem(name="new_status", value=status, type="STRING")]
        placeholders = []
        for idx, vid in enumerate(validation_ids):
            p_name = f"id_{idx}"
            placeholders.append(f":{p_name}")
            params.append(StatementParameterListItem(name=p_name, value=vid, type="STRING"))

        in_clause = ", ".join(placeholders)
        sql_update = (
            f"UPDATE {self.catalog}.{self.schema}.dq_validations_catalog SET status = :new_status "
            + "WHERE validation_id IN (" + in_clause + ")"
        )
        return self.execute_query(sql_update, parameters=params)

    def delete_drafts(self, validation_ids: List[str]):
        """Elimina de forma segura únicamente registros en estado Draft usando parámetros nativos."""
        if not validation_ids:
            return None

        for vid in validation_ids:
            if not vid.replace("-", "").replace("_", "").isalnum():
                raise ValueError(f"Identificador de regla no válido: {vid}")

        params = []
        placeholders = []
        for idx, vid in enumerate(validation_ids):
            p_name = f"del_id_{idx}"
            placeholders.append(f":{p_name}")
            params.append(StatementParameterListItem(name=p_name, value=vid, type="STRING"))

        in_clause = ", ".join(placeholders)
        sql_delete = (
            f"DELETE FROM {self.catalog}.{self.schema}.dq_validations_catalog "
            + "WHERE validation_id IN (" + in_clause + ") AND status = 'Draft'"
        )
        return self.execute_query(sql_delete, parameters=params)

    @staticmethod
    def generate_validation_definition(tech_rule: str, col_str: str, val_str: str, 
                                       range_str: str, expr_str: str, query_str: str, merge_str: str) -> str:
        """Construye el JSON técnico que consume el framework de ejecución DQX."""
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


# Helpers de nivel de módulo para compatibilidad directa con las vistas
_default_backend = DatabricksBackend()

def get_table_list() -> List[str]:
    return _default_backend.get_table_list()

def compute_table_profiling(
    table_name: str,
    limit: Optional[int] = None,
    correlations: bool = False
) -> Tuple[Dict[str, Any], pd.DataFrame, Optional[pd.DataFrame]]:
    return _default_backend.compute_table_profiling(
        table_name=table_name,
        limit=limit,
        correlations=correlations
    )