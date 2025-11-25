'''
Librería de utilidades del Framework DQ. Contiene funciones auxiliares de logging.

* `log_execution_start`: marca el inicio de la ejecución
* `log_execution_finish`: marca el final de la ejecución
* `log_validations_traceability`: trazabilidada de las validaciones de cada ejecución
* `log_evidences_staging`: registra las evidencias de cada tabla (una tabla staging por tabla ejecutadaa
'''

# 1. Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType, DateType
from delta.tables import DeltaTable
import uuid
from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType, DecimalType, IntegerType, LongType)

spark = SparkSession.builder.getOrCreate()

# 2. Funciones de log de trazabilidad de ejecución
def log_execution_start(execution_id: str, exec_timestamp: datetime , table_id: str, trace_table: str):
    """
    Inserta un registro inicial en la tabla dq_execution_traceability
    
    Args:
        execution_id (str): identificador único de la ejecución
        exec_timestamp (datetime): fecha y hora de inicio de la ejecución
        table_id (str): identificador de la tabla que se valida
        trace_table (DataFrame): dataFrame de trazabilidad donde se guarda el log
    """
    try:
        schema = StructType([
            StructField("execution_id", StringType(), True),
            StructField("table_id", StringType(), True),
            StructField("execution_timestamp", TimestampType(), True),
            StructField("execution_date", DateType(), True),
            StructField("status", StringType(), True),
            StructField("duration_seconds", DecimalType(10,2), True),
            StructField("validations_executed", IntegerType(), True),
            StructField("validations_failed", IntegerType(), True)
        ])

        # Fecha (tipo Date) a partir del timestamp
        exec_date = exec_timestamp.date()

        start_log_df = spark.createDataFrame(
            data=[(
                execution_id, table_id, exec_timestamp, exec_date, 
                "START", None, None, None
            )],
            schema=schema
        )
        
        (start_log_df.write
         .format("delta")
         .mode("append")
         .saveAsTable(trace_table))
        
    except Exception as e:
        print(f"Error fatal al iniciar el log de trazabilidad: {e}")
        raise

def log_execution_finish(execution_id, status, duration, validations_exec, validations_fail, trace_table):
    """
    Actualiza el registro de la ejecución en dq_execution_traceability
    con el estado final (SUCCESS/FAILED) y los KPIs resumen
    """

    try:
        
        schema = StructType([
            StructField("execution_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("duration_seconds", DecimalType(10,2), True),
            StructField("validations_executed", IntegerType(), True),
            StructField("validations_failed", IntegerType(), True)
        ])

        delta_trace_table = DeltaTable.forName(spark, trace_table)
        
        duration_value = Decimal(str(duration))
        # DataFrame con los datos a actualizar
        update_df = spark.createDataFrame(
            data=[(execution_id, status, duration_value, validations_exec, validations_fail)],
            schema=schema
        )

        # Actualizar la fila existente basándonos en el execution_id
        (delta_trace_table.alias("target")
         .merge(update_df.alias("source"), "target.execution_id = source.execution_id")
         .whenMatchedUpdate(set={
             "status": "source.status",
             "duration_seconds": "source.duration_seconds",
             "validations_executed": "source.validations_executed",
             "validations_failed": "source.validations_failed"
         })
         .execute())
        
    except Exception as e:
        print(f"Error al finalizar el log de trazabilidad para {execution_id}: {e}")

# 3. Funciones de log de resultados de validación
def log_validations_traceability(execution_id, exec_date, validation_id, rule_id, status, perimeter, failed_count, validations_table):
    """
    Escribe el resultado (PASSED/FAILED/ERROR) y el conteo 
    de la regla en la tabla dq_validations_traceability
    """
    
    try:
        schema = StructType([
            StructField("validation_trace_id", StringType(), True),
            StructField("execution_id", StringType(), True),
            StructField("validation_id", StringType(), True),
            StructField("rule_id", StringType(), True),
            StructField("execution_date", DateType(), True),
            StructField("status", StringType(), True),
            StructField("perimeter", LongType(), True),
            StructField("failed_records_count", LongType(), True),
            StructField("persistent_failures", LongType(), True),
            StructField("new_failures", LongType(), True),
            StructField("resolved_failures", LongType(), True)
        ])

        trace_df = spark.createDataFrame(
            data=[(
                str(uuid.uuid4()), execution_id, validation_id, rule_id, 
                exec_date, status, perimeter, failed_count, 0, 0, 0
            )],
            schema=schema
        )
        
        (trace_df.write
         .format("delta")
         .mode("append")
         .saveAsTable(validations_table))
        
    except Exception as e:
        print(f"Error al escribir en log de validación para {validation_id}: {e}")
        print(f"  > AVISO: No se pudo loguear el resultado para la validación {validation_id}.")


def log_evidences_staging(df_failed, staging_table_name, execution_id, exec_date, validation_id, primary_key_col, failed_field, catalog, schema):
    """
    Escribe los registros fallidos en una tabla de staging temporal para cada tabla
    """
    
    if df_failed.isEmpty():
        print("  > No hay registros fallidos que escribir en staging")
        return
    
    try:

        # 1. Selecciona las columnas que peuden causar problemas al colisionar el nombre en algunos casos:
        #    ej: primary_key_col == failed_field
        df_transformed = (df_failed.select(
            col(primary_key_col).alias("table_pk"),
            col(failed_field).alias("failed_value")
        ))

        # Transforma el DataFrame al esquema de 'dq_evidences'
        df_to_log = (df_transformed
                     .withColumn("execution_id", lit(execution_id))
                     .withColumn("validation_id", lit(validation_id))
                     .withColumn("execution_date", lit(exec_date)) 
                     .select(
                         lit(str(uuid.uuid4())).alias("evidence_id"),
                         col("execution_id"),
                         col("validation_id"),
                         col("execution_date"),
                         col("failed_value").cast("string"),
                         col("table_pk").cast("string")
                         # La columna 'is_new_failure' se añade en el script post-proceso
                     )
                    )

        full_table_name = f"{catalog}.{schema}.{staging_table_name}"

        # Escribir en la tabla de staging SOBRESCRIBIENDO
        (df_to_log.write
         .format("delta")
         .mode("append") 
         .option("mergeSchema", "true") 
         .saveAsTable(full_table_name))
        
    except Exception as e:
        print(f"Error al escribir en la tabla de STAGING {staging_table_name}: {e}")
        print(f"  > AVISO: No se pudieron guardar las evidencias para la validación {validation_id}.")