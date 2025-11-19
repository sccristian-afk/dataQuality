
# MAGIC %md
# MAGIC #  Libreria de Reglas de Calidad Custom (Python) (v6)
# MAGIC 
# MAGIC Este notebook contiene las definiciones de todas las funciones de reglas de negocio personalizadas (tipo `CUSTOM`).
# MAGIC 
# MAGIC **IMPORTANTE:** El nombre de cada funci贸n aqu铆 definida debe coincidir *exactamente* con el `technical_rule_name` registrado en la tabla `dq_rule_library`.
# MAGIC 
# MAGIC Este notebook no se ejecuta solo, es invocado v铆a `%run` desde el notebook orquestador principal.

# COMMAND ----------

# DBTITLE 1, 1. Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

# Sesion para usar librería
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
def test_print();
    return "f"
# DBTITLE 2, 2. Definici贸n de Reglas Custom (Python)

# --- RUL_CUS_001: custom_validate_dni ---
def custom_validate_dni(df, columns):
    """
    Valida un DNI/NIF espa帽ol.
    - df: El DataFrame de entrada.
    - columns: Una lista de columnas a validar (esta regla solo usa la primera).
    
    Devuelve un DataFrame S脫LO con las filas que NO cumplen la validaci贸n.
    """
    
    # Extraemos el nombre de la columna a validar.
    # El orquestador siempre pasa una lista, incluso si es de un solo elemento.
    if not columns or len(columns) == 0:
        raise ValueError("custom_validate_dni requiere al menos una columna en el par谩metro 'columns'")
    
    col_to_validate = columns[0]

    # --- L贸gica de la UDF ---
    def is_valid_dni_logic(dni):
        if dni is None:
            return False
        
        dni_upper = str(dni).upper().strip()
        
        if len(dni_upper) != 9:
            return False
            
        tabla = "TRWAGMYFPDXBNJZSQVHLCKE"
        digitos = dni_upper[:-1]
        letra = dni_upper[-1]
        
        # Manejar NIE (X, Y, Z)
        if digitos[0] == 'X':
            digitos = '0' + digitos[1:]
        elif digitos[0] == 'Y':
            digitos = '1' + digitos[1:]
        elif digitos[0] == 'Z':
            digitos = '2' + digitos[1:]

        if not digitos.isdigit() or not letra.isalpha():
            return False
            
        return letra == tabla[int(digitos) % 23]
    # -------------------------

    # Registrar la UDF
    is_valid_dni_udf = udf(is_valid_dni_logic, BooleanType())
    
    # Filtramos para quedarnos S脫LO con los registros que fallan la regla
    # (donde la UDF devuelve False)
    return df.filter(~is_valid_dni_udf(col(col_to_validate)))
