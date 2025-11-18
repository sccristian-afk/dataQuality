# 🛡️ Data Quality Framework (Metadata-Driven)

Este repositorio contiene el código del **Framework de Calidad de Datos** modular para Databricks. Permite definir reglas de calidad de negocio y técnicas mediante configuración (Excel) sin necesidad de modificar código para cada nueva validación.

El motor utiliza **Unity Catalog** para el almacenamiento de resultados, trazas de auditoría y gestión de tablas Delta.

---
##  Organización del Proyecto

A continuación se muestra la estructura lógica de los scripts y su función dentro del framework de calidad:

```text
📦 Data Quality Project
 ┃
 ┣ 📂 config
 ┃ ┣  00_setup_framework.ipynb         # DDLs iniciales (Creación de tablas y esquemas)
 ┃ ┗  sync_catalog_from_excel.ipynb    # Sincronizador (Excel -> Tablas Delta)
 ┃
 ┣ 📂 engine
 ┃ ┣  dq_framework_runner.ipynb        # Orquestador principal (Ejecuta las validaciones)
 ┃ ┗  dq_utils.ipynb                   # Librería de funciones auxiliares y logging
 ┃
 ┣ 📂 job_config
 ┃ ┣  job1_setup.yaml                  # Job de inicialización (DDLs y creación de tablas)
 ┃ ┣  job2_sync.yaml                   # Job de sincronización (Excel -> Metadatos Delta)
 ┃ ┣  job3_SQL.yaml                    # Job de despliegue de funciones SQL (UDFs persistentes)
 ┃ ┗  job4_engine.yaml                 # Job del motor de ejecución (Runner principal)
 ┃
 ┣ 📂 utils
   ┣  custom_rules_library_py.ipynb    # Reglas de calidad Custom en Python
   ┣  custom_rules_library_sql.ipynb   # Reglas de Calidad Custom en SQL
   ┣  dq_utils.ipynb                   # Librería de Utilidades del Framework DQ
   ┣  04_calculate_persistence.ipynb   # Cálculo de persistencia (Nuevo vs Persistente)
   ┗  05_unify_evidences.ipynb         # Consolidador de evidencias (Staging -> Final)
```

## Guía de Ejecución Secuencial (Workflow)

Este apartado describe el ciclo de vida completo para desplegar y ejecutar el framework, desde la configuración en Excel hasta la obtención de resultados.

### Paso 0: Carga de Configuración (Manual)
Antes de ejecutar cualquier automatismo, se debe definir la estrategia de calidad.

1.  Edita el archivo Excel de plantilla (ej. `configValidaciones.xlsx`) definiendo las reglas en las pestañas **Tablas**, **Reglas** y **Validaciones**.
2.  Sube este archivo al **Volumen de Unity Catalog** designado (ej. `/Volumes/workspace/framework_dq/configexcel/`).


### Paso 1: Inicialización del Entorno (`job1_setup`)
> **Frecuencia:** Ejecución única (One-off). Solo al desplegar en un entorno nuevo.

* **Script:** `config/00_setup_framework.ipynb`
* **Función:** Prepara el entorno. Ejecuta los DDLs (Data Definition Language) para crear el esquema `framework_dq` y las tablas Delta vacías necesarias para almacenar la configuración y los resultados (`dq_tables_config`, `dq_evidences`, etc.).


### Paso 2: Sincronización de Metadatos (`job2_sync`)
> **Frecuencia:** Cada vez que se modifique o suba un nuevo Excel.

* **Script:** `config/sync_catalog_from_excel.ipynb`
* **Función:** Lee el Excel del Paso 0 y vuelca su contenido en las tablas Delta de configuración.
* **Mecanismo:** Utiliza operaciones `MERGE` para insertar nuevas reglas, actualizar las existentes o desactivar las que se hayan borrado, asegurando que la base de datos de configuración sea un reflejo exacto del Excel.


### Paso 3: Despliegue de Librerías SQL (`job3_SQL`)
> **Frecuencia:** Bajo demanda. Solo se ejecuta cuando el equipo de desarrollo crea una nueva regla de negocio SQL (ej. validar un nuevo formato de ID)

* **Script:** `utils/custom_rules_library_sql.ipynb`
* **Función:** Compila y registra funciones persistentes en **Unity Catalog**.
* **Detalle Técnico:** Ejecuta sentencias `CREATE OR REPLACE FUNCTION`. Esto permite que, posteriormente, el motor de calidad pueda invocar funciones complejas (como `is_valid_nif_es()`) directamente dentro de las sentencias SQL generadas dinámicamente, sin necesidad de redefinir la lógica en cada ejecución.


###  Paso 4: Ejecución del Motor (`job4_engine`)
> **Frecuencia:** Recurrente (Diaria/Horaria). Es el job de producción.

* **Script:** `engine/dq_framework_runner.ipynb`
* **Parámetros:** Recibe `table_name` (ej. `clientes_run1`) para saber qué tabla validar.
* **Flujo Interno:**
    1.  **Lectura:** Consulta `dq_validations_catalog` para obtener las reglas activas para esa tabla
    2.  **Validación:** Genera y ejecuta las consultas dinámicas contra los datos
    3.  **Staging:** Guarda los registros fallidos en tablas temporales
    4.  **Unificación:** Invoca a `utils/05_unify_evidences` para consolidar los fallos
    5.  **Persistencia:** Invoca a `utils/04_calculate_persistence` para comparar con la ejecución anterior y etiquetar los fallos como *Nuevos* o *Persistentes*
---
