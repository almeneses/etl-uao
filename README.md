# Proyecto ETL de Calidad del Aire – Dashboard Analítico

## Descripción General

🌐 [Dashboard](https://etl-uao-am.streamlit.app/)

Este proyecto implementa un **sistema ETL (Extract, Transform, Load)** y un **dashboard interactivo en Streamlit** para el análisis de datos de **calidad del aire** (concentraciones horarias de contaminantes como PM10, PM2.5, O₃, NO₂, CO, etc.) provenientes de estaciones ambientales de Cali (Ermita, Flora, Pance, Univalle).

El sistema está diseñado para automatizar la extracción, transformación y carga de los datos en una base de datos relacional optimizada para análisis, y ofrecer visualizaciones interactivas que permiten explorar la evolución temporal y correlación de los contaminantes.

---

## Estructura del Proyecto

```
.
├── run_etl.py                            # Entrada principal del ETL (API)
├── init_db.py                            # Inicializa/esquema de la base de datos
├── requirements.txt                      # Dependencias del proyecto
├── LICENSE                               # Licencia del proyecto
├── data/
│   └── etl_database.db                   # Base de datos SQLite
|   └── logs/
|       └── etl_<anio>_<mes>_<dia>.log    # Logs de ejecución del ETL
├── diagramas/
│   └── diagrama_db.dbml                  # Modelo entidad‑relación (DBML)
├── etl/
│   ├── __init__.py
│   ├── config.py                         # Paths y parámetros del ETL
│   ├── extract.py                        # Extracción (API CKAN)
│   ├── transform.py                      # Limpieza y normalización
│   ├── load.py                           # Carga a tablas principales
│   ├── load_ica.py                       # Carga del índice ICA
│   ├── ica_calculator.py                 # Cálculo del ICA
│   ├── etl_utils.py                      # Utilidades generales (logs, helpers)
│   ├── etl_logger.py                     # Registro de ejecuciones del ETL
│   └── models.py                         # Modelos/DDL auxiliares
└── dashboard/
    ├── app.py                            # Aplicación Streamlit principal
    ├── config.py                         # Configuración del dashboard
    └── utils/
        ├── db_utils.py
        ├── normativa_utils.py
        ├── plot_utils.py
        └── ica_utils.py
```

---

## Requisitos

- **Python 3.10+**
- **Pipenv** o **venv**
- **SQLite**
- Sistema operativo: Linux / macOS / Windows

Instalar dependencias:
```bash
pip install -r requirements.txt

```

---

## Inicialización de la Base de Datos

```bash
python init_db.py
```

Este proceso realiza:
- **Inicialización:** Inicializa una base de datos local en SQLite.

---

## Ejecución del ETL

1. Configurar las variables de conexión en `etl/config.py` (si se requiere):
   ```python
   DB_PATH = "data/etl_database.db"
   ```

2. Ejecutar el proceso ETL:
   ```bash
   python run_etl.py
   ```

   Este proceso realiza:
   - **Extracción:** Datos desde API CKAN dela alcaldía de Cali (cuando esté disponible)
   - **Transformación:** limpieza, unificación, conversión de fechas, eliminación de duplicados, imputación de datos
   - **Carga:** inserción en tablas relacionales (`medicion`, `estacion`, `contaminante`, `tiempo`, etc.)
   - **Registro de actividad:** inserción en tabla `etl_log` indicando hora, estado y número de registros cargados

---

## Ejecución del Dashboard

1. Asegúrate de que el ETL haya generado o actualizado la base de datos.

2. Desde la carpeta raíz del proyecto, ejecuta:
   ```bash
   streamlit run dashboard/app.py
   ```

3. Se abrirá automáticamente en tu navegador:
   ```
   http://localhost:8501
   ```

4. El dashboard permite:
   - Seleccionar **estación(es)** y **contaminante(s)**  
   - Elegir **rango temporal** y **frecuencia** (hora, día, mes)
   - Ver **gráficas de línea**, **mapas de calor** y **resúmenes estadísticos**
   - Explorar **matrices de correlación** entre contaminantes

---

## Estructura de la Base de Datos

Modelo relacional (ver archivo `diagramas/diagrama_db.dbml`):

**Tablas principales:**
- `estacion`: información de las estaciones de monitoreo  
- `contaminante`: lista de contaminantes con unidades y límites normativos  
- `tiempo`: tabla dimensional (año, mes, día, hora)  
- `medicion`: valores medidos por contaminante, estación y tiempo  
- `indice_ica`: índice ICA calculado por estación, fecha y contaminante  
- `etl_log`: registro de ejecuciones del proceso ETL  

---

## Ejemplo de flujo

```bash
# 1. Construir la base de datos
python init_db.py

# 2. Ejecutar el ETL
python run_etl.py

# 3. Iniciar el dashboard
streamlit run dashboard/app.py
```

Resultado esperado:
- Base de datos `etl_database.db` con datos limpios y estructurados
- Dashboard local interactivo para explorar y analizar los datos

---

## Herramientas y Librerías Clave

- **Pandas** – Manipulación y limpieza de datos  
- **SQLAlchemy** – Conexión y manejo de base de datos (solo en ETL)
- **Streamlit** – Visualización interactiva  
- **Seaborn / Plotly** – Gráficas y correlaciones  
- **DBML / SQLite** – Modelado y almacenamiento de datos  
- **Datetime / Re / Logging** – Manejo de fechas, regex y logs  

---

## Logs del ETL

Cada ejecución genera un registro en la tabla `etl_log`, con los siguientes campos:

| Campo | Descripción |
|--------|-------------|
| id_log | Identificador |
| fecha_ejecucion | Fecha/hora de inicio |
| fuente_datos | Nombre de fuente o archivo |
| registros_insertados | Número de filas cargadas |
| estado | Éxito / Error |
| mensaje | Descripción detallada o error |

---

## Trabajo a futuro

- Conexión directa con la API de **SISAIRE / AQICN**
- Automatización con **cron jobs** o **Airflow**
- Migración a **PostgreSQL** o **Data Warehouse**
- Modelos predictivos de contaminación (RNN, Random Forest)

---

## Autor

**Alejandro Meneses**  
Proyecto ETL – Maestría / UAO - Cali
📧 alejandro.meneses@uao.edu.co  
🌐 [Dashboard](https://etl-uao-am.streamlit.app/)

---

## Licencia

Este proyecto está bajo licencia MIT – uso libre con atribución.
