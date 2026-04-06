# Delta Lakehouse — Arquitectura Medallón local
### MySQL → Bronze → Silver → Gold con PySpark + Delta Lake

---

## Estructura del proyecto

```
delta_lakehouse/
├── config/
│   └── spark_session.py       # SparkSession con Delta Lake
├── jobs/
│   ├── 01_bronze_extractor.py # Extrae raw desde MySQL
│   ├── 02_silver_processor.py # Limpieza y transformaciones
│   ├── 03_gold_processor.py   # KPIs y agregaciones
│   ├── scheduler.py           # Job nocturno (APScheduler)
│   └── delta_utils.py         # Time travel y utilidades
├── bronze/                    # Delta Tables raw (creadas al ejecutar)
├── silver/                    # Delta Tables limpias
├── gold/                      # Delta Tables KPIs
├── logs/                      # Logs por capa
├── requirements.txt
└── .env.example               # → copiar a .env y configurar
```

---

## Instalación

```bash
# 1. Crear entorno virtual (recomendado)
python -m venv venv
source venv/bin/activate          # Linux/Mac
# venv\Scripts\activate           # Windows

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar credenciales
cp .env.example .env
# Editar .env con los datos de tu MySQL
```

---

## Configuración (.env)

```env
DB_HOST=localhost
DB_PORT=3306
DB_NAME=nombre_de_tu_base
DB_USER=usuario_readonly
DB_PASSWORD=tu_password

LAKEHOUSE_BASE_PATH=/ruta/absoluta/a/este/directorio

# Tablas a extraer (vacío = todas)
TABLES_TO_EXTRACT=clientes,ventas,productos,empleados

# Hora del job nocturno
JOB_HOUR=2
JOB_MINUTE=0
```

---

## Personalizar transformaciones Silver

En `jobs/02_silver_processor.py`, agrega una función por cada tabla:

```python
def transform_mi_tabla(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["id"])
        .withColumn("campo", F.trim(F.col("campo")))
        .filter(F.col("id").isNotNull())
        .drop("_bronze_ts", "_tabla_origen", "_row_hash")
    )

# Registrarla en el diccionario:
TRANSFORMATIONS = {
    "mi_tabla": transform_mi_tabla,
    ...
}
```

---

## Personalizar KPIs Gold

En `jobs/03_gold_processor.py`, agrega funciones `build_*` y regístralas:

```python
def build_mi_kpi(spark):
    df = read_silver(spark, "mi_tabla")
    resultado = df.groupBy("campo").agg(F.sum("valor"))
    write_gold(resultado, "mi_kpi")

KPI_BUILDERS = [
    ("mi_kpi", build_mi_kpi),
    ...
]
```

---

## Ejecución

### Probar cada capa por separado
```bash
python jobs/01_bronze_extractor.py   # extrae desde MySQL
python jobs/02_silver_processor.py   # limpia Bronze
python jobs/03_gold_processor.py     # genera KPIs
```

### Probar el pipeline completo ahora
```bash
python jobs/scheduler.py --run-now
```

### Iniciar el job nocturno (daemon)
```bash
python jobs/scheduler.py
# Corre en segundo plano hasta Ctrl+C
# Ejecuta automáticamente a las 02:00 AM (configurable en .env)
```

---

## Utilidades Delta Lake

```bash
# Ver todas las Delta Tables
python jobs/delta_utils.py --listar

# Ver historial de cambios de una tabla
python jobs/delta_utils.py --historial bronze/clientes

# Ver schema
python jobs/delta_utils.py --schema silver/ventas

# Time travel: leer tabla en versión anterior
python jobs/delta_utils.py --leer silver/clientes --version 2

# Restaurar a versión anterior
python jobs/delta_utils.py --restaurar bronze/clientes --version 1

# Limpiar versiones antiguas (conservar 7 días)
python jobs/delta_utils.py --vacuum gold/ventas_mensual --horas 168
```

---

## Leer resultados Gold desde Python

```python
from config.spark_session import get_spark_session

spark = get_spark_session()
df = spark.read.format("delta").load("gold/ventas_mensual")
df.show()
```

---

## ¿Por qué Delta Lake?

- **_delta_log/** registra cada cambio (ACID, como Git para datos)
- **Time travel**: `versionAsOf=N` para ver datos de ayer/semana pasada
- **Schema evolution**: puedes agregar columnas sin reescribir todo
- **Overwrite atómico**: nunca quedan datos a medias si el job falla
