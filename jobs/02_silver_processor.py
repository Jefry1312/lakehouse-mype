"""
jobs/02_silver_processor.py
━━━━━━━━━━━━━━━━━━━━━━━━━━
Capa SILVER — Limpieza, normalización y joins.

Lee desde Bronze (Delta), aplica transformaciones de calidad
y escribe en Silver (Delta). Registra linaje y schema.

PERSONALIZA las funciones transform_* con la lógica de tu negocio.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta import DeltaTable

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.spark_session import get_spark_session

load_dotenv()

BASE_PATH   = os.getenv("LAKEHOUSE_BASE_PATH", str(Path(__file__).parent.parent))
BRONZE_PATH = Path(BASE_PATH) / "bronze"
SILVER_PATH = Path(BASE_PATH) / "silver"
LOG_PATH    = Path(BASE_PATH) / "logs"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SILVER] %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH / "silver.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════
# TRANSFORMACIONES POR TABLA
# Agrega aquí una función por cada tabla de tu empresa.
# ══════════════════════════════════════════════════════════

def transform_clientes(df: DataFrame) -> DataFrame:
    """
    Limpieza de la tabla 'clientes'.
    Adapta los nombres de columnas a los de tu base de datos.
    """
    return (
        df
        # Eliminar filas duplicadas
        .dropDuplicates()
        # Normalizar texto
        .withColumn("nombre",    F.trim(F.upper(F.col("nombre"))))
        .withColumn("email",     F.trim(F.lower(F.col("email"))))
        # Validar email básico
        .withColumn("email_valido",
            F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$").cast("boolean"))
        # Estandarizar teléfono (solo dígitos)
        .withColumn("telefono",
            F.regexp_replace(F.col("telefono").cast(StringType()), r"[^\d]", ""))
        # Eliminar filas sin identificador clave
        .filter(F.col("id").isNotNull())
        # Quitar columnas de control Bronze (no las necesitamos en Silver)
        .drop("_bronze_ts", "_tabla_origen", "_row_hash")
    )


def transform_ventas(df: DataFrame) -> DataFrame:
    """Limpieza de la tabla 'ventas'."""
    return (
        df
        .dropDuplicates(["id"])
        # Asegurar tipos correctos
        .withColumn("monto",     F.col("monto").cast("double"))
        .withColumn("fecha",     F.to_date(F.col("fecha")))
        # Agregar año y mes para particionado
        .withColumn("anio",      F.year(F.col("fecha")))
        .withColumn("mes",       F.month(F.col("fecha")))
        # Filtrar ventas con monto negativo (datos sucios)
        .filter(F.col("monto") >= 0)
        .filter(F.col("id").isNotNull())
        .drop("_bronze_ts", "_tabla_origen", "_row_hash")
    )


def transform_productos(df: DataFrame) -> DataFrame:
    """Limpieza de la tabla 'productos'."""
    return (
        df
        .dropDuplicates(["id"])
        .withColumn("precio",    F.col("precio").cast("double"))
        .withColumn("nombre",    F.trim(F.col("nombre")))
        .withColumn("categoria", F.upper(F.trim(F.col("categoria"))))
        # Precio no puede ser nulo ni negativo
        .filter(F.col("precio").isNotNull() & (F.col("precio") > 0))
        .drop("_bronze_ts", "_tabla_origen", "_row_hash")
    )


def transform_empleados(df: DataFrame) -> DataFrame:
    """Limpieza de la tabla 'empleados'."""
    return (
        df
        .dropDuplicates(["id"])
        .withColumn("nombre",    F.trim(F.initcap(F.col("nombre"))))
        .withColumn("apellido",  F.trim(F.initcap(F.col("apellido"))))
        .withColumn("salario",   F.col("salario").cast("double"))
        .filter(F.col("id").isNotNull())
        .drop("_bronze_ts", "_tabla_origen", "_row_hash")
    )


def transform_generic(df: DataFrame) -> DataFrame:
    """
    Transformación genérica para tablas sin función específica.
    Solo elimina duplicados y columnas Bronze.
    """
    return (
        df
        .dropDuplicates()
        .drop("_bronze_ts", "_tabla_origen", "_row_hash")
    )


# ── Registro de transformaciones disponibles ────────────────
TRANSFORMATIONS = {
    "clientes":   transform_clientes,
    "ventas":     transform_ventas,
    "productos":  transform_productos,
    "empleados":  transform_empleados,
    # Agrega más aquí: "mi_tabla": transform_mi_tabla,
}


def get_available_bronze_tables() -> list[str]:
    """Lista las tablas disponibles en Bronze."""
    if BRONZE_PATH.exists():
        return [d.name for d in BRONZE_PATH.iterdir() if d.is_dir()]
    return []


def add_silver_metadata(df: DataFrame, table_name: str) -> DataFrame:
    """Agrega columnas de linaje Silver."""
    return (
        df
        .withColumn("_silver_ts",    F.lit(datetime.utcnow().isoformat()))
        .withColumn("_fuente_bronze", F.lit(str(BRONZE_PATH / table_name)))
    )


def write_silver_delta(df: DataFrame, table_name: str) -> None:
    """Escribe la tabla limpia en Silver como Delta Table."""
    silver_path = str(SILVER_PATH / table_name)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    # Log de versiones
    dt = DeltaTable.forPath(df.sparkSession, silver_path)
    latest = dt.history(1).select("version", "timestamp").collect()[0]
    log.info(f"    Silver v{latest['version']} guardada — {latest['timestamp']}")


def run_silver() -> dict:
    """Punto de entrada del procesador Silver."""
    start = datetime.utcnow()
    log.info("=" * 60)
    log.info(f"INICIO procesamiento Silver — {start.isoformat()}")

    spark = get_spark_session("Silver_Processor")
    summary = {"status": "ok", "tables": [], "errors": []}

    try:
        tables = get_available_bronze_tables()
        log.info(f"Tablas Bronze disponibles: {tables}")

        for table in tables:
            try:
                bronze_path = str(BRONZE_PATH / table)
                log.info(f"  Procesando: {table}")

                df_bronze = spark.read.format("delta").load(bronze_path)
                log.info(f"    Bronze → {df_bronze.count()} filas")

                # Aplicar transformación específica o genérica
                transform_fn = TRANSFORMATIONS.get(table, transform_generic)
                df_clean = transform_fn(df_bronze)
                df_silver = add_silver_metadata(df_clean, table)

                write_silver_delta(df_silver, table)
                summary["tables"].append(table)
                log.info(f"  ✓ {table} → Silver OK ({df_silver.count()} filas)")

            except Exception as e:
                log.error(f"  ✗ Error en tabla {table}: {e}", exc_info=True)
                summary["errors"].append({"table": table, "error": str(e)})

    finally:
        spark.stop()

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info(f"FIN Silver — {elapsed:.1f}s")
    return summary


if __name__ == "__main__":
    run_silver()
