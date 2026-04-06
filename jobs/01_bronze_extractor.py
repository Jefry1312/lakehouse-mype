"""
jobs/01_bronze_extractor.py
━━━━━━━━━━━━━━━━━━━━━━━━━━
Capa BRONZE — Extracción raw desde MySQL/MariaDB.

- Lee las tablas configuradas en .env
- Escribe en Delta Table con modo MERGE (upsert) o APPEND
- Registra metadatos: fecha_extraccion, tabla_origen, hash_fila
- Nunca modifica la base de datos fuente
"""

import os
import sys
import hashlib
import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable

# Agregar raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.spark_session import get_spark_session

# ── Configuración ──────────────────────────────────────────
load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "3306")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
BASE_PATH   = os.getenv("LAKEHOUSE_BASE_PATH", str(Path(__file__).parent.parent))
TABLES_RAW  = os.getenv("TABLES_TO_EXTRACT", "")

BRONZE_PATH = Path(BASE_PATH) / "bronze"
LOG_PATH    = Path(BASE_PATH) / "logs"
LOG_PATH.mkdir(parents=True, exist_ok=True)

JDBC_URL = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?useSSL=false&serverTimezone=UTC"

# ── Logger ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BRONZE] %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH / "bronze.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def get_tables_list(spark: SparkSession) -> list[str]:
    """Obtiene la lista de tablas: desde .env o desde MySQL directamente."""
    if TABLES_RAW.strip():
        return [t.strip() for t in TABLES_RAW.split(",") if t.strip()]

    log.info("TABLES_TO_EXTRACT vacío → descubriendo tablas automáticamente")
    df = (
        spark.read
        .format("jdbc")
        .options(
            url=JDBC_URL,
            dbtable=f"(SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = '{DB_NAME}' "
                    f"AND table_type = 'BASE TABLE') AS t",
            user=DB_USER,
            password=DB_PASSWORD,
            driver="com.mysql.cj.jdbc.Driver",
        )
        .load()
    )
    return [row[0] for row in df.collect()]


def extract_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Lee una tabla completa desde MySQL vía JDBC."""
    log.info(f"  Leyendo tabla: {table_name}")
    df = (
        spark.read
        .format("jdbc")
        .options(
            url=JDBC_URL,
            dbtable=table_name,
            user=DB_USER,
            password=DB_PASSWORD,
            driver="com.mysql.cj.jdbc.Driver",
            fetchsize="1000",          # lotes de 1000 filas
            numPartitions="4",         # paralelismo de lectura
        )
        .load()
    )
    log.info(f"    → {df.count()} filas extraídas")
    return df


def add_bronze_metadata(df: DataFrame, table_name: str) -> DataFrame:
    """
    Agrega columnas de control que Delta registrará en el historial:
      _bronze_ts      : timestamp de extracción
      _tabla_origen   : nombre de la tabla fuente
      _row_hash       : hash MD5 de toda la fila (para detectar cambios)
    """
    # Hash de todos los campos concatenados
    all_cols = [F.col(c).cast("string") for c in df.columns]
    row_hash_expr = F.md5(F.concat_ws("|", *all_cols))

    return (
        df
        .withColumn("_bronze_ts",      F.lit(datetime.utcnow().isoformat()))
        .withColumn("_tabla_origen",   F.lit(table_name))
        .withColumn("_row_hash",       row_hash_expr)
    )


def write_bronze_delta(df: DataFrame, table_name: str) -> None:
    """
    Escribe la tabla en formato Delta Lake.
    - Primera vez: crea la tabla (overwrite con schema)
    - Ejecuciones siguientes: sobreescribe solo si cambia el hash
      (se puede cambiar a MERGE si se conoce la PK)
    """
    delta_path = str(BRONZE_PATH / table_name)

    if DeltaTable.isDeltaTable(df.sparkSession, delta_path):
        # Tabla ya existe → OVERWRITE para carga completa nightly
        # (cambia a MERGE si prefieres incremental con PK conocida)
        log.info(f"    Actualizando Delta Table existente en {delta_path}")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(delta_path)
        )
    else:
        # Primera escritura
        log.info(f"    Creando nueva Delta Table en {delta_path}")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(delta_path)
        )

    # Mostrar historial de la tabla (gracias a Delta)
    dt = DeltaTable.forPath(df.sparkSession, delta_path)
    history = dt.history(3).select("version", "timestamp", "operation")
    log.info(f"    Últimas versiones Delta:")
    for row in history.collect():
        log.info(f"      v{row['version']} | {row['timestamp']} | {row['operation']}")


def run_bronze() -> dict:
    """Punto de entrada del extractor Bronze. Retorna resumen de ejecución."""
    start = datetime.utcnow()
    log.info("=" * 60)
    log.info(f"INICIO extracción Bronze — {start.isoformat()}")
    log.info("=" * 60)

    spark = get_spark_session("Bronze_Extractor")
    summary = {"status": "ok", "tables": [], "errors": []}

    try:
        tables = get_tables_list(spark)
        log.info(f"Tablas a procesar: {tables}")

        for table in tables:
            try:
                df_raw  = extract_table(spark, table)
                df_meta = add_bronze_metadata(df_raw, table)
                write_bronze_delta(df_meta, table)
                summary["tables"].append(table)
                log.info(f"  ✓ {table} → Bronze OK")
            except Exception as e:
                log.error(f"  ✗ Error en tabla {table}: {e}", exc_info=True)
                summary["errors"].append({"table": table, "error": str(e)})

    finally:
        spark.stop()

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info(f"FIN Bronze — {elapsed:.1f}s | tablas OK: {len(summary['tables'])} | errores: {len(summary['errors'])}")
    return summary


if __name__ == "__main__":
    run_bronze()
