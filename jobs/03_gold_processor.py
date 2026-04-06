"""
jobs/03_gold_processor.py
━━━━━━━━━━━━━━━━━━━━━━━━
Capa GOLD — Agregaciones, KPIs y vistas de negocio.

Lee desde Silver (Delta), genera tablas analíticas optimizadas
para consumo directo por dashboards o reportes.

PERSONALIZA las funciones build_* con los KPIs de tu negocio.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import DeltaTable

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.spark_session import get_spark_session

load_dotenv()

BASE_PATH   = os.getenv("LAKEHOUSE_BASE_PATH", str(Path(__file__).parent.parent))
SILVER_PATH = Path(BASE_PATH) / "silver"
GOLD_PATH   = Path(BASE_PATH) / "gold"
LOG_PATH    = Path(BASE_PATH) / "logs"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GOLD] %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH / "gold.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def read_silver(spark: SparkSession, table_name: str) -> DataFrame | None:
    """Lee una tabla desde Silver si existe."""
    path = str(SILVER_PATH / table_name)
    if not (SILVER_PATH / table_name).exists():
        log.warning(f"  Tabla Silver '{table_name}' no encontrada — saltando")
        return None
    return spark.read.format("delta").load(path)


def write_gold(df: DataFrame, gold_table_name: str) -> None:
    """Escribe un KPI/vista en Gold como Delta Table."""
    gold_path = str(GOLD_PATH / gold_table_name)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )
    dt = DeltaTable.forPath(df.sparkSession, gold_path)
    v = dt.history(1).collect()[0]
    log.info(f"    Gold '{gold_table_name}' v{v['version']} guardada")


# ══════════════════════════════════════════════════════════
# KPIs DE NEGOCIO
# ══════════════════════════════════════════════════════════

def build_ventas_resumen_mensual(spark: SparkSession) -> None:
    """
    KPI: Resumen de ventas agrupado por año/mes y producto.
    Tabla Gold: ventas_mensual
    """
    ventas    = read_silver(spark, "ventas")
    productos = read_silver(spark, "productos")
    if ventas is None:
        return

    df = ventas.groupBy("anio", "mes", "id_producto").agg(
        F.count("id").alias("num_transacciones"),
        F.sum("monto").alias("monto_total"),
        F.avg("monto").alias("monto_promedio"),
        F.min("monto").alias("monto_minimo"),
        F.max("monto").alias("monto_maximo"),
    )

    # Enriquecer con nombre de producto si Silver lo tiene
    if productos is not None:
        df = (
            df.join(
                productos.select("id", "nombre", "categoria")
                         .withColumnRenamed("id", "id_producto")
                         .withColumnRenamed("nombre", "producto_nombre"),
                on="id_producto",
                how="left",
            )
        )

    df = df.withColumn("_gold_ts", F.lit(datetime.utcnow().isoformat()))
    write_gold(df, "ventas_mensual")
    log.info(f"  ✓ ventas_mensual → {df.count()} filas")


def build_ranking_clientes(spark: SparkSession) -> None:
    """
    KPI: Top clientes por monto total de compras (lifetime value).
    Tabla Gold: ranking_clientes
    """
    ventas   = read_silver(spark, "ventas")
    clientes = read_silver(spark, "clientes")
    if ventas is None or clientes is None:
        return

    df_ltv = ventas.groupBy("id_cliente").agg(
        F.sum("monto").alias("ltv_total"),
        F.count("id").alias("num_compras"),
        F.avg("monto").alias("ticket_promedio"),
        F.max("fecha").alias("ultima_compra"),
    )

    # Ventana para ranking
    window_rank = Window.orderBy(F.desc("ltv_total"))
    df_ranked = df_ltv.withColumn("ranking", F.rank().over(window_rank))

    df = df_ranked.join(
        clientes.select("id", "nombre", "email")
                .withColumnRenamed("id", "id_cliente"),
        on="id_cliente",
        how="left",
    ).withColumn("_gold_ts", F.lit(datetime.utcnow().isoformat()))

    write_gold(df, "ranking_clientes")
    log.info(f"  ✓ ranking_clientes → {df.count()} clientes")


def build_inventario_valor(spark: SparkSession) -> None:
    """
    KPI: Valor total del inventario por categoría de producto.
    Tabla Gold: inventario_valor
    """
    productos = read_silver(spark, "productos")
    if productos is None:
        return

    df = (
        productos
        .groupBy("categoria")
        .agg(
            F.count("id").alias("num_productos"),
            F.sum(F.col("precio") * F.col("stock")).alias("valor_inventario"),
            F.avg("precio").alias("precio_promedio"),
        )
        .withColumn("_gold_ts", F.lit(datetime.utcnow().isoformat()))
    )

    write_gold(df, "inventario_valor")
    log.info(f"  ✓ inventario_valor → {df.count()} categorías")


def build_resumen_empleados(spark: SparkSession) -> None:
    """
    KPI: Resumen de empleados por departamento.
    Tabla Gold: empleados_departamento
    """
    empleados = read_silver(spark, "empleados")
    if empleados is None:
        return

    df = (
        empleados
        .groupBy("departamento")
        .agg(
            F.count("id").alias("num_empleados"),
            F.avg("salario").alias("salario_promedio"),
            F.sum("salario").alias("masa_salarial"),
            F.min("salario").alias("salario_minimo"),
            F.max("salario").alias("salario_maximo"),
        )
        .withColumn("_gold_ts", F.lit(datetime.utcnow().isoformat()))
    )

    write_gold(df, "empleados_departamento")
    log.info(f"  ✓ empleados_departamento → {df.count()} departamentos")


def run_gold() -> dict:
    """Punto de entrada del procesador Gold."""
    start = datetime.utcnow()
    log.info("=" * 60)
    log.info(f"INICIO procesamiento Gold — {start.isoformat()}")

    spark = get_spark_session("Gold_Processor")
    summary = {"status": "ok", "kpis": [], "errors": []}

    # Registro de KPIs a construir
    KPI_BUILDERS = [
        ("ventas_mensual",         build_ventas_resumen_mensual),
        ("ranking_clientes",       build_ranking_clientes),
        ("inventario_valor",       build_inventario_valor),
        ("empleados_departamento", build_resumen_empleados),
        # Agrega más KPIs aquí
    ]

    try:
        for kpi_name, builder_fn in KPI_BUILDERS:
            try:
                log.info(f"  Construyendo KPI: {kpi_name}")
                builder_fn(spark)
                summary["kpis"].append(kpi_name)
            except Exception as e:
                log.error(f"  ✗ Error en KPI {kpi_name}: {e}", exc_info=True)
                summary["errors"].append({"kpi": kpi_name, "error": str(e)})
    finally:
        spark.stop()

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info(f"FIN Gold — {elapsed:.1f}s | KPIs OK: {len(summary['kpis'])}")
    return summary


if __name__ == "__main__":
    run_gold()
