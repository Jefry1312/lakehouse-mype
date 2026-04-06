"""
config/spark_session.py
Crea y devuelve una SparkSession local con soporte Delta Lake.
"""

from pyspark.sql import SparkSession
import os


def get_spark_session(app_name: str = "DeltaLakehouse") -> SparkSession:
    """
    Retorna una SparkSession configurada para Delta Lake en modo local.
    Llama a esta función al inicio de cada script procesador.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")   # usa todos los núcleos disponibles
        # ---------- Delta Lake ----------
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ---------- Rendimiento local ----------
        .config("spark.sql.shuffle.partitions", "4")   # bajar de 200 para local
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        # ---------- Delta Lake jars (descarga automática) ----------
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "mysql:mysql-connector-java:8.0.33",
        )
        .getOrCreate()
    )

    # Silenciar logs de Spark (solo errores visibles)
    spark.sparkContext.setLogLevel("ERROR")
    return spark
