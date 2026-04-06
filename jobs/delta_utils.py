"""
jobs/delta_utils.py
━━━━━━━━━━━━━━━━━━
Utilidades para explotar las capacidades de Delta Lake:
  - Ver historial de cambios (time travel)
  - Restaurar versiones anteriores
  - Ver diferencias entre versiones
  - Vacuum (limpieza de versiones antiguas)
  - Consultas rápidas a las capas

USO:
  python jobs/delta_utils.py --historial bronze/clientes
  python jobs/delta_utils.py --restaurar bronze/clientes --version 2
  python jobs/delta_utils.py --vacuum gold/ventas_mensual --horas 168
  python jobs/delta_utils.py --listar
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.spark_session import get_spark_session

load_dotenv()

BASE_PATH = os.getenv("LAKEHOUSE_BASE_PATH", str(Path(__file__).parent.parent))


def get_full_path(tabla: str) -> str:
    """Convierte 'bronze/clientes' → ruta absoluta."""
    return str(Path(BASE_PATH) / tabla)


def cmd_historial(tabla: str, n: int = 10) -> None:
    """Muestra el historial de versiones de una Delta Table."""
    from delta import DeltaTable
    spark = get_spark_session("DeltaUtils_Historial")
    try:
        path = get_full_path(tabla)
        dt = DeltaTable.forPath(spark, path)
        print(f"\n{'='*60}")
        print(f"HISTORIAL: {tabla}")
        print(f"{'='*60}")
        dt.history(n).select(
            "version", "timestamp", "operation",
            "operationParameters", "operationMetrics"
        ).show(truncate=False)
    finally:
        spark.stop()


def cmd_leer_version(tabla: str, version: int) -> None:
    """Lee una tabla en una versión específica (time travel)."""
    spark = get_spark_session("DeltaUtils_TimeTravel")
    try:
        path = get_full_path(tabla)
        df = (
            spark.read
            .format("delta")
            .option("versionAsOf", version)
            .load(path)
        )
        print(f"\nTabla '{tabla}' en versión {version}: {df.count()} filas")
        df.show(20)
    finally:
        spark.stop()


def cmd_restaurar(tabla: str, version: int) -> None:
    """Restaura una Delta Table a una versión anterior."""
    from delta import DeltaTable
    spark = get_spark_session("DeltaUtils_Restore")
    try:
        path = get_full_path(tabla)
        dt = DeltaTable.forPath(spark, path)
        confirm = input(
            f"\n⚠ ¿Restaurar '{tabla}' a versión {version}? "
            f"Esto creará una nueva versión. [s/N]: "
        )
        if confirm.lower() == "s":
            dt.restoreToVersion(version)
            print(f"✓ Restaurado '{tabla}' a versión {version}")
        else:
            print("Cancelado.")
    finally:
        spark.stop()


def cmd_vacuum(tabla: str, horas: int = 168) -> None:
    """
    Limpia versiones antiguas de la Delta Table.
    Default: conserva últimas 168h (7 días).
    ⚠ No ejecutar antes de ese período si necesitas time travel.
    """
    from delta import DeltaTable
    spark = get_spark_session("DeltaUtils_Vacuum")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        path = get_full_path(tabla)
        dt = DeltaTable.forPath(spark, path)
        confirm = input(
            f"\n⚠ VACUUM '{tabla}' (conservar {horas}h). "
            f"Se borrarán versiones anteriores. [s/N]: "
        )
        if confirm.lower() == "s":
            dt.vacuum(horas)
            print(f"✓ Vacuum completado en '{tabla}'")
        else:
            print("Cancelado.")
    finally:
        spark.stop()


def cmd_listar() -> None:
    """Lista todas las Delta Tables en el lakehouse."""
    lakehouse = Path(BASE_PATH)
    print(f"\n{'='*60}")
    print(f"DELTA TABLES en: {lakehouse}")
    print(f"{'='*60}")
    for capa in ["bronze", "silver", "gold"]:
        capa_path = lakehouse / capa
        if capa_path.exists():
            tablas = [d.name for d in capa_path.iterdir() if d.is_dir()]
            print(f"\n  [{capa.upper()}]")
            for t in sorted(tablas):
                print(f"    • {t}  →  {capa}/{t}")


def cmd_schema(tabla: str) -> None:
    """Muestra el schema de una Delta Table."""
    spark = get_spark_session("DeltaUtils_Schema")
    try:
        path = get_full_path(tabla)
        df = spark.read.format("delta").load(path)
        print(f"\nSchema de '{tabla}':")
        df.printSchema()
        print(f"Filas actuales: {df.count()}")
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Utilidades Delta Lake — Medallón Lakehouse"
    )
    parser.add_argument("--historial",   metavar="TABLA",
                        help="Ver historial: bronze/clientes")
    parser.add_argument("--version",     metavar="N",    type=int,
                        help="Versión para time travel o restaurar")
    parser.add_argument("--restaurar",   metavar="TABLA",
                        help="Restaurar tabla a --version N")
    parser.add_argument("--vacuum",      metavar="TABLA",
                        help="Limpiar versiones antiguas")
    parser.add_argument("--horas",       metavar="H",    type=int, default=168,
                        help="Horas a conservar en vacuum (default 168)")
    parser.add_argument("--listar",      action="store_true",
                        help="Listar todas las Delta Tables")
    parser.add_argument("--schema",      metavar="TABLA",
                        help="Ver schema: silver/ventas")
    parser.add_argument("--leer",        metavar="TABLA",
                        help="Leer tabla en --version N")

    args = parser.parse_args()

    if args.listar:
        cmd_listar()
    elif args.historial:
        cmd_historial(args.historial)
    elif args.restaurar and args.version is not None:
        cmd_restaurar(args.restaurar, args.version)
    elif args.vacuum:
        cmd_vacuum(args.vacuum, args.horas)
    elif args.schema:
        cmd_schema(args.schema)
    elif args.leer and args.version is not None:
        cmd_leer_version(args.leer, args.version)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
