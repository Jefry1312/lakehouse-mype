"""
jobs/scheduler.py
━━━━━━━━━━━━━━━━
Job nocturno con APScheduler.

Orquesta Bronze → Silver → Gold de forma secuencial.
Ejecuta automáticamente a la hora configurada en .env (default 02:00 AM).

USO:
  python jobs/scheduler.py             # inicia el daemon
  python jobs/scheduler.py --run-now  # ejecuta inmediatamente (para probar)
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv()

BASE_PATH = os.getenv("LAKEHOUSE_BASE_PATH", str(Path(__file__).parent.parent))
JOB_HOUR  = int(os.getenv("JOB_HOUR", "2"))
JOB_MIN   = int(os.getenv("JOB_MINUTE", "0"))
LOG_PATH  = Path(BASE_PATH) / "logs"
LOG_PATH.mkdir(parents=True, exist_ok=True)

# ── Logger global ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SCHEDULER] %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH / "scheduler.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def run_full_pipeline() -> None:
    """
    Pipeline completo: Bronze → Silver → Gold.
    Si un paso falla, los siguientes se omiten para no propagar datos corruptos.
    """
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log.info("=" * 70)
    log.info(f"▶  PIPELINE INICIADO  |  run_id={run_id}")
    log.info("=" * 70)

    # Importar aquí para evitar carga al inicio del scheduler
    from jobs.job_01_bronze_extractor import run_bronze
    from jobs.job_02_silver_processor import run_silver
    from jobs.job_03_gold_processor   import run_gold

    pipeline_ok = True

    # ── PASO 1: Bronze ─────────────────────────────────────
    log.info("── PASO 1/3  Bronze ──────────────────────────────")
    try:
        result = run_bronze()
        if result["errors"]:
            log.warning(f"  Bronze completó con {len(result['errors'])} errores")
        else:
            log.info("  Bronze OK")
    except Exception as e:
        log.error(f"  Bronze FALLÓ: {e}", exc_info=True)
        pipeline_ok = False

    # ── PASO 2: Silver ─────────────────────────────────────
    if pipeline_ok:
        log.info("── PASO 2/3  Silver ──────────────────────────────")
        try:
            result = run_silver()
            if result["errors"]:
                log.warning(f"  Silver completó con {len(result['errors'])} errores")
            else:
                log.info("  Silver OK")
        except Exception as e:
            log.error(f"  Silver FALLÓ: {e}", exc_info=True)
            pipeline_ok = False
    else:
        log.error("  Silver OMITIDO — Bronze falló")

    # ── PASO 3: Gold ───────────────────────────────────────
    if pipeline_ok:
        log.info("── PASO 3/3  Gold ────────────────────────────────")
        try:
            result = run_gold()
            log.info(f"  Gold OK — {len(result['kpis'])} KPIs generados")
        except Exception as e:
            log.error(f"  Gold FALLÓ: {e}", exc_info=True)
    else:
        log.error("  Gold OMITIDO — pipeline previo falló")

    log.info("=" * 70)
    log.info(f"◼  PIPELINE TERMINADO  |  run_id={run_id}")
    log.info("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Delta Lakehouse Scheduler")
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Ejecuta el pipeline ahora mismo (útil para pruebas)",
    )
    args = parser.parse_args()

    if args.run_now:
        log.info("Modo manual: ejecutando pipeline ahora...")
        run_full_pipeline()
        return

    # ── Modo daemon (job nocturno) ─────────────────────────
    scheduler = BlockingScheduler(timezone="America/Lima")

    scheduler.add_job(
        func=run_full_pipeline,
        trigger=CronTrigger(hour=JOB_HOUR, minute=JOB_MIN),
        id="medallion_pipeline",
        name="Pipeline Medallón nocturno",
        misfire_grace_time=3600,   # tolera hasta 1h de retraso
        max_instances=1,           # nunca dos ejecuciones paralelas
        coalesce=True,             # si perdió múltiples disparos, ejecuta solo una vez
    )

    log.info("=" * 60)
    log.info(f"Scheduler iniciado — job nocturno: {JOB_HOUR:02d}:{JOB_MIN:02d} (Lima)")
    log.info("Presiona Ctrl+C para detener")
    log.info("=" * 60)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler detenido manualmente")


if __name__ == "__main__":
    main()
