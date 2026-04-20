"""
bronze_seguimiento_tc_ingest.py
================================
Job de ingesta incremental — capa BRONZE
Fuente  : SFTP (WinSCP / paramiko)  →  OUT/ALTO_CONTACTO.zip
Destino : bronze/seguimiento_tc/Seguimiento_TC_YYYYMMDD_HHMM.xlsx

Lógica:
  1. Conecta al SFTP y descarga el ZIP
  2. Extrae Seguimiento_TC.xlsx del ZIP
  3. Renombra con timestamp para NO sobreescribir versiones anteriores
  4. Registra cada descarga en bronze/seguimiento_tc/_log_ingestas.csv
  5. Retorna la ruta del archivo guardado (para encadenar con silver)
"""

import paramiko
import zipfile
import os
import csv
import logging
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIGURACIÓN  (mover a config/sftp.yaml)
# ─────────────────────────────────────────────
SFTP_HOST       = "XXXXXXXXXXXXXXXX"
SFTP_PORT       = 22
SFTP_USER       = "XXXXXXXXXXXXXXXX"
SFTP_PASS       = "XXXXXXXXXXXXXXXX"
REMOTE_ZIP_PATH = "OUT/ALTO_CONTACTO.zip"

ARCHIVO_EN_ZIP  = "Seguimiento_TC.xlsx"
BRONZE_DIR      = Path("bronze/seguimiento_tc")
TMP_DIR         = Path("/tmp/lakehouse_bronze")
LOG_INGESTAS    = BRONZE_DIR / "_log_ingestas.csv"

# ─────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [BRONZE]  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _crear_directorios():
    """Crea carpetas bronze y tmp si no existen."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def _conectar_sftp() -> tuple[paramiko.Transport, paramiko.SFTPClient]:
    """Establece la conexión SFTP y retorna (transport, sftp_client)."""
    log.info(f"Conectando a SFTP  {SFTP_HOST}:{SFTP_PORT}  user={SFTP_USER}")
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    log.info("Conexión SFTP establecida.")
    return transport, sftp


def _descargar_zip(sftp: paramiko.SFTPClient, timestamp: str) -> Path:
    """Descarga el ZIP remoto a /tmp con nombre único por timestamp."""
    ruta_zip_local = TMP_DIR / f"alto_contacto_{timestamp}.zip"
    log.info(f"Descargando  {REMOTE_ZIP_PATH}  →  {ruta_zip_local}")
    sftp.get(REMOTE_ZIP_PATH, str(ruta_zip_local))
    log.info(f"ZIP descargado  ({ruta_zip_local.stat().st_size / 1024:.1f} KB)")
    return ruta_zip_local


def _extraer_xlsx(ruta_zip: Path, timestamp: str) -> Path:
    """
    Extrae Seguimiento_TC.xlsx del ZIP y lo mueve a bronze/
    con nombre versionado:  Seguimiento_TC_YYYYMMDD_HHMM.xlsx
    """
    nombre_destino = f"Seguimiento_TC_{timestamp}.xlsx"
    ruta_destino   = BRONZE_DIR / nombre_destino

    log.info(f"Extrayendo  '{ARCHIVO_EN_ZIP}'  del ZIP...")
    with zipfile.ZipFile(ruta_zip, "r") as z:
        # Verificar que el archivo existe dentro del ZIP
        archivos_en_zip = z.namelist()
        if ARCHIVO_EN_ZIP not in archivos_en_zip:
            raise FileNotFoundError(
                f"'{ARCHIVO_EN_ZIP}' no encontrado en el ZIP. "
                f"Archivos disponibles: {archivos_en_zip}"
            )
        z.extract(ARCHIVO_EN_ZIP, str(TMP_DIR))

    ruta_extraida = TMP_DIR / ARCHIVO_EN_ZIP
    ruta_extraida.rename(ruta_destino)
    log.info(f"Guardado en bronze:  {ruta_destino}")
    return ruta_destino


def _registrar_en_log(
    timestamp: str,
    ruta_archivo: Path,
    estado: str,
    detalle: str = "",
):
    """
    Agrega una línea al CSV de log de ingestas.
    Columnas: timestamp | archivo | estado | detalle
    """
    encabezado = not LOG_INGESTAS.exists()
    with open(LOG_INGESTAS, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if encabezado:
            writer.writerow(["timestamp", "archivo", "estado", "detalle"])
        writer.writerow([timestamp, str(ruta_archivo), estado, detalle])


def _limpiar_tmp(ruta_zip: Path):
    """Elimina el ZIP temporal para no acumular basura en /tmp."""
    try:
        ruta_zip.unlink()
        log.info(f"ZIP temporal eliminado:  {ruta_zip}")
    except Exception as e:
        log.warning(f"No se pudo eliminar ZIP temporal:  {e}")


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────

def ejecutar() -> Path:
    """
    Ejecuta la ingesta completa bronze.
    Retorna la ruta del archivo .xlsx guardado en bronze/.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    _crear_directorios()

    transport = None
    ruta_zip  = None

    try:
        # 1. Conectar
        transport, sftp = _conectar_sftp()

        # 2. Descargar ZIP
        ruta_zip = _descargar_zip(sftp, timestamp)

        # 3. Cerrar SFTP
        sftp.close()
        transport.close()
        log.info("Conexión SFTP cerrada.")

        # 4. Extraer xlsx con nombre versionado
        ruta_bronze = _extraer_xlsx(ruta_zip, timestamp)

        # 5. Registrar éxito en log
        _registrar_en_log(timestamp, ruta_bronze, "OK")

        # 6. Limpiar tmp
        _limpiar_tmp(ruta_zip)

        log.info(f"[OK] Ingesta bronze completada  →  {ruta_bronze}")
        return ruta_bronze

    except Exception as e:
        log.error(f"[ERROR] Ingesta bronze falló:  {e}")
        _registrar_en_log(timestamp, ruta_zip or "N/A", "ERROR", str(e))

        # Cerrar conexión si quedó abierta
        if transport and transport.is_active():
            transport.close()
        raise


# ─────────────────────────────────────────────
# EJECUCIÓN DIRECTA
# ─────────────────────────────────────────────

if __name__ == "__main__":
    ruta = ejecutar()
    print(f"\nArchivo listo para silver:\n  {ruta}")