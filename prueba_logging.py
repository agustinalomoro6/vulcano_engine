import logging
import os
import time
import glob
import gzip

from src.vulcano_telemetry.logging_engine import setup_vulcano_logging

log_file = "prueba_rotacion.log"

for archivo in glob.glob("prueba_rotacion.log*"):
    os.remove(archivo)

logger = setup_vulcano_logging(
    log_file=log_file,
    level=logging.INFO,
    max_bytes=200,
    backup_count=3,
)

for i in range(30):
    logger.info(
        f"Mensaje largo para forzar la rotacion numero {i}: "
        + ("X" * 100)
    )

time.sleep(1)

logger._vulcano_listener.stop()

print("ARCHIVOS GENERADOS:")

for archivo in glob.glob("prueba_rotacion.log*"):
    print(" -", archivo)

print("GZIP ENCONTRADO:", bool(glob.glob("prueba_rotacion.log.*.gz")))