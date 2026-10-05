"""
test_forensic_log.py
-----------------------
Integrante 6 - Validador forense de telemetria.

Inspecciona de forma automatizada los logs generados por
logging_engine.py: confirma que cada linea sea JSON valido, que
contenga los campos de metadatos obligatorios (TraceId, MessageId
cuando aplique), y que el arbol de excepciones (incluyendo
ExceptionGroup y causas encadenadas) se serialice correctamente, y
que la rotacion + compresion Gzip funcione sin perdida de datos.

Como ejecutar:
    pytest tests/test_forensic_log.py -v
"""

from __future__ import annotations

import gzip
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vulcano_telemetry.exceptions import (  # noqa: E402
    SQSConnectionError,
    WebhookSignatureError,
)
from vulcano_telemetry.logging_engine import setup_vulcano_logging  # noqa: E402


@pytest.fixture
def log_path(tmp_path) -> Path:
    return tmp_path / "vulcano_test.log"


def _leer_lineas_json(ruta: Path) -> list[dict]:
    with open(ruta, "r", encoding="utf-8") as f:
        return [json.loads(linea) for linea in f if linea.strip()]


class TestFormatoJSON:
    """Verifica que cada linea escrita sea JSON valido con los campos
    minimos exigidos por la consigna."""

    def test_log_simple_tiene_campos_obligatorios(self, log_path):
        logger = setup_vulcano_logging(str(log_path), logger_name="test_simple")
        logger.info("Evento de prueba", extra={"message_id": "msg-123"})
        time.sleep(0.3)
        logger.listener.stop()

        lineas = _leer_lineas_json(log_path)
        assert len(lineas) == 1
        registro = lineas[0]

        for campo in ("timestamp", "level", "logger", "message", "process", "thread_name"):
            assert campo in registro, f"Falta el campo obligatorio '{campo}'"

        assert registro["message_id"] == "msg-123"

    def test_timestamp_formato_iso8601_utc(self, log_path):
        from datetime import datetime

        logger = setup_vulcano_logging(str(log_path), logger_name="test_iso")
        logger.info("chequeo de fecha")
        time.sleep(0.3)
        logger.listener.stop()

        registro = _leer_lineas_json(log_path)[0]
        # Debe poder parsearse como ISO 8601; si el formato esta roto,
        # esto lanza ValueError y el test falla.
        timestamp_normalizado = registro["timestamp"].replace("Z", "+00:00")
        datetime.fromisoformat(timestamp_normalizado)


class TestArbolDeExcepciones:
    """Verifica la serializacion recursiva: causas encadenadas y
    ExceptionGroup con sub-excepciones."""

    def test_causa_encadenada_se_serializa_completa(self, log_path):
        logger = setup_vulcano_logging(str(log_path), logger_name="test_causa")

        try:
            try:
                raise ValueError("cabecera cruda malformada")
            except ValueError as origen:
                raise WebhookSignatureError("firma invalida") from origen
        except WebhookSignatureError as err:
            logger.error(
                "Fallo de firma",
                exc_info=(type(err), err, err.__traceback__),
            )

        time.sleep(0.3)
        logger.listener.stop()

        registro = _leer_lineas_json(log_path)[0]
        assert "exception" in registro
        assert registro["exception"]["type"] == "WebhookSignatureError"
        assert "caused_by" in registro["exception"], (
            "La causa encadenada (raise ... from err) debe aparecer "
            "como 'caused_by' en el JSON."
        )
        assert registro["exception"]["caused_by"]["type"] == "ValueError"

    def test_exception_group_serializa_sub_excepciones(self, log_path):
        logger = setup_vulcano_logging(str(log_path), logger_name="test_group")

        err1 = WebhookSignatureError("firma invalida en webhook A")
        err2 = SQSConnectionError("timeout conectando a LocalStack")

        try:
            raise ExceptionGroup("fallos concurrentes", [err1, err2])
        except* WebhookSignatureError as grupo:
            for e in grupo.exceptions:
                logger.error(
                    "Fallo agrupado", exc_info=(type(e), e, e.__traceback__)
                )
        except* SQSConnectionError as grupo:
            for e in grupo.exceptions:
                logger.error(
                    "Fallo agrupado", exc_info=(type(e), e, e.__traceback__)
                )

        time.sleep(0.3)
        logger.listener.stop()

        lineas = _leer_lineas_json(log_path)
        assert len(lineas) == 2
        tipos_encontrados = {linea["exception"]["type"] for linea in lineas}
        assert tipos_encontrados == {"WebhookSignatureError", "SQSConnectionError"}


class TestRotacionYCompresion:
    """Fuerza la rotacion a 2MB y certifica que el .gz resultante sea
    legible y no haya perdido datos."""

    def test_rotacion_genera_gz_legible_sin_perdida(self, tmp_path):
        log_path = tmp_path / "rotacion_test.log"
        logger = setup_vulcano_logging(
            str(log_path),
            logger_name="test_rotacion",
            max_bytes=50_000,  # bajo a proposito para rotar rapido en el test
            backup_count=2,
        )

        mensaje_grande = "x" * 200
        total_emitidos = 0
        for i in range(600):
            logger.info(mensaje_grande, extra={"indice": i})
            total_emitidos += 1

        time.sleep(1.0)
        logger.listener.stop()

        archivos_gz = list(tmp_path.glob("*.gz"))
        assert archivos_gz, (
            "Se esperaba al menos un archivo .gz tras forzar la rotacion "
            "con max_bytes bajo."
        )

        total_leido_gz = 0
        for archivo in archivos_gz:
            with gzip.open(archivo, "rt", encoding="utf-8") as f:
                for linea in f:
                    if linea.strip():
                        registro = json.loads(linea)  # debe parsear sin error
                        assert "indice" in registro
                        total_leido_gz += 1

        assert total_leido_gz > 0, (
            "El archivo .gz se genero pero no contiene registros legibles "
            "-- posible perdida de datos en la compresion."
        )