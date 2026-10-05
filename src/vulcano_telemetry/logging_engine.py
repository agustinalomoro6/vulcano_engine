"""
logging_engine.py
-----------------
Integrante 4 - Observabilidad.
"""

from __future__ import annotations

import gzip
import json
import logging
import logging.handlers
import os
import queue
import shutil
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class AsyncJSONFormatter(logging.Formatter):
    """Convierte cada LogRecord en una línea JSON forense."""

    def _serialize_exception(
        self,
        exc: Optional[BaseException],
        seen: Optional[set[int]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Serializa una excepción y sus causas."""

        if exc is None:
            return None

        if seen is None:
            seen = set()

        exc_id = id(exc)

        if exc_id in seen:
            return {
                "type": type(exc).__name__,
                "message": str(exc),
            }

        seen.add(exc_id)

        exception_data: Dict[str, Any] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }

        try:
            traceback_text = "".join(
                self.formatException(
                    (
                        type(exc),
                        exc,
                        exc.__traceback__,
                    )
                )
            )
            exception_data["traceback"] = traceback_text
        except Exception:
            exception_data["traceback"] = None

        # Causa explícita: raise X from Y
        if exc.__cause__ is not None:
            exception_data["caused_by"] = self._serialize_exception(
                exc.__cause__,
                seen,
            )

        # Causa implícita.
        elif exc.__context__ is not None and not exc.__suppress_context__:
            exception_data["caused_by"] = self._serialize_exception(
                exc.__context__,
                seen,
            )

        # ExceptionGroup.
        if isinstance(exc, BaseExceptionGroup):
            exception_data["exceptions"] = [
                self._serialize_exception(
                    sub_exc,
                    seen.copy(),
                )
                for sub_exc in exc.exceptions
            ]

        return exception_data

    def format(self, record: logging.LogRecord) -> str:
        """Genera un registro JSON."""

        data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.process,
            "thread_name": record.threadName,
        }

        if hasattr(record, "event_id"):
            data["event_id"] = record.event_id

        if hasattr(record, "queue_name"):
            data["queue_name"] = record.queue_name

        if hasattr(record, "message_id"):
            data["message_id"] = record.message_id

        if hasattr(record, "indice"):
            data["indice"] = record.indice

        if record.exc_info:
            exception = record.exc_info[1]

            if exception is not None:
                data["exception"] = self._serialize_exception(exception)

        return json.dumps(
            data,
            ensure_ascii=False,
        )


class RawQueueHandler(logging.handlers.QueueHandler):
    """QueueHandler que conserva el LogRecord original."""

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


class GzipRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Rota archivos y comprime los archivos rotados en Gzip."""

    def doRollover(self) -> None:
        super().doRollover()

        for index in range(self.backupCount, 0, -1):
            source = f"{self.baseFilename}.{index}"

            if os.path.exists(source) and not source.endswith(".gz"):
                destination = f"{source}.gz"

                try:
                    with open(source, "rb") as source_file:
                        with gzip.open(destination, "wb") as gzip_file:
                            shutil.copyfileobj(
                                source_file,
                                gzip_file,
                            )

                    os.remove(source)

                except OSError:
                    pass


def setup_vulcano_logging(
    log_file: str = "vulcano.log",
    level: int = logging.INFO,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3,
    logger_name: str = "vulcano",
) -> logging.Logger:
    """Configura el logging asíncrono JSON de Vulcano."""

    log_queue: queue.Queue = queue.Queue()

    queue_handler = RawQueueHandler(log_queue)

    file_handler = GzipRotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )

    file_handler.setFormatter(AsyncJSONFormatter())

    listener = logging.handlers.QueueListener(
        log_queue,
        file_handler,
        respect_handler_level=True,
    )

    logger = logging.getLogger(logger_name)

    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(queue_handler)

    listener.start()

    # Permite detener el listener desde tests y otros componentes.
    logger.listener = listener  # type: ignore[attr-defined]

    # Referencia interna.
    logger._vulcano_listener = listener  # type: ignore[attr-defined]

    return logger