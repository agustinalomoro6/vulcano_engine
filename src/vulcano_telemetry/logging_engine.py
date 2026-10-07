"""
logging_engine.py
-------------------
Integrante 4 - Observabilidad: formateador JSON forense, pipeline no
bloqueante (QueueHandler/QueueListener) y compresion Gzip al vuelo.

NOTA DE DISENO: el logging.handlers.QueueHandler ESTANDAR de Python
sobreescribe su metodo prepare() para renderizar el mensaje a texto y
BORRA record.exc_info antes de encolar. RawQueueHandler, abajo,
devuelve el LogRecord intacto para que el arbol de excepciones llegue
completo hasta el AsyncJSONFormatter del otro lado de la cola.
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

# Atributos "de fabrica" de un LogRecord: cualquier otra clave que
# aparezca en record.__dict__ llego via extra={...} y se vuelca al
# JSON tal cual, sin que cada desarrollador tenga que acordarse de
# agregar un 'if hasattr(record, "mi_campo_nuevo")' cada vez.
_CAMPOS_RESERVADOS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "taskName",
}


class AsyncJSONFormatter(logging.Formatter):
    """Convierte cada LogRecord en una línea JSON forense."""

    def _serialize_exception(
        self,
        exc: Optional[BaseException],
        seen: Optional[set[int]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Serializa una excepción, su cadena de causas y sus notas."""

        if exc is None:
            return None

        if seen is None:
            seen = set()

        exc_id = id(exc)
        if exc_id in seen:
            # Evita recursion infinita si una excepcion termina
            # siendo su propia causa (ciclo), algo que en teoria no
            # deberia pasar pero que no cuesta nada blindar.
            return {"type": type(exc).__name__, "message": str(exc)}

        seen.add(exc_id)

        exception_data: Dict[str, Any] = {
            "type": type(exc).__name__,
            "message": str(exc),
            # Campo estructurado para las notas agregadas con
            # .add_note() -- ANTES solo quedaban mezcladas dentro del
            # texto plano del traceback, dificiles de certificar
            # automaticamente. Ahora son una lista propia.
            "notes": list(getattr(exc, "__notes__", [])),
        }

        try:
            exception_data["traceback"] = "".join(
                self.formatException((type(exc), exc, exc.__traceback__))
            )
        except Exception:
            exception_data["traceback"] = None

        if exc.__cause__ is not None:
            exception_data["caused_by"] = self._serialize_exception(exc.__cause__, seen)
        elif exc.__context__ is not None and not exc.__suppress_context__:
            exception_data["caused_by"] = self._serialize_exception(exc.__context__, seen)

        if isinstance(exc, BaseExceptionGroup):
            exception_data["exceptions"] = [
                self._serialize_exception(sub_exc, seen.copy())
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

        # Cualquier campo pasado via extra={...} (trace_id, event_id,
        # queue_name, message_id, webhook_id, indice, etc.) se vuelca
        # automaticamente -- antes esto estaba hardcodeado a 4 campos
        # fijos y 'trace_id' en particular se perdia en silencio.
        for clave, valor in record.__dict__.items():
            if clave not in _CAMPOS_RESERVADOS and not clave.startswith("_"):
                data[clave] = valor

        if record.exc_info and record.exc_info[1] is not None:
            data["exception"] = self._serialize_exception(record.exc_info[1])

        return json.dumps(data, ensure_ascii=False, default=str)


class RawQueueHandler(logging.handlers.QueueHandler):
    """QueueHandler que conserva el LogRecord original (exc_info incluido)."""

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


def gzip_namer(default_name: str) -> str:
    """Namer: los archivos rotados pasan a llamarse 'vulcano.log.N.gz'."""
    return default_name + ".gz"


def gzip_rotator(source: str, dest: str) -> None:
    """Rotator: comprime 'source' en 'dest' de forma atomica.

    Se escribe primero un archivo temporal y recien al terminar se
    renombra con os.replace(), asi nunca queda un .gz a medio escribir.
    """
    tmp_dest = dest + ".tmp"
    with open(source, "rb") as f_in, gzip.open(tmp_dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.replace(tmp_dest, dest)
    os.remove(source)


def setup_vulcano_logging(
    log_file: str = "vulcano.log",
    level: int = logging.INFO,
    # 2 MB, tal como exige la consigna explicitamente (antes estaba
    # en 5 MB por default, y app_operator.py nunca lo sobreescribia).
    max_bytes: int = 2 * 1024 * 1024,
    backup_count: int = 3,
    logger_name: str = "vulcano_engine",
) -> logging.Logger:
    """Configura el logging asíncrono JSON de Vulcano.

    El nombre por defecto del logger ("vulcano_engine") es el mismo que
    usan core_sqs.py y webhook_receiver.py, para que sus mensajes lleguen
    al archivo de log.
    """

    log_queue: queue.Queue = queue.Queue()
    queue_handler = RawQueueHandler(log_queue)

    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.namer = gzip_namer
    file_handler.rotator = gzip_rotator
    file_handler.setFormatter(AsyncJSONFormatter())

    listener = logging.handlers.QueueListener(
        log_queue, file_handler, respect_handler_level=True
    )

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(queue_handler)

    listener.start()

    logger.listener = listener  # type: ignore[attr-defined]
    logger._vulcano_listener = listener  # type: ignore[attr-defined]

    return logger