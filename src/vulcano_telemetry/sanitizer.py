"""
sanitizer.py
-------------
Integrante 1 - Validadores declarativos para argparse (callables
custom, usados con type=... en app_operator.py).

Cada validador recibe el string crudo que escribio el usuario en la
consola y devuelve el valor ya convertido/validado, o lanza
argparse.ArgumentTypeError si el dato es invalido. argparse traduce
automaticamente esa excepcion en una salida limpia por consola con
codigo de salida 2, sin necesidad de capturarla nosotros.
"""

from __future__ import annotations

import argparse
import re
from urllib.parse import urlparse

# vulcano-<nombre>-queue : nombre en minusculas, numeros y guiones.
_QUEUE_NAME_PATTERN = re.compile(r"^vulcano-[a-z0-9-]+-queue$")


def parse_queue_name(value: str) -> str:
    """Valida que el nombre de la cola cumpla con 'vulcano-<nombre>-queue'."""
    if not _QUEUE_NAME_PATTERN.match(value):
        raise argparse.ArgumentTypeError(
            f"Nombre de cola invalido '{value}'. Debe seguir el patron "
            "'vulcano-<nombre>-queue' (ej: vulcano-events-queue)."
        )
    return value


def parse_wait_time(value: str) -> int:
    """Valida el tiempo de Long Polling (WaitTimeSeconds) para SQS: 1-20s."""
    try:
        val = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"WaitTimeSeconds invalido '{value}'. Debe ser un numero entero."
        )
    if not (1 <= val <= 20):
        raise argparse.ArgumentTypeError(
            f"WaitTimeSeconds invalido '{value}'. Debe estar entre 1 y 20 "
            "segundos (limite real de AWS SQS Long Polling)."
        )
    return val


def parse_webhook_url(value: str) -> str:
    """Valida que la URL del receptor de webhooks sea HTTP/HTTPS valida."""
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            f"URL de Webhook invalida '{value}'. Debe ser una URL HTTP o "
            "HTTPS completa (ej: http://localhost:8000/webhook)."
        )
    return value


def parse_secret_key(value: str) -> str:
    """Valida que la clave secreta HMAC tenga al menos 16 caracteres."""
    if len(value) < 16:
        raise argparse.ArgumentTypeError(
            "La clave secreta HMAC debe tener al menos 16 caracteres "
            f"(recibidos: {len(value)})."
        )
    return value