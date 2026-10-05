"""
webhook_crypto.py
-------------------
Integrante 3 - Seguridad criptografica HMAC-SHA256 para Webhooks.

Implementa el esquema de firma estilo Stripe: la cabecera
X-Webhook-Signature trae "t=<timestamp>,v1=<firma_hex>", y la firma
se calcula sobre "{timestamp}." + payload_bytes (los BYTES CRUDOS del
cuerpo, nunca sobre el dict ya parseado con .json() -- eso es clave
para que la verificacion sea fiel a lo que realmente llego por la red,
byte a byte, sin que un re-serializado de JSON con distinto orden de
claves o espacios rompa la firma).
"""

from __future__ import annotations

import hmac
import hashlib
import time

from .exceptions import WebhookSignatureError, WebhookTimestampError


def generate_signature(secret: str, timestamp: int, payload_bytes: bytes) -> str:
    """Genera la firma HMAC-SHA256 en hex sobre '{timestamp}.' + payload_bytes."""
    to_sign = f"{timestamp}.".encode("utf-8") + payload_bytes
    return hmac.new(secret.encode("utf-8"), to_sign, hashlib.sha256).hexdigest()


def _parse_signature_header(header_value: str) -> tuple[int, str]:
    """
    Parsea 't=1700000000,v1=abcdef...' en (timestamp, firma).
    Lanza WebhookSignatureError si el formato esta mal armado, en vez
    de dejar que un ValueError/KeyError crudo se escape hacia arriba.
    """
    if not header_value:
        raise WebhookSignatureError("Cabecera X-Webhook-Signature ausente.")

    partes: dict[str, str] = {}
    for item in header_value.split(","):
        if "=" not in item:
            raise WebhookSignatureError(
                f"Cabecera X-Webhook-Signature malformada: {header_value!r}"
            )
        clave, _, valor = item.partition("=")
        partes[clave.strip()] = valor.strip()

    if "t" not in partes or "v1" not in partes:
        raise WebhookSignatureError(
            "Cabecera X-Webhook-Signature incompleta: faltan los campos "
            "'t' (timestamp) y/o 'v1' (firma)."
        )

    try:
        timestamp = int(partes["t"])
    except ValueError:
        raise WebhookSignatureError(
            f"El campo 't' de la cabecera no es un timestamp valido: "
            f"{partes['t']!r}"
        )

    return timestamp, partes["v1"]


def verify_signature(
    secret: str,
    header_value: str,
    payload_bytes: bytes,
    tolerance_sec: int = 300,
) -> bool:
    """
    Verifica la cabecera X-Webhook-Signature sobre los bytes crudos del
    cuerpo. Protege contra dos ataques distintos:

      1. Replay Attack: si el timestamp esta mas viejo (o mas en el
         futuro) que `tolerance_sec`, se rechaza con
         WebhookTimestampError, aunque la firma en si sea valida --
         significa que alguien esta reenviando una peticion vieja
         capturada.
      2. Timing Attack: la comparacion final usa hmac.compare_digest(),
         que tarda el mismo tiempo sin importar en que caracter
         difieren las dos cadenas, para que un atacante no pueda
         "adivinar" la firma correcta midiendo cuanto tarda cada
         intento fallido.

    Devuelve True si es valida; en cualquier otro caso lanza la
    excepcion semantica correspondiente (nunca devuelve False en
    silencio).
    """
    timestamp, firma_recibida = _parse_signature_header(header_value)

    ahora = int(time.time())
    diferencia = abs(ahora - timestamp)
    if diferencia > tolerance_sec:
        raise WebhookTimestampError(
            f"Timestamp fuera de tolerancia: {diferencia}s de diferencia "
            f"(maximo permitido: {tolerance_sec}s). Posible Replay Attack."
        )

    firma_esperada = generate_signature(secret, timestamp, payload_bytes)

    # hmac.compare_digest en tiempo constante: NUNCA usar == acá.
    if not hmac.compare_digest(firma_esperada, firma_recibida):
        raise WebhookSignatureError("La firma HMAC-SHA256 no coincide.")

    return True