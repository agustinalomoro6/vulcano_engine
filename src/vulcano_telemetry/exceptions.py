"""
exceptions.py
--------------
Integrante 1 - Jerarquia de excepciones semanticas del ecosistema
Vulcano Pay (SQS + Webhooks HMAC).

Todas heredan de Exception (nunca de BaseException), para respetar
la "Hard Gate" de la consigna: capturar BaseException rompe el
Ctrl+C / SystemExit del interprete, asi que nuestras excepciones de
dominio jamas deben vivir ahi arriba en la jerarquia.
"""

from __future__ import annotations


class VulcanoError(Exception):
    """Excepcion base para todos los errores del ecosistema Vulcano."""


# ---------------------------------------------------------------------
# Dominio SQS (Integrante 2 - core_sqs.py)
# ---------------------------------------------------------------------

class SQSConnectionError(VulcanoError):
    """Fallo de red/transporte al comunicarse con SQS (LocalStack)."""


class QueueTimeoutError(VulcanoError):
    """El Long Polling o la operacion de cola supero el tiempo esperado."""


class CorruptedMessageError(VulcanoError):
    """El cuerpo de un mensaje SQS no es un JSON valido o esta mal formado."""


# ---------------------------------------------------------------------
# Dominio Webhook (Integrante 3 - webhook_crypto.py / webhook_receiver.py)
# ---------------------------------------------------------------------

class WebhookSignatureError(VulcanoError):
    """La firma HMAC-SHA256 del webhook es invalida, ausente o no coincide."""


class WebhookTimestampError(VulcanoError):
    """El timestamp del webhook esta fuera de la ventana de tolerancia
    (posible Replay Attack)."""


class WebhookDeliveryError(VulcanoError):
    """El envio HTTP POST del webhook hacia el destino del cliente fallo."""