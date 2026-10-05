"""
vulcano_telemetry
-------------------
Paquete de telemetria del motor de eventos Vulcano Pay (SQS + Webhooks
HMAC). Expone la API publica del proyecto para que pueda importarse
de forma limpia desde afuera, por ejemplo:

    from vulcano_telemetry import (
        get_sqs_client, produce_event, consume_events, delete_event,
        generate_signature, verify_signature,
        VulcanoError, SQSConnectionError, WebhookSignatureError, ...
        setup_vulcano_logging,
        parse_queue_name, parse_wait_time, parse_webhook_url, parse_secret_key,
    )
"""

from __future__ import annotations

from .core_sqs import (
    DEFAULT_ENDPOINT_URL,
    consume_events,
    delete_event,
    get_sqs_client,
    produce_event,
)
from .exceptions import (
    CorruptedMessageError,
    QueueTimeoutError,
    SQSConnectionError,
    VulcanoError,
    WebhookDeliveryError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from .logging_engine import (
    AsyncJSONFormatter,
    RawQueueHandler,
    setup_vulcano_logging,
)
from .sanitizer import (
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)
from .webhook_crypto import generate_signature, verify_signature

__all__ = [
    # core_sqs.py - Integrante 2
    "DEFAULT_ENDPOINT_URL",
    "get_sqs_client",
    "produce_event",
    "consume_events",
    "delete_event",
    # exceptions.py - Integrante 1
    "VulcanoError",
    "SQSConnectionError",
    "QueueTimeoutError",
    "CorruptedMessageError",
    "WebhookSignatureError",
    "WebhookTimestampError",
    "WebhookDeliveryError",
    # sanitizer.py - Integrante 1
    "parse_queue_name",
    "parse_wait_time",
    "parse_webhook_url",
    "parse_secret_key",
    # webhook_crypto.py - Integrante 3
    "generate_signature",
    "verify_signature",
    # logging_engine.py - Integrante 4
    "AsyncJSONFormatter",
    "RawQueueHandler",
    "setup_vulcano_logging",
]