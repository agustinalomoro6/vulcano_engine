"""
webhook_receiver.py
----------------------
Integrante 3 - Servidor HTTP receptor de Webhooks (FastAPI).

Flujo del endpoint POST /webhook:
  1. Lee los BYTES CRUDOS del cuerpo (await request.body()) -- NUNCA
     se llama a request.json() antes de verificar la firma, porque
     volver a serializar el JSON ya parseado puede dar bytes
     distintos a los que realmente llegaron por la red (distinto
     orden de claves, espacios, etc.), rompiendo la verificacion
     HMAC de forma silenciosa.
  2. Verifica la firma y el timestamp con webhook_crypto.verify_signature.
  3. Si la firma es invalida -> 401 Unauthorized.
  4. Si el timestamp esta fuera de tolerancia -> 401 Unauthorized
     (mismo codigo que una firma invalida, para no darle a un
     atacante informacion de diagnostico sobre cual de las dos
     validaciones fallo).
  5. Si todo es valido, responde 202 Accepted de inmediato y publica
     el evento en SQS -- el procesamiento pesado (reenvio del
     webhook a terceros) ocurre despues, de forma asincrona, en el
     Dispatcher (app_operator.py --mode start-dispatcher).
"""

from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request, HTTPException, status

from .webhook_crypto import verify_signature
from .core_sqs import get_sqs_client, produce_event
from .exceptions import WebhookSignatureError, WebhookTimestampError

logger = logging.getLogger("vulcano_engine")


def create_app(
    webhook_secret: str,
    queue_url: str,
    sqs_endpoint_url: str = "http://localhost:4566",
) -> FastAPI:
    """
    Factory que arma la app de FastAPI. Se usa una factory (en vez de
    una instancia global de modulo) para que los tests puedan crear
    una app con un secret/queue_url de prueba, sin pisar config real.
    """
    app = FastAPI(title="Vulcano Webhook Receiver")
    sqs_client = get_sqs_client(sqs_endpoint_url)

    @app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
    async def receive_webhook(request: Request):
        raw_bytes = await request.body()
        signature_header = request.headers.get("X-Webhook-Signature", "")
        trace_id = str(uuid.uuid4())

        try:
            verify_signature(webhook_secret, signature_header, raw_bytes)
        except (WebhookSignatureError, WebhookTimestampError) as err:
            logger.warning(
                "Webhook rechazado: autenticacion fallida",
                extra={"trace_id": trace_id, "reason": type(err).__name__},
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Firma o timestamp invalido.",
            )

        try:
            import json

            payload = json.loads(raw_bytes)
        except ValueError:
            logger.warning(
                "Webhook con firma valida pero cuerpo no-JSON",
                extra={"trace_id": trace_id},
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El cuerpo no es JSON valido.",
            )

        msg_id = produce_event(sqs_client, queue_url, payload, trace_id=trace_id)

        logger.info(
            "Webhook verificado y encolado en SQS",
            extra={"trace_id": trace_id, "message_id": msg_id},
        )
        return {"status": "accepted", "trace_id": trace_id, "message_id": msg_id}

    return app