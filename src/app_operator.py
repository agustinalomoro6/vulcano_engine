"""
app_operator.py
-----------------
Integrante 5 - Punto de entrada CLI ejecutable.

Subcomandos:
  produce-sqs       Publica un evento de prueba directamente en SQS.
  start-receiver    Levanta el servidor HTTP receptor de Webhooks (uvicorn).
  start-dispatcher  Consume eventos de SQS y los reenvia como Webhooks
                    firmados al destino configurado.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests  # noqa: E402

from vulcano_telemetry.sanitizer import (  # noqa: E402
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)
from vulcano_telemetry.core_sqs import (  # noqa: E402
    DEFAULT_ENDPOINT_URL,
    consume_events,
    delete_event,
    get_sqs_client,
    produce_event,
)
from vulcano_telemetry.webhook_crypto import generate_signature  # noqa: E402
from vulcano_telemetry.logging_engine import setup_vulcano_logging  # noqa: E402
from vulcano_telemetry.exceptions import (  # noqa: E402
    CorruptedMessageError,
    SQSConnectionError,
    WebhookDeliveryError,
)

logger = setup_vulcano_logging()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vulcano",
        description="Motor de eventos SQS y Webhooks HMAC de Vulcano Pay.",
    )
    parser.add_argument(
        "--queue",
        type=parse_queue_name,
        default="vulcano-events-queue",
        help="Nombre de la cola SQS (patron vulcano-<nombre>-queue).",
    )
    parser.add_argument(
        "--secret",
        type=parse_secret_key,
        default="mi_clave_secreta_super_segura_vulcano_123",
        help="Clave secreta HMAC (minimo 16 caracteres).",
    )
    parser.add_argument(
        "--endpoint-url",
        default=DEFAULT_ENDPOINT_URL,
        help="Endpoint de LocalStack (default: http://localhost:4566).",
    )

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    sp_produce = subparsers.add_parser(
        "produce-sqs", help="Publica un evento de prueba directamente en SQS."
    )
    sp_produce.set_defaults(func=cmd_produce_sqs)

    sp_receiver = subparsers.add_parser(
        "start-receiver", help="Levanta el servidor HTTP receptor de Webhooks."
    )
    sp_receiver.add_argument("--host", default="0.0.0.0")
    sp_receiver.add_argument("--port", type=int, default=8000)
    sp_receiver.set_defaults(func=cmd_start_receiver)

    sp_dispatcher = subparsers.add_parser(
        "start-dispatcher", help="Consume SQS y despacha Webhooks firmados."
    )
    sp_dispatcher.add_argument(
        "--target-url",
        type=parse_webhook_url,
        default="http://localhost:9000/incoming",
        help="URL del servidor externo que recibira el Webhook reenviado.",
    )
    sp_dispatcher.add_argument(
        "--wait-time",
        type=parse_wait_time,
        default=10,
        help="WaitTimeSeconds de Long Polling (1-20).",
    )
    sp_dispatcher.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Si se indica, el dispatcher hace esa cantidad de ciclos de "
             "polling y termina (util para pruebas); sin este flag corre "
             "indefinidamente.",
    )
    sp_dispatcher.set_defaults(func=cmd_start_dispatcher)

    return parser


def _resolve_queue_url(sqs_client, queue_name: str) -> str:
    """
    Resuelve el QueueUrl real llamando a get_queue_url() en vez de
    armar la URL a mano -- asi funciona igual contra LocalStack o
    contra AWS real, sin asumir el account id falso de LocalStack
    (000000000000) escrito en el codigo.
    """
    return sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]


def dispatch_webhook(target_url: str, secret: str, payload_dict: dict) -> requests.Response:
    """
    Firma el payload con HMAC-SHA256 y lo reenvia por HTTP POST al
    destino configurado, inyectando la cabecera X-Webhook-Signature.
    """
    import json

    payload_bytes = json.dumps(payload_dict).encode("utf-8")
    timestamp = int(time.time())
    signature = generate_signature(secret, timestamp, payload_bytes)
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Signature": f"t={timestamp},v1={signature}",
    }

    try:
        return requests.post(target_url, data=payload_bytes, headers=headers, timeout=10)
    except requests.RequestException as err:
        delivery_err = WebhookDeliveryError(f"Fallo al reenviar webhook a {target_url}")
        delivery_err.add_note(f"Error original: {err}")
        raise delivery_err from err


def cmd_produce_sqs(args: argparse.Namespace) -> None:
    sqs = get_sqs_client(args.endpoint_url)
    queue_url = _resolve_queue_url(sqs, args.queue)
    payload = {
        "transaction_id": "tx_demo_001",
        "amount": 199.99,
        "status": "COMPLETED",
    }
    msg_id = produce_event(sqs, queue_url, payload)
    logger.info(f"Evento de prueba publicado. MessageId={msg_id}")


def cmd_start_receiver(args: argparse.Namespace) -> None:
    import uvicorn

    from vulcano_telemetry.webhook_receiver import create_app

    sqs = get_sqs_client(args.endpoint_url)
    queue_url = _resolve_queue_url(sqs, args.queue)
    app = create_app(args.secret, queue_url, args.endpoint_url)
    logger.info(f"Iniciando receptor de Webhooks en {args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)


def cmd_start_dispatcher(args: argparse.Namespace) -> None:
    sqs = get_sqs_client(args.endpoint_url)
    queue_url = _resolve_queue_url(sqs, args.queue)

    iteracion = 0
    while args.max_iterations is None or iteracion < args.max_iterations:
        iteracion += 1
        eventos = consume_events(sqs, queue_url, wait_time=args.wait_time)

        for evento in eventos:
            response = dispatch_webhook(args.target_url, args.secret, evento["body"])
            if response.status_code < 400:
                delete_event(sqs, queue_url, evento["receipt_handle"])
                logger.info(
                    "Webhook despachado y mensaje eliminado de la cola.",
                    extra={"message_id": evento["message_id"]},
                )
            else:
                logger.warning(
                    f"El destino respondio {response.status_code}; el "
                    "mensaje NO se elimina y sera reintentado por SQS.",
                    extra={"message_id": evento["message_id"]},
                )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logger.info(f"Iniciando VulcanoCLI, subcomando='{args.subcommand}'")

    exit_code = 0
    try:
        args.func(args)

    except* WebhookDeliveryError as grupo:
        exit_code = 1
        logger.error(f"Errores al despachar Webhooks ({len(grupo.exceptions)}):")
        for exc in grupo.exceptions:
            logger.error(f"  - {exc}", exc_info=(type(exc), exc, exc.__traceback__))

    except* SQSConnectionError as grupo:
        exit_code = 1
        logger.error(f"Errores de comunicacion con SQS/LocalStack ({len(grupo.exceptions)}):")
        for exc in grupo.exceptions:
            logger.error(f"  - {exc}", exc_info=(type(exc), exc, exc.__traceback__))

    except* CorruptedMessageError as grupo:
        exit_code = 1
        logger.error(f"Mensajes SQS corruptos detectados ({len(grupo.exceptions)}):")
        for exc in grupo.exceptions:
            logger.error(f"  - {exc}", exc_info=(type(exc), exc, exc.__traceback__))

    finally:
        # Apagado ordenado del QueueListener. Sin return/break/continue
        # aca adentro (PEP 765 / Python 3.14).
        logger.info("Liberando recursos de observabilidad...")
        if hasattr(logger, "listener") and logger.listener:
            logger.listener.stop()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())