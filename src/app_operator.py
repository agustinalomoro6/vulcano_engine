"""
app_operator.py
Integrante 5 - Punto de entrada CLI ejecutable.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests

from vulcano_telemetry.sanitizer import (
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)

from vulcano_telemetry.core_sqs import (
    DEFAULT_ENDPOINT_URL,
    consume_events,
    delete_event,
    get_sqs_client,
    produce_event,
)

from vulcano_telemetry.webhook_crypto import generate_signature
from vulcano_telemetry.logging_engine import setup_vulcano_logging
from vulcano_telemetry.exceptions import WebhookDeliveryError


logger = setup_vulcano_logging()


def build_parser():
    parser = argparse.ArgumentParser(
        prog="vulcano",
        description="Motor de eventos SQS y Webhooks HMAC de Vulcano Pay.",
    )

    parser.add_argument(
        "--queue",
        type=parse_queue_name,
        default="vulcano-events-queue",
        help="Nombre de la cola SQS.",
    )

    parser.add_argument(
        "--secret",
        type=parse_secret_key,
        default="mi_clave_secreta_super_segura_vulcano_123",
        help="Clave secreta HMAC.",
    )

    parser.add_argument(
        "--endpoint-url",
        default=DEFAULT_ENDPOINT_URL,
        help="Endpoint de LocalStack.",
    )

    subparsers = parser.add_subparsers(
        dest="subcommand",
        required=True,
    )

    # PRODUCE SQS
    sp_produce = subparsers.add_parser(
        "produce-sqs",
        help="Publica un evento de prueba en SQS.",
    )

    sp_produce.set_defaults(func=cmd_produce_sqs)

    # RECEIVER
    sp_receiver = subparsers.add_parser(
        "start-receiver",
        help="Levanta el receptor HTTP.",
    )

    sp_receiver.add_argument(
        "--host",
        default="0.0.0.0",
    )

    sp_receiver.add_argument(
        "--port",
        type=int,
        default=8000,
    )

    sp_receiver.set_defaults(func=cmd_start_receiver)

    # DISPATCHER
    sp_dispatcher = subparsers.add_parser(
        "start-dispatcher",
        help="Consume SQS y envia Webhooks.",
    )

    sp_dispatcher.add_argument(
        "--target-url",
        type=parse_webhook_url,
        default="http://localhost:9000/incoming",
        help="URL del destino del webhook.",
    )

    sp_dispatcher.add_argument(
        "--wait-time",
        type=parse_wait_time,
        default=10,
        help="Tiempo de Long Polling de SQS.",
    )

    sp_dispatcher.set_defaults(func=cmd_start_dispatcher)

    return parser


def cmd_produce_sqs(args):

    client = get_sqs_client(args.endpoint_url)

    queue_url = client.get_queue_url(
        QueueName=args.queue
    )["QueueUrl"]

    payload = {
        "event": "payment",
        "amount": 100,
        "status": "approved",
    }

    message_id = produce_event(
        client,
        queue_url,
        payload,
    )

    print("PRODUCE OK:", message_id)

    return 0


def cmd_start_receiver(args):

    import uvicorn

    from vulcano_telemetry.webhook_receiver import create_app

    client = get_sqs_client(args.endpoint_url)

    queue_url = client.get_queue_url(
        QueueName=args.queue
    )["QueueUrl"]

    app = create_app(
        webhook_secret=args.secret,
        queue_url=queue_url,
        sqs_endpoint_url=args.endpoint_url,
    )

    print(
        f"Starting Vulcano Webhook Receiver "
        f"on {args.host}:{args.port}"
    )

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
    )

    return 0


def dispatch_webhook(target_url, secret, payload):

    import json

    payload_bytes = json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    timestamp = int(time.time())

    signature = generate_signature(
        secret,
        timestamp,
        payload_bytes,
    )

    header = f"t={timestamp},v1={signature}"

    try:

        response = requests.post(
            target_url,
            data=payload_bytes,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Signature": header,
            },
            timeout=10,
        )

    except requests.RequestException as exc:

        raise WebhookDeliveryError(
            f"Fallo al enviar webhook: {exc}"
        ) from exc

    if not 200 <= response.status_code < 300:

        raise WebhookDeliveryError(
            f"Destino respondio HTTP {response.status_code}"
        )


def cmd_start_dispatcher(args):

    client = get_sqs_client(args.endpoint_url)

    queue_url = client.get_queue_url(
        QueueName=args.queue
    )["QueueUrl"]

    print("DISPATCHER INICIADO")
    print("COLA:", args.queue)
    print("DESTINO:", args.target_url)
    print("ESPERANDO EVENTOS...")

    while True:

        try:

            events = consume_events(
                client,
                queue_url,
                wait_time=args.wait_time,
            )

            for event in events:

                message_id = event["message_id"]
                receipt_handle = event["receipt_handle"]
                payload = event["body"]

                try:

                    dispatch_webhook(
                        args.target_url,
                        args.secret,
                        payload,
                    )

                    delete_event(
                        client,
                        queue_url,
                        receipt_handle,
                    )

                    print(
                        "DISPATCH OK:",
                        message_id,
                    )

                except WebhookDeliveryError as exc:

                    print(
                        "DISPATCH ERROR:",
                        exc,
                        file=sys.stderr,
                    )

        except KeyboardInterrupt:

            print("\nDISPATCHER DETENIDO.")

            return 0

        except Exception as exc:

            print(
                "DISPATCHER ERROR:",
                exc,
                file=sys.stderr,
            )

            time.sleep(2)


def main():

    parser = build_parser()

    args = parser.parse_args()

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())