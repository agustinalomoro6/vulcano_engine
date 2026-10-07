"""
infra/setup_queues.py
-----------------------
Script de aprovisionamiento: crea las colas de SQS en LocalStack
(vulcano-events-queue y vulcano-events-dlq) y conecta la principal a
la Dead Letter Queue mediante una RedrivePolicy.

Esto NO es parte del paquete vulcano_telemetry (no se importa desde
ahi): es un script de infraestructura que se corre UNA VEZ al
levantar el entorno de desarrollo, igual que una migracion de base de
datos. Sin este paso, la cola principal existe pero nunca deriva los
mensajes venenosos a la DLQ, porque no tiene ninguna politica de
reintentos asociada.

Uso (con LocalStack corriendo en localhost:4566):
    python infra/setup_queues.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vulcano_telemetry.core_sqs import get_sqs_client  # noqa: E402

QUEUE_NAME = "vulcano-events-queue"
DLQ_NAME = "vulcano-events-dlq"

# Cuantas veces SQS intenta entregar un mensaje antes de darlo por
# "veneno" y derivarlo automaticamente a la DLQ.
MAX_RECEIVE_COUNT = 3


def main() -> int:
    sqs = get_sqs_client()

    print(f"Creando Dead Letter Queue '{DLQ_NAME}'...")
    dlq_response = sqs.create_queue(QueueName=DLQ_NAME)
    dlq_url = dlq_response["QueueUrl"]
    dlq_attrs = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])
    dlq_arn = dlq_attrs["Attributes"]["QueueArn"]
    print(f"  OK -> {dlq_url}")
    print(f"  ARN -> {dlq_arn}")

    print(f"\nCreando cola principal '{QUEUE_NAME}' con RedrivePolicy...")
    redrive_policy = json.dumps(
        {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": MAX_RECEIVE_COUNT}
    )
    main_response = sqs.create_queue(
        QueueName=QUEUE_NAME,
        Attributes={
            "RedrivePolicy": redrive_policy,
            "VisibilityTimeout": "30",
        },
    )
    main_url = main_response["QueueUrl"]
    print(f"  OK -> {main_url}")
    print(
        f"  Un mensaje que falle {MAX_RECEIVE_COUNT} veces se "
        f"derivara automaticamente a '{DLQ_NAME}'."
    )

    print("\nAprovisionamiento completo.")
    print(f"QUEUE_URL = {main_url}")
    print(f"DLQ_URL   = {dlq_url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())