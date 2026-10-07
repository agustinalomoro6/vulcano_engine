"""
core_sqs.py
------------
Integrante 2 - Ingenieria de mensajeria asincrona con Boto3 y
LocalStack (AWS SQS).

LocalStack emula los endpoints reales de AWS en
http://localhost:4566, asi que el codigo de este modulo es
identico al que se usaria contra la nube real de AWS: solo cambia
el `endpoint_url` y las credenciales (que en LocalStack son
siempre "test"/"test", porque no valida credenciales reales).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

from .exceptions import CorruptedMessageError, SQSConnectionError

logger = logging.getLogger("vulcano_engine")

DEFAULT_ENDPOINT_URL = "http://localhost:4566"


def get_sqs_client(endpoint_url: str = DEFAULT_ENDPOINT_URL):
    """
    Crea un cliente Boto3 de SQS apuntando a LocalStack.

    Las credenciales "test"/"test" son el valor convencional que
    LocalStack acepta sin validar: no son secretos reales, por lo
    que no violan la regla de "prohibido subir secretos al repo".
    """
    return boto3.client(
        "sqs",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def produce_event(
    sqs_client,
    queue_url: str,
    event_data: Dict[str, Any],
    trace_id: Optional[str] = None,
    event_topic: str = "vulcano.webhook.received",
) -> str:
    """
    Publica un evento estructurado en SQS con MessageAttributes de
    trazabilidad (TraceId, EventTopic, Timestamp). Devuelve el
    MessageId que asigna SQS.

    Cualquier error de transporte/credenciales que reporte Boto3
    (botocore.exceptions.ClientError) se traduce a SQSConnectionError
    con una nota forense (.add_note()) indicando la cola afectada,
    para que el resto del sistema nunca dependa directamente de los
    tipos de excepcion internos de botocore.
    """
    try:
        payload = json.dumps(event_data)
        message_attributes = {
            "EventTopic": {"DataType": "String", "StringValue": event_topic},
            "Timestamp": {"DataType": "String", "StringValue": str(int(time.time()))},
        }
        if trace_id:
            message_attributes["TraceId"] = {"DataType": "String", "StringValue": trace_id}

        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=payload,
            MessageAttributes=message_attributes,
        )
        msg_id = response.get("MessageId", "N/A")
        logger.info(
            "Evento publicado en SQS",
            extra={"message_id": msg_id, "queue_url": queue_url, "event_topic": event_topic},
        )
        return msg_id

    except (ClientError, EndpointConnectionError) as err:
        sqs_err = SQSConnectionError("Error al enviar mensaje a AWS SQS / LocalStack.")
        sqs_err.add_note(f"QueueUrl: {queue_url}")
        sqs_err.add_note(f"EventTopic: {event_topic}")
        raise sqs_err from err


def consume_events(
    sqs_client,
    queue_url: str,
    wait_time: int = 10,
    max_messages: int = 10,
    visibility_timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Consume mensajes de SQS aplicando Long Polling (WaitTimeSeconds).

    Si el cuerpo de un mensaje no es JSON valido (poison message), se
    registra un CorruptedMessageError en el log y ese mensaje se omite
    SIN borrarlo: el resto del lote se devuelve normalmente. SQS
    reentrega el mensaje corrupto tras el VisibilityTimeout y, al
    superar maxReceiveCount de la RedrivePolicy, lo deriva a la Dead
    Letter Queue (vulcano-events-dlq).

    NO borra los mensajes automaticamente: el borrado explicito con
    delete_message() queda a cargo del llamador, una vez que confirma
    que el procesamiento (el despacho del webhook) fue exitoso. Esto
    es clave para la resiliencia: si el proceso se cae a mitad de
    camino, el mensaje sigue en la cola y se vuelve a entregar tras el
    VisibilityTimeout.
    """
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
            MessageAttributeNames=["All"],
        )
    except (ClientError, EndpointConnectionError) as err:
        sqs_err = SQSConnectionError("Error en recepcion de mensajes SQS (Long Polling).")
        sqs_err.add_note(f"QueueUrl: {queue_url}")
        sqs_err.add_note(f"WaitTimeSeconds: {wait_time}")
        raise sqs_err from err

    mensajes_crudos = response.get("Messages", [])
    procesados: List[Dict[str, Any]] = []

    for msg in mensajes_crudos:
        try:
            body = json.loads(msg["Body"])
        except json.JSONDecodeError as err:
            corrupt_err = CorruptedMessageError(
                f"El mensaje SQS {msg.get('MessageId', '???')} no contiene "
                "un JSON valido en el Body."
            )
            corrupt_err.add_note(f"MessageId: {msg.get('MessageId')}")
            corrupt_err.add_note(f"ReceiptHandle: {msg.get('ReceiptHandle')}")
            corrupt_err.__cause__ = err
            # Poison message: se registra y se OMITE, pero NO se borra.
            # SQS lo reentrega tras el VisibilityTimeout y, al superar
            # maxReceiveCount, lo deriva a la DLQ.
            logger.error(
                "Mensaje veneno omitido; sera reintentado y derivado a la DLQ",
                exc_info=(type(corrupt_err), corrupt_err, None),
                extra={"message_id": msg.get("MessageId")},
            )
            continue

        procesados.append(
            {
                "message_id": msg["MessageId"],
                "receipt_handle": msg["ReceiptHandle"],
                "body": body,
                "attributes": msg.get("MessageAttributes", {}),
            }
        )

    return procesados


def delete_event(sqs_client, queue_url: str, receipt_handle: str) -> None:
    """
    Elimina explicitamente un mensaje de la cola tras procesarlo con
    exito. SQS NO borra los mensajes automaticamente al leerlos (a
    diferencia de otras colas): solo los oculta temporalmente
    (VisibilityTimeout). Si nunca se llama a este delete, el mensaje
    reaparece y SQS lo cuenta como un reintento mas, acercandolo a su
    maxReceiveCount y, eventualmente, a la Dead Letter Queue.
    """
    try:
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info("Mensaje eliminado de la cola tras procesamiento exitoso.",
                     extra={"queue_url": queue_url})
    except (ClientError, EndpointConnectionError) as err:
        sqs_err = SQSConnectionError("Error al eliminar el mensaje procesado de SQS.")
        sqs_err.add_note(f"QueueUrl: {queue_url}")
        raise sqs_err from err