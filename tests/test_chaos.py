"""
test_chaos.py
--------------
Integrante 6 - Suite de simulacion de caos para Vulcano Pay.

Prueba:
  1. Los validadores de sanitizer.py (Integrante 1) rechazan entradas
     invalidas y aceptan las validas, probando especialmente los
     limites exactos.
  2. webhook_crypto.py (Integrante 3): firmas alteradas, cabeceras
     malformadas y timestamps viejos (Replay Attack) se rechazan.
  3. El servidor HTTP receptor (webhook_receiver.py) responde 401
     ante webhooks no autenticados, sin publicar nada en SQS.
  4. core_sqs.py (Integrante 2) deriva correctamente un mensaje
     "veneno" (JSON corrupto) a CorruptedMessageError, sin tumbar el
     resto del lote.

Como ejecutar (parado en la carpeta raiz del proyecto, vulcano_engine):
    pip install pytest moto fastapi httpx --break-system-packages
    pytest tests/test_chaos.py -v

Nota: estos tests usan 'moto' para simular AWS SQS en memoria -- NO
necesitan LocalStack corriendo para pasar. Si ademas quieren probar
contra LocalStack real (con Docker), hay que correr
'docker compose up -d' o 'localstack start' antes, y apuntar
TRITON_SQS_ENDPOINT a ese endpoint real en vez de usar el mock.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vulcano_telemetry.exceptions import (  # noqa: E402
    CorruptedMessageError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from vulcano_telemetry.sanitizer import (  # noqa: E402
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)
from vulcano_telemetry.webhook_crypto import (  # noqa: E402
    generate_signature,
    verify_signature,
)


# =====================================================================
# Grupo 1 - Validadores de frontera (sanitizer.py, Integrante 1)
# =====================================================================

class TestValidadores:
    """Prueba los limites exactos de los validadores CLI."""

    def test_queue_name_valido(self):
        assert parse_queue_name("vulcano-events-queue") == "vulcano-events-queue"

    @pytest.mark.parametrize(
        "nombre", ["cola-mala", "vulcano-MAYUS-queue", "vulcano-sin-sufijo", ""]
    )
    def test_queue_name_invalido(self, nombre):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_queue_name(nombre)

    def test_wait_time_limite_inferior_valido(self):
        assert parse_wait_time("1") == 1

    def test_wait_time_limite_superior_valido(self):
        assert parse_wait_time("20") == 20

    @pytest.mark.parametrize("valor", ["0", "21", "-5", "abc"])
    def test_wait_time_invalido(self, valor):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_wait_time(valor)

    @pytest.mark.parametrize(
        "url", ["http://localhost:8000/webhook", "https://clientes.vulcanopay.com/hook"]
    )
    def test_webhook_url_valida(self, url):
        assert parse_webhook_url(url) == url

    @pytest.mark.parametrize("url", ["ftp://mal", "no-es-una-url", ""])
    def test_webhook_url_invalida(self, url):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_webhook_url(url)

    def test_secret_key_limite_exacto_valido(self):
        # Exactamente 16 caracteres: el limite esta incluido (>=).
        assert parse_secret_key("a" * 16) == "a" * 16

    def test_secret_key_muy_corta_invalida(self):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_secret_key("a" * 15)


# =====================================================================
# Grupo 2 - Inyeccion de caos criptografico (webhook_crypto.py, Integrante 3)
# =====================================================================

class TestCaosCriptografico:
    """Fuerza firmas invalidas y timestamps viejos contra el verificador."""

    SECRET = "mi_clave_secreta_super_segura_123"

    def test_firma_alterada_se_rechaza(self):
        payload = b'{"amount": 100}'
        ts = int(time.time())
        sig = generate_signature(self.SECRET, ts, payload)
        payload_falsificado = b'{"amount": 999999}'
        with pytest.raises(WebhookSignatureError):
            verify_signature(self.SECRET, f"t={ts},v1={sig}", payload_falsificado)

    def test_replay_attack_timestamp_viejo_se_rechaza(self):
        payload = b'{"amount": 100}'
        ts_viejo = int(time.time()) - 3600  # hace 1 hora
        sig = generate_signature(self.SECRET, ts_viejo, payload)
        with pytest.raises(WebhookTimestampError):
            verify_signature(self.SECRET, f"t={ts_viejo},v1={sig}", payload)

    def test_cabecera_malformada_se_rechaza(self):
        with pytest.raises(WebhookSignatureError):
            verify_signature(self.SECRET, "esto-no-tiene-el-formato-correcto", b"{}")

    def test_cabecera_ausente_se_rechaza(self):
        with pytest.raises(WebhookSignatureError):
            verify_signature(self.SECRET, "", b"{}")


# =====================================================================
# Grupo 3 - Webhooks no autenticados contra el servidor HTTP real
# (webhook_receiver.py, Integrante 3) y mensajes "veneno" en SQS
# (core_sqs.py, Integrante 2)
# =====================================================================

class TestCaosEndToEnd:
    """
    Prueba el sistema completo: servidor HTTP + SQS (simulado con
    moto, sin necesitar LocalStack corriendo).
    """

    SECRET = "mi_clave_secreta_super_segura_123"

    @mock_aws
    def test_webhook_firma_invalida_responde_401_y_no_publica_en_sqs(self):
        from fastapi.testclient import TestClient

        import vulcano_telemetry.webhook_receiver as receiver_mod

        sqs = boto3.client("sqs", region_name="us-east-1")
        queue_url = sqs.create_queue(QueueName="vulcano-events-queue")["QueueUrl"]
        receiver_mod.get_sqs_client = lambda endpoint_url=None: sqs

        app = receiver_mod.create_app(self.SECRET, queue_url)
        client = TestClient(app)

        payload = json.dumps({"transaction_id": "tx_ataque"}).encode()
        response = client.post(
            "/webhook",
            content=payload,
            headers={"X-Webhook-Signature": "t=1700000000,v1=firma_falsa"},
        )

        assert response.status_code == 401, (
            "Un webhook con firma invalida debe ser rechazado con 401, "
            f"pero respondio {response.status_code}"
        )

        mensajes = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        assert "Messages" not in mensajes, (
            "Un webhook rechazado por firma invalida NUNCA debe llegar "
            "a publicarse en SQS."
        )

    @mock_aws
    def test_mensaje_veneno_en_sqs_no_tumba_el_resto_del_lote(self):
        """
        Cola con [valido, veneno, valido]: consume_events debe devolver
        los 2 mensajes validos, omitir el corrupto y NO borrarlo (asi
        SQS lo reintenta y termina en la DLQ).
        """
        from vulcano_telemetry.core_sqs import consume_events

        sqs = boto3.client("sqs", region_name="us-east-1")
        queue_url = sqs.create_queue(QueueName="vulcano-events-queue")["QueueUrl"]

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"id": 1}))
        sqs.send_message(QueueUrl=queue_url, MessageBody="esto-no-es-json{{{")
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"id": 2}))

        eventos = consume_events(sqs, queue_url, wait_time=1)

        assert sorted(e["body"]["id"] for e in eventos) == [1, 2]

        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["All"]
        )["Attributes"]
        total = int(attrs["ApproximateNumberOfMessages"]) + int(
            attrs["ApproximateNumberOfMessagesNotVisible"]
        )
        assert total == 3, "El mensaje veneno no debe borrarse de la cola."
