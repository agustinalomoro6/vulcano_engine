import time

from src.vulcano_telemetry.webhook_crypto import (
    generate_signature,
    verify_signature,
)
from src.vulcano_telemetry.exceptions import WebhookTimestampError

secret = "vulcano-secret"

# Timestamp de hace 10 minutos.
timestamp = int(time.time()) - 600

payload = b"payment-test"

signature = generate_signature(
    secret,
    timestamp,
    payload,
)

header = f"t={timestamp},v1={signature}"

try:
    verify_signature(
        secret,
        header,
        payload,
    )

    print("ERROR: el timestamp vencido fue aceptado")

except WebhookTimestampError as exc:
    print("REPLAY ATTACK RECHAZADO:", exc)