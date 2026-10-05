import time
import requests

from src.vulcano_telemetry.webhook_crypto import generate_signature

payload = b'{"event":"payment","amount":100,"status":"approved"}'
secret = "vulcano-secret"

timestamp = int(time.time())

signature = generate_signature(
    secret,
    timestamp,
    payload,
)

headers = {
    "Content-Type": "application/json",
    "X-Webhook-Signature": f"t={timestamp},v1={signature}",
}

response = requests.post(
    "http://127.0.0.1:8000/webhook",
    data=payload,
    headers=headers,
)

print("STATUS:", response.status_code)
print("RESPONSE:", response.text)