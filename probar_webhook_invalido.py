import requests

payload = b'{"event":"payment","amount":100,"status":"approved"}'

headers = {
    "Content-Type": "application/json",
    "X-Webhook-Signature": "t=1791176315,v1=0000000000000000000000000000000000000000000000000000000000000000",
}

response = requests.post(
    "http://127.0.0.1:8000/webhook",
    data=payload,
    headers=headers,
)

print("STATUS:", response.status_code)
print("RESPONSE:", response.text)