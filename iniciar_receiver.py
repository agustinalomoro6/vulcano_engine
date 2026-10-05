import uvicorn

from src.vulcano_telemetry.webhook_receiver import create_app

QUEUE_URL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/vulcano-events-queue"
SECRET = "vulcano-secret"

app = create_app(SECRET, QUEUE_URL)

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
    )