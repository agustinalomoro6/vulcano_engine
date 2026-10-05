from fastapi import FastAPI, Request

app = FastAPI()


@app.post("/incoming")
async def incoming(request: Request):
    body = await request.body()

    print("WEBHOOK RECIBIDO")
    print("BODY:", body.decode("utf-8"))
    print("SIGNATURE:", request.headers.get("X-Webhook-Signature"))

    return {"status": "ok"}