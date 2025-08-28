from fastapi import FastAPI, Request, HTTPException
import json
import asyncio
from aiokafka import AIOKafkaProducer
from config.kafka_config import kafka_config
from config.settings import app_settings
import ssl

app = FastAPI()

@app.get("/")
@app.head("/")
async def root():
    return {"message": "Hii from Render!"}

async def create_kafka_producer():
    ssl_context = None
    if kafka_config.security_protocol in ["SSL", "SASL_SSL"]:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_config.bootstrap_servers.split(","),
        security_protocol=kafka_config.security_protocol,
        ssl_context=ssl_context,
        sasl_mechanism=kafka_config.sasl_mechanism,
        sasl_plain_username=kafka_config.sasl_plain_username,
        sasl_plain_password=kafka_config.sasl_plain_password.get_secret_value(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    return producer

async def close_kafka_producer(producer: AIOKafkaProducer):
    await producer.stop()

@app.on_event("startup")
async def startup_event():
    app.state.kafka_producer = await create_kafka_producer()

@app.on_event("shutdown")
async def shutdown_event():
    await close_kafka_producer(app.state.kafka_producer)

@app.post("/webhook")
async def handle_webhook(request: Request):
    payload = await request.json()
    auth_token = payload.get("token")

    if auth_token != app_settings.webhook_token:
        raise HTTPException(status_code=403, detail="Invalid token")

    topic = kafka_config.topic_target
    await app.state.kafka_producer.send(topic, value=payload)
    return {"status": "success", "topic": topic}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
