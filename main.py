import json
import os

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": KAFKA_API_KEY,
    "sasl.password": KAFKA_API_SECRET,
})


class TransactionPayload(BaseModel):
    user: str
    transactionType: str
    itemName: str
    price: float


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


@app.post("/api/transactions")
def create_transaction(payload: TransactionPayload):
    try:
        producer.produce(
            KAFKA_TOPIC,
            value=json.dumps(payload.model_dump()),
            callback=delivery_report,
        )
        undelivered = producer.flush(timeout=5)
        if undelivered > 0:
            raise HTTPException(status_code=500, detail="Failed to deliver message to Kafka")
        return {"status": "published", "topic": KAFKA_TOPIC}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
