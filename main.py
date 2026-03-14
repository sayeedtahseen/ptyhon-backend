import json
import os

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

producer = Producer({"bootstrap.servers": KAFKA_BROKER})


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
        producer.flush()
        return {"status": "published", "topic": KAFKA_TOPIC}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
