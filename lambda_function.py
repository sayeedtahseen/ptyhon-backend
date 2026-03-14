import json
import os

from confluent_kafka import Producer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

REQUIRED_FIELDS = {"user", "transactionType", "itemName", "price"}


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def lambda_handler(event, context):
    # Parse body
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON body"})

    # Validate required fields
    missing = REQUIRED_FIELDS - body.keys()
    if missing:
        return _response(400, {"error": f"Missing fields: {sorted(missing)}"})

    # Validate price is a number
    try:
        body["price"] = float(body["price"])
    except (TypeError, ValueError):
        return _response(400, {"error": "price must be a number"})

    # Publish to Kafka
    try:
        producer.produce(
            KAFKA_TOPIC,
            value=json.dumps(body),
            callback=delivery_report,
        )
        producer.flush()
    except Exception as e:
        return _response(500, {"error": str(e)})

    return _response(200, {"status": "published", "topic": KAFKA_TOPIC})


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
