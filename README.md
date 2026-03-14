# Python Backend — Kafka Transaction Producer

A simple Python backend that receives transaction data via HTTP and publishes it to a Kafka topic. Available in two flavors: FastAPI (local/server) and AWS Lambda.

## Files

| File | Description |
|---|---|
| `main.py` | FastAPI app for local development or server deployment |
| `lambda_function.py` | AWS Lambda handler (no FastAPI dependency) |
| `requirements.txt` | Python dependencies |

## API

### `POST /api/transactions`

**Request body:**
```json
{
  "user": "john_doe",
  "transactionType": "purchase",
  "itemName": "Laptop",
  "price": 999.99
}
```

**Response:**
```json
{
  "status": "published",
  "topic": "transactions"
}
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `transactions` | Kafka topic to publish to |

## Running Locally (FastAPI)

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`.

## Deploying to AWS Lambda

1. Install dependencies into a package folder:
   ```bash
   pip install confluent-kafka -t ./package
   ```

2. Zip the function:
   ```bash
   zip -r function.zip lambda_function.py package/
   ```

3. Upload `function.zip` to AWS Lambda.

4. Set the handler to `lambda_function.lambda_handler`.

5. Set environment variables `KAFKA_BROKER` and `KAFKA_TOPIC` in the Lambda console.

6. If using Amazon MSK, attach a VPC configuration matching your MSK cluster's VPC and subnets.
