from fastapi import FastAPI
from confluent_kafka import Producer

app = FastAPI()

# Kafka Producer 설정
producer_config = {
    "bootstrap.servers": "localhost:9092",  # Kafka 브로커 주소
}

producer = Producer(producer_config)


@app.post("/send_message/")
async def send_message(message: str):
    """
    메시지를 Kafka 토픽으로 전송하는 API 엔드포인트
    """
    try:
        producer.produce("test-topic", message.encode("utf-8"))
        producer.flush()  # 메시지가 전송될 때까지 기다림
        return {"status": "Message sent", "message": message}
    except Exception as e:
        return {"status": "Error", "error": str(e)}
