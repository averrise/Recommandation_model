from fastapi import FastAPI
from confluent_kafka import Consumer, KafkaException

app = FastAPI()

# Kafka Consumer 설정
consumer_config = {
    "bootstrap.servers": "localhost:9092",  # Kafka 브로커 주소
    "group.id": "fastapi-group",  # Consumer 그룹 ID
    "auto.offset.reset": "earliest",  # 처음부터 메시지를 읽음
}

consumer = Consumer(consumer_config)
consumer.subscribe(["test-topic"])  # 구독할 토픽 지정


@app.get("/read_messages/")
async def read_messages():
    """
    Kafka에서 메시지를 읽는 API 엔드포인트
    """
    try:
        messages = []
        for _ in range(5):  # 5개의 메시지만 읽기
            msg = consumer.poll(1.0)  # 1초 동안 메시지 기다림
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            messages.append(msg.value().decode("utf-8"))

        return {"messages": messages}

    except Exception as e:
        return {"status": "Error", "error": str(e)}
