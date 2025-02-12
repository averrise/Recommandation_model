import os
import socket
from fastapi import FastAPI
from confluent_kafka import Producer
from pydantic import BaseModel

app = FastAPI()

# 실행 환경에 따라 Kafka 브로커 주소 선택
if os.getenv("KAFKA_BROKER"):  # Docker 내부에서 실행 중
    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
else:  # 로컬 머신에서 실행 중
    KAFKA_BROKER = "localhost:29092"

print(f"✅ Using Kafka broker: {KAFKA_BROKER}")

# Kafka Producer 설정
producer_config = {
    "bootstrap.servers": KAFKA_BROKER  # Kafka 브로커 주소
}

producer = Producer(producer_config)

class Movie(BaseModel):
    Movie_Name: str
    Similarity_weight: int
    top_n: int

@app.post("/send_message/")
async def send_message(movie: Movie):
    """
    Kafka로 메시지를 전송하는 API 엔드포인트
    """
    try:
        message = {
            "Movie_Name": movie.Movie_Name,
            "Similarity_weight": movie.Similarity_weight,
            "top_n": movie.top_n
        }
        producer.produce("test-topic", key=None, value=str(message).encode("utf-8"))
        producer.flush()

        return {"status": "Message sent", "message": message}
    except Exception as e:
        return {"status": "Error", "error": str(e)}
