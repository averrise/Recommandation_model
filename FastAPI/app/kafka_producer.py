from fastapi import FastAPI
from confluent_kafka import Producer
from pydantic import BaseModel

app = FastAPI()

# Kafka Producer 설정
producer_config = {
    "bootstrap.servers": "localhost:9092",  # Kafka 브로커 주소
}

producer = Producer(producer_config)

class Movie(BaseModel):
    Movie_Name : str
    Similarity_weight : int
    top_n : int

@app.post("/send_message/")
async def send_message(movie : Movie):
    """
    메시지를 Kafka 토픽으로 전송하는 API 엔드포인트
    """
    try:
        # 메시지를 JSON 형식으로 변환
        message = {
            "Movie_Name": movie.Movie_Name,
            "Similarity_weight": movie.Similarity_weight,
            "top_n": movie.top_n
        }
        # Kafka로 메시지 전송
        producer.produce("test-topic", str(message).encode("utf-8"))
        producer.flush()  # 메시지가 전송될 때까지 대기

        return {"status": "Message sent", "message": message}
    except Exception as e:
        return {"status": "Error", "error": str(e)}
