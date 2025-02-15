import json
import time
from fastapi import FastAPI
from confluent_kafka import Producer
from pydantic import BaseModel
from fastapi.responses import JSONResponse

app = FastAPI()

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
    start_time = time.perf_counter()  # 메시지 전송 시작 시간 기록
    """
    Kafka로 메시지를 전송하고 JSON 응답을 반환하는 API 엔드포인트
    """
    try:
        message = {
            "Movie_Name": movie.Movie_Name,
            "Similarity_weight": movie.Similarity_weight,
            "top_n": movie.top_n
        }
        producer.produce("test-topic", key=None, value=json.dumps(message, ensure_ascii=False).encode("utf-8"))
        producer.flush()

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        response_data = {
            "status": "Message sent",
            "message": message,
            "elapsed_time": f"{elapsed_time:.5f} 초"
        }

        print(f"⏳ [FastAPI 응답 속도] {elapsed_time:.5f} 초")

        return JSONResponse(content=response_data, status_code=200)
    except Exception as e:
        error_response = {
            "status": "Error",
            "error": str(e)
        }
        return JSONResponse(content=error_response, status_code=500)