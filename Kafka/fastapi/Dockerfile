# Python 3.9 기반 FastAPI 실행 환경
FROM python:3.9

# 작업 디렉토리 설정
WORKDIR /fastapi
# 필요한 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# FastAPI 코드 복사
COPY . .

# FastAPI 실행
CMD ["uvicorn", "kafka_producer:app", "--host", "0.0.0.0", "--port", "8000"]