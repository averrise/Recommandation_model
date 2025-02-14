import sys
import time
from pyflink.table import EnvironmentSettings, TableEnvironment
from Model.Recommander import MovieRecommender


class FlinkKafkaProcessor:
    def __init__(self, kafka_broker, kafka_topic, content_df_path):
        """Flink Kafka 설정 및 추천 모델 초기화"""
        self.env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        self.table_env = TableEnvironment.create(self.env_settings)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.is_running = True
        self.start_time = None  # 전체 실행 시작 시간 저장 변수

        # Flink Kafka 커넥터 JAR 추가
        self.table_env.get_config().get_configuration().set_string(
            "pipeline.jars",
            "file:///Users/hangyeongmin/PycharmProjects/Recommandation_model/Kafka/flink-plugins/flink-sql-connector-kafka-3.4.0-1.20.jar"
        )

        # 추천 시스템 초기화
        self.recommender = MovieRecommender(content_df_path)

    def setup_kafka_source(self):
        """Kafka 소스 테이블 생성"""
        self.table_env.execute_sql(f"""
            CREATE TABLE kafka_source (
                Movie_Name STRING,
                Similarity_weight FLOAT,
                top_n INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.kafka_topic}',
                'properties.bootstrap.servers' = '{self.kafka_broker}',
                'format' = 'json',
                'scan.startup.mode' = 'earliest-offset'
            )
        """)

    def process_stream(self):
        """Kafka에서 실시간 데이터를 받아 추천 모델 실행"""
        kafka_start_time = time.perf_counter()

        result_table = self.table_env.sql_query("SELECT * FROM kafka_source")
        results = result_table.execute()

        kafka_end_time = time.perf_counter()  # Kafka에서 메시지 읽기 완료 시간
        kafka_elapsed_time = kafka_end_time - kafka_start_time
        print(f"📡 [Kafka → Flink 메시지 읽기 속도] {kafka_elapsed_time:.5f} 초")

        message_count = 0

        for row in results.collect():
            model_start_time = time.perf_counter()
            if not self.is_running:
                break

            message_count += 1
            movie_name = row[0]
            similarity_weight = row[1]
            top_n = row[2]

            #원래는 서비스 종료 신호가 오면 닫아야함.
            if message_count >= 5:
                print("✅ [처리된 메시지가 5개 이상이므로 Flink 프로세스를 종료합니다.]")
                self.shutdown()  # 종료 함수 호출
                break

            print(f"📌 [추천 요청] {movie_name}, 가중치: {similarity_weight}, Top-{top_n}")
            recommendations = self.recommender.predict(movie_name, similarity_weight, top_n)

            model_end_time = time.perf_counter()  # 모델 실행 완료 시간
            model_elapsed_time = model_end_time - model_start_time
            print(f"🤖 [Flink → 모델 실행 속도] {model_elapsed_time:.5f} 초")
            print(recommendations)

    def run(self):
        """Kafka 연결 후 실시간 데이터 처리 실행"""
        self.start_time = time.perf_counter()  # 전체 실행 시작 시간 기록
        print(f"🚀 [Flink 실행 시작] {self.start_time:.5f} 초")

        self.setup_kafka_source()
        self.process_stream()

    def shutdown(self):
        """Flink 프로세스를 종료하는 함수"""
        print("⛔ [Flink 프로세스 종료 요청됨]")
        """Flink 프로세스를 종료하는 함수"""
        end_time = time.perf_counter()  # 종료 시간 기록
        total_elapsed_time = end_time - self.start_time  # 총 실행 시간 계산

        print(f"🏁 [전체 실행 종료] {end_time:.5f} 초")
        print(f"🚀 총 실행 시간: {total_elapsed_time:.5f} 초")
        print("⛔ [Flink 프로세스 종료 요청됨]")

        self.is_running = False  # 실행 플래그 변경
        sys.exit(0)  # 정상 종료
