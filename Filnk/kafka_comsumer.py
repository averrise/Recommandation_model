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

        result_table = self.table_env.sql_query("SELECT * FROM kafka_source")
        results = result_table.execute()

        for row in results.collect():
            movie_name = row[0]
            similarity_weight = row[1]
            top_n = row[2]

            print(f"📌 [추천 요청] {movie_name}, 가중치: {similarity_weight}, Top-{top_n}")
            start_time = time.perf_counter()  # 시작 시간 측정
            recommendations = self.recommender.predict(movie_name, similarity_weight, top_n)
            end_time = time.perf_counter() # 종료 시간 측정
            print(recommendations)

            print(f"⏱ 실행 시간: {end_time - start_time:.5f} 초")  # 실행 시간 출력

    def run(self):
        """Kafka 연결 후 실시간 데이터 처리 실행"""
        total_start_time = time.perf_counter()  # 전체 실행 시간 시작
        print(f"🚀 [전체 실행 시작] {total_start_time:.5f} 초")

        self.setup_kafka_source()
        self.process_stream()

        total_end_time = time.perf_counter()  # 전체 실행 시간 종료
        print(f"🏁 [전체 실행 종료] {total_end_time:.5f} 초")
        total_end_time = time.perf_counter()  # 전체 실행 시간 종료
        print(f"🚀 총 실행 시간: {total_end_time - total_start_time:.5f} 초")

if __name__ == "__main__":
    kafka_processor = FlinkKafkaProcessor(
        kafka_broker="localhost:29092",
        kafka_topic="test-topic",
        content_df_path="/Users/hangyeongmin/PycharmProjects/Recommandation_model/Model/content_df.csv"
    )
    kafka_processor.run()
