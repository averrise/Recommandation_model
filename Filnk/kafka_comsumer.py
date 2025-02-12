from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# 실행 환경 설정
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# ✅ JAR 파일을 명시적으로 추가
t_env.get_config().set(
    "pipeline.jars",
"file:///Users/hangyeongmin/Downloads/flink-connector-kafka-3.4.0-1.20.jar"
)

# ✅ Kafka 소스 테이블 생성
t_env.execute_sql("""
    CREATE TABLE kafka_source (
        Movie_Name STRING,
        Similarity_weight INT,
        top_n INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'test-topic',
        'properties.bootstrap.servers' = 'localhost:29092',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
""")

# ✅ Kafka에서 데이터를 조회하는 SQL 실행
result_table = t_env.sql_query("SELECT * FROM kafka_source")



