from pyflink.table import EnvironmentSettings, TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)

table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///Users/hangyeongmin/PycharmProjects/Recommandation_model/Kafka/flink-plugins/flink-sql-connector-kafka-3.4.0-1.20.jar")

# Kafka 소스 테이블 생성
table_env.execute_sql(f"""
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

result = table_env.sql_query("SELECT * FROM kafka_source")
result.execute().print()