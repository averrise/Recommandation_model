#baseline
import pandas as pd
from pyflink.table.expressions import col
from pyflink.table import DataTypes
from pyflink.table.schema import Schema
from pyflink.table.expressions import *
from pyflink.table.udf import udtf
from pyflink.table import EnvironmentSettings, TableEnvironment
# Flink 실행 환경 설정

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

# SQL 실행: Data Generator Table 생성
#table_env.execute_sql(
#    """
#    CREATE TABLE datagen (
#        id INT,
#        data STRING
#    ) WITH (
#        'connector' = 'datagen',
#        'fields.id.kind' = 'sequence',
#        'fields.id.start' = '1',
#        'fields.id.end' = '30',
#        'number-of-rows' = '30'
#    )
#    """
#)

# 데이터 읽기
#source_table = table_env.from_path("datagen")
#source_table.execute().print()

#"""table = table_env.from_elements([(1,'praveen'),(2,'sex'), (3,'emitting')], DataTypes.ROW([
 #   DataTypes.FIELD('id', DataTypes.INT()),
 #   DataTypes.FIELD('name', DataTypes.STRING())
#]))

#table.execute().print()"""

# CSV 파일을 테이블로 변환하는 SQL 실행
table_env.execute_sql(
    """
    CREATE TABLE CsvTable (
        id INT,
        name STRING,
        city STRING,
        pin INT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///Users/hangyeongmin/PycharmProjects/Recommandation_model/Filnk/input_csv.csv',
        'format' = 'csv',
        'csv.field-delimiter' = ','
    )
    """
)

source_table = table_env.from_path("CsvTable")
result = source_table.rename_columns(
    col("name").alias("unji_name")
)

import socket

def check_kafka_port(host="localhost", port=29092):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)  # 3초 동안 응답 없으면 실패 처리
    try:
        sock.connect((host, port))
        print(f"✅ Kafka 포트 {port} 가 열려 있습니다!")
        return True
    except socket.error:
        print(f"❌ Kafka 포트 {port} 가 닫혀 있습니다.")
        return False
    finally:
        sock.close()

# 실행
check_kafka_port()
