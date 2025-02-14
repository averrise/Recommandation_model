from Filnk.kafka_comsumer import FlinkKafkaProcessor

if __name__ == "__main__":
    kafka_processor = FlinkKafkaProcessor(
        kafka_broker="localhost:29092",
        kafka_topic="test-topic",
        content_df_path="/Users/hangyeongmin/PycharmProjects/Recommandation_model/Model/content_df.csv"
    )
    kafka_processor.run()