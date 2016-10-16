# Run kafka producer
# python kafka_data_ingestion.py CMCSA stock-analyzer 192.168.99.103:9092

# run spark-streaming
# run kafka producer first, then run this one
# sh /Users/pengtang/Downloads/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --jars ~/big_data_client_packages/src/spark-streaming-kafka-0-8-assembly_2.11-2.0.0.jar ~/big_data_client_packages/src/stream-processing.py stock-analyzer stock-average 192.168.99.103:9092

# Consume data generated by spark-streaming
# sh /Users/pengtang/big_data_client_packages/kafka/bin/kafka-console-consumer.sh --zookeeper `docker-machine ip bigdata`:2181 --topic stock-average
