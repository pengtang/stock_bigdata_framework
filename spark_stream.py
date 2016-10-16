# Spark streaming application - Get data from Kafka, process the data and then sends back the new result to Kafka
# Run example: ./spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.0.0.jar spark_stream.py stock-analyzer average-stock-price 192.168.99.100:9092 
# @param $1 - "source kafka topic", "write to kafka topic", $3 - "kafka broker address"

import atexit
import sys
import logging
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

topic = ""
new_topic = ""
kafka_broker = ""

def shutdown_hook(producer):
	try:
		logger.info('flush pending messages to kafka')
		producer.flush(10)
		logger.info('finish flushing pending messages')
	except KafkaError as kafka_error:
		logger.warn('Failed to flush pending messages to kafka')
	finally:
		try:
			producer.close()
		except Exception as e:
			logger.warn('Fail to close kafka connection')		

def process(timeobj, rdd):
	# calculate average
	num_of_record = rdd.count()
	if num_of_record == 0:
		return
	price_sum = rdd.map(lambda record: float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice'))).reduce(lambda a, b: a+b)
	average = price_sum / num_of_record
	logger.info('Received %d records from Kafka, average price is %f' % (num_of_record, average))

	# write back to kafka
	# (timestamp, average)
	data = json.dumps({
		'timestamp': time.time(),
		'average': average
		})
	kafka_producer.send(new_topic, value = data)


if __name__ == '__main__':
	if len(sys.argv) != 4:
		print ('Usage: spark_stream [topic] [new_topic] [kafka-broker]')
		exit(1)

	topic, new_topic, kafka_broker = sys.argv[1:]

	# setup connection to spark cluster
	sc = SparkContext("local[2]", "StockAveragePrice")
	ssc = StreamingContext(sc, 5)

	# create a data stream from spark
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_broker})

	# for each RDD, work
	directKafkaStream.foreachRDD(process)

	# instantiate kafka producer
	kafka_producer = KafkaProducer(bootstrap_servers = kafka_broker)

	# setup proper shutdown hook
	atexit.register(shutdown_hook, kafka_producer)

	ssc.start()
	ssc.awaitTermination()