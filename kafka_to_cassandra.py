# Need to read from kafka, topic

from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import argparse
import logging
import json


topic_name = 'stock-analyzer'
kafka_broker = '127.0.0.1:9002'
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-producer')

logger.setLevel(logging.DEBUG)

topic_name = 'stock_analyzer'
kafka_broker = '127.0.0.1:9092'
keyspace = 'stock'
data_table = ''
cassandra_broker = '127.0.0.1:9042'


def persist_data(stock_data, cassandra_session):
	"""

	@param cassandra_session, a session created using cassandra-driver
	@param stock_data, [{'symbol', ''}]
	"""
	logger.debug('Start to persist data to cassandra %s', stock_data)
	parsed = json.loads(stock_data)[0]
	symbol = parsed.get('StockSymbol')
	price = float(parsed.get('LastTradePrice'))
	tradetime = parsed.get('LastTradeDateTime')
	statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES (%s,%s, %f)" % (data_table)
	cassandra_session.execute(statement)
	logger.info('Persisted data into cassandra for symbol: %s, price %f, tradetime %s' %(symbol, price, ))


def shutdown_hook(consumer, session):
	consumer.close()
	logger.info('Kafka consumer closed')
	session.shutdown()
	logger.info('Cassandra ')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help = 'the symbol of the stock')
    parser.add_argument('topic_name', help = 'the kafka topic')
    parser.add_argument('kafka_broker', help = 'the location of kafka broker')
    parser.add_argument('keyspace', help = 'the keyspace to be used in cassandra')
    # assume cassandra_broker is '127.0.0.1, 127.0.0.2'
    parser.add_argument('cassandra_broker', help = 'the location of cassandra cluster')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    keyspace = args.keyspace
    cassandra_broker = args.cassandra_broker

    # setup a kafka consumer
    consumer = KafkaConsumer(topic_name, bootstrap_server = kafka_broker)

    # setup a cassandra session
    cassandra_cluster = Cluster(contct_points = cassandra_broker.split(','))
    session = cassandra_cluster.connect(keyspace)

    for msg in consumer:
    	persist_data()


