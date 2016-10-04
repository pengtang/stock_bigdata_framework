# Kafka Producer to grab stock data from Google finance python api, running via docker

# run example: python kafka_data_ingestion.py CMCSA stock_analyzer 192.168.99.100:9092
# @param $1 - "symbol of the stock", $2 - "kafka topic name", $3 "docker machine ip : kafka port"

from googlefinance import getQuotes
from kafka import KafkaProducer
from kafka.errors import KafkaError

import argparse
import atexit
import json
import time
import logging
import schedule


def fetch_price(producer, symbol):
    """
    helper function to get stock data and send to kafka
    @param producer - instance of a kafka producer
    @param symbol - symbol of the stock, string type
    @return None
    """
    logger.debug('Start to fetch stock price for %s', symbol)
    try:
        price = json.dumps(getQuotes(symbol))
        logger.debug('Get stock info %s', price)
        producer.send(topic = topic_name, value = price, timestamp_ms = time.time())
        logger.debug('Sent stock price for %s to kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error))
    except Exception:
        logger. warn('Fail to get stock price for %s', symbol)

def shutdown_hook(producer):
    try:
        producer.flush(10)
    except KafkaError as KafkaError:
        logger.warn('Failed to flush pending messages to kafka')
    finally:
        try:
            producer.close()
        except Exception as e:
            logger.warn('Fail to close kafka connection')



# setup default parameters
topic_name = 'stock_analyzer'
kafka_broker = '127.0.0.1:9002'
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-producer')

logger.setLevel(logging.DEBUG)

# Trace DEBUG INFO WARNING ERROR


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help = 'the symbol of the stock')
    parser.add_argument('topic_name', help = 'the kafka topic')
    parser.add_argument('kafka_broker', help = 'the location of kafka broker')

    # - parse argument
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # initiate a kafka producer
    producer = KafkaProducer(
        bootstrap_servers = kafka_broker
    )

    schedule.every(1).second.do(fetch_price, producer, symbol)

    atexit.register(shutdown_hook, producer)

    # setup proper shutdown_hook
    while True:
        schedule.run_pending()
        time.sleep(1)
