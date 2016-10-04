# Stock in big data framework

## Project Description
This project acquires the live stock data from Google finance python API, and push it into Kafka. From Kafka we consume the data and send to Cassandra.

From Kafka the data could also be sent to Redis which has better performance with fast changing data. 

With Spark streaming stock data could be fast processed before sending to Kafka.

## Source Data example:
```js
[{
"Index": "NASDAQ",
"LastTradeWithCurrency": "66.19",
"LastTradeDateTime": "2016-10-03T16:00:01Z",
"LastTradePrice": "66.19",
"Yield": "1.66",
"LastTradeTime": "4:00PM EDT",
"LastTradeDateTimeLong": "Oct 3, 4:00PM EDT",
"Dividend": "0.28",
"StockSymbol": "CMCSA",
"ID": "131136"
}]
```

## Project Dependency
Docker-machine, Docker

### Easy way to install dependencies
'''sh
pip install -r requirements.txt
```

### Dependency for kafka_data_ingestion.py 
googlefinance, kafka-python, schedule

### Dependency for kafka_to_cassandra.py
cassandra-driver

### Dependency for spark_stream.py
pyspark, kafka-python

