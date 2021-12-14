import json
from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import DateType, DecimalType, IntegerType, StructType, StructField, StringType, TimestampType
from secrets import secrets
# import common as c

#Create Schema
final_schema = StructType([
    StructField('trade_dt', DateType(), True),
    StructField('rec_type', StringType(), True),
    StructField('symbol', StringType(), True),
    StructField('exchange', StringType(), True),
    StructField('event_tm', TimestampType(), True),
    StructField('event_seq_nb', IntegerType(), True),
    StructField('arrival_tm', TimestampType(), True),
    StructField('trade_pr', DecimalType(30,15), True),
    StructField('trade_size', IntegerType(), True),
    StructField('bid_pr', DecimalType(30,15), True),
    StructField('bid_size', IntegerType(), True),
    StructField('ask_pr', DecimalType(30,15), True),
    StructField('ask_size', IntegerType(), True),
    StructField('partition', StringType(), True),
  ])

def common_event(
    trade_dt: DateType, 
    rec_type: StringType, 
    symbol: StringType, 
    exchange: StringType, 
    event_tm: TimestampType,
    event_seq_nb: IntegerType,
    arrival_tm: TimestampType,
    trade_pr: DecimalType(30,15),
    trade_size: IntegerType,
    bid_pr: DecimalType(30,15),
    bid_size: IntegerType,
    ask_pr: DecimalType(30,15),
    ask_size: IntegerType,
    partition: StringType,
    line: StringType):
    
    if partition == "B":
        # return line
        pass
    else:
        return [trade_dt, rec_type, symbol, exchange, 
                event_tm, event_seq_nb, arrival_tm, 
                trade_pr, trade_size, bid_pr, bid_size, ask_pr, ask_size, partition]

def parse_json(line:str):
    record = json.loads(line)
    record_type = record["event_type"]
    try:
        # [logic to parse records]
        if record_type == "T":
            event = common_event(datetime.strptime(record["trade_dt"], "%Y-%m-%d"), record["event_type"], record["symbol"], record["exchange"], 
                datetime.strptime(record["event_tm"], '%Y-%m-%d %H:%M:%S.%f'), int(record["event_seq_nb"]), datetime.strptime(record["file_tm"], '%Y-%m-%d %H:%M:%S.%f'), 
                Decimal(record["price"]), int(record["size"]), None, None, None, None, "T", None)
        elif record_type == "Q":
            event = common_event(datetime.strptime(record["trade_dt"], "%Y-%m-%d"), record["event_type"], record["symbol"], record["exchange"], 
                datetime.strptime(record["event_tm"], '%Y-%m-%d %H:%M:%S.%f'), int(record["event_seq_nb"]), datetime.strptime(record["file_tm"], '%Y-%m-%d %H:%M:%S.%f'), 
                None, None, Decimal(record["bid_pr"]), int(record["bid_size"]), Decimal(record["ask_pr"]), int(record["ask_size"]), "Q", None)
            return event
    except Exception as e:
        return common_event(None, None, None, None, None, None, None, None, None, None, None, None, None, "B", line)
        

###############

from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set(
    "fs.azure.account.key.azblobstoragedwilde.blob.core.windows.net",
    secrets.get('azblobkey')
    )
raw_json = spark.sparkContext.textFile("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/data/json/2020-08-05/NASDAQ/part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.txt")
parsed_rdd_json = raw_json.map(lambda line: parse_json(line))
data_json = spark.createDataFrame(parsed_rdd_json, schema=final_schema)
data_json.printSchema()
data_json.show(10)

