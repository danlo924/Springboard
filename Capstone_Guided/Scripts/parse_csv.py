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


def parse_csv(line:str):
    record_type_pos = 2
    record = line.split(",")
    try:
        # [logic to parse records]
        if record[record_type_pos] == "T":
            event = common_event(datetime.strptime(record[0], "%Y-%m-%d"), record[2], record[3], record[6], 
                datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'), int(record[5]), datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'), 
                Decimal(record[7]), int(record[8]), None, None, None, None, "T", None)
            return event
        elif record[record_type_pos] == "Q":
            event = common_event(datetime.strptime(record[0], "%Y-%m-%d"), record[2], record[3], record[6], 
                datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'), int(record[5]), datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'), 
                None, None, Decimal(record[7]), int(record[8]), Decimal(record[9]), int(record[10]), "Q", None)
            return event
    except Exception as e:
        # [save record to dummy event in bad partition]
        return common_event(None, None, None, None, None, None, None, None, None, None, None, None, None, "B", line)

###############

from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set(
    "fs.azure.account.key.azblobstoragedwilde.blob.core.windows.net",
    secrets.get('azblobkey')
    )
raw_csv = spark.sparkContext.textFile("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/data/csv/2020-08-05/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
parsed_rdd_csv = raw_csv.map(lambda line: parse_csv(line))
data_csv = spark.createDataFrame(parsed_rdd_csv, schema=final_schema)
data_csv.printSchema()
data_csv.show(truncate=False)
