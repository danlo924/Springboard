from pyspark.sql.types import DateType, DecimalType, IntegerType, StructType, StructField, StringType, TimestampType
from secrets import secrets

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
