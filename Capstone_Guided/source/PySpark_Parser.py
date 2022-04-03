import os
import json
from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import DateType, DecimalType, IntegerType, StructType, StructField, StringType, TimestampType

#### Create Final Output Schema ####
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

#### COMMON FUNCTION FOR RETURNING DATA IN SAME FORMAT ####
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
        return line     #### put bad lines into their own file
    else:
        return [trade_dt, rec_type, symbol, exchange, 
                event_tm, event_seq_nb, arrival_tm, 
                trade_pr, trade_size, bid_pr, bid_size, ask_pr, ask_size, partition]

#### FUNCTION FOR PARSING ALL .CSV FILES ####
def parse_csv(line:str):
    record_type_pos = 2
    record = line.split(",")
    try:
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
        return common_event(None, None, None, None, None, None, None, None, None, None, None, None, None, "B", line)  

#### FUNCTION FOR PARSING ALL .JSON FILES ####
def parse_json(line:str):
    record = json.loads(line)
    record_type = record["event_type"]
    try:
        if record_type == "T":
            event = common_event(datetime.strptime(record["trade_dt"], "%Y-%m-%d"), record["event_type"], record["symbol"], record["exchange"], 
                datetime.strptime(record["event_tm"], '%Y-%m-%d %H:%M:%S.%f'), int(record["event_seq_nb"]), datetime.strptime(record["file_tm"], '%Y-%m-%d %H:%M:%S.%f'), 
                Decimal(record["price"]), int(record["size"]), None, None, None, None, "T", None)
            return event
        elif record_type == "Q":
            event = common_event(datetime.strptime(record["trade_dt"], "%Y-%m-%d"), record["event_type"], record["symbol"], record["exchange"], 
                datetime.strptime(record["event_tm"], '%Y-%m-%d %H:%M:%S.%f'), int(record["event_seq_nb"]), datetime.strptime(record["file_tm"], '%Y-%m-%d %H:%M:%S.%f'), 
                None, None, Decimal(record["bid_pr"]), int(record["bid_size"]), Decimal(record["ask_pr"]), int(record["ask_size"]), "Q", None)
            return event
    except Exception as e:
        return common_event(None, None, None, None, None, None, None, None, None, None, None, None, None, "B", line) 

class DataLoader:
    def __init__(self, spark):
        self.spark = spark

    #### COMMON FILE PROCESSING FUNCTION ####
    def process_files(self, folder_path, parser_func, spark):
        for path, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".txt"):
                    file_path = os.path.join(path, file)
                    print("Processing File: " + file_path)
                    raw = spark.sparkContext.textFile(file_path)
                    parsed = raw.map(lambda line: parser_func(line))
                    data = spark.createDataFrame(parsed, schema=final_schema)
                    data.write.partitionBy("partition").mode("append").parquet("output_dir")



 