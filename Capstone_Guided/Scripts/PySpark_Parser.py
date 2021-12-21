import json
from datetime import datetime
from decimal import Decimal
from secrets import secrets
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, IntegerType, StructType, StructField, StringType, TimestampType
from notebookutils import mssparkutils

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

#### RECURSE AZURE BLOB STORAGE PATH AND LIST ALL FILES ####
def get_files_recursive(path: str, folder_depth: int):
    all_files = mssparkutils.fs.ls(path)
    for f in all_files:
        if f.size != 0:
            yield f
    if folder_depth > 1:
        for f in all_files:
            if f.size != 0:
                continue
            for p in get_files_recursive(f.path, folder_depth - 1):
                yield p
    else:
        for f in all_files:
            if f.size == 0:
                yield f

#### COMMON FILE PROCESSING FUNCTION ####
def process_files(azure_container_path, parser_func):
    all_files = get_files_recursive(azure_container_path, folder_depth=10)
    for file in all_files:
        if file.path.endswith(".txt"):
            print("Processing File: " + file.path)
            raw = spark.sparkContext.textFile(file.path)
            parsed = raw.map(lambda line: parser_func(line))
            data = spark.createDataFrame(parsed, schema=final_schema)
            data.write.partitionBy("partition").mode("append").parquet("output_dir")


#### CREATE SPARK SESSION ####       
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set(
    "fs.azure.account.key.azblobstoragedwilde.blob.core.windows.net",
    secrets.get('azblobkey')
    )

#### PROCESS ALL CSV FILES IN THE data/csv DIRECTORY ####
process_files("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/data/csv", parse_csv)

#### PROCESS ALL CSV FILES IN THE data/json DIRECTORY ####
process_files("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/data/json", parse_json)


 