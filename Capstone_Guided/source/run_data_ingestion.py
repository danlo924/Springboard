import logging
from secrets import secrets
from datetime import datetime
from DataLoader import DataLoader
from pyspark.sql import SparkSession
from PySpark_Parser import parse_csv, parse_json

logging.basicConfig(
    filename=datetime.now().strftime('ignore/logs/run_data_ingestion_%Y_%m_%d_%H_%M_%S.log'),
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG
)

def log_message(msg, show=False):
    '''logs msg parameter to file; optionally, show/print messages to terminal'''
    logging.debug(msg)
    if show==True:
        print(msg)

def run_data_ingestion():
    log_message("running data ingestion now", show=True)
    try:
        #### CREATE SPARK SESSION ####       
        spark = SparkSession.builder.master('local').appName('app').getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        dataloader = DataLoader(spark)

        #### PROCESS ALL CSV FILES IN THE data/csv DIRECTORY ####
        dataloader.process_files(secrets.get('csv_path'), parse_csv, spark)

        #### PROCESS ALL JSON FILES IN THE data/json DIRECTORY ####
        dataloader.process_files(secrets.get('json_path'), parse_json, spark)

        #### EOD Load ####
        ## read parquet files from azure blob storage
        trade_common = spark.read.parquet("output_dir/partition=T")
        quote_common = spark.read.parquet("output_dir/partition=Q")

        ## get relevant fields from trade and quote dataframes
        trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")
        quote = quote_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

        ## define fields for uniquely identifying records and sorting
        partition_columns = ["trade_dt","symbol","exchange","event_tm","event_seq_nb"]
        order_by_col = "arrival_tm"

        ### get latest / corrected data for each data frame using the latest "arrival_tm"
        trade_corrected = dataloader.applyLatest(trade, partition_columns, order_by_col)
        quote_corrected = dataloader.applyLatest(quote, partition_columns, order_by_col)

        ### write corrected data back to storage
        trade_corrected.write.partitionBy("trade_dt").parquet("output_dir/eod/trade".format("trade_dt"), mode="overwrite")
        quote_corrected.write.partitionBy("trade_dt").parquet("output_dir/eod/quote".format("trade_dt"), mode="overwrite")

        log_message("success", show=True)
    
    except Exception as e:
        log_message("failed" + e, show=True)

    return    

if __name__ == "__main__":
    run_data_ingestion()