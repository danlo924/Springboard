import logging
from secrets import secrets
from Reporter import Reporter
from datetime import datetime
from pyspark.sql import SparkSession

logging.basicConfig(
    filename=datetime.now().strftime('ignore/logs/run_reporter_%Y_%m_%d_%H_%M_%S.log'),
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
    log_message("running reporter now", show=True)
    try:
        ## GET ETL DATE
        trade_date = secrets.get('etl_date')
        dir = secrets.get('eod_dir')

        #### CREATE SPARK SESSION ####       
        spark = SparkSession.builder.master('local').appName('app').getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        reporter = Reporter(spark)
        reporter.report(spark, trade_date, dir)

        log_message("success", show=True)
    
    except Exception as e:
        log_message("failed" + e, show=True)

    return    

if __name__ == "__main__":
    run_data_ingestion()