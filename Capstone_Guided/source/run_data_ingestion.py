from secrets import secrets
from pyspark.sql import SparkSession
# import PySpark_Parser
from PySpark_Parser import DataLoader, parse_csv, parse_json
# import PySpark_EOD_data_correction

def run_data_ingestion():

    print('running data ingestion now')

    #### CREATE SPARK SESSION ####       
    spark = SparkSession.builder.master('local').appName('app').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dataloader = DataLoader(spark)

    #### PROCESS ALL CSV FILES IN THE data/csv DIRECTORY ####
    dataloader.process_files(secrets.get('csv_path'), parse_csv, spark)

    # #### PROCESS ALL CSV FILES IN THE data/json DIRECTORY ####
    # # dataloader.process_files(secrets.get('json_path'), parse_json, spark)

if __name__ == "__main__":
    run_data_ingestion()