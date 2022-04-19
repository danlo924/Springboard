import os
from PySpark_Parser import final_schema
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc

class DataLoader:
    def __init__(self, spark):
        self.spark = spark

    #### COMMON FILE PROCESSING FUNCTION FOR LIST ALL FILES IN A FOLDER PATH####
    def process_files(self, folder_path, parser_func, spark):
        for path, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".txt"):
                    file_path = os.path.join(path, file)  
                    self.parse_file(file_path, parser_func, spark)        

    ## PARSE FILE USING parser_func AND spark 
    def parse_file(self, file_path, parser_func, spark):
        print("Processing File: " + file_path)
        raw = spark.sparkContext.textFile(file_path)
        parsed = raw.map(lambda line: parser_func(line))
        data = spark.createDataFrame(parsed, schema=final_schema)
        data.write.partitionBy("partition").mode("append").parquet("output_dir")        

    ### get latest trade / quote info for each set of partition_columns
    def applyLatest(self, data, partition_columns, order_by_col):
        w = Window.partitionBy(*partition_columns).orderBy(desc(order_by_col))
        return data.withColumn("row_num", row_number().over(w)).filter("row_num = 1")        
