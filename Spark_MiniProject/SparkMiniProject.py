from secrets import secrets
from pyspark.sql import SparkSession

#### COMMON FILE PROCESSING FUNCTION ####
def process_file(file):
    print("Processing File: " + file)
    make, year = None, None
    raw = spark.sparkContext.textFile(file)
    vin_kv = raw.map(lambda x: extract_vin_key_value(x))
    enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
    make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
    final_results = make_kv.reduceByKey(lambda x, y: x + y).collect()
    for result in final_results:
        print(str(result[0]) + "," + str(result[1]))

def extract_vin_key_value(line):
    line = line.strip()
    line = line.split(",")
    vin = line[2]
    vals = (line[2],line[1],line[3],line[5]) #(vin, inc_type, make, year)
    return (vin, vals)

def populate_make(values):
    output = []
    for mkyr in values:
        if mkyr[1]=='I':
            make = mkyr[2]
            year = mkyr[3]
    for acc in values:
        if acc[1] == 'A':
            output.append((acc[0], acc[1], make, year))
    return output

def extract_make_key_value(x):
    return (x[2] + "-" + x[3], 1)

#### CREATE SPARK SESSION ####       
spark = SparkSession.builder.master('local').appName('My Application').getOrCreate()
spark.conf.set(
    "fs.azure.account.key.azblobstoragedwilde.blob.core.windows.net",
    secrets.get('azblobkey')
    )

#### PROCESS CSV FILE IN THE SparkMiniProject DIRECTORY ####
process_file("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/SparkMiniProject/data.csv")


 