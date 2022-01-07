from secrets import secrets
from pyspark.sql import SparkSession

#### GLOBAL VARIABLES
make, year = None, None

def process_file(file):
    """Process a file by first propagating vehicle make and model year to all accident records (incident_type='A')
    based off of VIN number from initial sales (incident_type='I') and then sum all accident records grouping by 
    vehicle make and model year"""
    print("Processing File: " + file)
    raw = spark.sparkContext.textFile(file)
    vin_kv = raw.map(lambda x: extract_vin_key_value(x))
    enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
    make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
    final_results = make_kv.reduceByKey(lambda x, y: x + y).collect()
    for result in final_results:
        print(str(result[0]) + "," + str(result[1]))

def extract_vin_key_value(line):
    """Create a key / value tuple with VIN and vehicle info"""
    line = line.strip()
    line = line.split(",")
    vin = line[2]
    vals = (line[2],line[1],line[3],line[5]) #(vin, inc_type, make, year)
    return (vin, vals)

def populate_make(values):
    """Propagate vehicle info to all accident records (incident_type='A)"""
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
    """Create Key/Value tuples with Make-Year and Count of 1"""
    return (x[2] + "-" + x[3], 1)

#### CREATE SPARK SESSION ####       
spark = SparkSession.builder.master('local').appName('My Application').getOrCreate()
spark.conf.set(
    "fs.azure.account.key.azblobstoragedwilde.blob.core.windows.net",
    secrets.get('azblobkey')
    )

#### PROCESS CSV FILE IN THE SparkMiniProject DIRECTORY ####
process_file("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/SparkMiniProject/data.csv")


 