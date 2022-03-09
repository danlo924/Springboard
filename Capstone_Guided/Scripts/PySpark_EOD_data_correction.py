from secrets import secrets
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, desc
from notebookutils import mssparkutils

#### CREATE SPARK SESSION ####       
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set(
    "fs.azure.account.key.azblobstoragedwilde.blob.core.windows.net",
    secrets.get('azblobkey')
    )

### get latest trade / quote info for each set of partition_columns
def applyLatest(data, partition_columns, order_by_col):
    w = Window.partitionBy(*partition_columns).orderBy(desc(order_by_col))
    return data.withColumn("row_num", row_number().over(w)).filter("row_num = 1")

### read parquet files from azure blob storage
trade_common = spark.read.parquet("output_dir/partition=T")
quote_common = spark.read.parquet("output_dir/partition=Q")

### get relevant fields from trade and quote dataframes
trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "trade_pr")
quote = quote_common.select("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

### define fields for uniquely identifying records and sorting
partition_columns = ["trade_dt","symbol","exchange","event_tm","event_seq_nb"]
order_by_col = "arrival_tm"

### get latest / corrected data for each data frame using the latest "arrival_tm"
trade_corrected = applyLatest(trade, partition_columns, order_by_col)
quote_corrected = applyLatest(quote, partition_columns, order_by_col)

### write corrected data back to azure blob storage
trade_corrected.write.partitionBy("trade_dt").parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/trade".format("trade_dt"), mode="overwrite")
quote_corrected.write.partitionBy("trade_dt").parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/quote".format("trade_dt"), mode="overwrite")

