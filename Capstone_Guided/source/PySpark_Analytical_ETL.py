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

### read trade parquet files from azure blob storage
trades_all = spark.read.parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/trade")
trades_all.createOrReplaceTempView("trades")
trades = spark.sql("select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr from trades")
trades.createOrReplaceTempView("tmp_trade_moving_avg")

### create moving average price column and write to "temp_trade_moving_avg" table
mov_avg_df = spark.sql("""
SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr, 
    AVG(trade_pr) OVER (PARTITION BY symbol ORDER BY CAST(event_tm AS timestamp) 
        RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING and CURRENT ROW) as mov_avg_pr
FROM tmp_trade_moving_avg
""")

### write output to "temp_trade_moving_avg" table
mov_avg_df.write.mode("overwrite").saveAsTable("temp_trade_moving_avg")

############################################

from datetime import datetime, timedelta
date = datetime.strptime('2020-08-06', '%Y-%m-%d')
prev_date_str = date - timedelta(1)

### get previous days trades from temp "trades" view
trades_prev = spark.sql("""SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr FROM trades WHERE trade_dt = '{}'""".format(prev_date_str))
trades_prev.createOrReplaceTempView("tmp_last_trade")

### get last trade by symbol / exchange from previous days trades in "tmp_last_trade" view, and save to "temp_last_trade" table
last_pr_df = spark.sql("""
    select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr, RowNum
    from 
    (  
        select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr,
            ROW_NUMBER() OVER (PARTITION BY symbol, exchange ORDER BY event_tm DESC) AS RowNum
        FROM tmp_last_trade
    ) a
    WHERE RowNum = 1
    ORDER BY symbol, exchange
""")

### write output to "temp_last_trade" table
last_pr_df.write.mode("overwrite").saveAsTable("temp_last_trade")

############################################

### read quote parquet files from azure blob storage
quotes_all = spark.read.parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/quote")
quotes_all.createOrReplaceTempView("quotes")

### create union table with all trade and quote info sorted by event_tm and save as "quote_union" view
quote_union = spark.sql("""
    SELECT trade_dt, 'T' as rec_type, symbol, event_tm, event_seq_nb, exchange, NULL as bid_pr, NULL as bid_size, NULL as ask_pr, NULL as ask_size, trade_pr, mov_avg_pr 
    FROM temp_trade_moving_avg
    UNION
    SELECT trade_dt, 'Q' as rec_type, symbol, event_tm, event_seq_nb, exchange, bid_pr, bid_size, ask_pr, ask_size, NULL as trade_pr, NULL as mov_avg_pr 
    FROM quotes
    """)
quote_union.createOrReplaceTempView("quote_union")

### get latest trade price and moving avg price onto the quote records, and save as "quote_union_update" view
quote_union_update = spark.sql("""
    SELECT *,
        LAST_VALUE(trade_pr, true) OVER(PARTITION BY symbol ORDER BY event_tm ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_trade_pr,
        LAST_VALUE(mov_avg_pr, true) OVER(PARTITION BY symbol ORDER BY event_tm ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_mov_avg_pr
    FROM quote_union
    ORDER BY symbol, event_tm ASC
    """)
quote_union_update.createOrReplaceTempView("quote_union_update")

### filter to only show quote records
quote_update = spark.sql("""
    SELECT trade_dt, symbol, event_tm, event_seq_nb, exchange, bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
    FROM quote_union_update
    WHERE rec_type = 'Q'
    """)
quote_update.createOrReplaceTempView("quote_update")

### join "quote_update" table with "temp_last_trade" using BROADCAST to get "quote_final" table
quote_final = spark.sql("""
    SELECT trade_dt, symbol, event_tm, event_seq_nb, exchange, bid_pr, bid_size, ask_pr, ask_size, 
        last_trade_pr, last_mov_avg_pr,
        (bid_pr - close_pr) as bid_pr_mv, 
        (ask_pr - close_pr) as ask_pr_mv
    FROM 
    (
        SELECT /*+ BROADCAST(temp_last_trade) */ q.trade_dt, q.symbol, q.event_tm, q.event_seq_nb, q.exchange, q.bid_pr, q.bid_size, q.ask_pr, q.ask_size, 
            q.last_trade_pr, q.last_mov_avg_pr,
            t.trade_pr as close_pr 
        FROM quote_update q
            LEFT OUTER JOIN temp_last_trade t ON q.symbol = t.symbol
                AND q.exchange = t.exchange
    ) a
    """)

### persist final dataframe back to azure blob storage
quote_final.write.partitionBy("trade_dt").parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/quote-trade-analytical".format("trade_dt"), mode="overwrite")


