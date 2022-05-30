import logging
from datetime import datetime
from pyspark.sql import SparkSession

class Analyzer:
    def __init__(self, spark):
        self.spark = spark

    def analyze(self, spark, divs_and_prices):
        """
        For each Ticker, get average return vs S&P return for each combination of 1-42 days (6 weeks) Before the Ex-Dividend date,
            and 0-42 days (6 weeks) After the Ex-Dividend Date
        """
        divs_and_prices.createOrReplaceTempView("all_data")

        div_returns = spark.sql("""
            SELECT 
                b.Ticker,
                b.FreqType, 
                b.ExDivDate,
                b.ExDivDays AS b_ExDivDays,
                a.ExDivDays AS a_ExDivDays,
                b.AvgPrice AS b_AvgPrice,
                a.AvgPrice AS a_AvgPrice,
                b.AdjAmount AS Dividend,
                b.SandPAvgPrice AS b_SandPAvgPrice,
                a.SandPAvgPrice AS a_SandPAvgPrice,
                ((a.AvgPrice + a.AdjAmount) - b.AvgPrice) / b.AvgPrice AS TickerReturn,
                (a.SandPAvgPrice - b.SandPAvgPrice) / b.SandPAvgPrice AS SandPReturn
            FROM all_data b
                INNER JOIN all_data a ON b.Ticker = a.Ticker
                    AND b.ExDivDate = a.ExDivDate
            WHERE b.ExDivDays BETWEEN -42 AND -1 
               AND a.ExDivDays BETWEEN 0 AND 42
            """)

        ## Store Results in Temp div_returns View
        div_returns.createOrReplaceTempView("div_returns")

        ## Get Average Return for each Ticker vs the S&P Index Return for "b" days before the Ex-Div Date and "a" days after
        avg_by_ticker_and_days = spark.sql("""
            SELECT Ticker, 
                b_ExDivDays AS BeforeDays, 
                a_ExDivDays AS AfterDays,
                a_ExDivDays - b_ExDivDays as TotalDaysHeld,
                COUNT(*) as NumExDivDates,
                AVG(TickerReturn - SandPReturn) AS ReturnVsSandP
            FROM div_returns
            GROUP BY Ticker, b_ExDivDays, a_ExDivDays
            ORDER BY ReturnVsSandP DESC
            """)
                
        avg_by_ticker_and_days.write.partitionBy("Ticker").parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/returns".format("Ticker"), mode="overwrite") 

        avg_all_by_days = spark.sql("""
            SELECT '_ALL_DAYS' AS Ticker, 
                b_ExDivDays AS BeforeDays, 
                a_ExDivDays AS AfterDays,
                a_ExDivDays - b_ExDivDays as TotalDaysHeld,
                COUNT(*) as NumExDivDates,
                AVG(TickerReturn - SandPReturn) AS ReturnVsSandP
            FROM div_returns
            GROUP BY b_ExDivDays, a_ExDivDays            
            ORDER BY ReturnVsSandP DESC
            """)   

        avg_all_by_days.write.partitionBy("Ticker").parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/returns".format("Ticker"), mode="append") 

        avg_all_by_weeks = spark.sql("""
            SELECT Ticker,
                BeforeWeeks,
                AfterWeeks,
                TotalWeeksHeld,
                COUNT(*) as NumExDivDates,
                AVG(ReturnVsSandP) AS ReturnVsSandP
            FROM
            (
                SELECT '_ALL_WEEKS' AS Ticker, 
                    FLOOR(b_ExDivDays / 7) AS BeforeWeeks, 
                    FLOOR(a_ExDivDays / 7) AS AfterWeeks,
                    FLOOR(a_ExDivDays / 7) - FLOOR(b_ExDivDays / 7) AS TotalWeeksHeld,
                    (TickerReturn - SandPReturn) AS ReturnVsSandP
                FROM div_returns
            ) w
            GROUP BY Ticker, BeforeWeeks, AfterWeeks, TotalWeeksHeld           
            ORDER BY ReturnVsSandP DESC
            """)   

        avg_all_by_weeks.write.partitionBy("Ticker").parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/returns".format("Ticker"), mode="append")         


logging.basicConfig(
    filename=datetime.now().strftime('ignore/logs/run_analyzer_%Y_%m_%d_%H_%M_%S.log'),
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

def run_return_by_days_analysis():
    '''
    Analyzer:
    1. If price history exists days_before and days_after and S&P hitory exists then
        evaluate buying 42-1 days before and selling 0-42 days after and get optimal Buy and Sell CAGR vs S&P,
        otherwide pass and give reason for passing this ex-div date
    2. Aggregate all dividend dates for each ticker to get an average best buy / sell dates per ticker
    3. Output final results to parquet files
    '''
    log_message("running analyzer now", show=True)
    try:

        ## CREATE SPARK SESSION ##       
        spark = SparkSession.builder.master('local').appName('app').getOrCreate()
        spark.conf.set(
            "fs.azure.account.key.azblobstoragedwilde.blob.core.windows.net",
            "0ZxdhT9CBrrHGmBKo6tJ+YNEI6XSX94hSqwcN6NfuNm1gzGof8BQmfgGppQ84IfA+9GFFTntSJ5owSXDsDDNyw=="
            )

        analyzer = Analyzer(spark)

        ## READ ALL PARQUET FILES ##
        divs_and_prices = spark.read.parquet("wasbs://mycontainer@azblobstoragedwilde.blob.core.windows.net/divs_and_prices")

        analyzer.analyze(spark, divs_and_prices)

        log_message("success", show=True)
    
    except Exception as e:
        log_message("failed" + e, show=True)

    return    

if __name__ == "__main__":
    run_return_by_days_analysis()