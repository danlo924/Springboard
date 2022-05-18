import ssl
import json
import requests
import sqls as s
import pandas as pd
import yfinance as yf
import log_helpers as l
import urllib.request as req
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StructType, StructField, StringType, TimestampType
from sqlalchemy import create_engine

# PIPELINE CLASS DEFINITION
class Pipeline:
    '''create a data pipeline with DB connection, urls, and other connection info'''
    def __init__(self, db_url: str, companies_data_url: str, div_data_url: str, sa_api_key: str):
        self.db_url = db_url
        self.companies_data_url = companies_data_url
        self.div_data_url = div_data_url
        self.sa_api_key = sa_api_key
        self.create_db_conn()
        self.log_pipeline_details()

    def log_pipeline_details(self):
        l.log_message(
            '\nPipeline Run Details (' + str(datetime.now()) + '):'
            + '\nDatabase connection info: ' + self.db_url  
            + '\nCompanies data url: ' + self.companies_data_url 
            + '\nDividend data API url: ' + self.div_data_url
            + '\nSeeking Aplha API key: ' + self.sa_api_key
            )

    def create_db_conn(self):
        '''creates DB engine and DB conneciton for runing queries and executing procedures'''
        try:
            self.db_engine = create_engine(self.db_url)
            self.db_conn = self.db_engine.raw_connection()
        except Exception as error:
            l.log_message(error, show=True)

    def load_df_to_stg(self, df: pd.DataFrame, stg_tbl_name: str):
        '''loads a dataframe directly to a staging table in the DB, using stg_tbl_name parameter for the name of the table'''
        try:
            df.to_sql(con=self.db_engine, name=stg_tbl_name, if_exists='replace')
        except Exception as error:
            l.log_message(error, show=True)           

    def call_proc(self, proc_name: str, args=None):
        '''executes a proc in the db; args are optional'''
        try:
            cursor = self.db_conn.cursor()
            if args==None:
                cursor.callproc(proc_name)
            else:
                cursor.callproc(proc_name, args)
            results = list(cursor.fetchall())
            cursor.close()
            self.db_conn.commit()
        except Exception as error:
            l.log_message(error, show=True)
        return results

    def get_sandp_companies_list(self):
        '''gets the current and historical list of S&P companies from wikipedia and updates the companies table in the DB'''
        try:
            context = ssl._create_unverified_context()
            response = req.urlopen(url=self.companies_data_url, context=context)
            html = response.read()
            sandp_companies = pd.read_html(html)
            sp_curr = sandp_companies[0]
            l.log_df_details('S&P Current Company List', sp_curr)
            self.load_df_to_stg(sp_curr, '_stg_sp_current')
            sp_hist = sandp_companies[1]
            l.log_df_details('S&P Historical Company List', sp_hist)
            self.load_df_to_stg(sp_hist, '_stg_sp_history')
            self.call_proc('sp_refresh_companies')   
        except Exception as error:
            l.log_message(error, show=True)         

    def get_ticker_list(self, sql: str):
        '''generates a dataframe with list of tickers using the sql statement run on the DB'''
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql)
            df_tickers = pd.DataFrame(cursor.fetchall(), columns=['Ticker'])
            cursor.close()
            self.db_conn.commit()     
        except Exception as error:
            l.log_message(error, show=True) 
        l.log_df_details('Tickers', df_tickers)
        return df_tickers        

    def refresh_prices(self, max_tickers: int, lookback_window: int):
        '''
        calls the routine to refresh the prices table for (max_tickers) number of companies,
        by quering the DB for companies that have no price history for the past (lookback_window) number of days
        '''
        try:
            sql = s.get_price_refresh_sql(max_tickers, lookback_window)
            l.log_message('List of Tickers for Price Refresh:')
            tickers = self.get_ticker_list(sql)        
            for _, row in tickers.iterrows():
                ticker = str(row['Ticker'])
                l.log_message('Get the price history for: ' + ticker, show=True)
                yf_ticker = yf.Ticker(ticker)
                df_price_hist = yf_ticker.history(period='40y')
                if df_price_hist.empty == False:
                    l.log_df_details(ticker + ' Price History', df_price_hist)
                    self.load_df_to_stg(df_price_hist, '_stg_price_hist') 
                    df_div = pd.DataFrame(yf_ticker.dividends)
                    if df_div.empty == False:
                        l.log_df_details(ticker + ' Dividends', df_div)
                        self.load_df_to_stg(df_div, '_stg_has_div') # for updating company.HasDividend column
                    self.call_proc('sp_refresh_prices_company_data', args=[ticker,])
            l.log_message('Prices refreshed successfully', show=True)
        except Exception as error:
            l.log_message(error, show=True) 

    def refresh_dividends(self, max_tickers: int, lookback_window: int):
        '''
        calls the routine to refresh the dividends table for (max_tickers) number of companies,
        by quering the DB for companies that have no dividend history for the past (lookback_window) number of days
        based on dividends.ExDivDate
        '''        
        try:
            sql = s.get_div_refresh_sql(max_tickers, lookback_window)
            l.log_message('List of Tickers for Dividend Refresh:')
            tickers = self.get_ticker_list(sql)  
            querystring = {
                'symbol':'<ticker>',
                'years':'40',
                'group_by':'month'
            }
            headers = {
                'x-rapidapi-host': 'seeking-alpha.p.rapidapi.com',
                'x-rapidapi-key': self.sa_api_key
            }
            for _, row in tickers.iterrows():
                ticker = str(row['Ticker'])
                l.log_message('Get the dividend history for: ' + ticker, show=True)
                querystring['symbol']=ticker
                response = requests.request('GET', url=self.div_data_url, headers=headers, params=querystring)
                json_data = json.loads(response.text)
                df_div_hist = pd.json_normalize(json_data, record_path =['data'])
                if df_div_hist.empty == False:
                    l.log_df_details(ticker + ' Full Dividend History', df_div_hist)
                    self.load_df_to_stg(df_div_hist, '_stg_div_hist') 
                    self.call_proc('sp_refresh_dividends', args=[ticker,])
            l.log_message('Dividends refreshed successfully', show=True)
        except Exception as error:
            l.log_message(error, show=True) 

    def get_divs_and_prices_to_parquet(self):            
        '''
        for each Ticker, for each combination of days up to 30 days before and after the ex-dividend date,
        ''' 
        try:
            #### CREATE SPARK SESSION ####       
            spark = SparkSession.builder.master('local').appName('app').getOrCreate()
            spark.sparkContext.setLogLevel("WARN")  
            output_schema = StructType([
                StructField('Ticker', StringType(), True),
                StructField('FreqType', StringType(), True),
                StructField('ExDivDate', TimestampType(), True),
                StructField('AdjAmount', FloatType(), True),
                StructField('PriceDate', TimestampType(), True),
                StructField('ExDivDays', IntegerType(), True),
                StructField('AvgPrice', FloatType(), True),
                StructField('SandPAvgPrice', FloatType(), True)
            ])            
            sql = s.get_div_refresh_sql(max_tickers=1000, lookback_window=-365)  # use numbers that will get all div paying stocks
            l.log_message('List of Tickers for Dividend Analysis:')  
            tickers = self.get_ticker_list(sql)      
            cnt = 1
            for _, row in tickers.iterrows():
                ticker = str(row['Ticker'])
                l.log_message('Analyze returns for : ' + ticker + ' Stock Number: ' + str(cnt), show=True)  
                div_prices = self.call_proc('sp_get_dividends_and_prices_by_ticker', args=[ticker,])
                div_prices_rdd = spark.sparkContext.parallelize(div_prices)
                div_prices_df = spark.createDataFrame(div_prices_rdd,output_schema)
                div_prices_df.write.partitionBy("Ticker").parquet("output".format("Ticker"), mode="append")
                cnt += 1     
            l.log_message('Dividend Analysis completed successfully', show=True)
        except Exception as error:
            l.log_message(error, show=True) 

    def close_db_conn(self):
        '''closes the DB connection'''
        self.db_conn.close()