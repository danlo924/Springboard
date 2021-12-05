import ssl
import datetime
import logging
import json
from pandas.core.frame import DataFrame
import requests
import pandas as pd
import yfinance as yf
import urllib.request as req
from sqlalchemy import create_engine
from secrets import secrets

logging.basicConfig(
    filename='ignore\logs\pipeline.log',
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG
)

# PIPELINE CLASS DEFINITION
class Pipeline:
    def __init__(self, db_url, companies_data_url, div_data_url, sa_api_key):
        self.db_url = db_url
        self.companies_data_url = companies_data_url
        self.div_data_url = div_data_url
        self.sa_api_key = sa_api_key
        self.create_db_conn()
        self.log_pipeline_details()

    def log_pipeline_details(self):
        logging.debug('\nPipeline Run Details (' + str(datetime.datetime.now()) + '):'
        + '\nDatabase connection info: ' + self.db_url  
        + '\nCompanies data url: ' + self.companies_data_url 
        + '\nDividend data API url: ' + self.div_data_url
        + '\nSeeking Aplha API key: ' + self.sa_api_key)

    def log_df_details(self, df_name, df):
        try:
            logging.debug(df_name + ' DataFrame Loaded: ' + str(df.shape[0]) + ' Rows, ' + str(df.shape[1]) + ' Columns.')
        except Exception as error:
            logging.debug(error)
            print(error)   

    def create_db_conn(self):
        try:
            self.db_engine = create_engine(self.db_url)
            self.db_conn = self.db_engine.raw_connection()
        except Exception as error:
            logging.debug(error)
            print(error)


    def get_sandp_companies_list(self):
        try:
            url = self.companies_data_url
            context = ssl._create_unverified_context()
            response = req.urlopen(url, context=context)
            html = response.read()
            sandp_companies = pd.read_html(html)
            sp_curr = sandp_companies[0]
            self.log_df_details('S&P Current Company List', sp_curr)
            sp_hist = sandp_companies[1]
            self.log_df_details('S&P Historical Company List', sp_hist)
            self.load_df_to_stg(sp_curr, '_stg_sp_current')
            self.load_df_to_stg(sp_hist, '_stg_sp_history')
            self.call_proc('sp_refresh_companies')   
        except Exception as error:
            logging.debug(error)
            print(error)               

    def load_df_to_stg(self, df, stg_tbl_name):
        try:
            df.to_sql(con=self.db_engine, name=stg_tbl_name, if_exists='replace')
        except Exception as error:
            logging.debug(error)
            print(error)            

    def call_proc(self, proc_name, args=None):
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
            logging.debug(error)
            print(error)
        return results

    def get_price_refresh_sql(self, max_tickers, lookback_window):
        sql = '''
            SELECT REPLACE(c.Ticker,'.','-') AS Ticker
            FROM companies c
            WHERE NOT EXISTS 
            (
                SELECT 1
                FROM companies c2
                    INNER JOIN prices p ON c2.CompanyID = p.CompanyID
                WHERE c2.CompanyID = c.CompanyID
                    AND p.Date > date_sub(now(), interval ''' + str(lookback_window) + ''' day)
                LIMIT 1
            )
            ORDER BY Ticker
            LIMIT ''' + str(max_tickers) + ''';'''
        logging.debug('SQL for Price Refresh: ' + sql)
        return sql

    def get_div_refresh_sql(self, max_tickers, lookback_window):
        sql = '''
            SELECT REPLACE(c.Ticker,'.','-') AS Ticker
            FROM companies c
            WHERE c.HasDividend = 1
                AND NOT EXISTS 
                (
                    SELECT 1
                    FROM companies c2
                        INNER JOIN dividends d ON c2.CompanyID = d.CompanyID
                    WHERE c2.CompanyID = c.CompanyID
                        AND d.ExDivDate > date_sub(now(), interval ''' + str(lookback_window) + ''' day)
                    LIMIT 1
                )
            ORDER BY Ticker
            LIMIT ''' + str(max_tickers) + ''';'''
        logging.debug('SQL for Dividends Refresh: ' + sql)
        return sql

    def get_ticker_list(self, sql):
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql)
            df_tickers = pd.DataFrame(cursor.fetchall(), columns=['Ticker'])
            cursor.close()
            self.db_conn.commit()     
        except Exception as error:
            logging.debug(error)
            print(error)
        logging.debug(df_tickers)
        return df_tickers        

    def refresh_prices(self, max_tickers, lookback_window):
        try:
            sql = self.get_price_refresh_sql(max_tickers, lookback_window)
            logging.debug('List of Tickers for Price Refresh:')
            tickers = self.get_ticker_list(sql)        
            for _, row in tickers.iterrows():
                ticker = str(row['Ticker'])
                logging.debug('Get the price history for: ' + ticker)
                yf_ticker = yf.Ticker(ticker)
                df_price_hist = yf_ticker.history(period='40y')
                if df_price_hist.empty == False:
                    self.log_df_details(ticker + ' Price History', df_price_hist)
                    self.load_df_to_stg(df=df_price_hist, stg_tbl_name='_stg_price_hist') 
                    df_div = DataFrame(yf_ticker.dividends)
                    if df_div.empty == False:
                        self.log_df_details(ticker + ' Dividends', df_div)
                        self.load_df_to_stg(df=df_div, stg_tbl_name='_stg_has_div') # for updating company.HasDividend column
                    self.call_proc('sp_refresh_prices_company_data',args=[ticker,])
        except Exception as error:
            logging.debug(error)
            print(error)

    def refresh_dividends(self, max_tickers, lookback_window):
        try:
            sql = self.get_div_refresh_sql(max_tickers, lookback_window)
            logging.debug('List of Tickers for Dividend Refresh:')
            tickers = self.get_ticker_list(sql)  
            url = self.div_data_url
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
                logging.debug('Get the dividend history for: ' + ticker)
                querystring['symbol']=ticker
                response = requests.request('GET', url, headers=headers, params=querystring)
                json_data = json.loads(response.text)
                df_div_hist = pd.json_normalize(json_data, record_path =['data'])
                if df_div_hist.empty == False:
                    self.log_df_details(ticker + ' Full Dividend History', df_div_hist)
                    self.load_df_to_stg(df=df_div_hist, stg_tbl_name='_stg_div_hist') 
                    self.call_proc('sp_refresh_dividends',args=[ticker,])
        except Exception as error:
            logging.debug(error)
            print(error)

    def close_db_conn(self):
        self.db_conn.close()

# FULL PIPELINE RUN DEFINITION
def run_full_pipline():
    '''
    1. extract S&P companies and refresh in DB
        a. pull data from wikipedia into a pandas DataFrame
        b. load data to stage table(s) in the MySQL database
        c. call sp_refresh_companies to get latest list into the companies table
    2. refresh prices
        a. get list of tickers needing price refresh into list based on SQL query
        b. iterate the list of Tickers and:  
            i. get full price history to a staging table
            ii. get any dividend history to staging table
            iii. call sp_refresh_prices_company_data to refresh the prices table, and update the companies.HasDividend column
    3. refresh dividends
        a. get list of tickers needing dividend information refreshed into list based on SQL query using comapnies.HasDividend column
        b. iterate the list of Tickers and
            i. get dividend history to a staging table
            ii. call sp_refresh_dividends to refresh the dividends table
    '''

    # instantiate pipeline object with db url, wikipedia url, seeking alpha url, and api key.
    pipeline1 = Pipeline(
            db_url='mysql+pymysql://{user}:{pw}@{host}/{db}'.format(
                host=secrets.get('db_host'),
                db=secrets.get('db_name'),
                user=secrets.get('db_user'),
                pw=secrets.get('db_pw')),
            companies_data_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
            div_data_url = 'https://seeking-alpha.p.rapidapi.com/symbols/get-dividend-history',
            sa_api_key = secrets.get('sa_rapidapi_key')
            )

    # get fresh list of S&P 500 companies
    pipeline1.get_sandp_companies_list()

    # refresh price history data
    pipeline1.refresh_prices(max_tickers=25, lookback_window=0)

    # refresh dividend history
    pipeline1.refresh_dividends(max_tickers=5, lookback_window=50)

    # close db connection at end of run
    pipeline1.close_db_conn()

if __name__ == '__main__':
    run_full_pipline()
