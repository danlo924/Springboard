import ssl
import urllib.request as req
import pandas as pd
from sqlalchemy import create_engine
import yfinance as yf

# CREATE ENGINE FOR MYSQL INVESTING DATABASE
investing_db_engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
            host='127.0.0.1'
            ,db='investing'
            ,user='root'
            ,pw='Nuggy557$'
        )
    )

# CREATE A NEW CONNECTION TO THE DB
investing_db_conn = investing_db_engine.raw_connection()

# GET FULL ACTIVE LIST OF S&P 500 COMPANIES, ALONG WITH COMPANIES ADDED / REMOVED IN THE PAST 20 YEARS
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
context = ssl._create_unverified_context()
response = req.urlopen(url, context=context)
html = response.read()

SandP500List=pd.read_html(html)    
sp_curr = SandP500List[0]
sp_hist = SandP500List[1]

# IMPORT CURRENT AND HISTORICAL S&P LISTS TO STAGING TABLES
sp_curr.to_sql(con=investing_db_engine, name='_stg_sp_current', if_exists='replace')
sp_hist.to_sql(con=investing_db_engine, name='_stg_sp_history', if_exists='replace')

# CALL sp_refresh_companies STORED PROC TO REFRESH S&P LIST
try:
    cursor = investing_db_conn.cursor()
    cursor.callproc('sp_refresh_companies')
    results = list(cursor.fetchall())
    cursor.close()
    investing_db_conn.commit()
except Exception as error:
    print(error)

# FIND ALL STOCKS THAT NEED DATA REFRESHED
sql = '''SELECT REPLACE(c.Ticker,'.','-')
    FROM companies c
    WHERE NOT EXISTS 
    (
        SELECT 1
        FROM companies c2
            INNER JOIN prices p ON c2.CompanyID = p.CompanyID
        WHERE c2.CompanyID = c.CompanyID
            AND p.Date > date_sub(now(), interval 0 day)
        LIMIT 1
    )
    ORDER BY Ticker
    LIMIT 5;
    '''

# ITERATE THROUGH STOCKS AND GET PRICE HISTORY
try:
    cursor = investing_db_conn.cursor()
    cursor.execute(sql)
    df_results = pd.DataFrame(cursor.fetchall(), columns=['Ticker'])
    cursor.close()
    investing_db_conn.commit()     
    for _, row in df_results.iterrows():
        ticker = str(row['Ticker'])
        print('Get the price history for: ' + ticker)
        try:
            price_hist = yf.Ticker(ticker)
            price_hist_df = price_hist.history(period="45y") # get at most 45 years of price history, which should cover 1980 to 2021
            price_hist_df.to_sql(con=investing_db_engine, name='_stg_price_hist', if_exists='replace') 
            price_hist.dividends.to_sql(con=investing_db_engine, name='_stg_has_div', if_exists='replace')            
            cursor_pr = investing_db_conn.cursor()
            cursor_pr.callproc('sp_refresh_prices_company_data', args=[ticker,])
            results = list(cursor_pr.fetchall())
            cursor_pr.close()
            investing_db_conn.commit()
        except Exception as error:
            print(error)
except Exception as error:
    print(error)
finally:
    investing_db_conn.close()

