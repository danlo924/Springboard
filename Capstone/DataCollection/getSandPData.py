import os, ssl
import pandas as pd
from sqlalchemy import create_engine
import yfinance as yf

# BYPASS CERTIFICATE FOR NOW
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context

# url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
# context = ssl._create_unverified_context()
# response = request.urlopen(url, context=context)
# html = response.read()

# SandP500List=pd.read_html(html)    

# CREATE ENGINE FOR MYSQL INVESTING DATABASE
investing_db_engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
            host='127.0.0.1'
            ,db='investing'
            ,user='root'
            ,pw='<pw_here>'
        )
    )

# CREATE A NEW CONNECTION TO THE DB
investing_db_conn = investing_db_engine.raw_connection()

# GET FULL ACTIVE LIST OF S&P 500 COMPANIES, ALONG WITH COMPANIES ADDED / REMOVED IN THE PAST 20 YEARS
SandP500List=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
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

# ITERATE THROUGH STOCKS AND GET PRICE HISTORY
sql = '''SELECT c.Ticker
    FROM companies c
    WHERE NOT EXISTS 
    (
        SELECT 1
        FROM companies c2
            INNER JOIN prices p ON c2.CompanyID = p.CompanyID
        WHERE c2.CompanyID = c.CompanyID
        LIMIT 1
    )
    ORDER BY Ticker
    LIMIT 10;
    '''

try:
    cursor = investing_db_conn.cursor()
    cursor.execute(sql)
    df_results = pd.DataFrame(cursor.fetchall(), columns=['Ticker'])
    cursor.close()
    investing_db_conn.commit()     
    for index, row in df_results.iterrows():
        ticker = str(row['Ticker'])
        print('Get the price history for: ' + ticker)
        price_hist = yf.Ticker(ticker)
        price_hist_df = price_hist.history(period='max')
        price_hist_df.to_sql(con=investing_db_engine, name='_stg_price_hist', if_exists='replace')
        try:
            cursor_pr = investing_db_conn.cursor()
            cursor_pr.callproc('sp_refresh_prices', args=[ticker,])
            results = list(cursor_pr.fetchall())
            cursor_pr.close()
            investing_db_conn.commit()
        except Exception as error:
            print(error)
except Exception as error:
    print(error)

investing_db_conn.close()