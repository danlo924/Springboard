import os, ssl
import urllib.request as req
import pandas as pd
from sqlalchemy import create_engine
import yfinance as yf
from yahoofinancials import YahooFinancials

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

# FIND ALL STOCKS THAT NEED DATA REFRESHED
sql = '''SELECT 'AAPL' AS Ticker;'''

# ITERATE THROUGH STOCKS AND GET FINANCIALS
try:
    cursor = investing_db_conn.cursor()
    cursor.execute(sql)
    df_results = pd.DataFrame(cursor.fetchall(), columns=['Ticker'])
    cursor.close()
    investing_db_conn.commit()     
    for _, row in df_results.iterrows():
        ticker = str(row['Ticker'])
        print('Get Financial Info for: ' + ticker)
        try:
            ticker_info = YahooFinancials(ticker)
            # summary_df = pd.DataFrame.from_dict(ticker_info.get_summary_data())
            # print(summary_df)
            stock_earnings_df = pd.DataFrame.from_dict(ticker_info.get_stock_earnings_data())
            print(stock_earnings_df)
            # financial_stmts_df = ticker_info.get_financial_stmts('quarterly','income')
            # summary_df.to_sql(con=investing_db_engine, name='_stg_summary', if_exists='replace')            
            stock_earnings_df.to_sql(con=investing_db_engine, name='_stg_stock_earnings', if_exists='replace')            
            # financial_stmts_df.to_sql(con=investing_db_engine, name='_stg_financial_stmts', if_exists='replace')            
            # cursor_pr = investing_db_conn.cursor()
            # cursor_pr.callproc('sp_refresh_prices', args=[ticker,])
            # results = list(cursor_pr.fetchall())
            # cursor_pr.close()
            investing_db_conn.commit()
        except Exception as error:
            print(error)
except Exception as error:
    print(error)
finally:
    investing_db_conn.close()

