from sqlalchemy import create_engine
import requests
import pandas as pd
import json

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

url = "https://seeking-alpha.p.rapidapi.com/symbols/get-dividend-history"

querystring = {
        "symbol":"<ticker>",
        "years":"40",
        "group_by":"month"
        }

headers = {
    'x-rapidapi-host': "seeking-alpha.p.rapidapi.com",
    'x-rapidapi-key': "4e36d82008mshddcfe888e5c9e3cp1b8083jsn7222cce4b0ec"
    }

# FIND ALL STOCKS THAT NEED DATA REFRESHED
sql = '''SELECT REPLACE(c.Ticker,'.','-') AS Ticker
    FROM companies c
    WHERE NOT EXISTS 
    (
        SELECT 1
        FROM companies c2
            INNER JOIN dividends d ON c2.CompanyID = d.CompanyID
        WHERE c2.CompanyID = c.CompanyID
            AND d.ExDivDate > date_sub(now(), interval 50 day)
        LIMIT 1
    )
		AND c.IsIndex != 1
        AND c.HasDividend = 1
    ORDER BY Ticker
    LIMIT 5;
    '''

# ITERATE THROUGH STOCKS AND GET DIVIDEND HISTORY
try:
    cursor = investing_db_conn.cursor()
    cursor.execute(sql)
    df_results = pd.DataFrame(cursor.fetchall(), columns=['Ticker'])
    cursor.close()
    investing_db_conn.commit()     
    for _, row in df_results.iterrows():
        ticker = str(row['Ticker'])
        print('Get the dividend history for: ' + ticker)
        try:
            querystring['symbol']=ticker
            response = requests.request("GET", url, headers=headers, params=querystring)
            json_data = json.loads(response.text)
            div_hist_df = pd.json_normalize(json_data, record_path =['data'])
            div_hist_df.to_sql(con=investing_db_engine, name='_stg_div_hist', if_exists='replace')
            cursor_pr = investing_db_conn.cursor()
            cursor_pr.callproc('sp_refresh_dividends', args=[ticker,])
            results = list(cursor_pr.fetchall())
            cursor_pr.close()
            investing_db_conn.commit()
        except Exception as error:
            print(error)
except Exception as error:
    print(error)
finally:
    investing_db_conn.close()
