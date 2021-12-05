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
            # ,pw='<pw_here>'
        )
    )

# CREATE A NEW CONNECTION TO THE DB
investing_db_conn = investing_db_engine.raw_connection()

url = "https://seeking-alpha.p.rapidapi.com/symbols/get-dividend-history"

querystring = {
        "symbol":"pg",
        "years":"40",
        "group_by":"month"
        }

headers = {
    'x-rapidapi-host': "seeking-alpha.p.rapidapi.com",
    'x-rapidapi-key': "4e36d82008mshddcfe888e5c9e3cp1b8083jsn7222cce4b0ec"
    }

response = requests.request("GET", url, headers=headers, params=querystring)
data = json.loads(response.text)
df_nested_list = pd.json_normalize(data, record_path =['data'])
print(df_nested_list)
