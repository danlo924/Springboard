import datetime as dt
import pandas as pd
import glob as g
import yfinance as yf
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

def get_ticker_hist(**kwargs):
    ticker = kwargs['ticker']
    start_date = dt.date.today()
    end_date = start_date + dt.timedelta(days=1)
    ticker_df = yf.download(ticker, start=start_date, end=end_date, interval='1m')
    ticker_df.to_csv('/tmp/data/' + str(start_date.strftime('%Y-%m-%d')) + '/' + ticker + '.csv', header=False)

def get_tickers_daily_summary():
    todays_date = dt.date.today()
    file_suffix = '_' + todays_date.strftime('%Y%m%d') + '.csv'
    price_files = []
    col_names = ['Date_Time','Open','High','Low','Close','AdjClose','Volume']
    for filename in g.glob('/tmp/data/PriceHistory/*' + file_suffix):
        ticker = filename.replace('/tmp/data/PriceHistory/','').replace(file_suffix,'')
        price_file = pd.read_csv(filename, names=col_names, header=None)
        price_file.insert(0,'Ticker',ticker)
        price_files.append(price_file)
    df_todays_prices = pd.concat(price_files, axis=0)
    df_summary = df_todays_prices.groupby('Ticker').agg({'Low':'min','High':'max','Volume':'sum'})
    df_summary.to_json(path_or_buf='/tmp/data/PriceHistory/Summary_' + str(todays_date.strftime('%Y%m%d') + '.json'), orient='index')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": dt.datetime(2022, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
}

dag = DAG(
    "marketprices",
    description="A simple DAG",
    schedule_interval="0 18 * * 1-5",
    default_args=default_args,
    catchup=False,
)

# t1 = DummyOperator(task_id="dummy_task", retries=3, dag=dag)
t0 = BashOperator(task_id="create_tmp_dir", bash_command='mkdir -p /tmp/data/$(date +%Y-%m-%d)', dag=dag)
t1 = PythonOperator(task_id="get_aapl_hist", python_callable=get_ticker_hist, op_kwargs={'ticker':'AAPL'}, dag=dag)
t2 = PythonOperator(task_id="get_tsla_hist", python_callable=get_ticker_hist, op_kwargs={'ticker':'TSLA'}, dag=dag)
t3 = BashOperator(task_id="move_aapl_file", bash_command='mv /tmp/data/$(date +%Y-%m-%d)/AAPL.csv /tmp/data/PriceHistory/AAPL_$(date +%Y%m%d).csv', dag=dag)
t4 = BashOperator(task_id="move_tsla_file", bash_command='mv /tmp/data/$(date +%Y-%m-%d)/TSLA.csv /tmp/data/PriceHistory/TSLA_$(date +%Y%m%d).csv', dag=dag)
t5 = PythonOperator(task_id="query_todays_data", python_callable=get_tickers_daily_summary, dag=dag)

# sets downstream
t0.set_downstream([t1, t2])
t1 >> t3
t2 >> t4
t5.set_upstream([t3, t4])