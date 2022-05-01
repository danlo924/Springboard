import logging
import pipeline as p
from datetime import datetime
from secrets import secrets

logging.basicConfig(
    filename=datetime.now().strftime(secrets.get('log_dir') + 'run_full_pipeline_%Y_%m_%d_%H_%M_%S.log'),
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG
)

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
    4. analyze prices and get trading returns
        a. analyze price changes N number of days before and after each ex-dividend date 
        b. normalize returns against the S&P averages during the same period
        c. group and summarize average returns for each Ticker + each possible trading window surrounding ex-dividend dates
    '''

    # instantiate pipeline object with db url, wikipedia url, seeking alpha url, and api key.
    pipeline1 = p.Pipeline(
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
    pipeline1.refresh_prices(max_tickers=1, lookback_window=0)

    # refresh dividend history
    pipeline1.refresh_dividends(max_tickers=1, lookback_window=0)

    # analyze prices and trading returns surrounding ex-dividend dates
    pipeline1.get_trading_returns(days_before=30, days_after=30)    

    # close db connection at end of run
    pipeline1.close_db_conn()

if __name__ == '__main__':
    run_full_pipline()
