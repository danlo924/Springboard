import log_helpers as l

def get_price_refresh_sql(max_tickers: int, lookback_window: int):
    '''generates the sql statement to send to the DB for getting a list of tickers that need their price history refreshed'''
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
    l.log_message('SQL for Price Refresh: ' + sql)
    return sql

def get_div_refresh_sql(max_tickers: int, lookback_window: int):
    '''generates the sql statement to send to the DB for getting a list of tickers that need their dividend history refreshed'''
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
    l.log_message('SQL for Dividends Refresh: ' + sql)
    return sql      