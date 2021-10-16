# Import Libraries
import sys
import yfinance as yf
from contextlib import redirect_stdout

# Get Ticker Info:
GetTickerInfo = yf.Ticker('AAPL')
 
# Output price history of each Ticker to File:
with open ('ticker_hist.txt', 'w') as th:
    with redirect_stdout(th):
        print(GetTickerInfo.history(period='6mo'))

th.close()
