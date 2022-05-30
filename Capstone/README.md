# Open-Ended Capstone Project: Data-Driven Investing

## Overview:
#### The purpose of this project is to craft a data solution whereby investors can test the returns on a theoretical portfolio of stocks over a given timeframe using actual historical stock price and dividend data. 
#### The data solution will consist of financial data including historical prices and dividends
#### An analyst will be able to opimize a dividend-capture investing method for each dividend paying stock in the S&P 500 by calculating optimal Buy and Sell dates surrounding the Ex-Dividend date
#### The solution will serve as a tool for optimizing returns for a dividend-capture investing method. 

### **DataCollection** - Python Scripts:
#### **run_full_pipeline.py** - python script that refreshes a list of S&P Companies in the Companies table, updates the Prices and Dividends tables for each stock Ticker, and outputs relevant dividends and price history to parquet files, partitioned by Ticker
#### **run_analyzer.py## - pySpark script run on Azure Synapse Analytics that reads output from the run_full_pipeline.py steps and generates optimal Buy (days Before) and Sell (days After) for each Ticker's dividends by comparing the average return for each combination of days with the S&P return over the same time period

### **InvestingDB** - MySQL Database Tables:
#### **Companies** (Stock Tickers) and Major Indexes with metadata
#### **Prices** (Daily: Open, High, Low, Close, Volume) for each Company or Index
#### **Dividends** (Frequency, Dates, Amount, SplitAdjFactor) for each Company or Index





