# Open-Ended Capstone Project: Data-Driven Investing

## Overview:
#### The purpose of this project is to craft a data solution whereby investors can test the returns on a theoretical portfolio of stocks over a given timeframe using actual historical stock price and dividend data. 
#### The data solution will consist of financial data including historical prices, dividends, and monthly macroeconomic data. 
#### An analyst will then be able to “back-test” a particular investing strategy given certain inputs, thresholds, and assumptions. 
#### The solution will serve as a tool for determining where and when to invest. 

### **DataCollection** - Python Scripts:
#### **pipeline.py** - python script that refreshes a list of S&P Companies in the Companies table, and updates the Prices and Dividends tables for each stock Ticker

### **InvestingDB** - MySQL Database Tables:
#### **Companies** (Stock Tickers) and Major Indexes with metadata
#### **Prices** (Daily: Open, High, Low, Close, Volume) for each Company or Index
#### **Dividends** (Frequency, Dates, Amount, SplitAdjFactor) for each Company or Index





