import logging

from pandas import DataFrame

def log_message(msg: str, show=False):
    '''logs msg parameter to file; optionally, show/print messages to terminal'''
    logging.debug(msg)
    if show==True:
        print(msg)

def log_df_details(df_name: str, df: DataFrame):
    '''logs the DataFrame details: Name of DataFrame, number of rows, number of columns'''
    try:
        log_message(df_name + ' DataFrame Loaded: ' + str(df.shape[0]) + ' Rows, ' + str(df.shape[1]) + ' Columns.')
    except Exception as error:
        log_message(error, show=True)  