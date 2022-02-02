# Aiflow Log Analyzer Mini-Project

## Overview:
#### Python script will find all files by recursively searching the 'airflow_logs' root log directory
#### The 'lookback_hours' parameter will be used to only look at log files modified since now() - lookback_hours
#### If a log file contains the string 'ERROR' in it, then the File metadata will be printed along with each line containing the error
#### Total errors will be summed and printed for each file, as well as an overall total count of errors