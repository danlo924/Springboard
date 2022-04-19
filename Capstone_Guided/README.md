# Data Engineering Career Track - Guided Capstone: Step Five - Pipeline Orchestration
Contains local files, Python scripts, and PySpark commands for processing files using Spark-Submit

Overview:
/scripts - contains powershell scripts for running spark-submit commands
/source - contains all PySpark source code
    /source/PySpark_Parser.py - contains schema StructType and csv / json parse functions
    /source/DataLoader.py - contains logic for loading / processing files
    /source/Reporter.py - contains PySpark and SQL logic for analyzing parsed files
/data - contains source csv and json files
/output_dir - contains results - parquet files 

To Run Pipeline:
1. from Windows Powershell, cd to path Capstone_Guided
2. scripts/run_data_ingestion.ps1
3. scripts/run_reporter.ps1