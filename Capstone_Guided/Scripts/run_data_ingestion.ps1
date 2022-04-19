echo 'running: run_data_ingestion.ps1 script'
spark-submit --master local --py-files source/PySpark_Parser.py --jars jars/mysql-connector-java-8.0.26.jar source/run_data_ingestion.py 