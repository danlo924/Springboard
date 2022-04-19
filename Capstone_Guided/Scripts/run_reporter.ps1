echo 'running: run_reporter.ps1 script'
spark-submit --master local --jars jars/mysql-connector-java-8.0.26.jar source/run_reporter.py 
