echo 'running: run_data_ingestion.sh script'
spark-submit \
--master local \
--py-files dist/guidedcapstone-1.0-py3.7.egg \
--jars jars/postgresql-42.2.14.jar \
source/run_data_ingestion.py \
# config/config.ini
$SHELL

