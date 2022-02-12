# Kafka Mini-Project - Instructions

### 1. Start Kafka Broker and Zooker Services in an Isolated Cluster:
docker-compose -f docker-compose.kafka.yml up

### 2. Start Consumer for Legit Transactions:
docker-compose -f docker-compose.kafka.yml exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming.transactions.legit | Out-File -FilePath .\legit_trans.txt

### 3. Start Consumer for Fraudulent Transactions:
docker-compose -f docker-compose.kafka.yml exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic streaming.transactions.fraud | Out-File -FilePath .\fraud_trans.txt

### 4. Start Generator and Detector Services - Let them run for ~10s and Stop Services (Ctrl-C)
docker-compose up | Out-File -FilePath .\generator_and_detector.txt
