## Kafka commands to create , show and delete topics 

topic name : covidKafka

### bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic covidKafka --create --partitions 1 --replication-factor 1

### bin/kafka-topics.sh --bootstrap-server  localhost:9092 --list

### bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic covidKafka

### bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic covidKafka

