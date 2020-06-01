../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-54-163-48-106.compute-1.amazonaws.com:9092,ec2-3-84-120-48.compute-1.amazonaws.com:9092,ec2-3-92-228-76.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 3 --topic topic_1 &

