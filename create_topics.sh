../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-54-152-87-210.compute-1.amazonaws.com:9092,ec2-3-95-34-83.compute-1.amazonaws.com:9092,ec2-52-23-188-244.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 3 --topic topic_1 &

