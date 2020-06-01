../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-3-92-62-17.compute-1.amazonaws.com:9092,ec2-3-92-60-71.compute-1.amazonaws.com:9092,ec2-18-206-40-97.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 3 --topic topic_1 &

