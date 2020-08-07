../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-34-227-31-0.compute-1.amazonaws.com:9092,ec2-54-209-152-196.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 2 --topic topic_1 &

../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-34-227-31-0.compute-1.amazonaws.com:9092,ec2-54-209-152-196.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 1 --topic topic_2 &

../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-34-227-31-0.compute-1.amazonaws.com:9092,ec2-54-209-152-196.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 1 --topic topic_3 &

