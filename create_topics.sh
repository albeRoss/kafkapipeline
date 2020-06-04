../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-54-211-228-201.compute-1.amazonaws.com:9092,ec2-100-24-4-145.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 2 --topic topic_1 &

../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-54-211-228-201.compute-1.amazonaws.com:9092,ec2-100-24-4-145.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 1 --topic topic_2 &

../kafka_2.12-2.3.1/bin/kafka-topics.sh --create --bootstrap-server ec2-54-211-228-201.compute-1.amazonaws.com:9092,ec2-100-24-4-145.compute-1.amazonaws.com:9092 --replication-factor 2 --partitions 1 --topic topic_3 &

