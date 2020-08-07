#it sends properties configuration files to all the servers in the cluster
#it also sends start_kafka_cluster.sh to first server
for ((i=0;i <= $1;i++))
do
scp -i kafka-pipeline.pem /server$i/server0.properties ubuntu@$2:kafka_2.12-2.3.1/config/
done

scp -i kafka-pipeline.pem start_kafka_cluster.sh ubuntu@$2:kafka_2.12-2.3.1/