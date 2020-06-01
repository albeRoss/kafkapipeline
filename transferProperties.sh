#!/usr/bin/env bash

#it sends properties configuration files a server in the cluster
#it also sends start_kafka_cluster.sh to first server
scp -i kafka-pipeline.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no server$1/server0.properties ubuntu@$2:kafka_2.12-2.3.1/config/

if [ $1 -eq 0 ]; then
scp -i kafka-pipeline.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no server$1/start_kafka_broker.sh ubuntu@$2:kafka_2.12-2.3.1/

fi
