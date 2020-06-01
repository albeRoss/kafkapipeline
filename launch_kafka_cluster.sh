#!/usr/bin/env bash

#standalone ssh to launch scripts on instances

ssh -i "kafka-pipeline.pem" -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu@$1  << EOF
cd kafka_2.12-2.3.1

./start_kafka_broker.sh
EOF
#echo "kafka cluster up and running"
