#!/usr/bin/env bash


# if [[ $# -ne 3 ]]; then
#    echo "Illegal number of parameters"
#    echo "usage: kafka_start_cluster.sh [partitions] [topic-name] [replication-factor]"
#    exit 1
# fi

# PARTITIONS=$1
# NAME_TOPIC=$2
# REPLICATION_FACTOR=$3

./bin/zookeeper-server-start.sh config/zookeeper.properties


# ./bin/kafka-server-start.sh config/server.properties


# ./bin/kafka-topics.sh --create --topic $NAME_TOPIC --replication-factor 
# $REPLICATION_FACTOR --partitions $PARTITIONS --zookeeper localhost:2181