#!/usr/bin/env bash

#utility bash to  remove logs from tmp folders in EC2 instances
#it assumes Linux distribution undernith

rm -r /../../../tmp/kafka-logs && rm -r /../../../tmp/zookeeper
