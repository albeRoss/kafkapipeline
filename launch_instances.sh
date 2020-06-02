#!/usr/bin/env bash

# make sure you have aws CLI installed
# it assumes an available AMI with Java JRE (version >=1.8) Kafka (version >= 2.12-2.31)
# it expects the number of instances to be launched
# I used an AWS launch template for cloud formation on t2.xlarge,linux 16.04.6 LTS
#It is advisable to use MSK instead

aws ec2 run-instances --image-id ami-0cf15dcf89c55d8a7 --key-name kafka-pipeline --subnet-id subnet-61804740 \
--launch-template LaunchTemplateName=KafkaTemplate --count $1

