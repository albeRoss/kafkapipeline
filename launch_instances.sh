#!/usr/bin/env bash

# make sure you have aws CLI installed
# it assumes an available AMI with Java JRE (version >=1.8) Kafka (version >= 2.12-2.31)
# it expects the number of instances to be launched
# I used an AWS launch template for cloud formation on t2.xlarge,linux 16.04.6 LTS
# It is advisable to use MSK instead

aws ec2 run-instances --image-id image_id --key-name key-name --subnet-id subnet-id \
--launch-template LaunchTemplateName=template-name --count $1

