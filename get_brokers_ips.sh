#!/usr/bin/env bash

# extracts a json file from aws describe-instances
# needs to wait for run-instances to finish

aws ec2 describe-instances --filters "Name=instance-state-name,Values=pending,running"   --query "Reservations[*].Instances[*].{DNS:PublicDnsName,IP:PrivateIpAddress}" --output json > ./cluster.json
