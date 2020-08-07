#!/usr/bin/env bash

#script with ssh to remove tmp on instances
#expects a list of elastic ips
IFS=',' read -ra my_array <<< "$1"
for j in "${my_array[@]}"
do
ssh -i "kafka-pipeline.pem" -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu@$j << EOF

./remove_tmp.sh
echo "removed tmps"
EOF
done
#echo "kafka cluster up and running"
