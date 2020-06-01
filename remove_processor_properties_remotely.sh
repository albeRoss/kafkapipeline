#!/usr/bin/env bash

#standalone ssh to remove processors properties on instances
#expects a list of elastic ips
IFS=',' read -ra my_array <<< "$1"
for j in "${my_array[@]}"
do
ssh -i "kafka-pipeline.pem" -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu@$j << EOF

find .  -name '*processor*' -exec rm -f {} \;
echo "properties removed"
EOF
done
