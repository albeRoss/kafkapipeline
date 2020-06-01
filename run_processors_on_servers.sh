#!/usr/bin/env bash

#runs processors on servers
#expects a list of elastic ips

ssh -i "kafka-pipeline.pem" -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ubuntu@$1 << EOF
./run_processors.sh
EOF