#!/usr/bin/env bash

# returns the number of cores in the lauched cluster

aws ec2 describe-instances --filters "Name=instance-state-name,Values=pending,running" --query "Reservations[*].Instances[*].CpuOptions.[CoreCount]" --output text |python -c "import sys; print(sum(int(l) for l in sys.stdin))"

