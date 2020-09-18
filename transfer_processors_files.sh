#!/usr/bin/env bash

#deploys configuration files to the cloud

array=()
find ./server$2 -name "*processor*" -print0 >tmpfile
while IFS=  read -r -d $'\0'; do
    array+=("$REPLY")
done <tmpfile
rm -f tmpfile

for j in "${array[@]}"
do
scp -i kafka-pipeline.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $j ubuntu@$1:
done