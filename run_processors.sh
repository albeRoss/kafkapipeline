#!/usr/bin/env bash

#runs processors on the instances
array=()
find . -name "*processor*" -print0 >tmpfile
while IFS=  read -r -d $'\0'; do
    array+=("$REPLY")
done <tmpfile
rm -f tmpfile

for j in "${array[@]}"
do
{ java -cp processor-1.0-SNAPSHOT-jar-with-dependencies.jar org.middleware.project.ProcessorStarter $j &}
done