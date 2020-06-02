#!/usr/bin/env bash
#utility bash to clean processors configurations

for ((i=0;i <= $1;i++)) do
    find ./server$i/  -name '*processor*' -exec rm -f {} \;
done

