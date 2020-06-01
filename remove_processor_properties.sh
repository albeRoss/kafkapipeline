#!/usr/bin/env bash

for ((i=0;i <= $1;i++)) do
    find ./server$i/  -name '*processor*' -exec rm -f {} \;
done

