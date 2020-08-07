#!/usr/bin/env bash

#utility bash to clean servers configurations
find ./config/ ! -name 'server.properties' -name '*server*' -exec rm -f {} \;

