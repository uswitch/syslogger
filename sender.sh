#!/usr/bin/env bash

i=0

while true; do
    echo -n '.'
    logger "hi from sender $i"
    i=$[$i+1]
    sleep 1
done

