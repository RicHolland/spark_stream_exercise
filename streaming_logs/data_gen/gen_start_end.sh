#!/bin/bash

[ -n "$1" ] && ID=$1 || ID=1

[ $ID -ge 100 ] && exit

echo "{\"id\": $ID, \"script\": \"$(basename $0)\", \"start\": $(date +%s)}"

sleep $((RANDOM%4+6))

$0 $((ID+1)) &
pid=$!

sleep $((RANDOM%54+6))

echo "{\"id\": $ID, \"script\": \"$(basename $0)\", \"end\": $(date +%s)}"

wait $pid
