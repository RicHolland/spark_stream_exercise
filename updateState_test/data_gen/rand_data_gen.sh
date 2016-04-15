#!/bin/bash

KAFKA=/nas/used_by_hadoop/hadoop-kn-p2/h7743735/kafka/

while true; do

  id=$((RANDOM%9+1))
  str=$(shuf -n 1 /usr/share/dict/linux.words)

  echo ${id},${str} | \
    $KAFKA/bin/kafka-console-producer.sh \
    --broker-list localhost:9092,localhost:9093,localhost:9094 \
    --topic updateStateTest
  echo ${id},${str},$(date +%Y-%m-%d" "%H:%M:%S) | tee -a .rand_data.dat

  sleep $((RANDOM%5+2))

done
