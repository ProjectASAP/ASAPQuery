#!/bin/bash

if [ $# -ne 4 ]; then
  echo "Usage: $0 <path to prometheus-kafka-adapter directory> <kafka broker> <kafka topic> <serialization_format>"
  exit 1
fi

DIR=$1
KAFKA_BROKER=$2
KAFKA_TOPIC=$3
SERIALIZATION_FORMAT=$4

cd $DIR
KAFKA_BROKER_LIST=$KAFKA_BROKER KAFKA_TOPIC=$KAFKA_TOPIC SERIALIZATION_FORMAT=$SERIALIZATION_FORMAT ./prometheus-kafka-adapter-musl
