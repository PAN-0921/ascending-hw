#!/usr/bin/env bash

export SPARK_KAFKA_VERSION=0.10

/opt/cloudera/parcels/CDH/bin/spark-submit \
--master yarn \
--deploy-mode client \
--class data.engineer.training.rsvp_streaming5 \
de_project-1.0-SNAPSHOT.jar
