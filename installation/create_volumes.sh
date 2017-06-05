#!/bin/bash

# Script used to create volumes on a single node VM

set -x

# Create a list of hostnames running Vertica.
VERTICA_HOSTS="maprdemo"

for HOSTNAME in $VERTICA_HOSTS; do

# Create the data volume
maprcli volume remove -name vertica.${HOSTNAME}.data -force true
maprcli volume create -name vertica.${HOSTNAME}.data -path /vertica/${HOSTNAME}/data -createparent true -replication 1

# Create the catalog volume
maprcli volume remove -name vertica.${HOSTNAME}.catalog -force true
maprcli volume create -name vertica.${HOSTNAME}.catalog -path /vertica/${HOSTNAME}/catalog -createparent true -replication 1

# Create the tmp volume
maprcli volume remove -name vertica.${HOSTNAME}.tmp -force true
maprcli volume create -name vertica.${HOSTNAME}.tmp -path /vertica/${HOSTNAME}/tmp -createparent true -localvolumehost ${HOSTNAME} -replication 1

# Create the parquet volume
maprcli volume create -name parquet -path /parquet -createparent true -replication 1

done

for HOSTNAME in $VERTICA_HOSTS; do

# Disable MapR compression 
hadoop mfs -setcompression off /vertica/${HOSTNAME}/data 
hadoop mfs -setcompression off /vertica/${HOSTNAME}/catalog 
hadoop mfs -setcompression off /vertica/${HOSTNAME}/tmp
hadoop mfs -setcompression off /parquet 

done

exit
