#!/bin/bash

batchid=`cat /home/acadgild/project/logs/current-batch.txt`
LOGFILE=/home/acadgild/project/logs/log_batch_$batchid

echo "Placing data files from local to HDFS..." >> $LOGFILE

hadoop fs -rm -r /user/acadgild/project/batch1/web/
hadoop fs -rm -r /user/acadgild/project/batch1/formattedweb/
hadoop fs -rm -r /user/acadgild/project/batch1/mob/

hadoop fs -mkdir -p /user/acadgild/project/batch1/web/
hadoop fs -mkdir -p /user/acadgild/project/batch1/mob/

hadoop fs -put /home/acadgild/project/data/web/* /user/acadgild/project/batch1/web/
hadoop fs -put /home/acadgild/project/data/mob/* /user/acadgild/project/batch1/mob/

echo "Running pig script for data formatting..." >> $LOGFILE

pig -param batchid=$batchid /home/acadgild/project/scripts/dataformatting.pig

echo "Running hive script for formatted data load..." >> $LOGFILE

hive -hiveconf batchid=$batchid -f /home/acadgild/project/scripts/formatted_hive_load.hql
