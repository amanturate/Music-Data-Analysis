#!/bin/bash

batchid=`cat /home/acadgild/project/logs/current-batch.txt`
LOGFILE=/home/acadgild/project/logs/log_batch_$batchid

echo "Running spark script for data analysis..." >> $LOGFILE

cat '/home/acadgild/project/scripts/formatted_load.scala' | spark-shell

echo "Batch - $batchid completed" >> $LOGFILE

echo "Incrementing batchid..." >> $LOGFILE

batchid=`expr $batchid + 1`
echo -n $batchid > /home/acadgild/project/logs/current-batch.txt
