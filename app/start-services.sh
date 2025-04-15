#!/bin/bash

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

mapred --daemon start historyserver


jps -lm
hdfs dfsadmin -report

hdfs dfsadmin -safemode leave

hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -chmod 744 /apps/spark/jars


hdfs dfs -put /usr/local/spark/jars/* /apps/spark/jars/
hdfs dfs -chmod +rx /apps/spark/jars/


scala -version

jps -lm

hdfs dfs -mkdir -p /user/root

