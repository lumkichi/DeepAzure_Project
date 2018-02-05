#!/bin/bash

# Set the Root Project Folder
ROOT=DeepAzure_Project

# Check if the PATH contains java
echo "Setting env variables"
java_setup=`echo $PATH | grep 'java-8'`
if [ -z "$java_setup" ]
then
  export PATH=${JAVA_HOME}/bin:$PATH
fi

# Check if HADOOP_CLASSPATH exists, otherwise set it up
if [ -z "${HADOOP_CLASSPATH}" ]
then
  export HADOOP_CLASSPATH=.:${JAVA_HOME}/lib/tools.jar
fi

# Restore any SLF4J jars that has been disabled by this script.  SLF4J causes
# excessive logging in the Mongo Driver, so it has to be disabled for the
# duration of the run.
sudo mv /usr/hdp/2.6.2.3-1/hadoop/lib/slf4j-log4j12-1.7.10.jarx /usr/hdp/2.6.2.3-1/hadoop/lib/slf4j-log4j12-1.7.10.jar  2>/dev/null

# Copy the input file to the HDFS system
hdfs dfs -mkdir -p /user/hadoop/mongo_input
hdfs dfs -put ~/$ROOT/data/RXBC_20180122_INSERT_EXTRACT.dat /user/hadoop/mongo_input

# Delete contents of old output folder and the output folder itself
hdfs dfs -rmr /user/hadoop/mongo_output

# Disable the SLF4J jar for the duration of the run to prevent excessive logging
# by the Mongo Driver
sudo mv /usr/hdp/2.6.2.3-1/hadoop/lib/slf4j-log4j12-1.7.10.jar /usr/hdp/2.6.2.3-1/hadoop/lib/slf4j-log4j12-1.7.10.jarx 2>/dev/null

# User parameters to MongoLoad.jar are  hdfs:/path/to/input/file   hdfs:/nonexistent/output/folder
hadoop jar ~/$ROOT/dist/MongoLoad.jar /user/hadoop/mongo_input/RXBC_20180122_INSERT_EXTRACT.dat /user/hadoop/mongo_output

# Restore the SLF4J driver
sudo mv /usr/hdp/2.6.2.3-1/hadoop/lib/slf4j-log4j12-1.7.10.jarx /usr/hdp/2.6.2.3-1/hadoop/lib/slf4j-log4j12-1.7.10.jar  2>/dev/null
