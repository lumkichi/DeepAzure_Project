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

# Iterate all jar files in lib folder and add to classpath
for II in `ls ~/$ROOT/lib`
do
  CP=$CP:~/$ROOT/lib/$II
done
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH$CP


# Create the output dirs
echo "Setting output dirs"
mkdir -p ~/$ROOT/build
mkdir -p ~/$ROOT/dist
rm -f ~/$ROOT/build/*.class

# Compile the individual classes
echo " "
echo "Compiling"
cd ~/$ROOT/src


echo "  MongoMap.java"
/usr/bin/hadoop com.sun.tools.javac.Main -d ~/$ROOT/build  com/relayhealth/hadoop/util/MongoMap.java
if [ $? != 0 ]
then
  exit
fi

echo "  MongoReduce.java"
/usr/bin/hadoop com.sun.tools.javac.Main -d ~/$ROOT/build  com/relayhealth/hadoop/util/MongoReduce.java
if [ $? != 0 ]
then
  exit
fi

echo "  MongoConnection.java"
/usr/bin/hadoop com.sun.tools.javac.Main -d ~/$ROOT/build  com/relayhealth/mongoloader/util/MongoConnection.java
if [ $? != 0 ]
then
  exit
fi

echo "  MongoLoad.java"
/usr/bin/hadoop com.sun.tools.javac.Main -d ~/$ROOT/build  com/relayhealth/mongoloader/MongoLoad.java 
if [ $? != 0 ]
then
  exit
fi

# Create the jar file
echo " "
echo "Building jar"
cd ~/$ROOT/build

# Unpack the contents of the Mongo driver and the No OP slf4j libraries
# Rename the respective MANIFEST.MF's to preserve the licensing info
unzip -qq -o ../lib/mongo*.jar
mv META-INF/MANIFEST.MF META-INF/MANIFEST-SLF4J.MF
unzip -qq -o ../lib/slf4j*.jar
mv META-INF/MANIFEST.MF META-INF/MANIFEST-MONGO.MF

# Assert our own MANIFEST.MF into the target Jar and build the new jar
# which includes all of the Mongo Driver and SLF4J libraries as well.
jar cfm ~/$ROOT/dist/MongoLoad.jar ~/$ROOT/dist/MANIFEST.MF ./*
echo "Jar file at ~/$ROOT/dist/MongoLoad.jar"
echo " "
echo "Complete"
