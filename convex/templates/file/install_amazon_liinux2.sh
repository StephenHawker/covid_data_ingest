#!/bin/bash
#install R
sudo yum update -y
#sudo yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
#sudo yum install -y R

sudo amazon-linux-extras install R3.4
sudo yum install -y awscli
sudo yum install -y openssl-devel
sudo yum install -y libcurl-devel
sudo yum install -y libxml2-devel

#Get and install hadoop
#wget https://downloads.apache.org/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
#tar -xvf hadoop-3.2.1/hadoop-3.2.1.tar.gz
#sudo mv hadoop-3.2.1 /usr/local/hadoop

#Get and install spark
#wget https://www.apache.org/dyn/closer.lua/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz
#tar -xvf spark-3.0.0-bin-hadoop3.2.tgz
#sudo mv spark-3.0.0-bin-hadoop3.2 /usr/local/
#sudo ln -s /usr/local/spark-3.0.0-bin-hadoop3.2/ /usr/local/spark
#export SPARK_HOME=/usr/local/spark
#$SPARK_HOME/sbin/start-master.sh
#$SPARK_HOME/sbin/start-slave.sh spark://ethane:7077
