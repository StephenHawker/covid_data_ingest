#!/bin/bash
#install R
sudo yum update
sudo yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
sudo yum install -y R
sudo yum install awscli