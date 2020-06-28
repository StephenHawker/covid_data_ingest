app.py - Parquet file processing using EC2 / S3 bucket
======================================================

## Description

Using any language or combination of languages except .NET (e.g. Bash script, Python, R, Java, Scala, etc) using AWS SDK libraries (e.g. boto, Java
SDK, except .NET) or the AWS CLI (bash script) write an app/script that:

Locally creates a Parquet file with some data in it (the nature of the data and size, is not important)
Creates an S3 bucket, and uploads the parquet file to the bucket
Creates an IAM Role that gives Read & List access to the S3 bucket
Spins up an EC2 instance that has the above IAM Role attached

Install R on the EC2 instance
Copies a “Parquet Reader” R Script (see below for details on this script) to the EC2 instance
Runs the “Parquet Reader” R Script

The AWS credentials used should be picked up in the usual way from ~/.aws/credentials, the particular profile used should be passed in as a
command line argument.
Any other configurable should be passed in as command line arguments. The app/script should be non-interactive.

Parquet Reader R Script
The R Script should
Use credentials via the Instance Profile associated to the IAM Role
Read the parquet file in the S3 bucket, and print out the second record in the parquet file to standard error.
The R libraries used to read and process the file are up to you.

## Written By Steve Hawker 19/06/2020

## Requirements

 - Python 3.7 or later.
 - The following Python libraries
   - configparser
   - import sys
   - import math
   - import logging
   - import logging.config
   - import io
   - pandas
   - requests
   - pyarrow (parquet file handling)
   - boto3 - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
   - paramiko (ssh client for EC2)
   - R - R is available in Amazon Linux Extra topic "R3.4"
     sudo amazon-linux-extras install R3.4
     https://aws.amazon.com/amazon-linux-2/faqs/#Amazon_Linux_Extras
    This version is too low for many libraries - https://stackoverflow.com/questions/59415973/how-to-install-newer-version-of-r-on-amazon-linux-2
    yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
    yum -y install R
   - Configure access via instance profile : https://aws.amazon.com/premiumsupport/knowledge-center/s3-instance-access-bucket/
     https://docs.amazonaws.cn/en_us/cli/latest/userguide/cli-configure-metadata.html
## Tools used
Pycharm Community 2020.1 IDS for python development
AWS command line interface to setup credentials : https://aws.amazon.com/cli/ 
AWS policy gen : https://awspolicygen.s3.amazonaws.com/policygen.html

### API keys

## Run	

Configure convex.ini with the appropriate parameters












