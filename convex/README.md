app.py - Parquet file processing
====================================

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
   - urllib 
   - import urllib.parse
   - import urllib.parse as urlparse
   - import sys
   - import math
   - import logging
   - import logging.config
   - import io
   - import ssl
   - pandas
   - requests
   - simplejson
   - boto3 - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket

### API keys

## Run	

Configure convex.ini with the appropriate parameters












