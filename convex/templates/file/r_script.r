# This is a program to read a parquet file and print line 2
# to standard error
# Author: Steve Hawker
#######################################################
setwd("/tmp")
#install.packages("devtools", quiet = TRUE)
install.packages('devtools', lib = "/usr/lib64/R/library", repos = "http://cran.us.r-project.org")
install.packages("aws.s3",  lib = "/usr/lib64/R/library", repos = "http://cran.us.r-project.org", quiet = TRUE)
#install.packages("SparkR", quiet = TRUE)
#install.packages("sparklyr", quiet = TRUE)
devtools::install_github("rstudio/sparklyr")
#library("datasets")
#library(aws.s3)
library(sparklyr)
spark_install ("3.0.0") 


Sys.setenv("AWS_DEFAULT_REGION" = "eu-west-2")
#library("aws.s3")

library(sparklyr)
conf <- spark_config()
conf$sparklyr.defaultPackages <- "org.apache.hadoop:hadoop-aws:2.7.3"

sc <- spark_connect (master = "local") #,
#                    spark_home = "/usr/local/spark/",
#                    config =  conf)

# load parquet file into a Spark data frame and coerce into R data frame
spark_df = spark_read_parquet(sc, name = "disp", path="<location><file_name>", repartition = 0, memory = TRUE)
#stream direct from s3
#spark_df = spark_read_parquet(sc, name = "disp", "s3a://<bucket_name>/<file_name>", repartition = 0, memory = TRUE)

# Convert to R data frame from spark
r_df <- collect(spark_df)

v <- function(...) cat(sprintf(...), sep='', file=stderr())

row <- r_df[2,]
v("%s", row)
