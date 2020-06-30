# This is a program to read a parquet file and print line 2
# to standard error
# Author: Steve Hawker
#######################################################
setwd("/tmp")
install.packages("arrow", quiet = TRUE)
install.packages("devtools")
install.packages("aws.s3", quiet = TRUE)
spark_install()
#library("datasets")
library(arrow)
install_arrow()
library(aws.s3)


obj <- get_object("s3://<bucket_name>/<file_name>")
library(arrow)
df <- read_parquet(obj)


#loop the df
by(df, 1:nrow(df), function(row) dostuff)


for(i in 1:nrow(df)) {
    row <- dataFrame[i,]
    # do stuff with row
    if (i==1) {
      #Write contents out to stderr
      write(row, stderr())
    }
}

