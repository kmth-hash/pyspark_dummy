# Data will be incoming on hourly basis 
# Source file will be csv file containing user data stored in s3 bucket
# Create pipeline which will read data and aggregate the data and store it in s3 buckets based on country
# Source file : file.csv 
# Source S3 bucket : userDataBucket/source 
# Destination S3 bucket : userDataBucket/dest/[country]
# Lambda function that will be triggered based on source file in source path 
# Delete source file post output file creation 
# 