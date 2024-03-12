read data from file . Select name , country , continent , status and send it to consumer
Topic 1 : covidKafka --> send data 

Accept data from Topic 1 (covidKafka) and categorize data based on status and continent

write data into hive tables partitioned by status value
store it into hive table --> /path/continentCol/status[recovered|dead|affected]

