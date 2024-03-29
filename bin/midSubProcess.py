from json import loads  
from kafka import KafkaConsumer    
import findspark

import os
os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'
os.environ['SPARK_HOME']='/home/ubuntu/prj/spark/spark-3.1.2-bin-hadoop3.2'

findspark.init()

from pyspark.sql import * 
from pyspark.sql.functions import * 

from pyspark.sql.types import StructField , StructType , StringType
def receiveData(topic ):
    # Package required for kafka to work 
    spark = SparkSession.builder.master('local[2]')\
        .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')\
        .appName('CovidKafka').getOrCreate()
    print(spark)

    kafka_options = {
        'kafka.bootstrap.servers':'localhost:9092' ,
        # 'kafka.sasl.mechanism':'SCRAM-SHA-256',
        # 'kafka.security.protocol':'SASL-SSL',
        # 'kafka.sasl.jaas.config':'something'
        'startingOffsets':'earliest',
        'subscribe':'readCovidKafka'
    }

    schema = StructType([
        StructField('name' , StringType()) , 
        StructField('gender' , StringType()) , 
        StructField('iso_code' , StringType()) , 
        StructField('continent' , StringType()) , 
        StructField('country' , StringType()) , 
        StructField('status' , StringType()) , 
    ])
    df = spark\
        .readStream \
        .format('kafka')\
        .option('kafka.bootstrap.servers','localhost:9092')\
        .option('subscribe',topic)\
        .option('startingOffsets','earliest')\
        .load() 
    
    valDF = df.select(from_json(col('value').cast('string') , schema ).alias('srcValue')).select(col('srcValue.*'))
    # valDF.printSchema()

    # print(type(valDF))
    # valDF = valDF.toDF(*sorted(valDF.columns))
    # valDF.show(10)
    valDF = valDF.withColumn('hdfsCol' , when(valDF.continent=='Asia','asi')\
                              .when(valDF.continent=='Africa','afr')\
                              .when(valDF.continent=='South America','sam')\
                              .when(valDF.continent=='North America','nam')\
                              .when(valDF.continent=='Africa','africa')\
                              .when(valDF.continent=='Europe','eur'))
    
    # valDF = valDF.withColumn('hdfsCol' , when(valDF.continent=='Asia' , 'asi').when)
    valDF.printSchema()

    # print(type(valDF))
    valDF.writeStream \
    .format('console')\
    .option('inferSchema','true')\
    .option('truncate','false')\
    .start()

    d = open('/home/ubuntu/prj/kafka_2.12-3.6.0/output/testfile.csv' , 'w')
    valDF.writeStream \
    .format('csv')\
    .option('path','/home/ubuntu/prj/kafka_2.12-3.6.0/output/asi')\
    .option('checkpointLocation' ,d )\
    .option('forceDeleteTempCheckpointLocation','false')\
    .outputMode('append')\
    .start()


    print('Done')
    spark.streams.awaitAnyTermination()

receiveData('covidKafka')