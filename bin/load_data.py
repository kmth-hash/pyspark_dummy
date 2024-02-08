import findspark

findspark.find()
import os

import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
import pyspark 
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from helpers import *

def loadNames(spark):
    print('Read usernames : ')
    spark = SparkSession.builder.master('local[2]').appName('Covid-dummy').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    df = spark.read.option('header',True).option('inferSchema',True).csv('./data/names.csv')
    df = df.select("Child's First Name",'Gender')
    # print(df.count())
    # df.show()
    df = df.distinct()
    # print(df.count())
    # df.printSchema()
    # df.show()
    df.show()
    df = df.withColumnRenamed("Child's First Name" , "FirstName")\
        .withColumn("GenderTemp" , when(col('Gender')=='MALE' , "M" ).otherwise("F"))\
        .drop('Gender')\
        .withColumnRenamed("GenderTemp" , "Gender")

    df.show()

    df2 = spark.read.option('header',True).option('inferSchema',True).csv('./data/firstnames.csv')
    df2 = df2.select('name','sex')

    df2 = df2.distinct()

    df2 = df2.withColumnRenamed("name" , "FirstName")\
        .withColumn("GenderTemp" , when(col('sex')=='boy' , "M" ).otherwise("F"))\
        .drop('sex')\
        .withColumnRenamed("GenderTemp" , "Gender")
    df2.show()

    finalDF = df.union(df2)
    finalDF = finalDF.distinct()
    finalDF.show(35)
    print(finalDF.count())
    # finalDF = finalDF.sample(0.05 , 3)
    # print(finalDF.count())
    # writeIntoFile(spark,finalDF,'./data/loadData',format='parquet')

def loadCovidData(spark):
    df = spark.read.option('header',True).option('inferSchema',True).csv('./data/location-data.csv')
    # df.printSchema()
    covCols = 'iso_code,continent,location,total_cases,new_cases,total_deaths'.split(',')
    # print(covCols , sep='\n')
    filtered_data = df.select(covCols).filter(col('continent').isNotNull())
    # filtered_data.show()
    # print(filtered_data.count())
    temp_ls = [
        StructField('iso_code' , StringType() , True) ,
        StructField('continent' , StringType() , True) ,
        StructField('location' , StringType() , True) ,
        StructField('total_cases' , FloatType() , True) ,
        StructField('new_cases' , FloatType() , True) ,
        StructField('total_deaths' , FloatType() , True) ,        
        ]
    # print(temp_ls)
    filtered_schema = StructType(temp_ls)
    filtered_data = spark.createDataFrame(filtered_data.rdd , filtered_schema )
    filtered_data = filtered_data.withColumn('total_cases2' , col('total_cases').cast('bigint'))\
        .drop('total_cases')\
        .withColumnRenamed('total_cases2' , 'total_cases')
    filtered_data = filtered_data.withColumn('new_cases2' , col('new_cases').cast('bigint'))\
        .drop('new_cases')\
        .withColumnRenamed('new_cases2' , 'new_cases')
    filtered_data = filtered_data.withColumn('total_deaths2' , col('total_deaths').cast('bigint'))\
        .drop('total_deaths')\
        .withColumnRenamed('total_deaths2' , 'total_deaths')
    
    filtered_data.show(truncate=False)
    filtered_data.printSchema()
    print(filtered_data.count())