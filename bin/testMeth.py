import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import findspark

findspark.find()

import pyspark 
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql.types import * 


def initSpark():
    spark = SparkSession.builder.master('local[1]').appName('Covid-dummy-test').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    
    return spark 

def filterNames(s):
    try : 
        # print(s.isascii())
        
        x = s.split(' ')[0]
        if x.isascii():
            return x
        else :
            return ''
    except :
        # print('Something went wrong')
        pass
    else :
        # print('False')
        return ''
    

def findMostNames(spark, filePath):
    df = spark.read.option('inferSchema',True).option('header',True).csv(filePath)
    df.show( truncate=False)
    return df

def testMethodExec():
    spark = initSpark()
    
    return spark 
if __name__=='__main__':
    spark = testMethodExec()
    df = findMostNames(spark,'./data/indian-male-names.csv')
    df.printSchema()
    # df.show(30)
    print(filterNames("_12_اف_"))
    print(filterNames("James Ford"))
    print(filterNames('कु0'))
    print(filterNames(''))
    # df2 = df.filter(df.name.isNull())
    # print(df2.count())
    # df2.show()
    
    validateNameUDF = udf(lambda f : filterNames(f) , StringType())
    # df.show()
    # df.dropna()
    df = df.withColumn('firstName', when(df.name.isNull(),"").otherwise(validateNameUDF(df.name)) )
    
    # df.show(10)
    df = df.dropna()
    df = df.select('firstName','gender')

    df2 = findMostNames(spark,'./data/indian-female-names.csv')
    df2.printSchema()
    df2 = df2.withColumn('firstName', when(df2.name.isNull(),"").otherwise(validateNameUDF(df2.name)) )
    df2 = df2.dropna()
    df2 = df2.select('firstName','gender')

    df = df.union(df2)
    print('Female count : ',df.filter(df.gender=='f').count())
    print('Male count : ',df.filter(df.gender=='m').count())
    

    df = df.repartition(10)
    df = df.distinct()
    print('Female count : ',df.filter(df.gender=='f').count())
    print('Male count : ',df.filter(df.gender=='m').count())
    
    df.show(50)
    df = df.coalesce(1)
    df.write.mode('overwrite').option('delimiter', '|').option('header', True).csv('ind-names')

    print('Write complete ')
