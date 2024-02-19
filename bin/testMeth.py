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
    except ex :
        # print('Something went wrong')
        pass
    else :
        # print('False')
        return ''
    

def findMostNames(spark, filePath):
    df = spark.read.option('header',True).csv(filePath)
    df.show( truncate=False)


def testMethodExec():
    spark = initSpark()
    findMostNames(spark,'./data/sourceData.csv')

if __name__=='__main__':
    # testMethodExec()
    print(filterNames("_12_اف_"))
    print(filterNames("James Ford"))
    print(filterNames('कु0'))
