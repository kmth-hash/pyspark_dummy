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
        if x.isalpha() and len(x)>2:
            return x
        else :
            return ''
    except :
        # print('Something went wrong')
        pass
    else :
        # print('False')
        return ''
    
def addStatusAttrib(spark , df):
    def getrandomStatus():
        from random import choice 
        x = choice(['Dead','Recovered','Infected'])
        # print(x)
        return x
    statusUDF = udf(lambda  : getrandomStatus() , StringType())
    df = df.withColumn('Status' , statusUDF())
    df.show()
    return df
def findMostNames(spark, filePath):
    df = spark.read.option('inferSchema',True).option('header',True).csv(filePath)
    df.show( truncate=False)
    return df
def readFilePipeDelimited(spark , filePath) :
    df = spark.read.option('inferSchema',True).option('delimiter','|').option('header',True).csv(filePath)
    df.show( truncate=False)
    return df
def testMethodExec():
    spark = initSpark()
    return spark

def add_country(spark , filePath ):
    df = spark.read.option('inferSchema',True).option('header',True).csv(filePath)
    # df.show( truncate=False)
    # print(df.count())
    
    # Some null values like continents 
    # df2 = df.filter(df['continent'].isNull())
    # df2.show()

    df = df.filter(df['continent'].isNotNull())
    df = df.withColumnRenamed('location' , 'country').select('continent' , 'country')
    # print(df.count())
    # df.show()
    return df
    
if __name__=='__main__':
    spark = testMethodExec()
    df = findMostNames(spark,'./data/indian-male-names.csv')

    
    df.printSchema()
    # df.show(30)
    # print(filterNames("_12_اف_"))
    # print(filterNames("James Ford"))
    # print(filterNames('कु0'))
    # print(filterNames(''))
    # df2 = df.filter(df.name.isNull())
    # print(df2.count())
    # df2.show()

    validateNameUDF = udf(lambda f : filterNames(f) , StringType())
    # df.show()
    # df.dropna()
    df = df.withColumn('firstName', when(df.name.isNull(),"").otherwise(validateNameUDF(df.name)) )
    df = df.withColumn('uppergender' , upper(df['gender']))
    # df.show(10)
    df = df.dropna('any')
    df = df.select('firstName','uppergender')

    df2 = findMostNames(spark,'./data/indian-female-names.csv')
    df2 = df2.withColumn('firstName', when(df2.name.isNull(),"").otherwise(validateNameUDF(df2.name)) )
    df2.printSchema()
    df2.show()
    df2 = df2.withColumn('uppergender' , upper(df2['gender']))
    df2 = df2.dropna('any')
    df2 = df2.select('firstName','uppergender')

    df3 = readFilePipeDelimited(spark,'./data/finalNamesDF.csv')
    df3.printSchema()
    df3 = df3.withColumn('firstName', when(df3.FirstName.isNull(),"").otherwise(validateNameUDF(df3.FirstName)) )
    df3 = df3.withColumnRenamed('gender' , 'Gender')
    df3 = df3.withColumn('uppergender' , upper(df3['gender']))
    df3 = df3.dropna('any')
    df3 = df3.select('firstName','uppergender')

    df = df.union(df2).union(df3)
    df = df.withColumnRenamed('uppergender' , 'gender')
    df = addStatusAttrib(spark , df)
    print('Final DF ')
    df.printSchema()
    df.show()
    print('Users : ', df.count())

    locDF = add_country(spark , './data/location-data.csv')
    print('Location dataset : ',locDF.count())

    finalDF = df.crossJoin(locDF)
    print(finalDF.count())

    df=finalDF
    # print('Female count : ',df.filter(df.gender=='F').count())
    # print('Male count : ',df.filter(df.gender=='M').count())


    df = df.repartition(10)
    df = df.distinct()
    # print('Female count : ',df.filter(df.gender=='F').count())
    # print('Male count : ',df.filter(df.gender=='M').count())

    df.show(50)
    df = df.coalesce(1)
    df.write.mode('overwrite').option('delimiter', '|').option('header', True).csv('loc-names')

    print('Write complete ')
