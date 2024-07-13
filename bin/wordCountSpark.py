import findspark

findspark.find()

import pyspark 
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from load_data import * 

def main():
    spark = SparkSession.builder.master('local[1]').appName('WordCount').getOrCreate()
    # print(spark)

    data = [('hello text1',),('world jack yaz' ,),('hello',),('world',)]
    columns = ['textStr']

    df = spark.createDataFrame(data=data , schema=StructType([StructField('textStr' , StringType())]))
    df.show()

    # FlatMap returns --> ['hello', 'text1', 'world', 'jack', 'yaz', 'hello', 'world']
    # f1 = df.rdd.flatMap(lambda x : (x['textStr'].split(' ')))
    # print(f1.collect())

    # Map returns --> [['hello', 'text1'], ['world', 'jack', 'yaz'], ['hello'], ['world']]
    # m1 = df.rdd.map(lambda x : (x['textStr'].split(' ')))
    # print(m1.collect())

    rdd = df.rdd.flatMap(lambda x : (x['textStr'].split(' '))).map(lambda x : (x,1))
    try:
      # print(rdd , type(rdd))
      df2 = rdd.toDF()
      df2.show()
      

      df2 = df2.groupBy('_1').count()
      df2 = df2.withColumnRenamed('_1' , 'word').withColumnRenamed('count','counter')
      df2 = df2.orderBy(df2.counter.asc_nulls_first())
      df2.show()


    except Exception as ex:
      print('An exception occurred')
      print(ex)



main()