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
from load_data import * 

def main_meth():
    print('Main method')
    spark = SparkSession.builder.master('local[1]').appName('Covid-dummy').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    
    
    loadNames(spark)
    loadCovidData(spark)

if __name__=='__main__':
    main_meth()


