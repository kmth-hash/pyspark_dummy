'''
Google Collab | init --> 

!apt-get update # Update apt-get repository.
!apt-get install openjdk-8-jdk-headless -qq > /dev/null # Install Java.
!wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz # Download Apache Sparks.
!tar xf spark-3.1.1-bin-hadoop3.2.tgz # Unzip the tgz file.
!pip install -q findspark # Install findspark. Adds PySpark to the System path during runtime.

# Set environment variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"

!ls 

# Initialize findspark
import findspark
findspark.init()

# Create a PySpark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark


'''



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
    
    
    names = loadNames(spark)
    covidmap = loadCovidData(spark)

    
    sourceData = names.crossJoin(covidmap)
    # sourceData.show()
    writeIntoFile(spark , sourceData , filename='sourceData',format='csv')
    # print(sourceData.count())

    # print(d)
if __name__=='__main__':
    main_meth()


