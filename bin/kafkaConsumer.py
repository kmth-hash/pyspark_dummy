from json import loads  
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dbMethods import * 
# import findspark

# import os
# os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'
# os.environ['SPARK_HOME']='/home/ubuntu/prj/spark/spark-3.1.2-bin-hadoop3.2'

# findspark.init()

# from pyspark.sql import * 
# from pyspark.sql.functions import * 
# from pyspark.sql.types import * 


def MongoDBInit():
    client = MongoClient('localhost',27017) 
    myDB = client['covidKafka']

    try:
    # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print('Server connected : ')
    
    except ConnectionFailure:
        print("Server not available")

    
    print(client.list_database_names())
    myColl = myDB['peopleData']
    return myColl

def simpleKafkaReader(myColl):
    my_consumer = KafkaConsumer(  
     bootstrap_servers = ['localhost : 9092'],  
     auto_offset_reset = 'earliest',  
     enable_auto_commit = True,  
     group_id = 'my-group',  
     value_deserializer = lambda x : loads(x.decode('utf-8'))  
     ) 

    my_consumer.subscribe(topics=['covidKafka','covidKafka2'])
    my_consumer.subscription()


    for message in my_consumer:  
        msg = message.value  
        print('Message received : ')
        addListIntoDB(myColl , msg)

# def kafkaStreamInit(spark):
#     kafka_options = {
#         'kafka.bootstrap.servers':'localhost:9092' ,
#         # 'kafka.sasl.mechanism':'SCRAM-SHA-256',
#         # 'kafka.security.protocol':'SASL-SSL',
#         # 'kafka.sasl.jaas.config':'something'
#         'startingOffsets':'earliest',
#         'subscribe':'covidKafka'
#     }

#     df = spark.readStream.format('kafka').options(**kafka_options).load()

#     deserializedDf = df.selectExpr("CAST(value AS STRING)")

#     query = deserializedDf.writeStream.outputMode('append').format('console').start()

#     time.sleep(10)
#     query.stop()
#     return

if __name__=="__main__" :
    # spark = SparkSession.builder.master('local[2]').appName('CovidKafka').getOrCreate()
    # print(spark)

    # kafkaStreamInit(spark)
    myColl = MongoDBInit()
    simpleKafkaReader(myColl)
    exit(0)

    