from json import loads  
from kafka import KafkaConsumer    

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

def simpleKafkaReader():
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
        print('Message : ',msg)



if __name__=="__main__" :
    # kafkaStreamInit(spark)
    simpleKafkaReader()
    exit(0)

    