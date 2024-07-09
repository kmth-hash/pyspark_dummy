# Simple Kafka Consumer using Python 

from kafka import KafkaConsumer 
import os 
from dotenv import load_dotenv 
import json 
import time 


currFilePath = os.path.abspath(__file__)
# Linux machine --> '/'
# Windows Machine --> '\\'
# env file in the same dir as this script . Rename .env.templete to .env with proper values
currPath = '/'.join(currFilePath.split('/')[:-1])+str('/.env')
currPath = currPath[0].upper()+currPath[1:]
load_dotenv(currPath)

def streamReader(consumer, topicName) : 
    consumer.subscribe(topics=[topicName])
    consumer.subscription()

    for message in consumer : 
        msgText = message.value 
        print(msgText) 



def main() : 
    topicName = os.getenv('topicNAME')
    bootstrapServer = [os.getenv('bootstrapServer')]
    print(topicName , bootstrapServer)
    consumer = KafkaConsumer(bootstrap_servers = bootstrapServer ,
                            #  auto_offset_reset = 'earliest',  
                             enable_auto_commit = True,
                             value_deserializer = lambda x : json.loads(x.decode('utf-8')))
    streamReader(consumer, topicName)
main()