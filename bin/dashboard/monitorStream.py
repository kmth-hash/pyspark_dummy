# Simple Kafka Consumer using Python 

from kafka import KafkaConsumer 
import os 
from dotenv import load_dotenv 
import json 
import time 
import sqlite3 


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

    conn = sqlite3.connect('test.db')
    cursor = conn.cursor()
    def initDB(cursor):
        cursor.execute('select * from test ;')
        result = cursor.fetchall()
        print('print:',result)
        cursor.execute('create table if not exists test (location string , temperature float , currenttime string,currendate string, humidity float);')
    initDB(cursor)
    for message in consumer : 
        msgText = message.value 
        jsonResponse = json.loads(msgText)
        print(jsonResponse , type(jsonResponse))
        # print(f'{str(jsonResponse['location'])}')
        query = f"insert into test values('{str(jsonResponse['location'])}',{float(jsonResponse['temperature'])},'{str(jsonResponse['currenttime'])}','{str(jsonResponse['currentdate'])}',{float(jsonResponse['humidity'])})" 
        print(query)
        cursor.execute(query)
        conn.commit()


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