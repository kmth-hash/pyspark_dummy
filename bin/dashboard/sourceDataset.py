from random import choice, randint, random
import json
import time
from datetime import datetime
import requests
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

location = ['Pune', 'Mumbai', 'Bangalore',
            'Chennai', 'Gurgaon', 'Kolkata', 'Hyderabad']
currFilePath = os.path.abspath(__file__)
# Linux machine --> '/'
# Windows Machine --> '\\'
# env file in the same dir as this script . Rename .env.templete to .env with proper values
currPath = '/'.join(currFilePath.split('/')[:-1])+str('/.env')
currPath = currPath[0].upper()+currPath[1:]
load_dotenv(currPath)


def generateDataLocally():
    res = dict()
    res['location'] = choice(location)
    res['temperature'] = randint(20, 37)
    res['currenttime'] = str(datetime.now())
    res['currentdate'] = str(datetime.now().strftime(f"%Y-%m-%d %X.000000"))
    res['humidity'] = randint(65, 75)

    return json.dumps(res)


def getDataFromAPI(apiKey, location):
    apiURL = f'https://api.openweathermap.org/data/2.5/weather?q={location}&appid={apiKey}&units=metric'
    print(apiURL)
    jsonResponse = requests.get(apiURL).json()
    response = dict()
    response['location'] = location
    response['temperature'] = f"{float(jsonResponse['main']['temp'])*( 1 + (random() / 10 )) :.2f}"
    response['currenttime'] = str(datetime.now())
    response['currentdate'] = jsonResponse['dt']
    response['humidity'] = f"{float(jsonResponse['main']['humidity']) *( 1 + (random() / 10 )) :.2f}"

    return (json.dumps(response))


def sourceStream(producer,topicName):
    def getData(src='local'):
        if src == 'local':
            # Replicating API to generate dummy data for streaming as source/producer

            while True:
                res = generateDataLocally()
                print(res)
                producer.send(topicName,res)
                time.sleep(random())
        else:
            # Fetch data from API based on location

            ind = 0
            ln = len(location)
            apiKey = os.getenv('APIKEY')
            while True:
                producer.send(topicName,getDataFromAPI(apiKey, location[ind]))
                ind += 1
                if ind == ln:
                    ind = 0
    getData('local')

def initProducer():
    topicName = os.getenv('topicNAME')
    bootstrapServer = [os.getenv('bootstrapServer')]
    print(topicName , bootstrapServer)
    producer = KafkaProducer(bootstrap_servers=bootstrapServer , value_serializer=lambda x : json.dumps(x).encode('utf-8'))
    sourceStream(producer,topicName)


    # Test this code usin Kafka Consumer in a separate terminal
    # bin/kafka-console-consumer.sh --topic <topic_name> --bootstrap-server <server | localhost:9092 >
    #Streams of data will be printed in consumer terminal 

initProducer()