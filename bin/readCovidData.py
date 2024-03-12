from kafka import KafkaProducer
from random import choice , randint
import csv , json 
from time import sleep

def statusUpdate():
    return choice(['dead','affected','recovered'])

def readCSVFile():
    output = []
    with open('/home/ubuntu/prj/kafka_2.12-3.6.0/scripts/sourceData.csv' , 'r') as f:
        contents = f.readlines()
        for i , itr in enumerate(contents[1:]):
            temp_ = itr.split('|')
            row = {'name':temp_[0], 'gender':temp_[1],'iso_code':temp_[2],'continent':temp_[3],'country':temp_[4],'status':statusUpdate()}
            output.append(json.dumps(row))
    return output


def readDataMethod():
    topicName = 'covidKafka'

    producer = KafkaProducer(  
        bootstrap_servers = ['localhost:9092'],  
        value_serializer = lambda x:x.encode('utf-8')  
        )
    
    srcData = readCSVFile()
    for i in srcData :
        print('Sending : ',i)
        producer.send(topicName , value=i)
        producer.flush()
        sleep(1)
    print('All records sent !')

readDataMethod()