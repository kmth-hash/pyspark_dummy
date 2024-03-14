  
from time import sleep  
from json import dumps  
import csv
from random import randint , choice 
from kafka import KafkaProducer

def readData(filePath , kafkaProd ):
    statusList = ['Recovered','Dead','Infected']
    with open(filePath, mode ='r')as file:
        csvFile = csv.reader(file , delimiter='|')
        # return csvFile
        for i in csvFile:
            sendData( [*i , 'src2' , choice(statusList) ] , kafkaProd)
            
            sleep(1/randint(1,10))

def sendData(data , kafkaProd):
    print('Sending : ', data)
    kafkaProd.send('covidKafka2', value=data)


def main():

    my_producer = KafkaProducer(  
        bootstrap_servers = ['localhost:9092'],  
        value_serializer = lambda x:dumps(x).encode('utf-8')  
        )
    
    readData('/home/ubuntu/prj/kafka_2.12-3.6.0/scripts/sourceFileData3.csv',my_producer)
    
    
main()