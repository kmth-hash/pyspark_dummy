  
from time import sleep  
from json import dumps  
import csv
from random import randint
# from kafka import KafkaProducer

# my_producer = KafkaProducer(  
#     bootstrap_servers = ['localhost:9092'],  
#     value_serializer = lambda x:dumps(x).encode('utf-8')  
#     )  

# for n in range(500):  
#     my_data = {'num' : n}  
#     my_producer.send('covidKafka', value = my_data)
#     print('Sent : ', my_data)  
#     sleep(5) 
def readData(filePath , kafkaProd ):
    
    with open(filePath, mode ='r')as file:
        csvFile = csv.reader(file)
        # return csvFile
        for i in csvFile:
            sendData( i , kafkaProd)
            sleep(1/randint(5,15))

def sendData(data , kafkaProd):
    print('Sending : ', data)
    kafkaProd.send('covidKafka', value=data)


def main():

    my_producer = KafkaProducer(  
        bootstrap_servers = ['localhost:9092'],  
        value_serializer = lambda x:dumps(x).encode('utf-8')  
        )
    
    readData('./data/sourceData.csv',my_producer)
    
    
main()