from random import choice , randint , random
import json 
import time 
from datetime import datetime
location = ['Pune','Mumbai','Bangalore','Chennai','Gurgaon','Kolkata','Hyderabad']


def generateRow() : 
    res = dict()
    res['location'] = choice(location)
    res['temperature'] = randint(20,37)
    res['currenttime'] = str(datetime.now())
    res['humidity'] = randint(65,75)

    return json.dumps(res)


def sourceStream():
    # Replicating API to generate dummy data for streaming as source/producer
    while True : 
        print(generateRow())
        time.sleep(random())

sourceStream()