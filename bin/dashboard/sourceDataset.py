from random import choice, randint, random
import json
import time
from datetime import datetime
import requests
import os
from dotenv import load_dotenv

location = ['Pune', 'Mumbai', 'Bangalore',
            'Chennai', 'Gurgaon', 'Kolkata', 'Hyderabad']
currFilePath = os.path.abspath(__file__)
currPath = '\\'.join(currFilePath.split('\\')[:-1])+str('\\.env')
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


def sourceStream():
    def getData(src='local'):
        if src == 'local':
            # Replicating API to generate dummy data for streaming as source/producer

            while True:
                print(generateDataLocally())
                time.sleep(random())
        else:
            # Fetch data from API based on location

            ind = 0
            ln = len(location)
            while True:
                print(getDataFromAPI(os.getenv('APIKEY'), location[ind]))
                ind += 1
                if ind == ln:
                    ind = 0
    getData('local')


sourceStream()
