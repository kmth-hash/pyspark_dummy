from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dbMethods import *
from kafkaConsumer import MongoDBInit
# myColl.delete_many({'Country':'India'})
# myColl.insert_one({'name':'Jack' , 'status' : 'Recovered' , 'Country' : 'India'})

myColl = MongoDBInit()

# findAllUsers(myColl)
# myColl.remove()
for x in myColl.find({'src':'src1'}):
    print(x)