from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def addIntoDB(dbObj , dbData):
    dbObj.insert_one(dbData)

def findAllUsers(dbObj ):
    for i , itr in enumerate(dbObj.find()):
        print(i , itr)

def findAllNames(dbObj) :
    for i , itr in enumerate(dbObj.find({} , {'name':1})):
        print(i , itr)

def addListIntoDB(dbObj , dbList) :
    d = {'name':dbList[0] ,
         'gender':dbList[1] , 
         'continent':dbList[2],
         'country':dbList[3],
         'src':dbList[4] , 
         'status' : dbList[5]}
    dbObj.insert_one(d)

