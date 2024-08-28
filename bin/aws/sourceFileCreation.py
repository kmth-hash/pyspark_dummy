from random import randint , choice 
import boto3 

srcFilePath = '/home/mcmac/prj/pyspark_dummy/bin/aws/file.csv'

def createCSV(fileName , num ):
    data = []
    headers = 'userID,salary,location,year,status'
    data.append(headers) 

    for i in range(1,num+1) : 
        tempLine = [i , randint(20,125)*1000 , choice(['NAM' , 'APAC' , 'LATAM']) , randint(2020,2024) , choice(['Y','N']) ]
        data.append(','.join(map(str , tempLine)))

    print(data)
    with open(file=fileName , mode='w') as fp : 
        for line in data : 
            fp.write(line+"\n")
     

def moveFileToS3(srcFile , S3loc, region) : 
    client = boto3.client(
        's3' , region_name='ap-south-1' 
    )
    res = client.list_buckets()
    print(res['Buckets'])

# createCSV(srcFilePath , 20)

moveFileToS3(None,None,None)