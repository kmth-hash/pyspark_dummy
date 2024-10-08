def writeIntoFile(spark, df, filename='finalDF', location='/', format='csv', printFlag=True):
    df = df.coalesce(1)
    format = format.lower()
    # df.write.mode('overwrite').option('delimiter','|').option('header',True).format(format).load(f'/finalNamesDF.{format}')

    if format == 'csv':

        df.write.mode('overwrite').option(
            'delimiter', '|').option('header', True).csv(filename)

    else:

        df.write.mode('overwrite').option('delimiter', '|').option(
            'header', True).parquet(filename)

    if printFlag:
        df.show(10, truncate=False)
    print(f' writing {filename} : complete')


def readFromFile(spark , filename='' , location='/data/' , format='csv'):
    if filename=='':
        print('Filename missing : ')
        return 
    df = spark.read.option('header',True).option('inferSchema',True).csv(f'{location}{filename}.{format}')
    return df 

