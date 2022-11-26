from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from operator import add
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
import requests
from bs4 import BeautifulSoup
import json
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import findspark as fs
import xml.etree.ElementTree as ET
import boto3
from io import StringIO
import s3fs
import pandas


def main():
    run_aggregation()

def getXMLparse(party):
     #doc = xml.dom.minidom.parse(party)
     print(type(party))
     result = party.find('SanctionsMeasure')
     print(result)
     print(type(result))

def createDataFrame():
    fs.init()
    sc = SparkContext(appName="SDN-Aggregator")
    spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())

    input_schema = StructType([
            StructField('StoreID', IntegerType(), True),
            StructField('Location', StringType(), True),
            StructField('Date', StringType(), True),
            StructField('ItemCount', IntegerType(), True)
        ])
    input_data = [(1, "Bangalore", "2021-12-01", 5),
                (2,"Bangalore" ,"2021-12-01",3),
                (5,"Amsterdam", "2021-12-02", 10),
                (6,"Amsterdam", "2021-12-01", 1),
                (8,"Warsaw","2021-12-02", 15),
                (7,"Warsaw","2021-12-01",99)]
    input_df = spark.createDataFrame(data=input_data, schema=input_schema)
    #2. Prepare an expected data frame which is the output that we expect.        
    expected_schema = StructType([
            StructField('Location', StringType(), True),
            StructField('TotalItemCount', LongType(), True)
            ])
    
    expected_data = [("Bangalore", 8),
                    ("Warsaw", 114),
                    ("Amsterdam", 11)]
    expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    #3. Apply our transformation to the input data frame
    transformed_df = transform_data(input_df)

    #4. Assert the output of the transformation to the expected data frame.
    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, transformed_df.schema.fields)]
    fields2 = [*map(field_list, expected_df.schema.fields)]
    # Compare schema of transformed_df and expected_df
    res = set(fields1) == set(fields2)

    expected_df.show()
    print(expected_df)
    if expected_df is not None:
        a = expected_df.collect()
    
    b = sorted(transformed_df.collect())
    # assert
    #assertTrue(res)
    if (res):
        print("res is true")
    # Compare data in transformed_df and expected_df
    c = sorted(expected_df.collect()), sorted(transformed_df.collect())
    print(c)

def testFunction():
    print("inside function 2")
    return "testString"

def transform_data(input_df):
    transformed_df = (input_df.groupBy('Location',).agg(sum('ItemCount').alias('TotalItemCount')))
    return transformed_df

def run_aggregation():

    fs.init()
    sc = SparkContext.getOrCreate()

    spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())

    url = 'https://www.w3schools.com/xml/simple.xml'

    url2= 'https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.xml'

    document = requests.get(url2)

    soup=BeautifulSoup(document.content,"lxml-xml")

    sanctionsEntries = soup.findAll("SanctionsEntry")
    print("****distinct sanctions****")
    print(sanctionsEntries.__len__())


    input_schema = StructType([
            StructField('SanctionsEntryID', StringType(), True),
            StructField('ListID', StringType(), True),
            StructField('ProfileID', StringType(), True),
            StructField('EntryEventTypeID', StringType(), True),
            StructField('SanctionsTypeID', StringType(), True)
        ])
    #input_data = [(1, "Bangalore", "2021-12-01", 5),
    #            (2,"Bangalore" ,"2021-12-01",3),
    #            (5,"Amsterdam", "2021-12-02", 10),
    #            (6,"Amsterdam", "2021-12-01", 1),
    #            (8,"Warsaw","2021-12-02", 15),
    #            (7,"Warsaw","2021-12-01",99)]
    input_data = []
    #export_df = spark.createDataFrame(data=input_data, schema=input_schema)

    partiesList = []
    #for party in distinctParties:
    for i in range(5):
        party = sanctionsEntries[i]
        print("****party****" + str(i))
        print(party)
        partyDF = getXMLparse(party)

        SanctionsEntryID = party['ID']
        ListID = party['ListID']
        profileID = party['ProfileID']
        EntryEventTypeID = party.find('EntryEvent')['ID']
        sanctionsTypeID = party.find('SanctionsMeasure').attrs['SanctionsTypeID']
#       party.find('SanctionsMeasure').attrs['ID'] <--STR
        print("*****Attributes: ******")
        print(SanctionsEntryID)
        print(ListID)
        print(profileID)
        print(EntryEventTypeID)
        print(sanctionsTypeID)

        partyTuple = (SanctionsEntryID,ListID,profileID,EntryEventTypeID,sanctionsTypeID)

        partiesList.append(partyTuple)

        print("****party****" + str(i))
    print("**** parties list *****")
    print(len(partiesList))
    export_parties_df = spark.createDataFrame(data=partiesList, schema=input_schema)

    s3 = boto3.resource('s3')
    # get a handle on the bucket that holds your file 
    bucket = s3.Bucket('emr-src')

    #export_parties_df.write.parquet("s3://emr-src/data/sdn/test.parquet",mode="overwrite")
    #csv_buffer = StringIO()
    #export_parties_df.to_csv(csv_buffer) 
    ##s3_resource = boto3.resource('s3')
    #s3_resource.Object(bucket, 'export_parties_df.csv').put(Body=csv_buffer.getvalue())

    pandasDF = export_parties_df.toPandas()
    pandasDF.to_csv('s3://emr-src/data/sdn/dummy.csv', index=False)


if __name__ == "__main__":
    main()